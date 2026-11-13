"""Online chunk migration: move one hash range to another shard under writes.

The protocol follows MongoDB's ``moveChunk`` in shape.  A chunk is copied
while the donor keeps serving it, and ownership flips only once the recipient
has caught up:

1. **arm**     -- the donor starts recording every mutation that lands in the
                  range, so writes accepted during the copy are not lost.
2. **clone**   -- the range is read from the donor in batches and ingested by
                  the recipient, which replicates each batch to its own
                  follower.
3. **catch-up**-- the donor's changelog is drained repeatedly while writes
                  continue, until the backlog is small enough to cut over.
4. **cutover** -- the donor freezes *writes* to the range (reads still work),
                  the last changes drain, the shard map's chunk owner flips,
                  and the map version bumps so routers re-plan.
5. **cleanup** -- the donor drops its copy and refuses traffic for the range,
                  telling any client with a stale map to retry.

Correctness rests on three things:

* arming happens **before** the clone snapshot is counted, under the donor's
  store lock, so no write can slip between the two;
* every shipped batch is idempotent (upsert by ``_id``, delete-if-present), so
  a replay can neither duplicate nor lose a document; and
* reads carry each shard's owned ranges, so during the window where donor and
  recipient both hold the documents, exactly one of them returns them.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import httpx

from .shardmap import Chunk, ShardInfo, ShardMap, ShardMapSnapshot

CLONE_BATCH = 500
CATCHUP_BATCH = 500
MAX_CATCHUP_ROUNDS = 20
CUTOVER_BACKLOG = 64


class MigrationError(RuntimeError):
    """Raised when a migration cannot start or cannot safely be committed."""


@dataclass
class MigrationReport:
    """Everything the Phase 3 write-up needs about one completed move."""

    migration_id: str
    chunk_id: int
    lo: int
    hi: int
    source: str
    destination: str
    documents_at_arm: int = 0
    documents_cloned: int = 0
    changes_applied: int = 0
    catchup_rounds: int = 0
    deletes_applied: int = 0
    removed_from_source: int = 0
    map_version_before: int = 0
    map_version_after: int = 0
    clone_ms: float = 0.0
    catchup_ms: float = 0.0
    cutover_ms: float = 0.0
    total_ms: float = 0.0
    verified: bool = False
    verification: dict[str, Any] = field(default_factory=dict)

    @property
    def documents_moved(self) -> int:
        """Documents that ended up on the recipient as a result of the move."""
        return self.verification.get("destination_documents", self.documents_cloned)

    def as_dict(self) -> dict[str, Any]:
        return {
            "migration_id": self.migration_id,
            "chunk": {"chunk_id": self.chunk_id, "lo": self.lo, "hi": self.hi,
                      "span": self.hi - self.lo},
            "source": self.source,
            "destination": self.destination,
            "documents_moved": self.documents_moved,
            "documents_at_arm": self.documents_at_arm,
            "documents_cloned": self.documents_cloned,
            "changes_applied": self.changes_applied,
            "deletes_applied": self.deletes_applied,
            "catchup_rounds": self.catchup_rounds,
            "removed_from_source": self.removed_from_source,
            "map_version": {"before": self.map_version_before,
                            "after": self.map_version_after},
            "timing_ms": {"clone": round(self.clone_ms, 3),
                          "catchup": round(self.catchup_ms, 3),
                          "cutover": round(self.cutover_ms, 3),
                          "total": round(self.total_ms, 3)},
            "verified": self.verified,
            "verification": self.verification,
        }


def active_node(snapshot: ShardMapSnapshot, shard_id: str) -> ShardInfo:
    """The node currently serving ``shard_id``, following any failover."""
    replica = snapshot.replica_for(shard_id)
    if replica:
        return snapshot.shard(replica["active_id"])
    return snapshot.shard(shard_id)


def epoch_of(snapshot: ShardMapSnapshot, shard_id: str) -> int:
    return snapshot.epoch_for(shard_id)


class ChunkMigrator:
    """Drives one chunk migration against a running cluster."""

    def __init__(self, shard_map_path: str, timeout: float = 30.0,
                 verbose: bool = True) -> None:
        self.map = ShardMap(shard_map_path)
        self.client = httpx.Client(timeout=timeout)
        self.verbose = verbose

    def close(self) -> None:
        self.client.close()
        self.map.close()

    def __enter__(self) -> "ChunkMigrator":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def log(self, message: str) -> None:
        if self.verbose:
            print(message, flush=True)

    # -- shard calls ----------------------------------------------------

    def _request(self, node: ShardInfo, method: str, path: str,
                 json_body: Any | None = None, epoch: int | None = None,
                 params: dict[str, Any] | None = None) -> dict[str, Any]:
        headers = {"X-Ownership-Epoch": str(epoch)} if epoch is not None else {}
        response = self.client.request(method, f"{node.base_url}{path}",
                                       json=json_body, params=params, headers=headers)
        if response.status_code >= 400:
            try:
                detail = response.json().get("detail", response.text)
            except ValueError:
                detail = response.text
            raise MigrationError(f"{node.shard_id} {path} -> {response.status_code}: {detail}")
        return response.json()

    # -- planning -------------------------------------------------------

    def plan(self, chunk_id: int, destination: str) -> tuple[Chunk, ShardInfo, ShardInfo]:
        snap = self.map.snapshot()
        chunk = next((c for c in snap.chunks if c.chunk_id == chunk_id), None)
        if chunk is None:
            raise MigrationError(f"no chunk {chunk_id} in the shard map")
        if chunk.owner == destination:
            raise MigrationError(f"chunk {chunk_id} is already owned by {destination}")
        if destination not in {s.shard_id for s in snap.shards}:
            raise MigrationError(f"unknown destination shard {destination!r}")

        donor = active_node(snap, chunk.owner)
        recipient = active_node(snap, destination)
        if donor.shard_id == recipient.shard_id:
            raise MigrationError("source and destination resolve to the same node")
        return chunk, donor, recipient

    # -- the migration --------------------------------------------------

    def migrate(self, chunk_id: int, destination: str,
                batch_size: int = CLONE_BATCH,
                cutover_backlog: int = CUTOVER_BACKLOG) -> MigrationReport:
        started = time.perf_counter()
        snap = self.map.snapshot()
        chunk, donor, recipient = self.plan(chunk_id, destination)
        migration_id = f"mig-{chunk_id}-{uuid.uuid4().hex[:8]}"
        recipient_epoch = epoch_of(snap, destination)

        report = MigrationReport(
            migration_id=migration_id, chunk_id=chunk_id, lo=chunk.lo, hi=chunk.hi,
            source=chunk.owner, destination=destination,
            map_version_before=snap.version,
        )
        self.log(f"migration {migration_id}: chunk {chunk_id} "
                 f"[{chunk.lo}, {chunk.hi}) {donor.shard_id} -> {recipient.shard_id}")

        # The recipient may have donated this range earlier; take it back so it
        # does not refuse the traffic it is about to own.
        self._request(recipient, "POST", "/internal/migration/accept",
                      {"migration_id": migration_id, "lo": chunk.lo, "hi": chunk.hi})

        armed = self._request(donor, "POST", "/internal/migration/arm",
                              {"migration_id": migration_id,
                               "lo": chunk.lo, "hi": chunk.hi})
        report.documents_at_arm = armed["documents_in_range"]
        self.log(f"  armed donor: {report.documents_at_arm} documents in range")

        try:
            self._clone(report, donor, recipient, chunk, recipient_epoch, batch_size)
            seq = self._catch_up(report, donor, recipient, recipient_epoch,
                                 cutover_backlog)
            self._cutover(report, donor, recipient, chunk, recipient_epoch, seq)
        except Exception:
            self.log("  migration failed; aborting and leaving the donor in charge")
            self._safe_abort(donor, migration_id)
            raise

        report.total_ms = (time.perf_counter() - started) * 1000
        return report

    def _clone(self, report: MigrationReport, donor: ShardInfo, recipient: ShardInfo,
               chunk: Chunk, recipient_epoch: int, batch_size: int) -> None:
        """Copy the range in batches while the donor keeps serving it.

        Pages are cursored on ``_id`` rather than offset: writes continue
        during the clone, and an offset window would step over a document
        whenever an earlier one was deleted underneath it.
        """
        t0 = time.perf_counter()
        cursor: str | None = None
        while True:
            filt = {"_id": {"$gt": cursor}} if cursor is not None else None
            page = self._request(donor, "POST", "/query", {
                "filter": filt, "sort": {"_id": 1}, "limit": batch_size,
                "hash_lo": chunk.lo, "hash_hi": chunk.hi,
            })
            documents = page["documents"]
            if not documents:
                break
            self._request(recipient, "POST", "/internal/migration/ingest",
                          {"migration_id": report.migration_id,
                           "documents": documents, "deletes": []},
                          epoch=recipient_epoch)
            report.documents_cloned += len(documents)
            cursor = str(documents[-1]["_id"])
            self.log(f"  cloned {report.documents_cloned} documents")
            if len(documents) < batch_size:
                break
        report.clone_ms = (time.perf_counter() - t0) * 1000

    def _drain(self, report: MigrationReport, donor: ShardInfo, recipient: ShardInfo,
               recipient_epoch: int, since_seq: int) -> tuple[int, int]:
        """Ship one page of captured changes; returns ``(last_seq, remaining)``."""
        page = self._request(donor, "GET", "/internal/migration/changes",
                             params={"migration_id": report.migration_id,
                                     "since_seq": since_seq, "limit": CATCHUP_BATCH})
        changes = page["changes"]
        if changes:
            documents = [c["document"] for c in changes if c["op"] == "upsert"]
            deletes = [c["doc_id"] for c in changes if c["op"] == "delete"]
            self._request(recipient, "POST", "/internal/migration/ingest",
                          {"migration_id": report.migration_id,
                           "documents": documents, "deletes": deletes},
                          epoch=recipient_epoch)
            report.changes_applied += len(changes)
            report.deletes_applied += len(deletes)
        return page["last_seq"], page["remaining"]

    def _catch_up(self, report: MigrationReport, donor: ShardInfo, recipient: ShardInfo,
                  recipient_epoch: int, cutover_backlog: int) -> int:
        """Drain the changelog until the backlog is small enough to freeze.

        Returns the changelog sequence reached, which the cutover resumes from.
        """
        t0 = time.perf_counter()
        seq = 0
        for round_index in range(MAX_CATCHUP_ROUNDS):
            seq, remaining = self._drain(report, donor, recipient, recipient_epoch, seq)
            report.catchup_rounds = round_index + 1
            if remaining <= cutover_backlog:
                break
            self.log(f"  catch-up round {round_index + 1}: {remaining} changes behind")
        report.catchup_ms = (time.perf_counter() - t0) * 1000
        return seq

    def _cutover(self, report: MigrationReport, donor: ShardInfo, recipient: ShardInfo,
                 chunk: Chunk, recipient_epoch: int, seq: int) -> None:
        """Freeze writes, drain the tail, flip the map, then release the donor."""
        t0 = time.perf_counter()
        self._request(donor, "POST", "/internal/migration/freeze",
                      {"migration_id": report.migration_id})
        try:
            while True:
                seq, remaining = self._drain(report, donor, recipient,
                                             recipient_epoch, seq)
                if remaining == 0:
                    break

            # Ownership may have moved under us -- a failover rewrites chunk
            # owners -- so re-check before committing the flip.
            current = self.map.snapshot()
            live = next(c for c in current.chunks if c.chunk_id == chunk.chunk_id)
            if live.owner != report.source:
                raise MigrationError(
                    f"chunk {chunk.chunk_id} changed owner to {live.owner!r} mid-migration")

            report.map_version_after = self.map.set_chunk_owner(
                chunk.chunk_id, report.destination, state="stable")
            self.log(f"  shard map updated: chunk {chunk.chunk_id} -> "
                     f"{report.destination} (version {report.map_version_after})")
        except Exception:
            self._safe_abort(donor, report.migration_id)
            raise

        committed = self._request(donor, "POST", "/internal/migration/commit",
                                  {"migration_id": report.migration_id,
                                   "delete_range": True})
        report.removed_from_source = committed["removed"]
        report.cutover_ms = (time.perf_counter() - t0) * 1000
        self._verify(report, donor, recipient, chunk)

    def _verify(self, report: MigrationReport, donor: ShardInfo,
                recipient: ShardInfo, chunk: Chunk) -> None:
        """Compare the two sides of the range after the move."""
        params = {"lo": chunk.lo, "hi": chunk.hi}
        source = self._request(donor, "GET", "/internal/migration/digest", params=params)
        dest = self._request(recipient, "GET", "/internal/migration/digest", params=params)
        report.verification = {
            "source_documents_left": source["documents"],
            "destination_documents": dest["documents"],
            "destination_digest": dest["digest"],
        }
        report.verified = source["documents"] == 0 and dest["documents"] > 0
        self.log(f"  moved {dest['documents']} documents; "
                 f"{source['documents']} left on the donor")

    def _safe_abort(self, donor: ShardInfo, migration_id: str) -> None:
        try:
            self._request(donor, "POST", "/internal/migration/abort",
                          {"migration_id": migration_id})
        except (MigrationError, httpx.HTTPError):
            pass
