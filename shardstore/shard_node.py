"""A shard process. Primaries synchronously replicate acknowledged writes.

Phase 3 adds the donor and recipient halves of a chunk migration.  A donor
arms the moving range (capturing concurrent writes), ships a snapshot, then
freezes the range for the brief cutover; a recipient ingests shipped batches
through the same replication path as any other write, so its follower ends up
with the moved documents too.
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

import httpx
from fastapi import FastAPI, Header, HTTPException, Response

from .api_models import (ArmRangeRequest, BulkInsertRequest, CountRequest,
                         InsertRequest, MigrationCommitRequest,
                         MigrationIngestRequest, QueryRequest,
                         ShardQueryResponse, UpdateRequest)
from .filters import FilterError
from .hashing import hash_key
from .storage import (DocumentNotFound, DocumentStore, DuplicateKeyError,
                      RangeNotOwned, in_any_range)

# The router recognises this marker, reloads the shard map and retries
# elsewhere.  409 is used so an ordinary client sees a conflict, not a crash.
MIGRATION_RETRY = "range is migrating; reload the shard map and retry"


def create_app(shard_id: str, db_path: str, shard_key: str = "_id",
               role: str = "primary", partner_url: str | None = None,
               shard_map_path: str | None = None, initial_epoch: int = 1) -> FastAPI:
    store = DocumentStore(db_path, shard_key=shard_key)
    started = time.time()
    # Ranges this node has handed over but may still physically hold, and the
    # range (if any) currently frozen for a cutover.  Both make this node
    # refuse key-routed traffic it no longer owns, so a client holding a stale
    # shard map is told to re-route instead of being served stale data.
    donated_ranges: list[tuple[int, int]] = []
    frozen_ranges: dict[str, tuple[int, int]] = {}
    migrations: dict[str, dict[str, Any]] = {}
    # Sync FastAPI endpoints run in a threadpool, so this connection is shared
    # across threads and every use must be serialised: concurrent execute() on
    # one sqlite3 connection interleaves cursor state, and a fetchone() can
    # then come back empty for a row that exists.
    state_lock = threading.Lock()
    state_db = sqlite3.connect(str(db_path) + ".node-state.db", check_same_thread=False)
    state_db.execute("CREATE TABLE IF NOT EXISTS node_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    state_db.execute("INSERT OR IGNORE INTO node_state VALUES('role', ?)", (role,))
    state_db.execute("INSERT OR IGNORE INTO node_state VALUES('epoch', ?)", (str(initial_epoch),))
    state_db.commit()
    # A follower promoted in an earlier run must not replicate writes back
    # to its former primary when the process is restarted.
    if role == "follower" and state_db.execute(
        "SELECT value FROM node_state WHERE key='role'"
    ).fetchone()[0] == "primary":
        partner_url = None

    def state(key: str) -> str:
        with state_lock:
            row = state_db.execute(
                "SELECT value FROM node_state WHERE key=?", (key,)).fetchone()
        if row is None:
            raise HTTPException(status_code=500,
                                detail=f"node state {key!r} is missing")
        return row[0]

    def set_state(key: str, value: str) -> None:
        with state_lock, state_db:
            state_db.execute("INSERT INTO node_state(key,value) VALUES(?,?) "
                             "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                             (key, str(value)))

    def validate_epoch(epoch: int, require_primary: bool = True) -> None:
        local_epoch = int(state("epoch"))
        if epoch != local_epoch:
            raise HTTPException(status_code=409,
                                detail=f"stale ownership epoch: request={epoch}, node={local_epoch}")
        if require_primary and state("role") != "primary":
            raise HTTPException(status_code=409, detail="writes are accepted only by the primary")
        # The shared persistent map is the fencing authority in this local cluster.
        if shard_map_path and Path(shard_map_path).exists():
            conn = sqlite3.connect(shard_map_path, timeout=2)
            try:
                row = conn.execute(
                    "SELECT active_id, epoch FROM replicas WHERE primary_id=? OR follower_id=?",
                    (shard_id, shard_id)).fetchone()
                if row and (row[0] != shard_id or int(row[1]) != epoch):
                    raise HTTPException(status_code=409,
                                        detail="stale primary rejected by shard-map ownership epoch")
            finally:
                conn.close()

    def sync_blocked_ranges() -> None:
        """Push the node's refused ranges down into the store.

        The store enforces them under its own lock, which is what closes the
        window between an HTTP-level check and the write it guards.
        """
        store.set_blocked_ranges(donated_ranges + list(frozen_ranges.values()))

    def guard_migration(doc_id: str, writing: bool) -> None:
        """Reject a *read* of a range this node has handed over.

        Writes are enforced in the store (see :func:`sync_blocked_ranges`);
        this only short-circuits reads, which the store does not block because
        a donor's copy stays readable right up to the cutover.
        """
        if not writing and in_any_range(hash_key(doc_id), donated_ranges):
            raise HTTPException(status_code=409, detail=MIGRATION_RETRY)

    @asynccontextmanager
    async def lifespan(inner: FastAPI) -> AsyncIterator[None]:
        # One pooled client for the life of the process.  Building a client per
        # write re-opened a connection to the follower every time, which under
        # migration load exhausted sockets and turned acknowledged writes into
        # "follower did not acknowledge" failures.
        inner.state.replication_client = httpx.AsyncClient(
            timeout=10.0,
            limits=httpx.Limits(max_connections=64, max_keepalive_connections=32),
        )
        yield
        await inner.state.replication_client.aclose()
        store.close()
        state_db.close()

    app = FastAPI(title=f"shard {shard_id}", version="0.2.0", lifespan=lifespan)
    app.state.shard_id = shard_id
    app.state.store = store

    @app.exception_handler(FilterError)
    def _bad_request(_request: Any, exc: FilterError) -> Response:
        raise HTTPException(status_code=400, detail=str(exc))

    @app.exception_handler(RangeNotOwned)
    def _moved(_request: Any, exc: RangeNotOwned) -> Response:
        raise HTTPException(status_code=409, detail=MIGRATION_RETRY)

    async def replicate(operation: str, payload: dict[str, Any], epoch: int) -> None:
        if state("role") != "primary" or not partner_url:
            return
        try:
            response = await app.state.replication_client.post(
                f"{partner_url}/internal/replicate",
                json={"operation": operation, "payload": payload, "epoch": epoch})
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"write applied locally but follower did not acknowledge: {exc}",
            ) from exc

    def apply_replication(operation: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            if operation == "insert":
                doc = store.insert(payload["document"])
                return {"inserted": True, "document": doc}
            if operation == "bulk":
                n = store.insert_many(payload["documents"], overwrite=payload.get("overwrite", False))
                return {"inserted": n}
            if operation == "update":
                doc, created = store.update(payload["doc_id"], payload["update"],
                                            upsert=payload.get("upsert", False))
                return {"matched": not created, "upserted": created, "document": doc}
            if operation == "delete":
                return {"deleted": int(store.delete(payload["doc_id"]))}
            if operation == "migrate":
                upserted, deleted = store.apply_migration_batch(
                    payload.get("documents", []), payload.get("deletes", []))
                return {"upserted": upserted, "deleted": deleted}
            if operation == "drop_range":
                return {"dropped": store.delete_range(payload["lo"], payload["hi"])}
            raise HTTPException(status_code=400, detail=f"unknown replication operation {operation}")
        except DuplicateKeyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except DocumentNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/health")
    def health():
        return {"shard_id": shard_id, "status": "online", "role": state("role"),
                "epoch": int(state("epoch")), "uptime_s": round(time.time()-started, 3),
                "documents": store.count()}

    @app.get("/stats")
    def stats():
        return {"shard_id": shard_id, "role": state("role"),
                "epoch": int(state("epoch")), **store.stats()}

    @app.post("/internal/replicate")
    def receive_replication(req: dict[str, Any]):
        if state("role") != "follower":
            raise HTTPException(status_code=409, detail="replication target is not a follower")
        if int(req.get("epoch", -1)) != int(state("epoch")):
            raise HTTPException(status_code=409, detail="replication rejected for stale epoch")
        result = apply_replication(req["operation"], req["payload"])
        return {"acknowledged": True, "shard_id": shard_id, **result}

    @app.post("/internal/replace-all")
    def replace_all(req: dict[str, Any]):
        if state("role") != "follower":
            raise HTTPException(status_code=409, detail="only a follower can be synchronised")
        if int(req.get("epoch", -1)) != int(state("epoch")):
            raise HTTPException(status_code=409, detail="sync rejected for stale epoch")
        n = store.replace_all(req.get("documents", []))
        return {"acknowledged": True, "documents": n}

    @app.post("/internal/promote")
    def promote(req: dict[str, Any]):
        nonlocal partner_url
        new_epoch = int(req["epoch"])
        current = int(state("epoch"))
        if new_epoch != current + 1:
            raise HTTPException(status_code=409,
                                detail=f"promotion epoch must be {current + 1}")
        set_state("epoch", str(new_epoch))
        set_state("role", "primary")
        partner_url = None
        return {"shard_id": shard_id, "role": "primary", "epoch": new_epoch}

    @app.post("/internal/sync-follower")
    async def sync_follower():
        if state("role") != "primary" or not partner_url:
            raise HTTPException(status_code=409, detail="this node has no active follower")
        docs = store.query(None).documents
        try:
            response = await app.state.replication_client.post(
                f"{partner_url}/internal/replace-all",
                json={"documents": docs, "epoch": int(state("epoch"))},
                timeout=60.0)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=503, detail=f"follower sync failed: {exc}") from exc
        return {"synchronised": len(docs), "follower": partner_url}

    # -- migration: donor side ------------------------------------------

    @app.post("/internal/migration/arm")
    def migration_arm(req: ArmRangeRequest):
        """Begin capturing writes to ``[lo, hi)`` before the copy starts."""
        if state("role") != "primary":
            raise HTTPException(status_code=409, detail="only a primary can donate a range")
        in_range = store.arm_range(req.migration_id, req.lo, req.hi)
        migrations[req.migration_id] = {"lo": req.lo, "hi": req.hi, "phase": "armed"}
        return {"shard_id": shard_id, "migration_id": req.migration_id,
                "documents_in_range": in_range, "epoch": int(state("epoch"))}

    @app.get("/internal/migration/changes")
    def migration_changes(migration_id: str, since_seq: int = 0, limit: int = 500):
        """Mutations captured since ``since_seq``, oldest first."""
        changes = store.changes_since(migration_id, since_seq, limit)
        return {"shard_id": shard_id, "migration_id": migration_id,
                "changes": [{"seq": c.seq, "doc_id": c.doc_id, "op": c.op,
                             "document": c.document} for c in changes],
                "last_seq": changes[-1].seq if changes else since_seq,
                "remaining": store.pending_changes(
                    migration_id, changes[-1].seq if changes else since_seq)}

    @app.post("/internal/migration/freeze")
    def migration_freeze(req: MigrationCommitRequest):
        """Enter the cutover critical section: refuse writes to the range."""
        record = migrations.get(req.migration_id)
        if record is None:
            raise HTTPException(status_code=404, detail="unknown migration")
        frozen_ranges[req.migration_id] = (record["lo"], record["hi"])
        sync_blocked_ranges()
        record["phase"] = "frozen"
        return {"shard_id": shard_id, "frozen": [record["lo"], record["hi"]],
                "pending_changes": store.pending_changes(req.migration_id, 0)}

    @app.post("/internal/migration/commit")
    async def migration_commit(req: MigrationCommitRequest,
                               x_ownership_epoch: int = Header(default=1)):
        """Finish: stop serving the range and drop the donor's copy.

        The drop is replicated, so the donor's follower sheds the handed-over
        documents too and stays a faithful copy of its primary.
        """
        record = migrations.get(req.migration_id)
        if record is None:
            raise HTTPException(status_code=404, detail="unknown migration")
        span = (record["lo"], record["hi"])
        donated_ranges.append(span)
        frozen_ranges.pop(req.migration_id, None)
        sync_blocked_ranges()
        removed = 0
        if req.delete_range:
            removed = store.delete_range(*span)
            await replicate("drop_range", {"lo": span[0], "hi": span[1]},
                            int(state("epoch")))
        store.disarm_range(req.migration_id)
        record["phase"] = "committed"
        return {"shard_id": shard_id, "migration_id": req.migration_id,
                "removed": removed, "donated": list(span)}

    @app.post("/internal/migration/abort")
    def migration_abort(req: MigrationCommitRequest):
        """Give up: unfreeze and keep serving the range as before."""
        frozen_ranges.pop(req.migration_id, None)
        sync_blocked_ranges()
        store.disarm_range(req.migration_id)
        record = migrations.pop(req.migration_id, None)
        return {"shard_id": shard_id, "aborted": req.migration_id,
                "phase_reached": record["phase"] if record else None}

    @app.get("/internal/migration/digest")
    def migration_digest(lo: int, hi: int):
        """Fingerprint of a range, so a move can be verified end to end."""
        return {"shard_id": shard_id, **store.range_digest(lo, hi)}

    @app.get("/internal/migration/state")
    def migration_state():
        return {"shard_id": shard_id, "armed": store.armed_range,
                "blocked": [list(r) for r in store.blocked_ranges],
                "frozen": {k: list(v) for k, v in frozen_ranges.items()},
                "donated": [list(r) for r in donated_ranges],
                "migrations": migrations}

    # -- migration: recipient side ---------------------------------------

    @app.post("/internal/migration/ingest")
    async def migration_ingest(req: MigrationIngestRequest,
                               x_ownership_epoch: int = Header(default=1)):
        """Apply a shipped batch here and on this node's follower.

        Routed through the ordinary replication path, which is what makes the
        recipient's follower receive the moved documents as well.
        """
        validate_epoch(x_ownership_epoch)
        upserted, deleted = store.apply_migration_batch(req.documents, req.deletes)
        await replicate("migrate",
                        {"documents": req.documents, "deletes": req.deletes},
                        x_ownership_epoch)
        return {"shard_id": shard_id, "upserted": upserted, "deleted": deleted,
                "migration_id": req.migration_id}

    @app.post("/internal/migration/accept")
    def migration_accept(req: ArmRangeRequest):
        """Take a range back: clear it from this node's donated list.

        Needed when a range is migrated away and later migrated back, which
        would otherwise keep being refused by :func:`guard_migration`.
        """
        donated_ranges[:] = [r for r in donated_ranges
                             if r[1] <= req.lo or r[0] >= req.hi]
        sync_blocked_ranges()
        return {"shard_id": shard_id, "accepted": [req.lo, req.hi],
                "donated": [list(r) for r in donated_ranges]}

    @app.post("/documents", status_code=201)
    async def insert(req: InsertRequest, x_ownership_epoch: int = Header(default=1)):
        validate_epoch(x_ownership_epoch)
        try:
            doc = store.insert(req.document)
        except DuplicateKeyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        await replicate("insert", {"document": req.document}, x_ownership_epoch)
        return {"shard_id": shard_id, "inserted": True, "document": doc,
                "epoch": x_ownership_epoch}

    @app.post("/documents/bulk", status_code=201)
    async def insert_many(req: BulkInsertRequest, x_ownership_epoch: int = Header(default=1)):
        validate_epoch(x_ownership_epoch)
        try:
            n = store.insert_many(req.documents, overwrite=req.overwrite)
        except DuplicateKeyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        await replicate("bulk", {"documents": req.documents, "overwrite": req.overwrite}, x_ownership_epoch)
        return {"shard_id": shard_id, "inserted": n, "epoch": x_ownership_epoch}

    @app.put("/documents/{doc_id}")
    async def update(doc_id: str, req: UpdateRequest,
                     x_ownership_epoch: int = Header(default=1)):
        validate_epoch(x_ownership_epoch)
        try:
            doc, created = store.update(doc_id, req.update, upsert=req.upsert)
        except DocumentNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        await replicate("update", {"doc_id": doc_id, **req.model_dump()}, x_ownership_epoch)
        return {"shard_id": shard_id, "matched": not created, "upserted": created,
                "document": doc, "epoch": x_ownership_epoch}

    @app.delete("/documents/{doc_id}")
    async def delete(doc_id: str, x_ownership_epoch: int = Header(default=1)):
        validate_epoch(x_ownership_epoch)
        if not store.delete(doc_id):
            raise HTTPException(status_code=404, detail=f"no document with _id {doc_id!r}")
        await replicate("delete", {"doc_id": doc_id}, x_ownership_epoch)
        return {"shard_id": shard_id, "deleted": 1, "epoch": x_ownership_epoch}

    @app.get("/documents/{doc_id}")
    def get(doc_id: str):
        guard_migration(doc_id, writing=False)
        doc = store.get(doc_id)
        if doc is None:
            raise HTTPException(status_code=404, detail=f"no document with _id {doc_id!r}")
        return {"shard_id": shard_id, "document": doc, "role": state("role"),
                "epoch": int(state("epoch"))}

    @app.post("/query", response_model=ShardQueryResponse)
    def query(req: QueryRequest):
        t0 = time.perf_counter()
        try:
            result = store.query(req.filter, sort=req.sort, limit=req.limit,
                                 skip=req.skip, fields=req.fields,
                                 hash_range=req.hash_range,
                                 owned_ranges=req.owned_ranges)
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return ShardQueryResponse(shard_id=shard_id, count=len(result.documents),
                                  documents=result.documents, examined=result.examined,
                                  indexed=result.indexed,
                                  took_ms=(time.perf_counter()-t0)*1000)

    @app.post("/count")
    def count(req: CountRequest):
        try:
            n = store.count(req.filter, hash_range=req.hash_range,
                            owned_ranges=req.owned_ranges)
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"shard_id": shard_id, "count": n}

    return app


def app_from_env() -> FastAPI:
    required = ("SHARD_ID", "SHARD_DB")
    missing = [key for key in required if key not in os.environ]
    if missing:
        raise RuntimeError(f"missing environment variables: {', '.join(missing)}")
    return create_app(
        os.environ["SHARD_ID"], os.environ["SHARD_DB"],
        os.environ.get("SHARD_KEY", "_id"),
        os.environ.get("SHARD_ROLE", "primary"),
        os.environ.get("PARTNER_URL") or None,
        os.environ.get("SHARD_MAP_PATH") or None,
        int(os.environ.get("SHARD_EPOCH", "1")))
