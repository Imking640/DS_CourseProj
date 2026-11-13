#!/usr/bin/env python3
"""Migrate a chunk while a writer keeps mutating the range, then prove nothing
was lost or duplicated.

A background writer inserts, updates and deletes documents through the router
for the whole run, concentrating on the chunk being moved.  It keeps its own
model of what the cluster should contain; afterwards that model is compared
against what the cluster actually returns.

    python scripts/migrate_under_load.py --chunk 3 --to shard-0 --writers 4

Writes results to results/phase3_migration_under_load.json.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from shardstore.config import ClusterConfig  # noqa: E402
from shardstore.hashing import hash_key  # noqa: E402
from shardstore.migration import ChunkMigrator, MigrationError  # noqa: E402
from shardstore.shardmap import ShardMap  # noqa: E402

RESULTS = ROOT / "results" / "phase3_migration_under_load.json"


class WriteLedger:
    """What the cluster should contain, as seen by the load generator.

    Three outcomes are tracked separately, because only two of them say
    anything about correctness:

    * **acknowledged** -- the router returned success, so the document must be
      present with that value afterwards;
    * **rejected** -- the router returned a definite client error, so nothing
      changed; and
    * **indeterminate** -- the write may or may not have been applied (a 5xx,
      or a dropped connection).  Phase 2 acknowledges a write only after the
      follower acknowledges it, so a write can be applied on the primary and
      still be reported as failed.  Such a document is excluded from the
      lost/stale comparison rather than counted as a bug in either direction.
    """

    def __init__(self) -> None:
        self.expected: dict[str, int] = {}     # doc_id -> last acknowledged counter
        self.deleted: set[str] = set()
        self.uncertain: set[str] = set()
        self.ops = Counter()
        self.errors: Counter = Counter()

    def acked_write(self, doc_id: str, counter: int, op: str) -> None:
        self.expected[doc_id] = counter
        self.deleted.discard(doc_id)
        self.ops[op] += 1

    def acked_delete(self, doc_id: str) -> None:
        self.expected.pop(doc_id, None)
        self.deleted.add(doc_id)
        self.ops["delete"] += 1

    def failed(self, op: str, reason: str, doc_id: str | None = None) -> None:
        self.errors[f"{op}:{reason}"] += 1
        if doc_id is not None:
            self.uncertain.add(doc_id)


async def writer_task(client: httpx.AsyncClient, ledger: WriteLedger, stop: asyncio.Event,
                      lo: int, hi: int, worker: int, rng: random.Random) -> None:
    """Insert / update / delete inside the migrating range until told to stop."""
    counter = 0
    mine: list[str] = []
    while not stop.is_set():
        counter += 1
        try:
            choice = rng.random()
            if mine and choice < 0.45:                       # update
                doc_id = rng.choice(mine)
                response = await client.put(f"/documents/{doc_id}",
                                            json={"update": {"$set": {"c": counter}}})
                if response.status_code == 200:
                    ledger.acked_write(doc_id, counter, "update")
                elif response.status_code == 404:
                    mine.remove(doc_id)
                else:
                    ledger.failed("update", str(response.status_code), doc_id)
            elif mine and choice < 0.60:                     # delete
                doc_id = mine.pop(rng.randrange(len(mine)))
                response = await client.delete(f"/documents/{doc_id}")
                if response.status_code == 200:
                    ledger.acked_delete(doc_id)
                elif response.status_code != 404:
                    ledger.failed("delete", str(response.status_code), doc_id)
            else:                                            # insert into the range
                doc_id = next_key_in_range(lo, hi, worker, counter)
                response = await client.post(
                    "/documents",
                    json={"document": {"_id": doc_id, "c": counter, "w": worker}})
                if response.status_code == 201:
                    ledger.acked_write(doc_id, counter, "insert")
                    mine.append(doc_id)
                elif response.status_code != 409:
                    ledger.failed("insert", str(response.status_code), doc_id)
        except httpx.HTTPError as exc:
            ledger.failed("transport", type(exc).__name__, locals().get("doc_id"))
        await asyncio.sleep(0)


def next_key_in_range(lo: int, hi: int, worker: int, counter: int) -> str:
    """A key that hashes into ``[lo, hi)`` so the write targets the chunk."""
    attempt = 0
    while True:
        candidate = f"load-{worker}-{counter}-{attempt}"
        if lo <= hash_key(candidate) < hi:
            return candidate
        attempt += 1


async def verify(client: httpx.AsyncClient, ledger: WriteLedger,
                 lo: int, hi: int) -> dict[str, Any]:
    """Compare the ledger against what the cluster actually holds."""
    found: dict[str, int] = {}
    duplicates: Counter = Counter()
    cursor: str | None = None
    while True:
        body: dict[str, Any] = {"filter": {"_id": {"$gt": cursor}} if cursor else None,
                                "sort": {"_id": 1}, "limit": 1000}
        response = await client.post("/query", json=body)
        response.raise_for_status()
        documents = response.json()["documents"]
        if not documents:
            break
        for doc in documents:
            doc_id = doc["_id"]
            if doc_id in found:
                duplicates[doc_id] += 1
            found[doc_id] = doc.get("c")
        cursor = str(documents[-1]["_id"])

    in_range = {k: v for k, v in found.items() if lo <= hash_key(k) < hi}
    # A document whose last outcome was indeterminate proves nothing either way.
    settled = {k: v for k, v in ledger.expected.items() if k not in ledger.uncertain}
    missing = sorted(set(settled) - set(found))
    resurrected = sorted((ledger.deleted - ledger.uncertain) & set(found))
    stale = sorted(k for k, c in settled.items() if k in found and found[k] != c)

    return {
        "documents_in_cluster": len(found),
        "documents_in_migrated_range": len(in_range),
        "acknowledged_live": len(ledger.expected),
        "acknowledged_deleted": len(ledger.deleted),
        "indeterminate_writes": len(ledger.uncertain),
        "checked_documents": len(settled),
        "lost_documents": len(missing),
        "duplicate_documents": len(duplicates),
        "resurrected_deletes": len(resurrected),
        "stale_values": len(stale),
        "examples": {"lost": missing[:5], "duplicated": list(duplicates)[:5],
                     "resurrected": resurrected[:5], "stale": stale[:5]},
    }


async def run(args: argparse.Namespace) -> int:
    cfg = ClusterConfig.load(args.config)
    with ShardMap(cfg.shard_map_file) as smap:
        snapshot = smap.snapshot()
    chunk = next((c for c in snapshot.chunks if c.chunk_id == args.chunk), None)
    if chunk is None:
        print(f"no chunk {args.chunk}", file=sys.stderr)
        return 1
    if chunk.owner == args.destination:
        print(f"chunk {args.chunk} already lives on {args.destination}", file=sys.stderr)
        return 1

    ledger = WriteLedger()
    stop = asyncio.Event()

    async with httpx.AsyncClient(base_url=cfg.router.base_url, timeout=30.0) as client:
        health = (await client.get("/health")).json()
        if health["status"] != "online":
            print(f"cluster is {health['status']}", file=sys.stderr)
            return 1

        print(f"chunk {args.chunk} [{chunk.lo}, {chunk.hi}) "
              f"{chunk.owner} -> {args.destination}")
        print(f"starting {args.writers} writers against the migrating range ...")
        writers = [
            asyncio.create_task(
                writer_task(client, ledger, stop, chunk.lo, chunk.hi, w,
                            random.Random(args.seed + w)))
            for w in range(args.writers)
        ]
        await asyncio.sleep(args.warmup)
        print(f"  {sum(ledger.ops.values())} writes accepted before the migration")

        migration_started = time.perf_counter()
        loop = asyncio.get_running_loop()

        def do_migration():
            with ChunkMigrator(str(cfg.shard_map_file), verbose=not args.quiet) as m:
                return m.migrate(args.chunk, args.destination, batch_size=args.batch)

        try:
            report = await loop.run_in_executor(None, do_migration)
        except MigrationError as exc:
            stop.set()
            await asyncio.gather(*writers, return_exceptions=True)
            print(f"migration failed: {exc}", file=sys.stderr)
            return 1
        migration_ms = (time.perf_counter() - migration_started) * 1000

        await asyncio.sleep(args.cooldown)
        stop.set()
        await asyncio.gather(*writers, return_exceptions=True)
        print(f"  {sum(ledger.ops.values())} writes accepted in total "
              f"({dict(ledger.ops)})")

        print("verifying ...")
        checks = await verify(client, ledger, chunk.lo, chunk.hi)

    clean = (checks["lost_documents"] == 0 and checks["duplicate_documents"] == 0
             and checks["resurrected_deletes"] == 0 and checks["stale_values"] == 0)
    payload = {
        "recorded_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "writers": args.writers,
        "migration": report.as_dict(),
        "migration_wall_ms": round(migration_ms, 3),
        "write_ops": dict(ledger.ops),
        "write_errors": dict(ledger.errors),
        "verification": checks,
        "passed": clean and report.verified,
    }
    RESULTS.parent.mkdir(exist_ok=True)
    RESULTS.write_text(json.dumps(payload, indent=2))

    print()
    print(f"documents moved        : {report.documents_moved}")
    print(f"  cloned               : {report.documents_cloned}")
    print(f"  live changes applied : {report.changes_applied} "
          f"({report.deletes_applied} deletes) over {report.catchup_rounds} round(s)")
    print(f"writes during migration: {sum(ledger.ops.values())} "
          f"({len(ledger.errors)} error kinds)")
    print(f"cutover freeze         : {report.cutover_ms:.1f} ms")
    print(f"documents checked      : {checks['checked_documents']} "
          f"({checks['indeterminate_writes']} indeterminate, excluded)")
    print(f"lost documents         : {checks['lost_documents']}")
    print(f"duplicate documents    : {checks['duplicate_documents']}")
    print(f"resurrected deletes    : {checks['resurrected_deletes']}")
    print(f"stale values           : {checks['stale_values']}")
    print(f"\n{'PASS' if payload['passed'] else 'FAIL'} -- wrote "
          f"{RESULTS.relative_to(ROOT)}")
    if not payload["passed"]:
        print(json.dumps(checks["examples"], indent=2), file=sys.stderr)
    return 0 if payload["passed"] else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=None)
    parser.add_argument("--chunk", type=int, required=True)
    parser.add_argument("--to", dest="destination", required=True)
    parser.add_argument("--writers", type=int, default=4)
    parser.add_argument("--warmup", type=float, default=2.0,
                        help="seconds of writes before the migration starts")
    parser.add_argument("--cooldown", type=float, default=1.0,
                        help="seconds of writes after the migration finishes")
    parser.add_argument("--batch", type=int, default=250)
    parser.add_argument("--seed", type=int, default=11)
    parser.add_argument("--quiet", action="store_true")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
