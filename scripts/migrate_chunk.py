#!/usr/bin/env python3
"""Move one chunk from its current shard to another while writes continue.

    python scripts/migrate_chunk.py --chunk 3 --to shard-0
    python scripts/migrate_chunk.py --list

Appends the result to results/phase3_migrations.json.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from shardstore.config import ClusterConfig  # noqa: E402
from shardstore.migration import ChunkMigrator, MigrationError  # noqa: E402
from shardstore.shardmap import ShardMap  # noqa: E402

RESULTS = ROOT / "results" / "phase3_migrations.json"


def show_chunks(cfg: ClusterConfig) -> int:
    with ShardMap(cfg.shard_map_file) as smap:
        snap = smap.snapshot()
    print(f"shard map version {snap.version}")
    print(f"{'chunk':>5}  {'lo':>12}  {'hi':>12}  owner")
    for chunk in snap.chunks:
        print(f"{chunk.chunk_id:>5}  {chunk.lo:>12}  {chunk.hi:>12}  {chunk.owner}")
    print("\ncoverage:")
    for shard_id, fraction in sorted(snap.coverage().items()):
        if fraction:
            print(f"  {shard_id:20} {fraction:6.1%}")
    return 0


def record(report_dict: dict) -> None:
    RESULTS.parent.mkdir(exist_ok=True)
    history = []
    if RESULTS.exists():
        history = json.loads(RESULTS.read_text())
    history.append({"recorded_at": time.strftime("%Y-%m-%d %H:%M:%S"), **report_dict})
    RESULTS.write_text(json.dumps(history, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=None)
    parser.add_argument("--chunk", type=int, help="chunk id to move")
    parser.add_argument("--to", dest="destination", help="destination shard id")
    parser.add_argument("--list", action="store_true", help="show the chunk table")
    parser.add_argument("--batch", type=int, default=500, help="clone batch size")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    cfg = ClusterConfig.load(args.config)
    if args.list or args.chunk is None or not args.destination:
        if not args.list:
            parser.error("--chunk and --to are required (or use --list)")
        return show_chunks(cfg)

    with ChunkMigrator(str(cfg.shard_map_file), verbose=not args.quiet) as migrator:
        try:
            report = migrator.migrate(args.chunk, args.destination, batch_size=args.batch)
        except MigrationError as exc:
            print(f"migration failed: {exc}", file=sys.stderr)
            return 1

    payload = report.as_dict()
    record(payload)
    print()
    print(f"documents moved : {payload['documents_moved']}")
    print(f"  cloned        : {payload['documents_cloned']}")
    print(f"  live changes  : {payload['changes_applied']} "
          f"over {payload['catchup_rounds']} catch-up round(s)")
    print(f"  left on donor : {payload['verification']['source_documents_left']}")
    print(f"map version     : {payload['map_version']['before']} -> "
          f"{payload['map_version']['after']}")
    print(f"cutover         : {payload['timing_ms']['cutover']:.1f} ms "
          f"(total {payload['timing_ms']['total']:.1f} ms)")
    print(f"verified        : {payload['verified']}")
    print(f"\nappended to {RESULTS.relative_to(ROOT)}")
    return 0 if payload["verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
