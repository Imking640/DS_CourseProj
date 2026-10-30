#!/usr/bin/env python3
"""Load synthetic documents into the cluster through the router.

    python scripts/seed_data.py --count 20000
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from shardstore.config import ClusterConfig  # noqa: E402
from shardstore.workload import make_documents  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=None)
    parser.add_argument("--count", type=int, default=10_000)
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--prefix", default="user")
    parser.add_argument("--overwrite", action="store_true",
                        help="replace documents that already exist")
    args = parser.parse_args()

    router = ClusterConfig.load(args.config).router.base_url
    documents = make_documents(args.count, prefix=args.prefix)

    inserted = 0
    t0 = time.perf_counter()
    with httpx.Client(base_url=router, timeout=60.0) as client:
        for start in range(0, len(documents), args.batch):
            batch = documents[start:start + args.batch]
            response = client.post(
                "/documents/bulk", json={"documents": batch, "overwrite": args.overwrite}
            )
            response.raise_for_status()
            inserted += response.json()["inserted"]
            print(f"\r  inserted {inserted}/{len(documents)}", end="", flush=True)
        elapsed = time.perf_counter() - t0
        print(f"\r  inserted {inserted} documents in {elapsed:.2f}s "
              f"({inserted / elapsed:,.0f} docs/s)")
        stats = client.get("/stats").json()

    print(f"\ncluster now holds {stats['documents']} documents")
    for shard in stats["shards"]:
        print(f"  {shard['shard_id']:10} {shard['documents']:>8} docs"
              f"  {shard['size_bytes'] / 1024:>8.0f} KiB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
