#!/usr/bin/env python3
"""Create (or rebuild) the persistent shard map from config/cluster.json."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shardstore.config import ClusterConfig
from shardstore.shardmap import ShardMap


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=None, help="path to cluster.json")
    parser.add_argument("--force", action="store_true", help="overwrite an existing map")
    args = parser.parse_args()

    cfg = ClusterConfig.load(args.config)
    with ShardMap(cfg.shard_map_file) as smap:
        snapshot = smap.bootstrap(
            shards=[(s.shard_id, s.host, s.port) for s in cfg.shards],
            num_chunks=cfg.num_chunks,
            shard_key=cfg.shard_key,
            force=args.force,
            followers=[(s.follower.shard_id, s.follower.host, s.follower.port, s.shard_id)
                       for s in cfg.shards if s.follower],
        )

    print(f"shard map  : {cfg.shard_map_file}")
    print(f"version    : {snapshot.version}   shard key: {snapshot.shard_key!r}")
    print(f"{'chunk':>5}  {'lo':>12}  {'hi':>12}  owner")
    for chunk in snapshot.chunks:
        print(f"{chunk.chunk_id:>5}  {chunk.lo:>12}  {chunk.hi:>12}  {chunk.owner}")
    print("\ncoverage:")
    for shard_id, frac in sorted(snapshot.coverage().items()):
        print(f"  {shard_id}: {frac:6.1%}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
