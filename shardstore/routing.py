"""Query planning: decide which shards a request touches, and merge replies.

Two routing modes matter for Phase 1:

* **direct / multi-key** -- the filter pins the shard key, so the shard map
  tells us the one (or few) shards that can hold the answer.
* **scatter-gather** -- the filter says nothing about the shard key, so every
  owning shard is asked and the router merges their replies.

Keeping the planner free of HTTP makes it unit-testable and keeps the router
service a thin dispatcher over it.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Any, Sequence

from .filters import Sort, extract_shard_keys, sort_comparator
from .shardmap import Chunk, ShardInfo, ShardMapSnapshot

__all__ = [
    "DIRECT", "MULTI_KEY", "SCATTER_GATHER", "RoutePlan", "extract_shard_keys",
    "merge_results", "plan_for_filter", "plan_for_key", "shard_limit",
]  # extract_shard_keys lives in filters.py; re-exported here as part of the planner API

DIRECT = "direct"
MULTI_KEY = "multi-key"
SCATTER_GATHER = "scatter-gather"


@dataclass(frozen=True)
class RoutePlan:
    """Where a request goes, and why -- echoed back to the caller."""

    mode: str
    targets: tuple[ShardInfo, ...]
    map_version: int
    shard_key: Any = None
    chunk: Chunk | None = None
    keys: tuple[Any, ...] = field(default_factory=tuple)

    @property
    def fanout(self) -> int:
        return len(self.targets)

    @property
    def is_scatter(self) -> bool:
        return self.mode == SCATTER_GATHER

    def as_info(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "shards": [s.shard_id for s in self.targets],
            "shard_key": self.shard_key,
            "chunk_id": self.chunk.chunk_id if self.chunk else None,
            "map_version": self.map_version,
            "fanout": self.fanout,
        }


def plan_for_key(snapshot: ShardMapSnapshot, key: Any) -> RoutePlan:
    """Single shard-key lookup: one bisect on the chunk table, one shard."""
    chunk = snapshot.chunk_for_key(key)
    return RoutePlan(
        mode=DIRECT,
        targets=(snapshot.shard(chunk.owner),),
        map_version=snapshot.version,
        shard_key=key,
        chunk=chunk,
        keys=(key,),
    )


def plan_for_filter(snapshot: ShardMapSnapshot, filt: Any) -> RoutePlan:
    """Direct-route when the filter pins the shard key, else scatter-gather."""
    keys = extract_shard_keys(filt, snapshot.shard_key)
    if keys is None or len(keys) == 0:
        return RoutePlan(
            mode=SCATTER_GATHER,
            targets=snapshot.owners(),
            map_version=snapshot.version,
        )
    values = tuple(keys)
    if len(values) == 1:
        return plan_for_key(snapshot, values[0])

    owners: dict[str, ShardInfo] = {}
    for value in values:
        shard = snapshot.shard_for_key(value)
        owners[shard.shard_id] = shard
    return RoutePlan(
        mode=MULTI_KEY,
        targets=tuple(owners.values()),
        map_version=snapshot.version,
        keys=values,
    )


def shard_limit(limit: int | None, skip: int, fanout: int) -> int | None:
    """How many documents to ask each shard for.

    With a global ``skip``/``limit`` and a sort, any shard could supply all of
    the first ``skip + limit`` results, so each must return that many; the
    router then applies the real window after merging.  A single-shard plan
    needs no inflation.
    """
    if limit is None:
        return None
    return limit if fanout == 1 and skip == 0 else limit + skip


def merge_results(
    per_shard: Sequence[Sequence[dict[str, Any]]],
    sort: Sort,
    limit: int | None = None,
    skip: int = 0,
) -> list[dict[str, Any]]:
    """Merge per-shard replies into one globally ordered, windowed result.

    Each shard already returns its own documents in order, so a sorted merge
    is a k-way heap merge; without a sort we simply concatenate.
    """
    if sort:
        merged = list(heapq.merge(*per_shard, key=sort_comparator(sort)))
    else:
        merged = [doc for shard_docs in per_shard for doc in shard_docs]
    if skip:
        merged = merged[skip:]
    if limit is not None:
        merged = merged[:limit]
    return merged
