"""Deterministic hashing of shard keys onto a fixed 32-bit ring.

Python's builtin ``hash`` is salted per process, so the router and every shard
would disagree about where a key lives.  We therefore hash the canonical JSON
encoding of the shard key with MD5 and keep the leading 32 bits.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

HASH_BITS = 32
HASH_SPACE = 1 << HASH_BITS
HASH_MAX = HASH_SPACE - 1


def canonical(value: Any) -> str:
    """Stable textual form of a shard-key value."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def hash_key(value: Any) -> int:
    """Map a shard-key value into ``[0, HASH_SPACE)``."""
    digest = hashlib.md5(canonical(value).encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big")


def even_ranges(num_chunks: int) -> list[tuple[int, int]]:
    """Split the hash space into ``num_chunks`` half-open ``[lo, hi)`` ranges."""
    if num_chunks < 1:
        raise ValueError("num_chunks must be >= 1")
    step, rest = divmod(HASH_SPACE, num_chunks)
    ranges: list[tuple[int, int]] = []
    lo = 0
    for i in range(num_chunks):
        hi = lo + step + (1 if i < rest else 0)
        ranges.append((lo, hi))
        lo = hi
    return ranges
