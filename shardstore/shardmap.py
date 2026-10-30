"""Persistent shard map: the cluster's single source of truth for routing.

The map is a small SQLite database (the analogue of a MongoDB config server).
It records the registered shards and the chunk table -- a partition of the
32-bit hash space into half-open ``[lo, hi)`` ranges, each owned by one shard.

Every mutation bumps a monotonic ``version``.  Readers (the query router) hold
an immutable :class:`ShardMapSnapshot` and only rebuild it when the version
moves, so routing a request costs one integer bisect.
"""

from __future__ import annotations

import sqlite3
import time
from bisect import bisect_right
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from .hashing import HASH_SPACE, even_ranges, hash_key

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shards (
    shard_id TEXT PRIMARY KEY,
    host     TEXT    NOT NULL,
    port     INTEGER NOT NULL,
    state    TEXT    NOT NULL DEFAULT 'online'
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id INTEGER PRIMARY KEY,
    lo       INTEGER NOT NULL,
    hi       INTEGER NOT NULL,
    owner    TEXT    NOT NULL REFERENCES shards(shard_id),
    state    TEXT    NOT NULL DEFAULT 'stable'
);

CREATE INDEX IF NOT EXISTS chunks_by_lo ON chunks(lo);
"""


class ShardMapError(RuntimeError):
    """Raised when the shard map is missing, empty or inconsistent."""


@dataclass(frozen=True, slots=True)
class ShardInfo:
    shard_id: str
    host: str
    port: int
    state: str = "online"

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def as_dict(self) -> dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "host": self.host,
            "port": self.port,
            "state": self.state,
            "base_url": self.base_url,
        }


@dataclass(frozen=True, slots=True)
class Chunk:
    """A half-open range ``[lo, hi)`` of the hash space owned by one shard."""

    chunk_id: int
    lo: int
    hi: int
    owner: str
    state: str = "stable"

    def contains(self, key_hash: int) -> bool:
        return self.lo <= key_hash < self.hi

    def as_dict(self) -> dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "lo": self.lo,
            "hi": self.hi,
            "owner": self.owner,
            "state": self.state,
        }


class ShardMapSnapshot:
    """Immutable, bisect-indexed view of one version of the shard map."""

    __slots__ = ("version", "shard_key", "shards", "chunks", "_lows", "_shards_by_id")

    def __init__(
        self,
        version: int,
        shard_key: str,
        shards: Iterable[ShardInfo],
        chunks: Iterable[Chunk],
    ) -> None:
        self.version = version
        self.shard_key = shard_key
        self.shards: tuple[ShardInfo, ...] = tuple(shards)
        self.chunks: tuple[Chunk, ...] = tuple(sorted(chunks, key=lambda c: c.lo))
        self._lows = [c.lo for c in self.chunks]
        self._shards_by_id = {s.shard_id: s for s in self.shards}
        self._validate()

    def _validate(self) -> None:
        if not self.chunks:
            raise ShardMapError("shard map has no chunks; run scripts/init_shardmap.py")
        expected = 0
        for chunk in self.chunks:
            if chunk.lo != expected:
                raise ShardMapError(
                    f"chunk {chunk.chunk_id} starts at {chunk.lo}, expected {expected}"
                )
            if chunk.hi <= chunk.lo:
                raise ShardMapError(f"chunk {chunk.chunk_id} is empty or inverted")
            if chunk.owner not in self._shards_by_id:
                raise ShardMapError(
                    f"chunk {chunk.chunk_id} owned by unknown shard {chunk.owner!r}"
                )
            expected = chunk.hi
        if expected != HASH_SPACE:
            raise ShardMapError(f"chunks cover {expected} of {HASH_SPACE} hash slots")

    # -- lookup ---------------------------------------------------------

    def chunk_for_hash(self, key_hash: int) -> Chunk:
        if not 0 <= key_hash < HASH_SPACE:
            raise ShardMapError(f"hash {key_hash} outside the key space")
        return self.chunks[bisect_right(self._lows, key_hash) - 1]

    def chunk_for_key(self, key: Any) -> Chunk:
        return self.chunk_for_hash(hash_key(key))

    def shard_for_key(self, key: Any) -> ShardInfo:
        """The shard-key-to-shard lookup used for every targeted operation."""
        return self._shards_by_id[self.chunk_for_key(key).owner]

    def shard(self, shard_id: str) -> ShardInfo:
        try:
            return self._shards_by_id[shard_id]
        except KeyError:
            raise ShardMapError(f"unknown shard {shard_id!r}") from None

    def owners(self) -> tuple[ShardInfo, ...]:
        """Every shard that currently owns at least one chunk."""
        ids = {c.owner for c in self.chunks}
        return tuple(s for s in self.shards if s.shard_id in ids)

    def chunks_of(self, shard_id: str) -> tuple[Chunk, ...]:
        return tuple(c for c in self.chunks if c.owner == shard_id)

    def coverage(self) -> dict[str, float]:
        """Fraction of the hash space owned by each shard."""
        totals: dict[str, int] = {s.shard_id: 0 for s in self.shards}
        for chunk in self.chunks:
            totals[chunk.owner] += chunk.hi - chunk.lo
        return {k: v / HASH_SPACE for k, v in totals.items()}

    def as_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "shard_key": self.shard_key,
            "hash_space": HASH_SPACE,
            "shards": [s.as_dict() for s in self.shards],
            "chunks": [c.as_dict() for c in self.chunks],
            "coverage": self.coverage(),
        }


class ShardMap:
    """Read/write handle on the persistent shard map database."""

    def __init__(self, path: str | Path, read_only: bool = False) -> None:
        self.path = Path(path)
        self.read_only = read_only
        if not read_only:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        elif not self.path.exists():
            raise ShardMapError(f"shard map {self.path} does not exist")
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        if not read_only:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "ShardMap":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- metadata -------------------------------------------------------

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default

    def set_meta(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )

    @property
    def version(self) -> int:
        """Monotonic version, bumped by every structural change."""
        return int(self.get_meta("version", "0") or 0)

    def _bump_version(self) -> int:
        new = self.version + 1
        self.set_meta("version", str(new))
        self.set_meta("updated_at", f"{time.time():.6f}")
        return new

    @property
    def is_initialised(self) -> bool:
        row = self._conn.execute("SELECT COUNT(*) AS n FROM chunks").fetchone()
        return bool(row["n"])

    # -- construction ---------------------------------------------------

    def bootstrap(
        self,
        shards: Sequence[tuple[str, str, int]],
        num_chunks: int,
        shard_key: str = "_id",
        force: bool = False,
    ) -> ShardMapSnapshot:
        """Create the initial chunk table, round-robin across the shards."""
        if self.is_initialised and not force:
            raise ShardMapError(
                f"shard map {self.path} is already initialised (version {self.version})"
            )
        if num_chunks < len(shards):
            raise ShardMapError("num_chunks must be at least the number of shards")
        with self._conn:
            self._conn.execute("DELETE FROM chunks")
            self._conn.execute("DELETE FROM shards")
            self._conn.executemany(
                "INSERT INTO shards(shard_id, host, port, state) VALUES(?, ?, ?, 'online')",
                shards,
            )
            ranges = even_ranges(num_chunks)
            self._conn.executemany(
                "INSERT INTO chunks(chunk_id, lo, hi, owner, state) VALUES(?, ?, ?, ?, 'stable')",
                [
                    (i, lo, hi, shards[i % len(shards)][0])
                    for i, (lo, hi) in enumerate(ranges)
                ],
            )
            self.set_meta("shard_key", shard_key)
            self.set_meta("hash_space", str(HASH_SPACE))
            self.set_meta("created_at", f"{time.time():.6f}")
            self.set_meta("version", "0")
            self._bump_version()
        return self.snapshot()

    def set_chunk_owner(self, chunk_id: int, owner: str, state: str = "stable") -> int:
        """Reassign a chunk; returns the new map version (used by migration)."""
        with self._conn:
            cur = self._conn.execute(
                "UPDATE chunks SET owner = ?, state = ? WHERE chunk_id = ?",
                (owner, state, chunk_id),
            )
            if cur.rowcount == 0:
                raise ShardMapError(f"unknown chunk {chunk_id}")
            return self._bump_version()

    # -- reading --------------------------------------------------------

    def snapshot(self) -> ShardMapSnapshot:
        shards = [
            ShardInfo(r["shard_id"], r["host"], r["port"], r["state"])
            for r in self._conn.execute("SELECT * FROM shards ORDER BY shard_id")
        ]
        chunks = [
            Chunk(r["chunk_id"], r["lo"], r["hi"], r["owner"], r["state"])
            for r in self._conn.execute("SELECT * FROM chunks ORDER BY lo")
        ]
        return ShardMapSnapshot(self.version, self.get_meta("shard_key", "_id"), shards, chunks)


class ShardMapCache:
    """Router-side cache that rebuilds its snapshot only when the version moves.

    The version probe is a single indexed SQLite read, so routing stays cheap
    while still noticing a chunk hand-over the moment it is committed.
    """

    def __init__(self, path: str | Path) -> None:
        self._map = ShardMap(path, read_only=True)
        self._snapshot = self._map.snapshot()
        self.reloads = 0

    @property
    def snapshot(self) -> ShardMapSnapshot:
        if self._map.version != self._snapshot.version:
            self._snapshot = self._map.snapshot()
            self.reloads += 1
        return self._snapshot

    def close(self) -> None:
        self._map.close()
