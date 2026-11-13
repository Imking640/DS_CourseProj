"""Per-shard JSON document storage on SQLite.

One table holds the documents of the one logical collection.  Alongside the
JSON body we persist ``key_hash`` -- the hash of the document's shard key --
so a shard can scan, count or hand over exactly the documents of one chunk
range without re-hashing every row.

Two Phase 3 facilities live here:

* **Owned-range filtering.**  A query may be restricted to the hash ranges the
  caller believes this shard owns.  During a migration both the donor and the
  recipient physically hold the moving documents, so without this filter a
  scatter-gather would return each of them twice.
* **The migration changelog.**  While a range is *armed*, every mutation that
  lands in it is appended to ``migration_log``, which is how writes accepted
  during the copy are carried over to the recipient.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from .filters import (
    FilterError, apply_sort, extract_shard_keys, matches, normalise_sort, project,
    validate_filter,
)
from .hashing import canonical, hash_key

SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    doc_id     TEXT    PRIMARY KEY,
    key_hash   INTEGER NOT NULL,
    body       TEXT    NOT NULL,
    version    INTEGER NOT NULL DEFAULT 1,
    updated_at REAL    NOT NULL
);

CREATE INDEX IF NOT EXISTS documents_by_hash ON documents(key_hash);

CREATE TABLE IF NOT EXISTS migration_log (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    migration_id TEXT    NOT NULL,
    doc_id       TEXT    NOT NULL,
    key_hash     INTEGER NOT NULL,
    op           TEXT    NOT NULL,   -- 'upsert' or 'delete'
    body         TEXT,               -- NULL for a delete
    logged_at    REAL    NOT NULL
);

CREATE INDEX IF NOT EXISTS migration_log_by_id ON migration_log(migration_id, seq);
"""

UPDATE_OPS = {"$set", "$unset", "$inc", "$mul", "$push", "$pull"}


class StorageError(RuntimeError):
    """Base class for storage-level failures."""


class DuplicateKeyError(StorageError):
    """Raised when inserting a document whose ``_id`` already exists."""


class DocumentNotFound(StorageError):
    """Raised when a targeted update or delete matches nothing."""


class RangeNotOwned(StorageError):
    """Raised when a write targets a range this shard must no longer accept.

    Checked while holding the store lock, so a write either completes (and is
    captured in the migration changelog) or is refused.  Checking at the HTTP
    layer instead would leave a window where a write passes the check, the
    range is frozen and drained, and the write then lands in a range that is
    about to be dropped -- an acknowledged write silently lost.
    """


@dataclass(slots=True)
class ScanResult:
    """Documents plus how they were found -- reported back by the router."""

    documents: list[dict[str, Any]]
    examined: int
    indexed: bool


@dataclass(slots=True)
class LoggedChange:
    """One mutation captured in an armed range while a migration was running."""

    seq: int
    doc_id: str
    op: str
    document: dict[str, Any] | None


Range = tuple[int, int]


def _range_clause(ranges: Sequence[Range], column: str = "key_hash") -> tuple[str, list[Any]]:
    """SQL matching ``column`` against a union of half-open ``[lo, hi)`` ranges."""
    parts = " OR ".join(f"({column} >= ? AND {column} < ?)" for _ in ranges)
    params: list[Any] = []
    for lo, hi in ranges:
        params.extend((lo, hi))
    return f"({parts})", params


def in_any_range(key_hash: int, ranges: Sequence[Range]) -> bool:
    return any(lo <= key_hash < hi for lo, hi in ranges)


# -- update operators --------------------------------------------------


def _set_path(doc: dict[str, Any], path: str, value: Any) -> None:
    parts = path.split(".")
    cursor = doc
    for part in parts[:-1]:
        nxt = cursor.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cursor[part] = nxt
        cursor = nxt
    cursor[parts[-1]] = value


def _unset_path(doc: dict[str, Any], path: str) -> None:
    parts = path.split(".")
    cursor = doc
    for part in parts[:-1]:
        cursor = cursor.get(part)
        if not isinstance(cursor, dict):
            return
    cursor.pop(parts[-1], None)


def _get_path(doc: dict[str, Any], path: str) -> Any:
    cursor: Any = doc
    for part in path.split("."):
        if not isinstance(cursor, dict) or part not in cursor:
            return None
        cursor = cursor[part]
    return cursor


def apply_update(doc: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
    """Apply an update document: operator form, or a full replacement."""
    if not isinstance(update, dict):
        raise FilterError("update must be a JSON object")
    operators = [k for k in update if k.startswith("$")]
    if not operators:
        replacement = dict(update)
        replacement["_id"] = doc["_id"]
        return replacement
    if len(operators) != len(update):
        raise FilterError("cannot mix update operators with plain fields")

    out = json.loads(json.dumps(doc))
    for op, args in update.items():
        if op not in UPDATE_OPS:
            raise FilterError(f"unsupported update operator {op!r}")
        if not isinstance(args, dict):
            raise FilterError(f"{op} expects a JSON object")
        for path, operand in args.items():
            if path == "_id":
                raise FilterError("the shard key _id is immutable")
            if op == "$set":
                _set_path(out, path, operand)
            elif op == "$unset":
                _unset_path(out, path)
            elif op in ("$inc", "$mul"):
                current = _get_path(out, path)
                if current is None:
                    current = 0 if op == "$inc" else 1
                if not isinstance(current, (int, float)) or isinstance(current, bool):
                    raise FilterError(f"{op} on non-numeric field {path!r}")
                _set_path(out, path, current + operand if op == "$inc" else current * operand)
            elif op == "$push":
                current = _get_path(out, path)
                if current is None:
                    current = []
                if not isinstance(current, list):
                    raise FilterError(f"$push on non-array field {path!r}")
                _set_path(out, path, [*current, operand])
            elif op == "$pull":
                current = _get_path(out, path)
                if isinstance(current, list):
                    _set_path(out, path, [v for v in current if v != operand])
    return out


# -- store -------------------------------------------------------------


class DocumentStore:
    """Thread-safe SQLite document store for a single shard."""

    def __init__(self, path: str | Path, shard_key: str = "_id") -> None:
        self.path = Path(path)
        self.shard_key = shard_key
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        # (migration_id, lo, hi) while a range is armed for migration capture.
        self._armed: tuple[str, int, int] | None = None
        # Ranges whose writes must be refused: frozen for cutover, or donated.
        self._blocked: list[Range] = []
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- helpers --------------------------------------------------------

    def shard_key_of(self, doc: dict[str, Any]) -> Any:
        if self.shard_key not in doc:
            raise FilterError(f"document is missing the shard key {self.shard_key!r}")
        return doc[self.shard_key]

    @staticmethod
    def _decode(row: sqlite3.Row) -> dict[str, Any]:
        return json.loads(row["body"])

    def _write(self, doc: dict[str, Any], version: int) -> None:
        doc_id = str(self.shard_key_of(doc))
        key_hash = hash_key(self.shard_key_of(doc))
        body = json.dumps(doc)
        self._conn.execute(
            "INSERT INTO documents(doc_id, key_hash, body, version, updated_at) "
            "VALUES(?, ?, ?, ?, ?) "
            "ON CONFLICT(doc_id) DO UPDATE SET "
            "  body = excluded.body, version = excluded.version, "
            "  updated_at = excluded.updated_at",
            (doc_id, key_hash, body, version, time.time()),
        )
        self._log_change(doc_id, key_hash, "upsert", body)

    def _log_change(self, doc_id: str, key_hash: int, op: str, body: str | None) -> None:
        """Record a mutation if it lands in the range armed for migration.

        Called with the store lock held and inside the caller's transaction, so
        a logged change and the write it describes commit together.
        """
        if self._armed is None:
            return
        migration_id, lo, hi = self._armed
        if not lo <= key_hash < hi:
            return
        self._conn.execute(
            "INSERT INTO migration_log(migration_id, doc_id, key_hash, op, body, logged_at) "
            "VALUES(?, ?, ?, ?, ?, ?)",
            (migration_id, doc_id, key_hash, op, body, time.time()),
        )

    def _check_writable(self, doc_id: str) -> None:
        """Refuse a write into a blocked range.  Caller holds the lock."""
        if self._blocked and in_any_range(hash_key(doc_id), self._blocked):
            raise RangeNotOwned(f"this shard no longer accepts writes for {doc_id!r}")

    def set_blocked_ranges(self, ranges: Sequence[Range]) -> None:
        """Replace the set of ranges whose writes are refused.

        Taking the store lock is what makes a cutover safe: once this returns,
        every write still in flight has already committed (and been logged),
        and every later write is rejected.
        """
        with self._lock:
            self._blocked = list(ranges)

    @property
    def blocked_ranges(self) -> list[Range]:
        return list(self._blocked)

    # -- writes ---------------------------------------------------------

    def insert(self, doc: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(doc, dict):
            raise FilterError("document must be a JSON object")
        doc_id = str(self.shard_key_of(doc))
        with self._lock, self._conn:
            self._check_writable(doc_id)
            exists = self._conn.execute(
                "SELECT 1 FROM documents WHERE doc_id = ?", (doc_id,)
            ).fetchone()
            if exists:
                raise DuplicateKeyError(f"document {doc_id!r} already exists")
            self._write(doc, version=1)
        return doc

    def replace_all(self, docs: Iterable[dict[str, Any]]) -> int:
        """Atomically replace this replica's complete document contents."""
        docs = list(docs)
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM documents")
            for doc in docs:
                self._write(doc, version=1)
        return len(docs)

    def insert_many(self, docs: Iterable[dict[str, Any]], overwrite: bool = False) -> int:
        docs = list(docs)
        with self._lock, self._conn:
            for doc in docs:
                self._check_writable(str(self.shard_key_of(doc)))
            if not overwrite:
                ids = [str(self.shard_key_of(d)) for d in docs]
                placeholders = ",".join("?" * len(ids))
                clash = self._conn.execute(
                    f"SELECT doc_id FROM documents WHERE doc_id IN ({placeholders})", ids
                ).fetchone() if ids else None
                if clash:
                    raise DuplicateKeyError(f"document {clash['doc_id']!r} already exists")
            for doc in docs:
                self._write(doc, version=1)
        return len(docs)

    def update(
        self,
        doc_id: str,
        update: dict[str, Any],
        upsert: bool = False,
    ) -> tuple[dict[str, Any], bool]:
        """Apply ``update`` to one document; returns ``(document, created)``."""
        with self._lock, self._conn:
            self._check_writable(doc_id)
            row = self._conn.execute(
                "SELECT * FROM documents WHERE doc_id = ?", (doc_id,)
            ).fetchone()
            if row is None:
                if not upsert:
                    raise DocumentNotFound(f"no document with _id {doc_id!r}")
                base = {"_id": doc_id}
                new_doc = apply_update(base, update)
                self._write(new_doc, version=1)
                return new_doc, True
            new_doc = apply_update(self._decode(row), update)
            self._write(new_doc, version=row["version"] + 1)
            return new_doc, False

    def delete(self, doc_id: str) -> bool:
        with self._lock, self._conn:
            self._check_writable(doc_id)
            row = self._conn.execute(
                "SELECT key_hash FROM documents WHERE doc_id = ?", (doc_id,)
            ).fetchone()
            if row is None:
                return False
            self._conn.execute("DELETE FROM documents WHERE doc_id = ?", (doc_id,))
            self._log_change(doc_id, row["key_hash"], "delete", None)
            return True

    def delete_range(self, lo: int, hi: int) -> int:
        """Drop every document whose shard key hashes into ``[lo, hi)``."""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "DELETE FROM documents WHERE key_hash >= ? AND key_hash < ?", (lo, hi)
            )
            return cur.rowcount

    # -- reads ----------------------------------------------------------

    def get(self, doc_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT body FROM documents WHERE doc_id = ?", (doc_id,)
            ).fetchone()
        return json.loads(row["body"]) if row else None

    def _candidates(
        self,
        filt: dict[str, Any] | None,
        hash_range: tuple[int, int] | None,
        owned_ranges: Sequence[Range] | None = None,
    ) -> tuple[list[sqlite3.Row], bool]:
        """Rows that could match, plus whether an index was used.

        A filter that pins the shard key becomes a primary-key lookup, so a
        directly routed query costs an index seek rather than a full scan of
        the shard.  Anything else falls back to scanning, optionally limited
        to one chunk's hash range and to the ranges this shard still owns.
        """
        keys = extract_shard_keys(filt, self.shard_key)
        clauses: list[str] = []
        params: list[Any] = []
        indexed = False

        if keys is not None and len(keys) > 0:
            ids = [str(k) for k in keys]
            clauses.append(f"doc_id IN ({','.join('?' * len(ids))})")
            params.extend(ids)
            indexed = True
        if hash_range is not None:
            clauses.append("key_hash >= ? AND key_hash < ?")
            params.extend(hash_range)
        if owned_ranges is not None:
            if not owned_ranges:
                return [], indexed          # owns nothing, so matches nothing
            clause, range_params = _range_clause(owned_ranges)
            clauses.append(clause)
            params.extend(range_params)

        sql = "SELECT body FROM documents"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        with self._lock:
            return self._conn.execute(sql, params).fetchall(), indexed

    def query(
        self,
        filt: dict[str, Any] | None = None,
        sort: Any = None,
        limit: int | None = None,
        skip: int = 0,
        fields: Sequence[str] | None = None,
        hash_range: tuple[int, int] | None = None,
        owned_ranges: Sequence[Range] | None = None,
    ) -> ScanResult:
        """Filter-query this shard; ``limit`` is applied after sorting."""
        validate_filter(filt)
        order = normalise_sort(sort)
        rows, indexed = self._candidates(filt, hash_range, owned_ranges)
        docs = [json.loads(r["body"]) for r in rows]
        matched = [d for d in docs if matches(d, filt)]
        matched = apply_sort(matched, order)
        if skip:
            matched = matched[skip:]
        if limit is not None:
            matched = matched[:limit]
        return ScanResult(
            documents=[project(d, fields) for d in matched],
            examined=len(rows),
            indexed=indexed,
        )

    def find(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        """``query`` without the scan statistics."""
        return self.query(*args, **kwargs).documents

    def count(
        self,
        filt: dict[str, Any] | None = None,
        hash_range: tuple[int, int] | None = None,
        owned_ranges: Sequence[Range] | None = None,
    ) -> int:
        validate_filter(filt)
        if not filt and hash_range is None and owned_ranges is None:
            with self._lock:
                return self._conn.execute("SELECT COUNT(*) AS n FROM documents").fetchone()["n"]
        rows, _ = self._candidates(filt, hash_range, owned_ranges)
        return sum(1 for r in rows if matches(json.loads(r["body"]), filt))

    # -- migration support ----------------------------------------------

    def arm_range(self, migration_id: str, lo: int, hi: int) -> int:
        """Start capturing mutations in ``[lo, hi)``; returns docs in range.

        Arming and counting happen under one lock, so a write either lands
        before the count (and is in the snapshot the recipient clones) or
        after it (and is in the changelog).  Nothing can fall between.

        Only one range may be armed at a time: a second concurrent donation
        from the same shard is refused rather than silently losing capture.
        """
        with self._lock, self._conn:
            if self._armed is not None and self._armed[0] != migration_id:
                raise StorageError(
                    f"shard is already donating range {self._armed[1:]} "
                    f"for migration {self._armed[0]!r}"
                )
            self._conn.execute("DELETE FROM migration_log WHERE migration_id = ?",
                               (migration_id,))
            self._armed = (migration_id, lo, hi)
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM documents WHERE key_hash >= ? AND key_hash < ?",
                (lo, hi),
            ).fetchone()
        return row["n"]

    def disarm_range(self, migration_id: str) -> None:
        with self._lock, self._conn:
            if self._armed and self._armed[0] == migration_id:
                self._armed = None
            self._conn.execute("DELETE FROM migration_log WHERE migration_id = ?",
                               (migration_id,))

    @property
    def armed_range(self) -> tuple[str, int, int] | None:
        return self._armed

    def changes_since(self, migration_id: str, since_seq: int,
                      limit: int = 500) -> list[LoggedChange]:
        """Mutations captured after ``since_seq``, oldest first."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT seq, doc_id, op, body FROM migration_log "
                "WHERE migration_id = ? AND seq > ? ORDER BY seq LIMIT ?",
                (migration_id, since_seq, limit),
            ).fetchall()
        return [
            LoggedChange(r["seq"], r["doc_id"], r["op"],
                         json.loads(r["body"]) if r["body"] else None)
            for r in rows
        ]

    def pending_changes(self, migration_id: str, since_seq: int) -> int:
        with self._lock:
            return self._conn.execute(
                "SELECT COUNT(*) AS n FROM migration_log WHERE migration_id = ? AND seq > ?",
                (migration_id, since_seq),
            ).fetchone()["n"]

    def apply_migration_batch(
        self,
        documents: Sequence[dict[str, Any]] = (),
        deletes: Sequence[str] = (),
    ) -> tuple[int, int]:
        """Idempotently apply a shipped batch; returns ``(upserted, deleted)``.

        Upserts overwrite by ``_id`` and deletes ignore absent documents, so
        replaying a batch after a retry cannot duplicate or lose anything.
        """
        with self._lock, self._conn:
            for doc in documents:
                self._write(doc, version=1)
            removed = 0
            for doc_id in deletes:
                cur = self._conn.execute("DELETE FROM documents WHERE doc_id = ?", (doc_id,))
                removed += cur.rowcount
        return len(documents), removed

    def range_digest(self, lo: int, hi: int) -> dict[str, Any]:
        """Cheap fingerprint of a range, used to verify a completed move."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT doc_id, body FROM documents "
                "WHERE key_hash >= ? AND key_hash < ? ORDER BY doc_id",
                (lo, hi),
            ).fetchall()
        digest = hashlib.md5()
        for row in rows:
            digest.update(row["doc_id"].encode())
            digest.update(canonical(json.loads(row["body"])).encode())
        return {"documents": len(rows), "digest": digest.hexdigest()}

    def stats(self) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n, MIN(key_hash) AS lo, MAX(key_hash) AS hi, "
                "MAX(updated_at) AS last FROM documents"
            ).fetchone()
        return {
            "documents": row["n"],
            "min_key_hash": row["lo"],
            "max_key_hash": row["hi"],
            "last_write_at": row["last"],
            "path": str(self.path),
            "size_bytes": self._disk_size(),
        }

    def _disk_size(self) -> int:
        """On-disk footprint, including the write-ahead log."""
        total = 0
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(self.path) + suffix)
            if candidate.exists():
                total += candidate.stat().st_size
        return total
