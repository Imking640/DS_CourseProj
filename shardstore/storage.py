"""Per-shard JSON document storage on SQLite.

One table holds the documents of the one logical collection.  Alongside the
JSON body we persist ``key_hash`` -- the hash of the document's shard key --
so a shard can scan, count or hand over exactly the documents of one chunk
range without re-hashing every row (used by the Phase 3 migration).
"""

from __future__ import annotations

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
from .hashing import hash_key

SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    doc_id     TEXT    PRIMARY KEY,
    key_hash   INTEGER NOT NULL,
    body       TEXT    NOT NULL,
    version    INTEGER NOT NULL DEFAULT 1,
    updated_at REAL    NOT NULL
);

CREATE INDEX IF NOT EXISTS documents_by_hash ON documents(key_hash);
"""

UPDATE_OPS = {"$set", "$unset", "$inc", "$mul", "$push", "$pull"}


class StorageError(RuntimeError):
    """Base class for storage-level failures."""


class DuplicateKeyError(StorageError):
    """Raised when inserting a document whose ``_id`` already exists."""


class DocumentNotFound(StorageError):
    """Raised when a targeted update or delete matches nothing."""


@dataclass(slots=True)
class ScanResult:
    """Documents plus how they were found -- reported back by the router."""

    documents: list[dict[str, Any]]
    examined: int
    indexed: bool


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
        self._conn.execute(
            "INSERT INTO documents(doc_id, key_hash, body, version, updated_at) "
            "VALUES(?, ?, ?, ?, ?) "
            "ON CONFLICT(doc_id) DO UPDATE SET "
            "  body = excluded.body, version = excluded.version, "
            "  updated_at = excluded.updated_at",
            (doc_id, hash_key(self.shard_key_of(doc)), json.dumps(doc), version, time.time()),
        )

    # -- writes ---------------------------------------------------------

    def insert(self, doc: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(doc, dict):
            raise FilterError("document must be a JSON object")
        doc_id = str(self.shard_key_of(doc))
        with self._lock, self._conn:
            exists = self._conn.execute(
                "SELECT 1 FROM documents WHERE doc_id = ?", (doc_id,)
            ).fetchone()
            if exists:
                raise DuplicateKeyError(f"document {doc_id!r} already exists")
            self._write(doc, version=1)
        return doc

    def insert_many(self, docs: Iterable[dict[str, Any]], overwrite: bool = False) -> int:
        docs = list(docs)
        with self._lock, self._conn:
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
            cur = self._conn.execute("DELETE FROM documents WHERE doc_id = ?", (doc_id,))
            return cur.rowcount > 0

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
    ) -> tuple[list[sqlite3.Row], bool]:
        """Rows that could match, plus whether an index was used.

        A filter that pins the shard key becomes a primary-key lookup, so a
        directly routed query costs an index seek rather than a full scan of
        the shard.  Anything else falls back to scanning (optionally limited
        to one chunk's hash range).
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
    ) -> ScanResult:
        """Filter-query this shard; ``limit`` is applied after sorting."""
        validate_filter(filt)
        order = normalise_sort(sort)
        rows, indexed = self._candidates(filt, hash_range)
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
    ) -> int:
        validate_filter(filt)
        if not filt and hash_range is None:
            with self._lock:
                return self._conn.execute("SELECT COUNT(*) AS n FROM documents").fetchone()["n"]
        rows, _ = self._candidates(filt, hash_range)
        return sum(1 for r in rows if matches(json.loads(r["body"]), filt))

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
