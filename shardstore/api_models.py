"""Request/response models shared by the shard nodes and the query router.

These are the API contracts handed to Phase 2; shard endpoints and router
endpoints deliberately speak the same shapes so the router is a thin proxy.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class InsertRequest(BaseModel):
    document: dict[str, Any]


class BulkInsertRequest(BaseModel):
    documents: list[dict[str, Any]] = Field(min_length=1)
    overwrite: bool = False


class UpdateRequest(BaseModel):
    update: dict[str, Any]
    upsert: bool = False


class QueryRequest(BaseModel):
    """A filter query.  ``hash_lo``/``hash_hi`` restrict a shard-local scan."""

    filter: dict[str, Any] | None = None
    sort: Any = None
    limit: int | None = Field(default=None, ge=0, le=10_000)
    skip: int = Field(default=0, ge=0)
    fields: list[str] | None = None
    hash_lo: int | None = None
    hash_hi: int | None = None

    @property
    def hash_range(self) -> tuple[int, int] | None:
        if self.hash_lo is None or self.hash_hi is None:
            return None
        return (self.hash_lo, self.hash_hi)


class CountRequest(BaseModel):
    filter: dict[str, Any] | None = None
    hash_lo: int | None = None
    hash_hi: int | None = None

    @property
    def hash_range(self) -> tuple[int, int] | None:
        if self.hash_lo is None or self.hash_hi is None:
            return None
        return (self.hash_lo, self.hash_hi)


class ShardQueryResponse(BaseModel):
    shard_id: str
    count: int
    documents: list[dict[str, Any]]
    scanned: int
    took_ms: float


class RoutingInfo(BaseModel):
    """How the router dispatched a request -- the headline Phase 1 metric."""

    mode: str                      # "direct" | "multi-key" | "scatter-gather"
    shards: list[str]
    shard_key: Any = None
    chunk_id: int | None = None
    map_version: int = 0
    fanout: int = 1


class RouterQueryResponse(BaseModel):
    routing: RoutingInfo
    count: int
    documents: list[dict[str, Any]]
    took_ms: float
    per_shard_ms: dict[str, float] = {}
