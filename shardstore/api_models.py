"""Request/response models shared by the shard nodes and the query router.

These are the API contracts handed to Phase 2; shard endpoints and router
endpoints deliberately speak the same shapes so the router is a thin proxy.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class InsertRequest(BaseModel):
    document: dict[str, Any]


class BulkInsertRequest(BaseModel):
    documents: list[dict[str, Any]] = Field(min_length=1)
    overwrite: bool = False


class UpdateRequest(BaseModel):
    update: dict[str, Any]
    upsert: bool = False


class OwnedRangesMixin(BaseModel):
    """Restricts a shard-local read to the ranges the router says it owns.

    The router fills this in from the shard map.  During a migration the donor
    and the recipient both physically hold the moving documents, so without it
    a scatter-gather would return each of them twice.
    """

    owned_ranges: list[tuple[int, int]] | None = None


class QueryRequest(OwnedRangesMixin):
    """A filter query.  ``hash_lo``/``hash_hi`` restrict a shard-local scan."""

    filter: dict[str, Any] | None = None
    sort: Any = None
    limit: int | None = Field(default=None, ge=0, le=10_000)
    skip: int = Field(default=0, ge=0)
    fields: list[str] | None = None
    hash_lo: int | None = None
    hash_hi: int | None = None
    read_preference: Literal["primary", "follower"] = "primary"

    @property
    def hash_range(self) -> tuple[int, int] | None:
        if self.hash_lo is None or self.hash_hi is None:
            return None
        return (self.hash_lo, self.hash_hi)


class CountRequest(OwnedRangesMixin):
    filter: dict[str, Any] | None = None
    hash_lo: int | None = None
    hash_hi: int | None = None
    read_preference: Literal["primary", "follower"] = "primary"

    @property
    def hash_range(self) -> tuple[int, int] | None:
        if self.hash_lo is None or self.hash_hi is None:
            return None
        return (self.hash_lo, self.hash_hi)


class ShardQueryResponse(BaseModel):
    shard_id: str
    count: int
    documents: list[dict[str, Any]]
    examined: int          # documents the shard had to look at
    indexed: bool          # True when the shard key turned it into a seek
    took_ms: float


class RoutingInfo(BaseModel):
    """How the router dispatched a request -- the headline Phase 1 metric."""

    mode: str                      # "direct" | "multi-key" | "scatter-gather"
    shards: list[str]
    shard_key: Any = None
    chunk_id: int | None = None
    map_version: int = 0
    fanout: int = 1
    read_preference: str = "primary"
    read_source: str = "primary"


class RouterQueryResponse(BaseModel):
    routing: RoutingInfo
    count: int
    documents: list[dict[str, Any]]
    examined: int = 0
    took_ms: float
    per_shard_ms: dict[str, float] = {}


# -- Phase 3: chunk migration ------------------------------------------


class ArmRangeRequest(BaseModel):
    """Donor: start capturing mutations in a range about to be handed over."""

    migration_id: str
    lo: int
    hi: int


class MigrationIngestRequest(BaseModel):
    """Recipient: apply a shipped batch, and replicate it to its follower."""

    migration_id: str
    documents: list[dict[str, Any]] = []
    deletes: list[str] = []


class MigrationCommitRequest(BaseModel):
    """Donor: the map now names someone else; drop the handed-over range."""

    migration_id: str
    delete_range: bool = True
