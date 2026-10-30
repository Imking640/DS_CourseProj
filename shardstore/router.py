"""The query router: the cluster's single entry point.

The router owns no data.  It reads the persistent shard map, turns each
request into a :class:`~shardstore.routing.RoutePlan`, forwards it to the
shards concerned and merges what comes back.  Every response carries a
``routing`` block saying whether the request was answered by one shard or by
a scatter-gather across all of them.

    SHARD_MAP=data/shardmap.db \
        uvicorn shardstore.router:app_from_env --factory --port 9000
"""

from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import httpx
from fastapi import FastAPI, HTTPException

from .api_models import (
    BulkInsertRequest,
    CountRequest,
    InsertRequest,
    QueryRequest,
    RouterQueryResponse,
    RoutingInfo,
    UpdateRequest,
)
from .filters import FilterError, normalise_sort, validate_filter
from .shardmap import ShardInfo, ShardMapCache
from .routing import RoutePlan, merge_results, plan_for_filter, plan_for_key, shard_limit

DEFAULT_TIMEOUT = 10.0


class ShardUnavailable(HTTPException):
    """A shard could not be reached -- Phase 2 turns this into a failover."""

    def __init__(self, shard_id: str, detail: str) -> None:
        super().__init__(status_code=503, detail=f"shard {shard_id} unavailable: {detail}")


def create_app(shard_map_path: str, timeout: float = DEFAULT_TIMEOUT) -> FastAPI:
    cache = ShardMapCache(shard_map_path)
    started = time.time()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        limits = httpx.Limits(max_connections=200, max_keepalive_connections=100)
        app.state.client = httpx.AsyncClient(timeout=timeout, limits=limits)
        yield
        await app.state.client.aclose()
        cache.close()

    app = FastAPI(title="shard router", version="0.1.0", lifespan=lifespan)
    app.state.cache = cache

    # -- shard calls ----------------------------------------------------

    async def call(
        shard: ShardInfo,
        method: str,
        path: str,
        json_body: Any | None = None,
    ) -> tuple[Any, float]:
        """One HTTP call to a shard; returns ``(payload, elapsed_ms)``."""
        client: httpx.AsyncClient = app.state.client
        t0 = time.perf_counter()
        try:
            response = await client.request(method, f"{shard.base_url}{path}", json=json_body)
        except httpx.HTTPError as exc:
            raise ShardUnavailable(shard.shard_id, str(exc)) from exc
        elapsed = (time.perf_counter() - t0) * 1000
        if response.status_code >= 400:
            detail = response.json().get("detail", response.text)
            raise HTTPException(status_code=response.status_code, detail=detail)
        return response.json(), elapsed

    async def fan_out(
        plan: RoutePlan,
        path: str,
        json_body: Any | None = None,
        method: str = "POST",
    ) -> tuple[list[Any], dict[str, float]]:
        """Issue the same request to every shard in the plan, concurrently."""
        results = await asyncio.gather(
            *(call(shard, method, path, json_body) for shard in plan.targets)
        )
        payloads = [payload for payload, _ in results]
        timings = {
            shard.shard_id: round(elapsed, 3)
            for shard, (_, elapsed) in zip(plan.targets, results)
        }
        return payloads, timings

    def snapshot():
        return cache.snapshot

    # -- introspection --------------------------------------------------

    @app.get("/shardmap")
    def shard_map() -> dict[str, Any]:
        return snapshot().as_dict()

    @app.get("/health")
    async def health() -> dict[str, Any]:
        snap = snapshot()

        async def probe(shard: ShardInfo) -> dict[str, Any]:
            try:
                payload, elapsed = await call(shard, "GET", "/health")
                return {**payload, "rtt_ms": round(elapsed, 3)}
            except HTTPException as exc:
                return {"shard_id": shard.shard_id, "status": "unreachable",
                        "detail": exc.detail}

        shards = await asyncio.gather(*(probe(s) for s in snap.shards))
        healthy = sum(1 for s in shards if s.get("status") == "online")
        return {
            "status": "online" if healthy == len(shards) else "degraded",
            "uptime_s": round(time.time() - started, 3),
            "map_version": snap.version,
            "map_reloads": cache.reloads,
            "shards_online": f"{healthy}/{len(shards)}",
            "shards": shards,
        }

    @app.get("/stats")
    async def stats() -> dict[str, Any]:
        snap = snapshot()
        results = await asyncio.gather(*(call(s, "GET", "/stats") for s in snap.shards))
        per_shard = [payload for payload, _ in results]
        return {
            "map_version": snap.version,
            "documents": sum(s["documents"] for s in per_shard),
            "coverage": snap.coverage(),
            "shards": per_shard,
        }

    @app.post("/explain")
    def explain(req: QueryRequest) -> dict[str, Any]:
        """Return the route plan for a filter without executing it."""
        try:
            validate_filter(req.filter)
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        plan = plan_for_filter(snapshot(), req.filter)
        return {"routing": plan.as_info(),
                "per_shard_limit": shard_limit(req.limit, req.skip, plan.fanout)}

    # -- writes ---------------------------------------------------------

    @app.post("/documents", status_code=201)
    async def insert(req: InsertRequest) -> dict[str, Any]:
        snap = snapshot()
        if snap.shard_key not in req.document:
            raise HTTPException(
                status_code=400,
                detail=f"document is missing the shard key {snap.shard_key!r}",
            )
        plan = plan_for_key(snap, req.document[snap.shard_key])
        payload, elapsed = await call(plan.targets[0], "POST", "/documents",
                                      {"document": req.document})
        return {"routing": plan.as_info(), "inserted": True,
                "document": payload["document"], "took_ms": round(elapsed, 3)}

    @app.post("/documents/bulk", status_code=201)
    async def insert_many(req: BulkInsertRequest) -> dict[str, Any]:
        """Group documents by owning shard, then one bulk call per shard."""
        snap = snapshot()
        batches: dict[str, tuple[ShardInfo, list[dict[str, Any]]]] = {}
        for doc in req.documents:
            if snap.shard_key not in doc:
                raise HTTPException(
                    status_code=400,
                    detail=f"document is missing the shard key {snap.shard_key!r}",
                )
            shard = snap.shard_for_key(doc[snap.shard_key])
            batches.setdefault(shard.shard_id, (shard, []))[1].append(doc)

        t0 = time.perf_counter()
        results = await asyncio.gather(
            *(
                call(shard, "POST", "/documents/bulk",
                     {"documents": docs, "overwrite": req.overwrite})
                for shard, docs in batches.values()
            )
        )
        return {
            "routing": {"mode": "grouped-write", "shards": list(batches),
                        "map_version": snap.version, "fanout": len(batches)},
            "inserted": sum(payload["inserted"] for payload, _ in results),
            "per_shard": {sid: len(docs) for sid, (_, docs) in batches.items()},
            "took_ms": round((time.perf_counter() - t0) * 1000, 3),
        }

    @app.put("/documents/{doc_id}")
    async def update(doc_id: str, req: UpdateRequest) -> dict[str, Any]:
        plan = plan_for_key(snapshot(), doc_id)
        payload, elapsed = await call(
            plan.targets[0], "PUT", f"/documents/{doc_id}", req.model_dump()
        )
        return {"routing": plan.as_info(), "matched": payload["matched"],
                "upserted": payload["upserted"], "document": payload["document"],
                "took_ms": round(elapsed, 3)}

    @app.delete("/documents/{doc_id}")
    async def delete(doc_id: str) -> dict[str, Any]:
        plan = plan_for_key(snapshot(), doc_id)
        payload, elapsed = await call(plan.targets[0], "DELETE", f"/documents/{doc_id}")
        return {"routing": plan.as_info(), "deleted": payload["deleted"],
                "took_ms": round(elapsed, 3)}

    # -- reads ----------------------------------------------------------

    @app.get("/documents/{doc_id}")
    async def get(doc_id: str) -> dict[str, Any]:
        """Key lookup: hashed shard key -> chunk -> owning shard, one hop."""
        plan = plan_for_key(snapshot(), doc_id)
        payload, elapsed = await call(plan.targets[0], "GET", f"/documents/{doc_id}")
        return {"routing": plan.as_info(), "document": payload["document"],
                "took_ms": round(elapsed, 3)}

    @app.post("/query", response_model=RouterQueryResponse)
    async def query(req: QueryRequest) -> RouterQueryResponse:
        try:
            validate_filter(req.filter)
            order = normalise_sort(req.sort)
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        t0 = time.perf_counter()
        plan = plan_for_filter(snapshot(), req.filter)
        per_shard_req = req.model_dump()
        per_shard_req["limit"] = shard_limit(req.limit, req.skip, plan.fanout)
        per_shard_req["skip"] = 0 if plan.fanout > 1 else req.skip

        payloads, timings = await fan_out(plan, "/query", per_shard_req)
        documents = merge_results(
            [payload["documents"] for payload in payloads],
            order,
            limit=req.limit,
            skip=req.skip if plan.fanout > 1 else 0,
        )
        return RouterQueryResponse(
            routing=RoutingInfo(**plan.as_info()),
            count=len(documents),
            documents=documents,
            took_ms=round((time.perf_counter() - t0) * 1000, 3),
            per_shard_ms=timings,
        )

    @app.post("/count")
    async def count(req: CountRequest) -> dict[str, Any]:
        try:
            validate_filter(req.filter)
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        t0 = time.perf_counter()
        plan = plan_for_filter(snapshot(), req.filter)
        payloads, timings = await fan_out(plan, "/count", req.model_dump())
        return {
            "routing": plan.as_info(),
            "count": sum(payload["count"] for payload in payloads),
            "per_shard": {payload["shard_id"]: payload["count"] for payload in payloads},
            "per_shard_ms": timings,
            "took_ms": round((time.perf_counter() - t0) * 1000, 3),
        }

    return app


def app_from_env() -> FastAPI:
    shard_map_path = os.environ.get("SHARD_MAP", "data/shardmap.db")
    return create_app(shard_map_path, timeout=float(os.environ.get("ROUTER_TIMEOUT", "10")))
