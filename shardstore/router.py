"""Query router with replica-aware routing and automatic follower promotion."""
from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import httpx
from fastapi import FastAPI, HTTPException

from .api_models import (BulkInsertRequest, CountRequest, InsertRequest,
                         QueryRequest, RouterQueryResponse, RoutingInfo,
                         UpdateRequest)
from .filters import FilterError, normalise_sort, validate_filter
from .shardmap import ShardInfo, ShardMap, ShardMapCache, ShardMapError
from .routing import RoutePlan, merge_results, plan_for_filter, plan_for_key, shard_limit

DEFAULT_TIMEOUT = 10.0


class ShardUnavailable(HTTPException):
    def __init__(self, shard_id: str, detail: str):
        super().__init__(status_code=503, detail=f"shard {shard_id} unavailable: {detail}")


def create_app(shard_map_path: str, timeout: float = DEFAULT_TIMEOUT) -> FastAPI:
    cache = ShardMapCache(shard_map_path)
    writer = ShardMap(shard_map_path)
    started = time.time()
    failover_lock = asyncio.Lock()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        limits = httpx.Limits(max_connections=200, max_keepalive_connections=100)
        app.state.client = httpx.AsyncClient(timeout=timeout, limits=limits)
        yield
        await app.state.client.aclose()
        cache.close()
        writer.close()

    app = FastAPI(title="shard router", version="0.2.0", lifespan=lifespan)
    app.state.cache = cache
    app.state.last_recovery_ms = None
    app.state.failover_count = 0

    def snapshot():
        return cache.snapshot

    async def promote_if_needed(primary_id: str) -> bool:
        """Promote the registered follower once; shard-map update is atomic."""
        recovery_started = time.perf_counter()
        async with failover_lock:
            snap = snapshot()
            replica = snap.replicas.get(primary_id)
            if not replica or replica["active_id"] != primary_id:
                return bool(replica and replica["active_id"] == replica["follower_id"])
            follower = snap.shard(replica["follower_id"])
            new_epoch = int(replica["epoch"]) + 1
            client: httpx.AsyncClient = app.state.client
            try:
                health = await client.get(f"{follower.base_url}/health")
                health.raise_for_status()
                if health.json().get("role") != "follower":
                    return False
                response = await client.post(
                    f"{follower.base_url}/internal/promote", json={"epoch": new_epoch})
                response.raise_for_status()
                writer.promote_follower(primary_id)
                app.state.last_recovery_ms = round(
                    (time.perf_counter() - recovery_started) * 1000, 3)
                app.state.failover_count += 1
                return True
            except (httpx.HTTPError, ShardMapError):
                return False

    async def call(shard: ShardInfo, method: str, path: str,
                   json_body: Any | None = None) -> tuple[Any, float]:
        """Call the currently active replica, promoting on primary loss."""
        client: httpx.AsyncClient = app.state.client
        original_id = shard.shard_id

        async def resolve(preference: str):
            snap = snapshot()
            replica = snap.replica_for(original_id)
            if not replica:
                return snap.shard(original_id), 1, None, original_id
            active_id = replica["active_id"]
            active = snap.shard(active_id)
            if preference == "follower":
                other_id = (replica["follower_id"] if active_id == replica["primary_id"]
                            else replica["primary_id"])
                candidate = snap.shard(other_id)
                try:
                    probe = await client.get(f"{candidate.base_url}/health", timeout=0.8)
                    if probe.status_code == 200 and probe.json().get("role") == "follower":
                        return candidate, int(replica["epoch"]), replica["primary_id"], active_id
                except httpx.HTTPError:
                    pass
            return active, int(replica["epoch"]), replica["primary_id"], active_id

        preference = "primary"
        if isinstance(json_body, dict) and (
            (method == "GET" and path.startswith("/documents/"))
            or (method == "POST" and path in ("/query", "/count"))
        ):
            preference = json_body.get("read_preference", "primary")
        target, epoch, primary_id, active_id = await resolve(preference)
        t0 = time.perf_counter()
        try:
            response = await client.request(
                method, f"{target.base_url}{path}", json=json_body,
                headers={"X-Ownership-Epoch": str(epoch)})
        except httpx.HTTPError as exc:
            if preference == "follower" and target.shard_id != active_id:
                target, epoch, primary_id, active_id = await resolve("primary")
                try:
                    response = await client.request(
                        method, f"{target.base_url}{path}", json=json_body,
                        headers={"X-Ownership-Epoch": str(epoch)})
                except httpx.HTTPError as retry_exc:
                    raise ShardUnavailable(target.shard_id, str(retry_exc)) from retry_exc
            elif primary_id and target.shard_id == primary_id:
                if await promote_if_needed(primary_id):
                    target, epoch, primary_id, active_id = await resolve("primary")
                    try:
                        response = await client.request(
                            method, f"{target.base_url}{path}", json=json_body,
                            headers={"X-Ownership-Epoch": str(epoch)})
                    except httpx.HTTPError as retry_exc:
                        raise ShardUnavailable(target.shard_id, str(retry_exc)) from retry_exc
                else:
                    raise ShardUnavailable(target.shard_id, str(exc)) from exc
            else:
                raise ShardUnavailable(target.shard_id, str(exc)) from exc
        elapsed = (time.perf_counter() - t0) * 1000
        if response.status_code >= 400:
            try:
                detail = response.json().get("detail", response.text)
            except ValueError:
                detail = response.text
            raise HTTPException(status_code=response.status_code, detail=detail)
        payload = response.json()
        if isinstance(payload, dict):
            payload["_read_source"] = "follower" if target.shard_id != active_id else "primary"
        return payload, elapsed

    def actual_route_info(plan: RoutePlan, payloads: list[dict[str, Any]] | None = None,
                          read_preference: str = "primary"):
        info = plan.as_info()
        if payloads:
            info["shards"] = list(dict.fromkeys(
                payload.get("shard_id", shard.shard_id)
                for shard, payload in zip(plan.targets, payloads)
            ))
        info["map_version"] = snapshot().version
        info["fanout"] = len(info["shards"])
        info["read_preference"] = read_preference
        sources = {p.get("_read_source", "primary") for p in (payloads or [])}
        info["read_source"] = ",".join(sorted(sources)) if sources else "primary"
        return info

    async def fan_out(plan: RoutePlan, path: str, json_body: Any | None = None,
                      method: str = "POST"):
        results = await asyncio.gather(
            *(call(shard, method, path, json_body) for shard in plan.targets))
        payloads = [payload for payload, _ in results]
        timings = {payload.get("shard_id", shard.shard_id): round(elapsed, 3)
                   for shard, (payload, elapsed) in zip(plan.targets, results)}
        return payloads, timings

    @app.get("/shardmap")
    def shard_map():
        return snapshot().as_dict()

    @app.get("/health")
    async def health():
        snap = snapshot()
        async def probe(shard: ShardInfo):
            try:
                payload, elapsed = await call(shard, "GET", "/health")
                return {**payload, "rtt_ms": round(elapsed, 3)}
            except HTTPException as exc:
                return {"shard_id": shard.shard_id, "status": "unreachable",
                        "detail": exc.detail}
        active = snap.owners()
        statuses = await asyncio.gather(*(probe(s) for s in active))
        healthy = sum(s.get("status") == "online" for s in statuses)
        return {"status": "online" if healthy == len(active) else "degraded",
                "uptime_s": round(time.time()-started, 3),
                "map_version": snapshot().version, "map_reloads": cache.reloads,
                "failover_count": app.state.failover_count,
                "last_recovery_ms": app.state.last_recovery_ms,
                "shards_online": f"{healthy}/{len(active)}", "shards": statuses}

    @app.get("/stats")
    async def stats():
        snap = snapshot()
        results = await asyncio.gather(*(call(s, "GET", "/stats") for s in snap.owners()))
        per_shard = [payload for payload, _ in results]
        return {"map_version": snapshot().version,
                "documents": sum(s["documents"] for s in per_shard),
                "coverage": snapshot().coverage(), "shards": per_shard}

    @app.post("/explain")
    def explain(req: QueryRequest):
        try:
            validate_filter(req.filter)
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        plan = plan_for_filter(snapshot(), req.filter)
        return {"routing": plan.as_info(),
                "per_shard_limit": shard_limit(req.limit, req.skip, plan.fanout)}

    @app.post("/documents", status_code=201)
    async def insert(req: InsertRequest):
        snap = snapshot()
        if snap.shard_key not in req.document:
            raise HTTPException(status_code=400,
                                detail=f"document is missing shard key {snap.shard_key!r}")
        plan = plan_for_key(snap, req.document[snap.shard_key])
        payload, elapsed = await call(plan.targets[0], "POST", "/documents",
                                      {"document": req.document})
        return {"routing": actual_route_info(plan, [payload]), "inserted": True,
                "document": payload["document"], "took_ms": round(elapsed, 3)}

    @app.post("/documents/bulk", status_code=201)
    async def insert_many(req: BulkInsertRequest):
        snap = snapshot()
        batches: dict[str, tuple[ShardInfo, list[dict[str, Any]]]] = {}
        for doc in req.documents:
            if snap.shard_key not in doc:
                raise HTTPException(status_code=400,
                                    detail=f"document is missing shard key {snap.shard_key!r}")
            shard = snap.shard_for_key(doc[snap.shard_key])
            batches.setdefault(shard.shard_id, (shard, []))[1].append(doc)
        t0 = time.perf_counter()
        results = await asyncio.gather(*(
            call(shard, "POST", "/documents/bulk",
                 {"documents": docs, "overwrite": req.overwrite})
            for shard, docs in batches.values()))
        return {"routing": {"mode": "grouped-write",
                             "shards": [p["shard_id"] for p, _ in results],
                             "map_version": snapshot().version,
                             "fanout": len(results)},
                "inserted": sum(p["inserted"] for p, _ in results),
                "per_shard": {sid: len(docs) for sid, (_, docs) in batches.items()},
                "took_ms": round((time.perf_counter()-t0)*1000, 3)}

    @app.put("/documents/{doc_id}")
    async def update(doc_id: str, req: UpdateRequest):
        plan = plan_for_key(snapshot(), doc_id)
        payload, elapsed = await call(plan.targets[0], "PUT", f"/documents/{doc_id}",
                                      req.model_dump())
        return {"routing": actual_route_info(plan, [payload]), "matched": payload["matched"],
                "upserted": payload["upserted"], "document": payload["document"],
                "took_ms": round(elapsed, 3)}

    @app.delete("/documents/{doc_id}")
    async def delete(doc_id: str):
        plan = plan_for_key(snapshot(), doc_id)
        payload, elapsed = await call(plan.targets[0], "DELETE", f"/documents/{doc_id}")
        return {"routing": actual_route_info(plan, [payload]), "deleted": payload["deleted"],
                "took_ms": round(elapsed, 3)}

    @app.get("/documents/{doc_id}")
    async def get(doc_id: str, read_preference: str = "primary"):
        if read_preference not in ("primary", "follower"):
            raise HTTPException(status_code=400, detail="read_preference must be primary or follower")
        plan = plan_for_key(snapshot(), doc_id)
        # GET document reads use the same preference resolver as query reads.
        payload, elapsed = await call(plan.targets[0], "GET", f"/documents/{doc_id}",
                                      {"read_preference": read_preference})
        return {"routing": actual_route_info(plan, [payload], read_preference), "document": payload["document"],
                "took_ms": round(elapsed, 3)}

    @app.post("/query", response_model=RouterQueryResponse)
    async def query(req: QueryRequest):
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
        documents = merge_results([p["documents"] for p in payloads], order,
                                  limit=req.limit,
                                  skip=req.skip if plan.fanout > 1 else 0)
        return RouterQueryResponse(
            routing=RoutingInfo(**actual_route_info(plan, payloads, req.read_preference)), count=len(documents),
            documents=documents, examined=sum(p["examined"] for p in payloads),
            took_ms=round((time.perf_counter()-t0)*1000, 3), per_shard_ms=timings)

    @app.post("/count")
    async def count(req: CountRequest):
        try:
            validate_filter(req.filter)
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        t0 = time.perf_counter()
        plan = plan_for_filter(snapshot(), req.filter)
        payloads, timings = await fan_out(plan, "/count", req.model_dump())
        return {"routing": actual_route_info(plan, payloads, req.read_preference), "count": sum(p["count"] for p in payloads),
                "per_shard": {p["shard_id"]: p["count"] for p in payloads},
                "per_shard_ms": timings, "took_ms": round((time.perf_counter()-t0)*1000, 3)}

    return app


def app_from_env() -> FastAPI:
    path = os.environ.get("SHARD_MAP", "data/shardmap.db")
    return create_app(path, timeout=float(os.environ.get("ROUTER_TIMEOUT", "10")))
