"""A shard process. Primaries synchronously replicate acknowledged writes."""
from __future__ import annotations

import os
import sqlite3
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

import httpx
from fastapi import FastAPI, Header, HTTPException, Response

from .api_models import (BulkInsertRequest, CountRequest, InsertRequest,
                         QueryRequest, ShardQueryResponse, UpdateRequest)
from .filters import FilterError
from .storage import DocumentNotFound, DocumentStore, DuplicateKeyError


def create_app(shard_id: str, db_path: str, shard_key: str = "_id",
               role: str = "primary", partner_url: str | None = None,
               shard_map_path: str | None = None, initial_epoch: int = 1) -> FastAPI:
    store = DocumentStore(db_path, shard_key=shard_key)
    started = time.time()
    state_db = sqlite3.connect(str(db_path) + ".node-state.db", check_same_thread=False)
    state_db.execute("CREATE TABLE IF NOT EXISTS node_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    state_db.execute("INSERT OR IGNORE INTO node_state VALUES('role', ?)", (role,))
    state_db.execute("INSERT OR IGNORE INTO node_state VALUES('epoch', ?)", (str(initial_epoch),))
    state_db.commit()
    # A follower promoted in an earlier run must not replicate writes back
    # to its former primary when the process is restarted.
    if role == "follower" and state_db.execute(
        "SELECT value FROM node_state WHERE key='role'"
    ).fetchone()[0] == "primary":
        partner_url = None

    def state(key: str) -> str:
        row = state_db.execute("SELECT value FROM node_state WHERE key=?", (key,)).fetchone()
        return row[0]

    def set_state(key: str, value: str) -> None:
        with state_db:
            state_db.execute("INSERT INTO node_state(key,value) VALUES(?,?) "
                             "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                             (key, str(value)))

    def validate_epoch(epoch: int, require_primary: bool = True) -> None:
        local_epoch = int(state("epoch"))
        if epoch != local_epoch:
            raise HTTPException(status_code=409,
                                detail=f"stale ownership epoch: request={epoch}, node={local_epoch}")
        if require_primary and state("role") != "primary":
            raise HTTPException(status_code=409, detail="writes are accepted only by the primary")
        # The shared persistent map is the fencing authority in this local cluster.
        if shard_map_path and Path(shard_map_path).exists():
            conn = sqlite3.connect(shard_map_path, timeout=2)
            try:
                row = conn.execute(
                    "SELECT active_id, epoch FROM replicas WHERE primary_id=? OR follower_id=?",
                    (shard_id, shard_id)).fetchone()
                if row and (row[0] != shard_id or int(row[1]) != epoch):
                    raise HTTPException(status_code=409,
                                        detail="stale primary rejected by shard-map ownership epoch")
            finally:
                conn.close()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        yield
        store.close()
        state_db.close()

    app = FastAPI(title=f"shard {shard_id}", version="0.2.0", lifespan=lifespan)
    app.state.shard_id = shard_id
    app.state.store = store

    @app.exception_handler(FilterError)
    def _bad_request(_request: Any, exc: FilterError) -> Response:
        raise HTTPException(status_code=400, detail=str(exc))

    async def replicate(operation: str, payload: dict[str, Any], epoch: int) -> None:
        if state("role") != "primary" or not partner_url:
            return
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.post(
                    f"{partner_url}/internal/replicate",
                    json={"operation": operation, "payload": payload, "epoch": epoch})
                response.raise_for_status()
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=503,
                                detail=f"write applied locally but follower did not acknowledge: {exc}") from exc

    def apply_replication(operation: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            if operation == "insert":
                doc = store.insert(payload["document"])
                return {"inserted": True, "document": doc}
            if operation == "bulk":
                n = store.insert_many(payload["documents"], overwrite=payload.get("overwrite", False))
                return {"inserted": n}
            if operation == "update":
                doc, created = store.update(payload["doc_id"], payload["update"],
                                            upsert=payload.get("upsert", False))
                return {"matched": not created, "upserted": created, "document": doc}
            if operation == "delete":
                return {"deleted": int(store.delete(payload["doc_id"]))}
            raise HTTPException(status_code=400, detail=f"unknown replication operation {operation}")
        except DuplicateKeyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except DocumentNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/health")
    def health():
        return {"shard_id": shard_id, "status": "online", "role": state("role"),
                "epoch": int(state("epoch")), "uptime_s": round(time.time()-started, 3),
                "documents": store.count()}

    @app.get("/stats")
    def stats():
        return {"shard_id": shard_id, "role": state("role"),
                "epoch": int(state("epoch")), **store.stats()}

    @app.post("/internal/replicate")
    def receive_replication(req: dict[str, Any]):
        if state("role") != "follower":
            raise HTTPException(status_code=409, detail="replication target is not a follower")
        if int(req.get("epoch", -1)) != int(state("epoch")):
            raise HTTPException(status_code=409, detail="replication rejected for stale epoch")
        result = apply_replication(req["operation"], req["payload"])
        return {"acknowledged": True, "shard_id": shard_id, **result}

    @app.post("/internal/replace-all")
    def replace_all(req: dict[str, Any]):
        if state("role") != "follower":
            raise HTTPException(status_code=409, detail="only a follower can be synchronised")
        if int(req.get("epoch", -1)) != int(state("epoch")):
            raise HTTPException(status_code=409, detail="sync rejected for stale epoch")
        n = store.replace_all(req.get("documents", []))
        return {"acknowledged": True, "documents": n}

    @app.post("/internal/promote")
    def promote(req: dict[str, Any]):
        nonlocal partner_url
        new_epoch = int(req["epoch"])
        current = int(state("epoch"))
        if new_epoch != current + 1:
            raise HTTPException(status_code=409,
                                detail=f"promotion epoch must be {current + 1}")
        set_state("epoch", str(new_epoch))
        set_state("role", "primary")
        partner_url = None
        return {"shard_id": shard_id, "role": "primary", "epoch": new_epoch}

    @app.post("/internal/sync-follower")
    async def sync_follower():
        if state("role") != "primary" or not partner_url:
            raise HTTPException(status_code=409, detail="this node has no active follower")
        docs = store.query(None).documents
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{partner_url}/internal/replace-all",
                    json={"documents": docs, "epoch": int(state("epoch"))})
                response.raise_for_status()
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=503, detail=f"follower sync failed: {exc}") from exc
        return {"synchronised": len(docs), "follower": partner_url}

    @app.post("/documents", status_code=201)
    async def insert(req: InsertRequest, x_ownership_epoch: int = Header(default=1)):
        validate_epoch(x_ownership_epoch)
        try:
            doc = store.insert(req.document)
        except DuplicateKeyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        await replicate("insert", {"document": req.document}, x_ownership_epoch)
        return {"shard_id": shard_id, "inserted": True, "document": doc,
                "epoch": x_ownership_epoch}

    @app.post("/documents/bulk", status_code=201)
    async def insert_many(req: BulkInsertRequest, x_ownership_epoch: int = Header(default=1)):
        validate_epoch(x_ownership_epoch)
        try:
            n = store.insert_many(req.documents, overwrite=req.overwrite)
        except DuplicateKeyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        await replicate("bulk", {"documents": req.documents, "overwrite": req.overwrite}, x_ownership_epoch)
        return {"shard_id": shard_id, "inserted": n, "epoch": x_ownership_epoch}

    @app.put("/documents/{doc_id}")
    async def update(doc_id: str, req: UpdateRequest,
                     x_ownership_epoch: int = Header(default=1)):
        validate_epoch(x_ownership_epoch)
        try:
            doc, created = store.update(doc_id, req.update, upsert=req.upsert)
        except DocumentNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        await replicate("update", {"doc_id": doc_id, **req.model_dump()}, x_ownership_epoch)
        return {"shard_id": shard_id, "matched": not created, "upserted": created,
                "document": doc, "epoch": x_ownership_epoch}

    @app.delete("/documents/{doc_id}")
    async def delete(doc_id: str, x_ownership_epoch: int = Header(default=1)):
        validate_epoch(x_ownership_epoch)
        if not store.delete(doc_id):
            raise HTTPException(status_code=404, detail=f"no document with _id {doc_id!r}")
        await replicate("delete", {"doc_id": doc_id}, x_ownership_epoch)
        return {"shard_id": shard_id, "deleted": 1, "epoch": x_ownership_epoch}

    @app.get("/documents/{doc_id}")
    def get(doc_id: str):
        doc = store.get(doc_id)
        if doc is None:
            raise HTTPException(status_code=404, detail=f"no document with _id {doc_id!r}")
        return {"shard_id": shard_id, "document": doc, "role": state("role"),
                "epoch": int(state("epoch"))}

    @app.post("/query", response_model=ShardQueryResponse)
    def query(req: QueryRequest):
        t0 = time.perf_counter()
        try:
            result = store.query(req.filter, sort=req.sort, limit=req.limit,
                                 skip=req.skip, fields=req.fields,
                                 hash_range=req.hash_range)
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return ShardQueryResponse(shard_id=shard_id, count=len(result.documents),
                                  documents=result.documents, examined=result.examined,
                                  indexed=result.indexed,
                                  took_ms=(time.perf_counter()-t0)*1000)

    @app.post("/count")
    def count(req: CountRequest):
        try:
            n = store.count(req.filter, hash_range=req.hash_range)
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"shard_id": shard_id, "count": n}

    return app


def app_from_env() -> FastAPI:
    required = ("SHARD_ID", "SHARD_DB")
    missing = [key for key in required if key not in os.environ]
    if missing:
        raise RuntimeError(f"missing environment variables: {', '.join(missing)}")
    return create_app(
        os.environ["SHARD_ID"], os.environ["SHARD_DB"],
        os.environ.get("SHARD_KEY", "_id"),
        os.environ.get("SHARD_ROLE", "primary"),
        os.environ.get("PARTNER_URL") or None,
        os.environ.get("SHARD_MAP_PATH") or None,
        int(os.environ.get("SHARD_EPOCH", "1")))
