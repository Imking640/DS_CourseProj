"""The shard node: a FastAPI process owning one SQLite document store.

A shard knows nothing about routing.  It stores whatever the router sends it
and answers key lookups and filter queries over its own documents only.
Launch one per shard:

    SHARD_ID=shard-0 SHARD_DB=data/shard-0.db \
        uvicorn shardstore.shard_node:app --port 9101
"""

from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI, HTTPException, Response

from .api_models import (
    BulkInsertRequest,
    CountRequest,
    InsertRequest,
    QueryRequest,
    ShardQueryResponse,
    UpdateRequest,
)
from .filters import FilterError
from .storage import DocumentNotFound, DocumentStore, DuplicateKeyError


def create_app(shard_id: str, db_path: str, shard_key: str = "_id") -> FastAPI:
    store = DocumentStore(db_path, shard_key=shard_key)
    started = time.time()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        yield
        store.close()

    app = FastAPI(title=f"shard {shard_id}", version="0.1.0", lifespan=lifespan)

    app.state.shard_id = shard_id
    app.state.store = store

    @app.exception_handler(FilterError)
    def _bad_request(_request: Any, exc: FilterError) -> Response:  # noqa: ANN401
        raise HTTPException(status_code=400, detail=str(exc))

    # -- introspection --------------------------------------------------

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "shard_id": shard_id,
            "status": "online",
            "role": "primary",
            "uptime_s": round(time.time() - started, 3),
            "documents": store.count(),
        }

    @app.get("/stats")
    def stats() -> dict[str, Any]:
        return {"shard_id": shard_id, **store.stats()}

    # -- writes ---------------------------------------------------------

    @app.post("/documents", status_code=201)
    def insert(req: InsertRequest) -> dict[str, Any]:
        try:
            doc = store.insert(req.document)
        except DuplicateKeyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"shard_id": shard_id, "inserted": True, "document": doc}

    @app.post("/documents/bulk", status_code=201)
    def insert_many(req: BulkInsertRequest) -> dict[str, Any]:
        try:
            n = store.insert_many(req.documents, overwrite=req.overwrite)
        except DuplicateKeyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"shard_id": shard_id, "inserted": n}

    @app.put("/documents/{doc_id}")
    def update(doc_id: str, req: UpdateRequest) -> dict[str, Any]:
        try:
            doc, created = store.update(doc_id, req.update, upsert=req.upsert)
        except DocumentNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"shard_id": shard_id, "matched": not created, "upserted": created,
                "document": doc}

    @app.delete("/documents/{doc_id}")
    def delete(doc_id: str) -> dict[str, Any]:
        if not store.delete(doc_id):
            raise HTTPException(status_code=404, detail=f"no document with _id {doc_id!r}")
        return {"shard_id": shard_id, "deleted": 1}

    # -- reads ----------------------------------------------------------

    @app.get("/documents/{doc_id}")
    def get(doc_id: str) -> dict[str, Any]:
        doc = store.get(doc_id)
        if doc is None:
            raise HTTPException(status_code=404, detail=f"no document with _id {doc_id!r}")
        return {"shard_id": shard_id, "document": doc}

    @app.post("/query", response_model=ShardQueryResponse)
    def query(req: QueryRequest) -> ShardQueryResponse:
        t0 = time.perf_counter()
        try:
            docs = store.find(
                req.filter,
                sort=req.sort,
                limit=req.limit,
                skip=req.skip,
                fields=req.fields,
                hash_range=req.hash_range,
            )
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return ShardQueryResponse(
            shard_id=shard_id,
            count=len(docs),
            documents=docs,
            scanned=store.count(hash_range=req.hash_range),
            took_ms=(time.perf_counter() - t0) * 1000,
        )

    @app.post("/count")
    def count(req: CountRequest) -> dict[str, Any]:
        try:
            n = store.count(req.filter, hash_range=req.hash_range)
        except FilterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"shard_id": shard_id, "count": n}

    return app


def app_from_env() -> FastAPI:
    try:
        shard_id = os.environ["SHARD_ID"]
        db_path = os.environ["SHARD_DB"]
    except KeyError as exc:
        raise RuntimeError(f"environment variable {exc} is required") from exc
    return create_app(shard_id, db_path, os.environ.get("SHARD_KEY", "_id"))


app = app_from_env() if os.environ.get("SHARD_ID") else None
