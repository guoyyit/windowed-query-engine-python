"""HTTP API (FastAPI).

Endpoints
---------
* ``POST /partitions/{key}/rows``  -- atomically append a batch of rows
* ``POST /queries``                -- execute a query on one snapshot
* ``GET  /queries/{id}``           -- fetch committed result / status
* ``POST /queries/{id}/cancel``    -- cooperatively cancel a running query
* ``GET  /health``                 -- liveness

Synchronous ``def`` endpoints run in the FastAPI worker thread pool, so a
long query executing in one request can be cancelled by another concurrent
request.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .config import Config
from .engine import Engine
from .models import (
    FrameType,
    FuncKind,
    FuncSpec,
    NullOrder,
    QueryResult,
    QuerySpec,
    WindowSpec,
)


class WriteIn(BaseModel):
    rows: list[list[Any]]


class WindowIn(BaseModel):
    frame: FrameType
    n: int = 0


class FuncIn(BaseModel):
    kind: FuncKind
    alias: str
    source: str = "value"
    offset: int = 1
    default: Any = None
    window: WindowIn | None = None


class QueryIn(BaseModel):
    partition_key: str = Field(min_length=1)
    functions: list[FuncIn]
    null_order: NullOrder = NullOrder.NULLS_LAST
    timeout_s: float | None = None


def _rows_to_json(rows: list[Any]) -> list[dict[str, Any]]:
    return [
        {
            "seq": r.seq,
            "sort_key": r.sort_key,
            "value": r.value,
            "outputs": r.outputs,
        }
        for r in rows
    ]


def _status_to_json(
    result: QueryResult, count: int | None = None
) -> dict[str, Any]:
    return {
        "query_id": result.query_id,
        "status": result.status.value,
        "error": result.error,
        "row_count": count,
        "rows": [],
    }


def create_app(config: Config | None = None) -> FastAPI:
    """Build the FastAPI application bound to an :class:`Engine`.

    The engine (and its database file) are created lazily in the ASGI
    lifespan, so merely importing this module never touches the filesystem.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.engine = Engine(config or Config.from_env())
        try:
            yield
        finally:
            app.state.engine.close()

    app = FastAPI(
        title="Windowed Query Engine",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/partitions/{partition_key}/rows")
    def write_rows(partition_key: str, body: WriteIn) -> dict[str, Any]:
        engine: Engine = app.state.engine
        try:
            parsed = [(pair[0], pair[1]) for pair in body.rows]
        except IndexError:
            raise HTTPException(status_code=400, detail="each row must be [sort_key, value]")
        try:
            written = engine.write_batch(partition_key, parsed)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"partition_key": partition_key, "written": written}

    @app.post("/queries", status_code=201)
    def submit_query(body: QueryIn) -> dict[str, Any]:
        engine: Engine = app.state.engine
        try:
            funcs = tuple(
                FuncSpec(
                    kind=f.kind,
                    alias=f.alias,
                    source=f.source,
                    offset=f.offset,
                    default=f.default,
                    window=(
                        None
                        if f.window is None
                        else WindowSpec(frame=f.window.frame, n=f.window.n)
                    ),
                )
                for f in body.functions
            )
            spec = QuerySpec(
                partition_key=body.partition_key,
                functions=funcs,
                null_order=body.null_order,
            )
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        try:
            result = engine.submit_query(spec, timeout_s=body.timeout_s)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        payload = _status_to_json(result, engine.count_results(result.query_id))
        if result.status.value == "committed":
            payload["rows"] = _rows_to_json(
                engine.fetch_result_page(result.query_id, 0, 1000) or []
            )
        return payload

    @app.get("/queries/{query_id}")
    def get_query(query_id: str, offset: int = 0, limit: int = 1000) -> dict[str, Any]:
        engine: Engine = app.state.engine
        result = engine.get_result(query_id)
        if result is None:
            raise HTTPException(status_code=404, detail="unknown query_id")
        if limit <= 0 or limit > 10_000 or offset < 0:
            raise HTTPException(status_code=400, detail="invalid pagination")
        payload = _status_to_json(result, engine.count_results(query_id))
        if result.status.value == "committed":
            rows = engine.fetch_result_page(query_id, offset, limit)
            payload["rows"] = _rows_to_json(rows or [])
            payload["offset"] = offset
            payload["limit"] = limit
        return payload

    @app.post("/queries/{query_id}/cancel")
    def cancel_query(query_id: str) -> dict[str, Any]:
        engine: Engine = app.state.engine
        signalled = engine.cancel_query(query_id)
        if not signalled:
            raise HTTPException(status_code=404, detail="query not running")
        return {"query_id": query_id, "cancelling": True}

    return app


app = create_app()
