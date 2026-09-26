"""HTTP 服务层：FastAPI 应用工厂与路由。

路由：
- GET  /health                         健康检查
- POST /datasets/{dataset}/rows        原子写入一批行，?batch_id= 提供幂等键
- POST /queries                        提交查询，立即返回 query_id 与快照版本
- GET  /queries/{query_id}             查询状态
- GET  /queries/{query_id}/results     分页读取已完成查询的结果
- POST /queries/{query_id}/cancel      取消查询
"""
from __future__ import annotations

import contextlib
import json
import os
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .engine import QueryEngine, validate_spec
from .storage import Storage
from .types import (
    AggFunc,
    EngineConfig,
    FrameMode,
    FrameSpec,
    LeadLagKind,
    LeadLagSpec,
    QuerySpec,
    QueryStatus,
    RankFunc,
    RowIn,
)

_FRAME_MODES = {m.value: m for m in FrameMode}
_AGGS = {a.value: a for a in AggFunc}
_RANKS = {r.value: r for r in RankFunc}
_LL_KINDS = {k.value: k for k in LeadLagKind}


class FrameIn(BaseModel):
    mode: str = FrameMode.ALL.value
    n: int = 0


class LeadLagIn(BaseModel):
    kind: str
    offset: int = 1
    default: Optional[float] = None
    name: str = ""


class QueryIn(BaseModel):
    dataset: str
    partition_key: str
    frame: FrameIn = Field(default_factory=FrameIn)
    aggregates: list[str] = Field(default_factory=list)
    rankings: list[str] = Field(default_factory=list)
    lead_lag: list[LeadLagIn] = Field(default_factory=list)
    timeout_ms: Optional[int] = None


class RowInApi(BaseModel):
    partition_key: str
    sort_key: Any = None
    value: Optional[float] = None


class RowsIn(BaseModel):
    rows: list[RowInApi]


def parse_spec(body: QueryIn) -> QuerySpec:
    if body.frame.mode not in _FRAME_MODES:
        raise HTTPException(400, f"未知 frame.mode: {body.frame.mode!r}")
    try:
        aggs = tuple(_AGGS[a] for a in body.aggregates)
    except KeyError as e:
        raise HTTPException(400, f"未知聚合函数: {e.args[0]!r}") from None
    try:
        ranks = tuple(_RANKS[r] for r in body.rankings)
    except KeyError as e:
        raise HTTPException(400, f"未知排名函数: {e.args[0]!r}") from None
    lls = []
    for ll in body.lead_lag:
        if ll.kind not in _LL_KINDS:
            raise HTTPException(400, f"未知 lead_lag.kind: {ll.kind!r}")
        lls.append(LeadLagSpec(_LL_KINDS[ll.kind], ll.offset, ll.default, ll.name))
    spec = QuerySpec(
        dataset=body.dataset,
        partition_key=body.partition_key,
        frame=FrameSpec(_FRAME_MODES[body.frame.mode], body.frame.n),
        aggregates=aggs,
        rankings=ranks,
        lead_lag=tuple(lls),
        timeout_ms=body.timeout_ms,
    )
    try:
        validate_spec(spec)
    except (ValueError, TypeError) as e:
        raise HTTPException(400, str(e)) from None
    return spec


def config_from_env() -> EngineConfig:
    """从环境变量读取配置（WINDOWED_ 前缀），缺省使用 EngineConfig 默认值。"""
    defaults = EngineConfig()
    get = os.environ.get
    return EngineConfig(
        db_path=get("WINDOWED_DB_PATH", defaults.db_path),
        batch_size=int(get("WINDOWED_BATCH_SIZE", defaults.batch_size)),
        max_in_memory_window=int(
            get("WINDOWED_MAX_IN_MEMORY_WINDOW", defaults.max_in_memory_window)
        ),
        query_workers=int(get("WINDOWED_QUERY_WORKERS", defaults.query_workers)),
        default_timeout_ms=int(
            get("WINDOWED_DEFAULT_TIMEOUT_MS", defaults.default_timeout_ms)
        ),
    )


def create_app(config: Optional[EngineConfig] = None) -> FastAPI:
    config = config or config_from_env()
    storage = Storage(config)
    engine = QueryEngine(storage, config)

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            yield
        finally:
            engine.shutdown()
            storage.close()

    app = FastAPI(title="windowed-query-engine", lifespan=lifespan)
    app.state.storage = storage
    app.state.engine = engine
    app.state.config = config

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "version": storage.current_version()}

    @app.post("/datasets/{dataset}/rows")
    def write_rows(dataset: str, body: RowsIn, batch_id: Optional[str] = None):
        if not body.rows:
            raise HTTPException(400, "rows 不能为空")
        rows = [
            RowIn(r.partition_key, r.sort_key, r.value)
            for r in body.rows
        ]
        try:
            version = storage.commit_batch(dataset, batch_id, rows)
        except (TypeError, ValueError) as e:
            raise HTTPException(400, str(e)) from None
        return {"dataset": dataset, "accepted": len(rows), "version": version}

    @app.post("/queries", status_code=202)
    def submit_query(body: QueryIn):
        spec = parse_spec(body)
        query_id = engine.submit(spec)
        info = engine.get_info(query_id)
        return {
            "query_id": query_id,
            "status": info.status.value if info else QueryStatus.PENDING.value,
            "version": info.version if info else None,
        }

    @app.get("/queries/{query_id}")
    def query_status(query_id: str):
        info = engine.get_info(query_id)
        if info is None:
            raise HTTPException(404, "query 不存在")
        return {
            "query_id": info.query_id,
            "status": info.status.value,
            "dataset": info.dataset,
            "partition_key": info.partition_key,
            "version": info.version,
            "error": info.error,
            "result_count": info.result_count,
        }

    @app.get("/queries/{query_id}/results")
    def query_results(
        query_id: str,
        offset: int = Query(0, ge=0),
        limit: int = Query(1000, ge=1, le=10_000),
    ):
        info = engine.get_info(query_id)
        if info is None:
            raise HTTPException(404, "query 不存在")
        if info.status == QueryStatus.DONE:
            payloads = storage.read_results(query_id, offset, limit)
            return {
                "query_id": query_id,
                "offset": offset,
                "count": len(payloads),
                "total": info.result_count,
                "rows": [json.loads(p) for p in payloads],
            }
        # 非完成状态：不存在部分结果泄露
        return JSONResponse(
            status_code=409,
            content={"query_id": query_id, "status": info.status.value, "error": info.error},
        )

    @app.post("/queries/{query_id}/cancel")
    def cancel_query(query_id: str):
        info = engine.get_info(query_id)
        if info is None:
            raise HTTPException(404, "query 不存在")
        cancelled = engine.cancel(query_id)
        return {"query_id": query_id, "cancel_requested": cancelled}

    return app
