"""窗口计算引擎。

执行模型（流式、有界内存）：
- 输入按 (dataset, partition_key) 从存储层以确定性顺序流式读出，逐行处理；
- 排名（row_number/rank/dense_rank）在单趟扫描中 O(1) 状态完成；
- lag 用容量 offset+1 的 FIFO；lead 与 AROUND 聚合通过"延迟发出"实现，
  结果负载在容量为 delay+1 的缓冲中等待未来行回填；
- 滑动聚合维护运行状态（sum/count 与单调队列 min/max），窗口容量、lag/lead
  缓冲、延迟缓冲超过 config.max_in_memory_window 时切换到磁盘溢出结构；
- ALL 帧聚合先流式预扫描一遍求总量，再主扫描逐行发出，两趟都是 O(1) 内存；
- 结果按批落盘（result_rows 表），只有状态置为 done 后才对客户端可见；
- 每 _CHECK_INTERVAL 行检查一次取消事件与超时截止时间，取消/超时/失败都会
  清理该查询的全部部分结果，不影响后续查询。
"""
from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

from .spill import SpilledQueue, SpilledTextMap, SpilledWindow
from .storage import Storage
from .types import (
    AggFunc,
    EngineConfig,
    FrameMode,
    LeadLagKind,
    QueryInfo,
    QuerySpec,
    QueryStatus,
    RankFunc,
)

_CHECK_INTERVAL = 256  # 每处理多少行检查一次取消/超时

_RESERVED_FIELDS = frozenset({
    "row_index", "partition_key", "sort_key", "value", "frame_rows",
    "sum", "count", "avg", "min", "max",
    "row_number", "rank", "dense_rank",
})

_UNSET = object()  # 上一行排序键的初始哨兵


class QueryCancelled(Exception):
    """查询被取消或超时。kind 为 "cancelled" 或 "timeout"。"""

    def __init__(self, kind: str) -> None:
        super().__init__(kind)
        self.kind = kind


class QueryHandle:
    def __init__(self, query_id: str) -> None:
        self.query_id = query_id
        self.cancel_event = threading.Event()

    def cancel(self) -> None:
        self.cancel_event.set()


def validate_spec(spec: QuerySpec) -> None:
    if not spec.dataset or not isinstance(spec.dataset, str):
        raise ValueError("dataset 必须为非空字符串")
    if not spec.partition_key or not isinstance(spec.partition_key, str):
        raise ValueError("partition_key 必须为非空字符串")
    if spec.frame.n < 0:
        raise ValueError("frame.n 必须 >= 0")
    names: list[str] = []
    for ll in spec.lead_lag:
        if ll.offset < 1:
            raise ValueError("lead/lag 的 offset 必须 >= 1")
        name = ll.name or f"{ll.kind.value}_{ll.offset}"
        if name in _RESERVED_FIELDS:
            raise ValueError(f"lead/lag 输出字段名与保留字段冲突: {name}")
        names.append(name)
        if ll.default is not None:
            d = ll.default
            if isinstance(d, bool) or not isinstance(d, (int, float)) or not math.isfinite(d):
                raise ValueError(f"lead/lag 默认值必须为有限数值或 None: {d!r}")
    if len(names) != len(set(names)):
        raise ValueError("lead/lag 输出字段名重复")
    if spec.timeout_ms is not None and spec.timeout_ms <= 0:
        raise ValueError("timeout_ms 必须为正数")


class _WindowState:
    """滑动窗口运行状态：sum/count 直接维护，min/max 用单调队列（或磁盘溢出）。"""

    def __init__(self, spilled: Optional[SpilledWindow]) -> None:
        self._sw = spilled
        self._vals: deque[tuple[int, Optional[float]]] = deque()
        self._minq: deque[tuple[int, float]] = deque()
        self._maxq: deque[tuple[int, float]] = deque()
        self.sum = 0.0
        self.cnt = 0   # 非 NULL 个数
        self.rows = 0  # 窗口行数

    def add(self, pos: int, value: Optional[float]) -> None:
        self.rows += 1
        if self._sw is not None:
            self._sw.add(pos, value)
        else:
            self._vals.append((pos, value))
            if value is not None:
                while self._minq and self._minq[-1][1] >= value:
                    self._minq.pop()
                self._minq.append((pos, value))
                while self._maxq and self._maxq[-1][1] <= value:
                    self._maxq.pop()
                self._maxq.append((pos, value))
        if value is not None:
            self.sum += value
            self.cnt += 1

    def evict_before(self, keep_from: int) -> None:
        if self._sw is not None:
            evicted = self._sw.evict_before(keep_from)
            self.rows -= len(evicted)
            for v in evicted:
                if v is not None:
                    self.sum -= v
                    self.cnt -= 1
            return
        vals = self._vals
        while vals and vals[0][0] < keep_from:
            _, v = vals.popleft()
            self.rows -= 1
            if v is not None:
                self.sum -= v
                self.cnt -= 1
        minq = self._minq
        while minq and minq[0][0] < keep_from:
            minq.popleft()
        maxq = self._maxq
        while maxq and maxq[0][0] < keep_from:
            maxq.popleft()

    def min(self) -> Optional[float]:
        if self._sw is not None:
            return self._sw.min()
        return self._minq[0][1] if self._minq else None

    def max(self) -> Optional[float]:
        if self._sw is not None:
            return self._sw.max()
        return self._maxq[0][1] if self._maxq else None


class _Pipeline:
    """单分区单趟流式流水线；通过 delay 缓冲统一处理 lead 与 AROUND 聚合。"""

    def __init__(
        self,
        storage: Storage,
        config: EngineConfig,
        spec: QuerySpec,
        query_id: str,
        spill_conn: Optional[sqlite3.Connection],
        totals: Optional[dict],
        delay: int,
        check: Callable[[], None],
    ) -> None:
        self._storage = storage
        self._config = config
        self._spec = spec
        self._qid = query_id
        self._totals = totals
        self._delay = delay
        self._check = check

        self._aggs = list(dict.fromkeys(spec.aggregates))
        self._ranks = list(dict.fromkeys(spec.rankings))
        self._lags = [
            (l.name or f"lag_{l.offset}", l.offset, l.default)
            for l in spec.lead_lag
            if l.kind == LeadLagKind.LAG
        ]
        self._leads = [
            (l.name or f"lead_{l.offset}", l.offset, l.default)
            for l in spec.lead_lag
            if l.kind == LeadLagKind.LEAD
        ]
        self._frame = spec.frame

        # 排名状态
        self._pos = 0
        self._last_sk: object = _UNSET
        self._rank = 0
        self._dense = 0

        # lag 状态（容量 max_lag + 1）
        max_lag = max((k for _, k, _ in self._lags), default=0)
        self._lag_cap = max_lag + 1 if self._lags else 0
        self._lag_dq: deque[Optional[float]] = deque()
        self._lag_sq: Optional[SpilledQueue] = None
        if self._lags and self._lag_cap > config.max_in_memory_window:
            assert spill_conn is not None
            self._lag_sq = SpilledQueue(spill_conn, query_id, "lag")

        # 滑动窗口状态
        self._win: Optional[_WindowState] = None
        if self._aggs and self._frame.mode != FrameMode.ALL:
            cap = (
                self._frame.n + 1
                if self._frame.mode == FrameMode.PRECEDING
                else 2 * self._frame.n + 1
            )
            spilled = None
            if cap > config.max_in_memory_window:
                assert spill_conn is not None
                spilled = SpilledWindow(spill_conn, query_id, "win")
            self._win = _WindowState(spilled)

        # 延迟发出的负载缓冲
        self._pend_mem: dict[int, dict] = {}
        self._pend_spill: Optional[SpilledTextMap] = None
        if delay > 0 and delay > config.max_in_memory_window:
            assert spill_conn is not None
            self._pend_spill = SpilledTextMap(spill_conn, query_id, "pend")

        # 输出批缓冲
        self._out: list[str] = []
        self._out_start = 0

    # ---- 延迟缓冲 ----

    def _pend_put(self, idx: int, payload: dict) -> None:
        if self._pend_spill is not None:
            self._pend_spill.put(idx, self._dumps(payload))
        else:
            self._pend_mem[idx] = payload

    def _pend_get(self, idx: int) -> Optional[dict]:
        if self._pend_spill is not None:
            text = self._pend_spill.get(idx)
            return None if text is None else json.loads(text)
        return self._pend_mem.get(idx)

    def _pend_pop(self, idx: int) -> Optional[dict]:
        if self._pend_spill is not None:
            text = self._pend_spill.get(idx)
            if text is not None:
                self._pend_spill.delete(idx)
                return json.loads(text)
            return None
        return self._pend_mem.pop(idx, None)

    # ---- 主流程 ----

    def feed(self, i: int, sort_key: object, value: Optional[float]) -> None:
        spec = self._spec
        payload: dict = {
            "row_index": i,
            "partition_key": spec.partition_key,
            "sort_key": sort_key,
            "value": value,
        }
        # 排名
        self._pos += 1
        if self._ranks:
            if self._last_sk is _UNSET or sort_key != self._last_sk:
                self._rank = self._pos
                self._dense += 1
                self._last_sk = sort_key
            for r in self._ranks:
                if r == RankFunc.ROW_NUMBER:
                    payload["row_number"] = self._pos
                elif r == RankFunc.RANK:
                    payload["rank"] = self._rank
                else:
                    payload["dense_rank"] = self._dense
        # lag
        if self._lags:
            self._lag_push(value)
            size = self._lag_size()
            for name, k, default in self._lags:
                payload[name] = self._lag_at(size - 1 - k) if size > k else default
        # lead 默认值（未来行存在时会被回填）
        for name, _, default in self._leads:
            payload[name] = default
        # ALL 帧聚合是与当前行无关的常量，立即填入
        if self._aggs and self._frame.mode == FrameMode.ALL:
            self._fill_aggs(payload)
        # 滑动窗口
        win = self._win
        if win is not None:
            win.add(i, value)
        if self._delay == 0:
            if win is not None:
                win.evict_before(i - self._frame.n)
                self._fill_aggs(payload)
            self._emit(i, payload)
            return
        self._pend_put(i, payload)
        if win is not None and self._frame.mode == FrameMode.AROUND:
            t = i - self._frame.n
            if t >= 0:
                win.evict_before(t - self._frame.n)
                p = self._pend_get(t)
                assert p is not None
                self._fill_aggs(p)
                self._pend_put(t, p)
        elif win is not None:  # PRECEDING：当前行即计算点
            win.evict_before(i - self._frame.n)
            self._fill_aggs(payload)
            self._pend_put(i, payload)
        # lead 回填
        if self._leads:
            for name, k, _ in self._leads:
                t = i - k
                if t >= 0:
                    p = self._pend_get(t)
                    if p is not None:
                        p[name] = value
                        self._pend_put(t, p)
        # 发出已就绪的行
        t = i - self._delay
        if t >= 0:
            p = self._pend_pop(t)
            assert p is not None
            self._emit(t, p)

    def finish(self, total: int) -> None:
        n = self._frame.n
        if self._delay > 0:
            if self._win is not None and self._frame.mode == FrameMode.AROUND:
                for t in range(max(0, total - n), total):
                    self._win.evict_before(t - n)
                    p = self._pend_get(t)
                    assert p is not None
                    self._fill_aggs(p)
                    self._pend_put(t, p)
                    if t % _CHECK_INTERVAL == 0:
                        self._check()
            for t in range(max(0, total - self._delay), total):
                p = self._pend_pop(t)
                assert p is not None
                self._emit(t, p)
                if t % _CHECK_INTERVAL == 0:
                    self._check()
        self._flush_out()

    # ---- 内部 ----

    def _lag_push(self, value: Optional[float]) -> None:
        if self._lag_sq is not None:
            self._lag_sq.push(value)
            while len(self._lag_sq) > self._lag_cap:
                self._lag_sq.pop()
        else:
            self._lag_dq.append(value)
            while len(self._lag_dq) > self._lag_cap:
                self._lag_dq.popleft()

    def _lag_size(self) -> int:
        return len(self._lag_sq) if self._lag_sq is not None else len(self._lag_dq)

    def _lag_at(self, j: int) -> Optional[float]:
        if self._lag_sq is not None:
            return self._lag_sq.peek_at(j)
        return self._lag_dq[j]

    def _fill_aggs(self, payload: dict) -> None:
        totals = self._totals
        if totals is not None:  # ALL 帧：使用预扫描总量
            payload["frame_rows"] = totals["rows"]
            for a in self._aggs:
                payload[a.value] = totals[a.value]
            return
        win = self._win
        assert win is not None
        payload["frame_rows"] = win.rows
        for a in self._aggs:
            if a == AggFunc.SUM:
                payload["sum"] = win.sum if win.cnt else None
            elif a == AggFunc.COUNT:
                payload["count"] = win.cnt
            elif a == AggFunc.AVG:
                payload["avg"] = (win.sum / win.cnt) if win.cnt else None
            elif a == AggFunc.MIN:
                payload["min"] = win.min()
            elif a == AggFunc.MAX:
                payload["max"] = win.max()

    @staticmethod
    def _dumps(payload: dict) -> str:
        return json.dumps(
            payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        )

    def _emit(self, t: int, payload: dict) -> None:
        self._out.append(self._dumps(payload))
        if len(self._out) >= self._config.batch_size:
            self._flush_out()
            self._check()

    def _flush_out(self) -> None:
        if self._out:
            self._storage.insert_result_rows(self._qid, self._out_start, self._out)
            self._out_start += len(self._out)
            self._out.clear()


class QueryEngine:
    def __init__(self, storage: Storage, config: EngineConfig) -> None:
        self._storage = storage
        self._config = config
        self._pool = ThreadPoolExecutor(
            max_workers=config.query_workers, thread_name_prefix="windowed-query"
        )
        self._handles: dict[str, QueryHandle] = {}
        self._handles_lock = threading.Lock()
        self._shutdown = False

    def submit(self, spec: QuerySpec) -> str:
        """提交查询，返回 query_id，异步执行。"""
        validate_spec(spec)
        if self._shutdown:
            raise RuntimeError("engine 已关闭")
        query_id = uuid.uuid4().hex
        version = self._storage.current_version()
        self._storage.create_query(query_id, spec, version)
        handle = QueryHandle(query_id)
        with self._handles_lock:
            self._handles[query_id] = handle
        self._pool.submit(self._run, handle, spec, version)
        return query_id

    def cancel(self, query_id: str) -> bool:
        """向正在执行的查询发出取消信号；查询不存在或已结束返回 False。"""
        with self._handles_lock:
            handle = self._handles.get(query_id)
        if handle is None:
            return False
        handle.cancel()
        return True

    def get_info(self, query_id: str) -> Optional[QueryInfo]:
        return self._storage.get_query(query_id)

    def shutdown(self) -> None:
        self._shutdown = True
        with self._handles_lock:
            handles = list(self._handles.values())
        for h in handles:
            h.cancel()
        self._pool.shutdown(wait=True, cancel_futures=True)

    # ---- 执行 ----

    def _run(self, handle: QueryHandle, spec: QuerySpec, version: int) -> None:
        qid = handle.query_id
        self._storage.set_query_status(qid, QueryStatus.RUNNING)
        timeout_ms = (
            spec.timeout_ms
            if spec.timeout_ms is not None
            else self._config.default_timeout_ms
        )
        deadline = time.monotonic() + timeout_ms / 1000.0
        try:
            count = self._execute(handle, spec, version, deadline)
            self._storage.set_query_status(qid, QueryStatus.DONE, result_count=count)
        except QueryCancelled as e:
            self._storage.delete_query_artifacts(qid)
            status = (
                QueryStatus.CANCELLED if e.kind == "cancelled" else QueryStatus.TIMEOUT
            )
            self._storage.set_query_status(qid, status, error=e.kind)
        except Exception as e:  # noqa: BLE001 - 任何失败都不能拖垮执行线程
            self._storage.delete_query_artifacts(qid)
            self._storage.set_query_status(
                qid, QueryStatus.FAILED, error=f"{type(e).__name__}: {e}"
            )
        finally:
            with self._handles_lock:
                self._handles.pop(qid, None)

    def _execute(
        self, handle: QueryHandle, spec: QuerySpec, version: int, deadline: float
    ) -> int:
        qid = handle.query_id
        cfg = self._config
        storage = self._storage

        def check() -> None:
            if handle.cancel_event.is_set():
                raise QueryCancelled("cancelled")
            if time.monotonic() > deadline:
                raise QueryCancelled("timeout")

        need_aggs = bool(spec.aggregates)
        frame = spec.frame
        max_lag = max(
            (l.offset for l in spec.lead_lag if l.kind == LeadLagKind.LAG), default=0
        )
        max_lead = max(
            (l.offset for l in spec.lead_lag if l.kind == LeadLagKind.LEAD), default=0
        )
        delay = max_lead
        win_cap = 0
        if need_aggs and frame.mode != FrameMode.ALL:
            win_cap = frame.n + 1 if frame.mode == FrameMode.PRECEDING else 2 * frame.n + 1
            if frame.mode == FrameMode.AROUND:
                delay = max(delay, frame.n)
        need_spill = (
            win_cap > cfg.max_in_memory_window
            or max_lag + 1 > cfg.max_in_memory_window
            or delay > cfg.max_in_memory_window
        )

        spill_conn: Optional[sqlite3.Connection] = None
        try:
            if need_spill:
                spill_conn = sqlite3.connect(storage.db_path, isolation_level=None)
                spill_conn.execute("PRAGMA busy_timeout=30000")
            totals = None
            if need_aggs and frame.mode == FrameMode.ALL:
                totals = self._compute_totals(handle, spec, version, check)
            pipe = _Pipeline(
                storage, cfg, spec, qid, spill_conn, totals, delay, check
            )
            i = 0
            for seq, sort_key, value in storage.iter_partition(
                spec.dataset, spec.partition_key, version
            ):
                if i % _CHECK_INTERVAL == 0:
                    check()
                pipe.feed(i, sort_key, value)
                i += 1
            check()
            pipe.finish(i)
            return i
        finally:
            if spill_conn is not None:
                spill_conn.close()
                # 无论成功失败，临时溢出状态都不应残留
                self._storage.delete_spill(qid)

    def _compute_totals(
        self, handle: QueryHandle, spec: QuerySpec, version: int, check: Callable[[], None]
    ) -> dict:
        s = 0.0
        c = 0
        mn: Optional[float] = None
        mx: Optional[float] = None
        n = 0
        for i, (_, _, v) in enumerate(
            self._storage.iter_partition(spec.dataset, spec.partition_key, version)
        ):
            if i % _CHECK_INTERVAL == 0:
                check()
            n += 1
            if v is not None:
                s += v
                c += 1
                mn = v if mn is None or v < mn else mn
                mx = v if mx is None or v > mx else mx
        return {
            "rows": n,
            "sum": s if c else None,
            "count": c,
            "avg": (s / c) if c else None,
            "min": mn,
            "max": mx,
        }
