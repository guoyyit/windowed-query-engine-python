"""窗口计算服务的公共类型定义。

本模块只包含类型与纯函数，不包含任何 I/O 逻辑。

确定性排序规则（任意两次相同输入必须得到完全一致的输出）：
- 排序键为 NULL 的行排在最前；
- 其后为数值型排序键，按数值升序；
- 再其后为字符串型排序键，按二进制字典序升序；
- 排序键完全相同的行（并列），按写入顺序（提交序号 seq）升序，先写入的排在前面。
"""
from __future__ import annotations

import enum
import math
from dataclasses import dataclass
from typing import Any, Optional, Union

# 排序键类型标签
SK_NULL = 0
SK_NUM = 1
SK_STR = 2

SortKey = Union[None, int, float, str]
Value = Optional[float]


def normalize_sort_key(key: SortKey) -> tuple[int, Optional[float], Optional[str]]:
    """把 JSON 排序键规范化为 (类型标签, 数值, 字符串) 三元组，用于确定性排序。"""
    if key is None:
        return (SK_NULL, None, None)
    if isinstance(key, bool):
        # bool 是 int 的子类，明确拒绝，避免隐式歧义
        raise TypeError("sort_key 不支持布尔类型")
    if isinstance(key, (int, float)):
        f = float(key)
        if not math.isfinite(f):
            raise ValueError("sort_key 必须为有限数值（不接受 NaN/Infinity）")
        return (SK_NUM, f, None)
    if isinstance(key, str):
        return (SK_STR, None, key)
    raise TypeError(f"不支持的 sort_key 类型: {type(key)!r}")


class FrameMode(str, enum.Enum):
    """窗口范围模式。

    - ALL:       分区内全部行；
    - PRECEDING: 当前行及之前 n 行，即 [i-n, i]，分区起点处截断；
    - AROUND:    当前行前后各 n 行，即 [i-n, i+n]，两端越界处截断。
    """

    ALL = "all"
    PRECEDING = "preceding"
    AROUND = "around"


class AggFunc(str, enum.Enum):
    SUM = "sum"
    COUNT = "count"  # 统计窗口内非 NULL value 的个数
    AVG = "avg"
    MIN = "min"
    MAX = "max"


class RankFunc(str, enum.Enum):
    ROW_NUMBER = "row_number"  # 1 起，并列按写入顺序决出名次，不重号
    RANK = "rank"              # 1 起，并列同名次，后续名次跳号
    DENSE_RANK = "dense_rank"  # 1 起，并列同名次，后续名次不跳号


class LeadLagKind(str, enum.Enum):
    LEAD = "lead"  # 取当前行之后第 offset 行的 value
    LAG = "lag"    # 取当前行之前第 offset 行的 value


class QueryStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


TERMINAL_STATES = frozenset(
    {QueryStatus.DONE, QueryStatus.FAILED, QueryStatus.CANCELLED, QueryStatus.TIMEOUT}
)


@dataclass(frozen=True)
class FrameSpec:
    mode: FrameMode = FrameMode.ALL
    n: int = 0  # 仅 PRECEDING / AROUND 有效，必须 >= 0


@dataclass(frozen=True)
class LeadLagSpec:
    kind: LeadLagKind
    offset: int = 1
    default: Value = None
    name: str = ""  # 输出字段名；为空时自动生成，如 "lag_1"


@dataclass(frozen=True)
class QuerySpec:
    dataset: str
    partition_key: str
    frame: FrameSpec = FrameSpec()
    aggregates: tuple[AggFunc, ...] = ()
    rankings: tuple[RankFunc, ...] = ()
    lead_lag: tuple[LeadLagSpec, ...] = ()
    timeout_ms: Optional[int] = None  # None 表示使用服务默认超时


@dataclass(frozen=True)
class RowIn:
    """一条待写入的输入行。"""

    partition_key: str
    sort_key: SortKey
    value: Value = None


@dataclass(frozen=True)
class QueryInfo:
    query_id: str
    status: QueryStatus
    dataset: str
    partition_key: str
    version: int  # 查询读取的快照版本
    error: Optional[str] = None
    result_count: Optional[int] = None


@dataclass(frozen=True)
class EngineConfig:
    """服务配置项（详见 README）。"""

    db_path: str = "windowed.db"
    batch_size: int = 1000               # 流式读取/结果落盘的批大小
    max_in_memory_window: int = 100_000  # 窗口状态在内存中的最大条目数，超出则落盘
    query_workers: int = 4               # 并发执行查询的线程数
    default_timeout_ms: int = 60_000     # 查询默认超时
