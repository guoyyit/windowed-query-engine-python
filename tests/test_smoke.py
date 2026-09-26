"""Smoke test: package imports and basic type construction."""

from wqe.config import Config
from wqe.models import FrameType, FuncKind, FuncSpec, QuerySpec, WindowSpec


def test_imports_and_types():
    w = WindowSpec(frame=FrameType.ROWS_PRECEDING, n=2)
    f = FuncSpec(kind=FuncKind.SUM, alias="s", window=w)
    q = QuerySpec(partition_key="p", functions=(f,))
    assert q.partition_key == "p"
    assert Config().page_size > 0
