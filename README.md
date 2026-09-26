# Windowed Query Engine (Python)

面向本地有序数据的窗口计算服务。数据按**分区键 + 排序键**组织，在单个分区内提供：

* **排名**：`row_number` / `rank` / `dense_rank`；
* **前后项取值**：`lag` / `lead`（区分“引用行不存在”与“引用值为 NULL”）；
* **滑动聚合**：`sum` / `count` / `avg` / `min` / `max`，
  窗口支持分区全量、当前行之前 N 行、当前行前后各 N 行。

排序由 `(sort_key, seq)` 唯一确定，并列值与缺失值的处理完全确定，相同输入恒定得到相同输出。

## 关键能力

| 需求 | 实现方式 |
|---|---|
| 三类窗口、边界确定 | ROWS 帧在边界处裁剪，语义见 `docs/window-semantics.md` |
| 并列/缺失确定 | `seq` 提交序号决胜，NULL 独立并列组，聚合忽略 NULL |
| 大分区、有界内存 | 键集分页 + 两遍流式扫描 + 结果临时落盘分页提交，见 `docs/memory-model.md` |
| 并发写/并发读快照隔离 | WAL + 每查询一个固定快照，批次/结果均为单事务 |
| 取消/超时 | 页边界轮询取消事件与截止时间，不残留半成品结果 |
| 崩溃恢复 | 结果行与 committed 状态同事务；启动时把残留 running 置为 cancelled |

## 快速开始

```bash
uv sync
uv run uvicorn main:app --host 0.0.0.0 --port 8000

# 写入
curl -X POST localhost:8000/partitions/p/rows \
  -H 'content-type: application/json' \
  -d '{"rows":[[1,10],[1,20],[null,5],[2,null]]}'

# 查询
curl -X POST localhost:8000/queries -H 'content-type: application/json' -d '{
  "partition_key":"p",
  "functions":[
    {"kind":"row_number","alias":"rn"},
    {"kind":"rank","alias":"rk"},
    {"kind":"lead","alias":"ld","offset":1,"default":"NA"},
    {"kind":"sum","alias":"s","window":{"frame":"partition"}},
    {"kind":"avg","alias":"a","window":{"frame":"rows_around","n":1}}
  ]
}'
```

## 配置

全部为 `WQE_*` 环境变量：`WQE_DB_PATH`、`WQE_PAGE_SIZE`、`WQE_MAX_BATCH_ROWS`、
`WQE_DEFAULT_TIMEOUT_S`、`WQE_BUSY_TIMEOUT_MS`（默认值与含义见 `docs/memory-model.md`）。

## 测试

```bash
uv run pytest
```

覆盖场景：

* `tests/test_window_semantics.py`：并列值、NULL、边界裁剪、确定性；
* `tests/test_large_partition.py`：数万行正确性 + `tracemalloc` 有界内存；
* `tests/test_concurrency.py`：并发写原子性、查询一致快照、快照不包含后写数据；
* `tests/test_cancellation.py`：确定性取消、超时、无部分结果、不影响后续查询；
* `tests/test_recovery.py`：子进程 `SIGKILL` 后已提交数据/结果不丢失、未完成查询不返回；
* `tests/test_api.py`：HTTP 端到端、分页、参数校验、超时。

## 文档

* [窗口语义](docs/window-semantics.md)
* [内存模型](docs/memory-model.md)
* [持久化与崩溃恢复](docs/persistence-recovery.md)
* [HTTP API](docs/api.md)

## 模块结构

```
wqe/
  models.py      领域类型（行、窗口、函数、查询、结果、状态）
  config.py      配置项 / 环境变量
  storage.py     SQLite 持久化、键集分页扫描、快照、事务与恢复
  aggregation.py 流式聚合（SUM/COUNT/AVG/MIN/MAX 单调队列）
  execution.py   两遍流式执行器、LAG/LEAD 环、结果临时落盘、取消
  engine.py      写入/查询/取消编排、超时、分页读取
  api.py         FastAPI HTTP 层
main.py          uvicorn 入口
```
