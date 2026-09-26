# 本地有序数据窗口计算服务

一个基于 Python + FastAPI + SQLite(WAL) 的本地窗口计算服务。数据按**分区键 + 排序键**组织，
在单个分区内提供**排名**、**前后项取值（lead/lag）**与**滑动聚合**（sum/count/avg/min/max），
支持超大分区流式执行、有界内存、并发快照隔离、查询取消/超时与崩溃恢复。

## 快速开始

```bash
uv sync
uv run uvicorn main:app --host 127.0.0.1 --port 8000
```

运行测试：

```bash
uv run pytest -q
```

### 写入数据

```bash
curl -s -X POST 'http://127.0.0.1:8000/datasets/sales/rows?batch_id=2026-09-26' \
  -H 'Content-Type: application/json' \
  -d '{"rows":[
    {"partition_key":"region-a","sort_key":1,"value":10.0},
    {"partition_key":"region-a","sort_key":2,"value":null},
    {"partition_key":"region-a","sort_key":2,"value":30.0}
  ]}'
# {"dataset":"sales","accepted":3,"version":1}
```

一批行在**单个事务**内原子提交，并分配一个全局单调递增的 `version`。
`batch_id` 是可选的幂等键：同一 `(dataset, batch_id)` 重复提交（含崩溃后重放）返回相同版本，
数据不重复。

### 提交查询

```bash
curl -s -X POST http://127.0.0.1:8000/queries \
  -H 'Content-Type: application/json' \
  -d '{
    "dataset":"sales","partition_key":"region-a",
    "frame":{"mode":"around","n":1},
    "aggregates":["sum","count","avg","min","max"],
    "rankings":["row_number","rank","dense_rank"],
    "lead_lag":[{"kind":"lag","offset":1,"default":0.0}],
    "timeout_ms":30000
  }'
# {"query_id":"...","status":"pending","version":1}
```

查询在**提交瞬间**绑定快照版本，之后完成前的任何写入对该查询不可见。

轮询状态并分页读取结果（仅 `done` 状态返回结果；其他状态返回 `409`，绝不返回部分结果）：

```bash
curl -s http://127.0.0.1:8000/queries/<query_id>
curl -s 'http://127.0.0.1:8000/queries/<query_id>/results?offset=0&limit=1000'
```

取消：`POST /queries/<query_id>/cancel`。

## 窗口语义

### 确定性排序规则

分区内行的全局顺序为：

1. `sort_key` 为 `null` 的行排在最前；
2. 然后是数值，按数值升序；
3. 然后是字符串，按 Unicode/二进制字典序升序；
4. 排序键完全相同（并列）时，按**写入顺序**（行的全局自增序号）决胜，先写入者在前。

排序键只接受 `null`、有限数值与字符串（不接受布尔、NaN、Infinity、数组、对象）。
任意两次相同输入（含相同 `batch_id` 的写入顺序）产出**字节级一致**的结果。

### 排名函数

| 函数 | 语义（以值 `1,1,2` 为例） |
| --- | --- |
| `row_number` | 1 起始的行号，并列也按写入顺序决出名次，不重号：`1,2,3` |
| `rank` | 1 起始，并列同名次，之后**跳号**：`1,1,3` |
| `dense_rank` | 1 起始，并列同名次，之后**不跳号**：`1,1,2` |

### 前后项取值

- `lag k`：取当前行**之前**第 k 行的 `value`；越界返回 `default`（未指定时为 `null`）。
- `lead k`：取当前行**之后**第 k 行的 `value`；越界返回 `default`。
- `null` 是一个**真实存在的取值**（会被原样取出），只有"目标行不存在（越界）"才使用 `default`。

### 窗口帧（frame）

设当前行为分区内第 `i` 行（从 0 开始）：

| mode | 窗口范围 | 边界行为 |
| --- | --- | --- |
| `all` | 全分区 `[0, N)` | 每一行看到的窗口都相同 |
| `preceding`（参数 n） | `[i-n, i]` | `i-n < 0` 时截断到分区起点 |
| `around`（参数 n） | `[i-n, i+n]` | 两端越界均截断到分区边界 |

`n=0` 合法：`preceding 0` 是仅当前行；`around 0` 等价于 `preceding 0`。
`n` 超出分区大小时窗口在边界处自然截断（不会报错）。

### 聚合与缺失值

- `count`：窗口内 `value` **非 null** 的行数；
- `sum`/`avg`/`min`/`max`：忽略 `null`，只对非空值计算；
- `frame_rows`：窗口包含的**物理行数**（null 行计入）；
- 窗口内没有任何非空值时：`count=0`，`sum/avg/min/max=null`；
- 数值以 IEEE-754 double 存储与累加，`sum` 按排序后的固定顺序累加，保证可重复。

## 内存模型（大分区）

执行是**流式**的：输入用游标按批（`WINDOWED_BATCH_SIZE`）读取，输出按批落盘到
`result_rows` 表，引擎从不把整个分区读入内存。各算子的额外状态：

- 排名：O(1)（上一个排序键、当前名次）；
- `lag k`：容量 k+1 的 FIFO；
- `lead k` / `around`：容量为 k（或 n）的"延迟发出"缓冲，等未来行回填后再发出；
- 滑动聚合：sum/count 为 O(1) 标量，min/max 用单调队列 O(1) 均摊，窗口容量
  `preceding: n+1`、`around: 2n+1`；
- `all` 帧聚合：先流式预扫描一遍求总量，再主扫描输出，两趟均为 O(1) 额外内存。

当窗口容量或偏移量超过 `WINDOWED_MAX_IN_MEMORY_WINDOW` 时，对应状态**溢出到 SQLite**
（`spill` / `spill_text` 表），内存占用退回 O(批大小)。因此：

> 单个分区行数可以远超可用内存；稳态内存占用为 O(batch_size + 窗口内的极小元数据)，
> 不随分区行数线性增长。

查询结束（成功/失败/取消/超时）后溢出数据立即删除；崩溃残留由启动恢复清理。

## 并发与快照

- 写入经单一写连接 + 互斥锁串行化，每批一个事务（`BEGIN IMMEDIATE`），原子可见。
- 行一旦提交永不修改；查询只读取 `version <= 快照版本` 的行，因此无需长事务即可获得
  **一致快照**：扫描进行到一半时提交的新写入对该查询完全不可见。
- 查询在有界线程池（`WINDOWED_QUERY_WORKERS`）中并发执行，互不阻塞写入。

## 取消、超时与失败

- 查询每处理 256 行检查一次取消标志与超时截止时间，粒度最坏为
  `256 行 + 一批读取`，不会被线程中断留下半成品。
- 取消/超时/异常后：删除该查询已写入的**全部部分结果与溢出数据**，状态分别记为
  `cancelled` / `timeout` / `failed`；结果接口永远不会返回这些查询的部分输出。
- 已结束（done/failed/cancelled/timeout）的查询不受取消影响；清理不触及其他任何查询。

## 崩溃恢复

数据、批次去重记录、查询元数据与结果全部在 SQLite 中并随事务落盘（WAL +
`synchronous=NORMAL`）。服务重新打开数据库时执行恢复：

1. 所有处于 `pending`/`running` 的查询标记为 `failed`（错误原因为
   `crashed: service restarted`），其部分结果与溢出数据被删除——**未完成的计算不会被
   当作完成结果返回**；
2. 已提交的行批次原样保留；`done` 查询的结果原样保留，可继续分页读取；
3. 客户端可用相同 `batch_id` 安全重放崩溃前未确认的写入，不会产生重复数据。

## 配置项

通过环境变量配置（均有默认值）：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WINDOWED_DB_PATH` | `windowed.db` | SQLite 数据库文件路径 |
| `WINDOWED_BATCH_SIZE` | `1000` | 读取输入与落盘结果的批大小；越大越快、内存越高 |
| `WINDOWED_MAX_IN_MEMORY_WINDOW` | `100000` | 窗口/lag/lead 缓冲超过该条目数即溢出到磁盘 |
| `WINDOWED_QUERY_WORKERS` | `4` | 并发查询工作线程数 |
| `WINDOWED_DEFAULT_TIMEOUT_MS` | `60000` | 查询默认超时；可被每个查询的 `timeout_ms` 覆盖 |

## HTTP 接口

| 方法与路径 | 说明 |
| --- | --- |
| `GET /health` | 健康检查，返回当前全局版本号 |
| `POST /datasets/{dataset}/rows?batch_id=` | 原子写入一批行（`batch_id` 可选幂等键） |
| `POST /queries` | 提交查询，`202` 返回 `query_id` 与快照版本 |
| `GET /queries/{id}` | 查询状态：pending/running/done/failed/cancelled/timeout |
| `GET /queries/{id}/results?offset=&limit=` | 分页结果（仅 done 返回 200，否则 409） |
| `POST /queries/{id}/cancel` | 请求取消正在执行的查询 |

每个结果行包含：`row_index`、`partition_key`、`sort_key`、`value`，以及按请求附加的
`row_number`/`rank`/`dense_rank`、`frame_rows` 与各聚合字段、lead/lag 字段。

## 代码结构

```
windowed/
  types.py    # 公共类型、配置、确定性排序键规范化、参数校验
  storage.py  # SQLite 持久化：版本化写入、快照扫描、结果、恢复
  spill.py    # 磁盘溢出的 FIFO / 单调队列窗口 / 文本缓冲
  engine.py   # 流式窗口流水线、线程池、取消与超时
  service.py  # FastAPI 路由与环境变量配置
tests/        # 语义、溢出/大分区、并发快照、取消超时、崩溃恢复、HTTP
```
