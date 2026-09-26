# HTTP API

同步 `def` 端点运行在 FastAPI 的工作线程池中，因此一个正在执行的长查询可以被
另一个并发的 cancel 请求取消。

## 写入

`POST /partitions/{partition_key}/rows`

```json
{ "rows": [ [sort_key, value], [sort_key, value] ] }
```

* 元素可为数字、字符串或 `null`；整批是一个原子事务。
* 成功：`200 {"partition_key": ..., "written": N}`。
* 非法值类型 / 批次超过 `max_batch_rows`：`400`。

## 提交查询

`POST /queries`

```json
{
  "partition_key": "p",
  "null_order": "nulls_last",
  "timeout_s": 5,
  "functions": [
    {"kind": "row_number", "alias": "rn"},
    {"kind": "rank", "alias": "rk"},
    {"kind": "dense_rank", "alias": "dr"},
    {"kind": "lag",  "alias": "lg", "offset": 1, "default": "NA"},
    {"kind": "lead", "alias": "ld", "offset": 1, "default": "NA"},
    {"kind": "sum",   "alias": "s",  "window": {"frame": "partition"}},
    {"kind": "count", "alias": "c",  "window": {"frame": "partition"}},
    {"kind": "avg",   "alias": "a",  "window": {"frame": "partition"}},
    {"kind": "min",   "alias": "mn", "window": {"frame": "partition"}},
    {"kind": "max",   "alias": "mx", "window": {"frame": "partition"}},
    {"kind": "sum", "alias": "rs",
     "window": {"frame": "rows_preceding", "n": 2}},
    {"kind": "avg", "alias": "aa",
     "window": {"frame": "rows_around", "n": 3}}
  ]
}
```

* `frame`：`partition` / `rows_preceding` / `rows_around`（后两者需要非负 `n`）。
* 返回 `201`，body 含 `query_id`、`status`（`committed` 或 `cancelled`）、
  `row_count`，以及第一页结果行。
* 规格非法（聚合缺 window、别名重复、offset 非法等）返回 `400`。

## 分页读取结果

`GET /queries/{query_id}?offset=0&limit=1000`

* `committed`：返回该窗口结果行及 `row_count`；
* `running/cancelled/failed`：返回状态（cancelled/failed 带 `error`），不含结果行；
* 未知 id：`404`；非法分页：`400`。

## 取消

`POST /queries/{query_id}/cancel`

* 查询正在执行：`200 {"cancelling": true}`，执行器在最近的页边界停止；
* 查询已结束或不存在：`404`。

## 健康检查

`GET /health` -> `{"status": "ok"}`

## 启动

```bash
uv run uvicorn main:app --host 0.0.0.0 --port 8000
```

通过 `WQE_*` 环境变量配置，见 `docs/memory-model.md`。
