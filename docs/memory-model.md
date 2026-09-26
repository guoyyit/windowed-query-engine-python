# 内存模型（Memory Model）

目标：单个分区的行数可以远超可用内存，查询仍能正确完成，且引擎的 Python 堆占用**不随分区行数线性增长**。

## 1. 总体结构

数据与结果都在 SQLite 中，按有序**页（page）**流动：

```
storage(分页有序扫描) -> executor(流式算子) -> 临时结果文件(分页) -> results 表(分页提交)
```

任何时刻应用层只持有至多若干页，而不是整个分区或整个结果集。

## 2. 读取侧：键集分页（keyset pagination）

`Storage.iter_partition` 使用排序游标
`(effective_null_flag, sort_value, seq)` 做键集翻页，每页 `page_size` 行：

* 不用 `OFFSET`（OFFSET 会让 SQLite 扫描并跳过前面全部行，退化为 O(N^2)）；
* 每页只通过索引区间定位，单页开销有界；
* 调用方一次只持有一页，扫描过程内存 O(page_size)。

## 3. 执行侧：两遍流式扫描

一个查询对同一快照最多做两遍有序扫描：

1. **分区聚合遍**：把 `partition` 帧的聚合规约为常量，状态 O(分区聚合函数个数)。
2. **发射遍**：逐行喂给各算子：
   * 排名：仅计数器（row_number、当前并列组起点、distinct 计数），O(1)；
   * LAG/LEAD：一个长度至多为 `最大 LAG offset + 最大 LEAD offset + 1` 的环形缓冲；
   * `rows_preceding/rows_around` 聚合：每个函数一个流式聚合器
     （SUM/COUNT/AVG 为 O(1)；MIN/MAX 为单调双端队列，O(帧宽) 最坏、O(1) 摊销）
     外加一个长度等于帧宽的行双端队列用于滑出。

执行期间最大常驻内存约为：

```
O( page_size + 最大帧宽(n) + 最大 LAG/LEAD offset + 函数个数 )
```

与分区行数 `N` 无关。

## 4. 结果侧：临时落盘 + 分页提交

* 发射出的结果行**不缓存在内存列表**，而是写入一个临时 JSON-lines 文件（一页一刷）。
* 提交时从该文件按 `page_size` 分页读回，在**单个** SQLite 事务中插入 `results` 并把查询状态翻为 `committed`。
* 查询接口（GET）同样以 `offset/limit` 分页读取，单页上限 10,000 行。

因此即使结果集本身非常大，引擎内存也有界。

## 5. 取消/超时时的内存

* 取消信号与截止时间在**每个页边界**轮询；
* 触发后丢弃临时结果文件、不写入任何 `results` 行、查询标记为 `cancelled`；
* 不会把半成品结果保留在内存或磁盘上。

## 6. 实际验证

* `tests/test_large_partition.py::test_memory_is_bounded_independent_of_partition_size`
  使用 `tracemalloc` 对比 5,000 行与 50,000 行分区的峰值堆内存，要求行数增长 10 倍时峰值显著次线性增长；
* `test_large_partition_correctness` 在数万行规模下逐行核对 row_number、分区 SUM/COUNT、滑动 SUM/MAX。

## 7. 可调参数

| 参数 | 环境变量 | 默认 | 对内存/性能的影响 |
|---|---|---|---|
| `page_size` | `WQE_PAGE_SIZE` | 1024 | 每页行数。越小越省内存但 SQL 往返更多 |
| `max_batch_rows` | `WQE_MAX_BATCH_ROWS` | 10000 | 单次写入批次上限，约束单次写入内存 |
| `default_timeout_s` | `WQE_DEFAULT_TIMEOUT_S` | 30.0 | 查询默认超时，0 表示不限时 |
| `busy_timeout_ms` | `WQE_BUSY_TIMEOUT_MS` | 5000 | SQLite 写锁等待上限（毫秒） |
| `db_path` | `WQE_DB_PATH` | `wqe.db` | SQLite 文件路径，`:memory:` 为易失库 |
