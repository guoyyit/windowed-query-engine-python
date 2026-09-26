# 持久化与崩溃恢复（Persistence & Recovery）

## 1. 存储布局

使用单个 SQLite 数据库（文件库默认开启 WAL）：

* `rows`：已提交的输入行。
  * `seq INTEGER PRIMARY KEY AUTOINCREMENT`：提交序号，也是排序决胜键；
  * `partition_key`、`sort_null`（是否为 NULL 键）、`sort_value`（无亲和性列）、
    `sort_json`/`value_json`（保真 JSON）。
  * 索引 `(partition_key, sort_null, sort_value, seq)` 支撑有序键集扫描。
* `queries`：查询生命周期（`running/committed/cancelled/failed`）、规格 JSON、错误、时间戳。
* `results`：已提交结果行，`(query_id, row_index)` 为主键，载荷为 JSON。

## 2. 原子性

* **写入**：一个批次的所有行在单个 `BEGIN IMMEDIATE` 事务中插入，要么全可见，要么全不可见。
* **结果提交**：查询结果的全部 `results` 行与把状态翻成 `committed` 的 UPDATE 处于
  **同一个事务**。因此“已提交”当且仅当“结果完整可见”，不存在只提交了一部分结果的状态。
* 进程内写操作由一把锁串行化；SQLite 的 `busy_timeout` 处理跨连接/跨进程写竞争。

## 3. 快照隔离

* 每个查询在开始时通过 `begin_snapshot()` 获得一个读事务，并在事务内立即执行一条读语句
  固定 WAL 快照（SQLite 的延迟事务在首条语句才确定快照）。
* 查询全程使用该连接，因此它读到的是某个一致时间点：
  * 不会读到执行途中其他线程/进程提交的新数据；
  * 不会出现“前半旧数据、后半新数据”的混合视图。
* 空分区返回零行，状态仍为 committed。

## 4. 取消与超时

* 取消是协作式的：`POST /queries/{id}/cancel` 设置一个事件，执行器在每个页边界轮询；
  超时由单调时钟截止时间在相同位置判定。
* 取消/超时后：
  * 查询状态置为 `cancelled`（携带原因）；
  * 临时结果文件删除，`results` 表无任何该查询的行；
  * 不影响后续查询（引擎状态干净，快照连接正常释放）。

## 5. 崩溃恢复

启动时 `Storage.recover()` 把所有残留的 `running` 查询置为 `cancelled`
（错误信息 `interrupted by restart`）：

* 已 `committed` 的查询：其结果行与状态在同一事务中落盘并经 `synchronous=FULL` 刷盘，
  崩溃后**不丢失、不重复**；
* 崩溃时仍 `running` 的查询：其事务未 COMMIT，SQLite 回滚，结果行不存在，
  恢复后**绝不会**被当作已完成结果返回；
* 已提交的输入行随事务持久化，未提交的批次随回滚消失。

`tests/test_recovery.py` 通过子进程真实 `SIGKILL` 验证：硬杀进程后重开，
已完成查询与数据完整、未完成查询变为 cancelled 且无结果行，重复重开幂等。

## 6. SQLite 相关配置

* 文件库：`journal_mode=WAL`、`synchronous=FULL`；
* 内存库（`:memory:`，主要用于测试）：共享缓存 + 一个保活连接；
* 连接以 `isolation_level=None` 打开，事务边界完全由代码显式控制。
