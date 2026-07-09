# Local 与 Kyuubi 引擎能力差异

本文记录 SparkOne local 引擎与 Kyuubi 引擎的能力边界。结论是：两者应追求 SQL 主路径对等，不应把 local 的进程内 DataFrame adapter 和文件备份能力硬搬到 Kyuubi。

## 完全对等

| 功能 | 说明 |
| --- | --- |
| 原生 Spark SQL 透传 | `select/create/show/insert/drop` 等语句由 compiler 原样交给所选 engine，再由 Spark SQL 解析。 |
| `view name as select ...` | 两边都编译成 `CREATE OR REPLACE TEMPORARY VIEW`，并在各自 session 中生效。 |
| `set name = literal` | 两边都在 SparkOne runtime 层维护变量 map。 |
| `set name as select ...` | 两边都执行查询取第一行第一列；local 通过 DataFrame，Kyuubi 通过 JDBC ResultSet。 |
| `load hive/doris ... as t` | 编译 SQL 一致；Kyuubi 需要在 Kyuubi/Spark engine 侧配置好 Hive/Doris catalog。 |
| `save append/overwrite ... as hive/doris` | 编译 SQL 一致；Kyuubi 侧实际提交给远端 Spark engine。 |

## 有条件对等

| 功能 | local | Kyuubi | 结论 |
| --- | --- | --- | --- |
| MySQL catalog SQL | 从 `engines.local.catalogs.mysql` 注入 SparkConf。 | 必须配置在 Kyuubi/Spark engine。 | SQL 语义可对等，配置归属不对等。 |
| Doris catalog SQL | 从 `engines.local.catalogs.doris` 注入 SparkConf。 | 必须配置在 Kyuubi/Spark engine。 | SQL 语义可对等，配置归属不对等。 |
| 外部 provider，例如 Excel | 可通过 `engines.local.jars.*` 注入进本地 SparkSession。 | 必须放到远端 engine classpath 或 `spark.jars`。 | connector 分发不对等。 |
| Preview | 通过 DataFrame collect。 | 通过 JDBC ResultSet collect。 | 行为接近，schema 类型名可能不同。 |
| 临时视图生命周期 | 跟 SparkOne 进程内 SparkSession 绑定。 | 跟 Kyuubi JDBC connection/session 绑定。 | Kyuubi server/engine 重启后临时视图会丢。 |
| Safe Save overwrite 拦截 | 支持显式确认、deny、文件目标 rename/trash/rollback。 | 支持显式确认、deny；不做文件备份。 | 策略层部分对等，文件备份不对等。 |

## 明确不对等

| 功能 | local | Kyuubi |
| --- | --- | --- |
| `load mysql` adapter | 支持 Spark JDBC reader，连接信息来自 `engines.local.datasources.mysql`。 | 支持 `mysql.\`catalog.db.table\``：无分片参数走远端 catalog SQL；带分片参数走 `sparkone_mysql` provider，并复用 Kyuubi/Spark engine 侧 `spark.sql.catalog.<catalog>.*`。 |
| `save mysql` adapter | 支持 Spark JDBC writer，受 MySQL overwrite 安全开关控制。 | 不支持；建议使用远端 catalog SQL 或显式 Spark SQL。 |
| 文件 overwrite 备份 | 支持 `rename`、`trash` 和失败回滚。 | 不支持；远端提交和权限边界由 Kyuubi/Spark/Hadoop 负责。 |
| 目标表存在性预检查 | 可通过 `spark.catalog.tableExists` 或本地 JDBC reader 检查。 | 当前主要依赖远端 SQL 执行时报错。 |
| `/api/compile` 语义 | local 下编译结果通常可直接执行。 | 若不做 engine-aware 校验，可能出现 Compile 成功但 Run 才发现 Kyuubi 不支持的情况。 |

## 已落地优化

| 优化 | 状态 |
| --- | --- |
| `/api/compile` 做 engine-aware 校验 | 已落地。Compile 会按请求 engine 编译；Kyuubi 支持 `mysql.\`catalog.db.table\``，并会提前拒绝不支持的 `save mysql` adapter。 |
| `/api/config` 暴露 engine capabilities | 已落地。前端可读取 `mysqlAdapter`、`fileSafeBackup`、`externalCatalogConfiguredBySparkOne` 等能力边界。 |
| Kyuubi compile diagnostics | 已落地第一版。Compile 成功时返回 Kyuubi 配置归属提示，说明 catalog/provider jars 需要在 Kyuubi/Spark engine 侧配置。 |
| Kyuubi-safe MySQL catalog load | 已落地。Kyuubi `load mysql.\`catalog.db.table\`` 不读取 SparkOne 本地 MySQL 密钥；大表 provider 只传 catalog/table/where/partition 参数。 |

## 后续优先级

1. Kyuubi 增加可选远端诊断能力，主动执行轻量 SQL 检查常见 catalog/provider 是否可用。
2. 对 `save hive/doris` 增强远端目标表存在性预检查，给出比 Spark SQL 原始异常更稳定的错误信息。
3. 保持架构原则：SparkOne 不直接替 Kyuubi 管理 Spark/YARN/Hive 执行用户、connector classpath、catalog 密钥和文件备份流程。
