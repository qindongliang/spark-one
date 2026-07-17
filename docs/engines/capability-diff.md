# Local 与 Kyuubi 引擎能力差异

本文记录 SparkOne local 引擎与 Kyuubi 引擎的能力边界。结论是：两者应追求 SQL 主路径和写入决策对等，进程内 DataFrame adapter 等执行差异保持在各自 engine 边界内。

## 完全对等

| 功能 | 说明 |
| --- | --- |
| 原生 Spark SQL 查询/只读检查 | `select/show/describe/explain/use` 等语句由 compiler 原样交给所选 engine；其他原生 command 在编译阶段统一拒绝。 |
| `view name as select ...` | 两边都编译成 `CREATE OR REPLACE TEMPORARY VIEW`，并在各自 session 中生效。 |
| `set name = literal` | 两边都在 SparkOne runtime 层维护变量 map。 |
| `set name as select ...` | 两边都执行查询取第一行第一列；local 通过 DataFrame，Kyuubi 通过 JDBC ResultSet。 |
| `load hive/doris ... as t` | 编译 SQL 一致；Kyuubi 需要在 Kyuubi/Spark engine 侧配置好 Hive/Doris catalog。 |
| 受控 HDFS 文件 load | 两边都只接受 workspace 相对路径，并使用相同内部命令和 driver extension 注册临时视图；原生文件 provider relation 统一拒绝。 |
| 固定写入能力矩阵 | 两边都在提交前使用同一个 `WritePlan` 和矩阵；Hive/Doris/MySQL overwrite 永久拒绝。 |
| Catalog append | Hive/Doris 以及 Kyuubi MySQL 都要求目标存在、源和目标列名集合一致，并通过显式目标列清单和源列投影写入；类型不兼容在写入前失败。 |
| 文件路径写入边界 | 两边都永久拒绝文件 append 和 external path 写入；受控 HDFS overwrite 使用相同的内部命令和 driver extension。 |

## 有条件对等

| 功能 | local | Kyuubi | 结论 |
| --- | --- | --- | --- |
| MySQL catalog SQL | 从 `engines.local.catalogs.mysql` 注入 SparkConf。 | 必须配置在 Kyuubi/Spark engine。 | SQL 语义可对等，配置归属不对等。 |
| Doris catalog SQL | 从 `engines.local.catalogs.doris` 注入 SparkConf。 | 必须配置在 Kyuubi/Spark engine。 | SQL 语义可对等，配置归属不对等。 |
| 外部 provider，例如 Excel | 可通过 `engines.local.jars.*` 注入进本地 SparkSession。 | 必须放到远端 engine classpath 或 `spark.jars`。 | connector 分发不对等。 |
| Preview | 通过 DataFrame collect。 | 通过 JDBC ResultSet collect。 | 行为接近，schema 类型名可能不同。 |
| 临时视图生命周期 | 跟 SparkOne 进程内 SparkSession 绑定。 | 跟逻辑租户独立的 Kyuubi JDBC connection/session 绑定。 | Kyuubi server/engine 重启后临时视图会丢。 |
| 原生命令保护 | compiler 使用 `StatementIntent` 和 Spark LogicalPlan 白名单。 | 使用相同策略，拒绝后不会提交 JDBC。 | Compile/Run 策略对等。 |
| 受控 HDFS staging overwrite | SparkOne 进程内直接注册 extension，ZK/HDFS 参数来自 local HOCON。 | extension jar 和 SparkConf 必须部署到 Kyuubi Spark engine。 | 执行语义对等，部署归属不对等。 |

## 明确不对等

| 功能 | local | Kyuubi |
| --- | --- | --- |
| `load mysql` adapter | 支持 Spark JDBC reader，连接信息来自 `engines.local.datasources.mysql`。 | 支持 `mysql.\`catalog.db.table\``：无分片参数走远端 catalog SQL；带分片参数走 `sparkone_mysql` provider，并复用 Kyuubi/Spark engine 侧 `spark.sql.catalog.<catalog>.*`。 |
| `save mysql` | `mysql.\`connection.table\`` 走 Spark JDBC writer，连接来自 `engines.local.datasources.mysql`。 | `mysql.\`catalog.db.table\`` 走远端 JDBC Catalog SQL，连接来自 Kyuubi/Spark engine；不接受 SQL OPTIONS。 |
| Catalog append 预检查 | 通过 `spark.catalog.tableExists`、DataFrame schema 和最终 SQL 的 `EXPLAIN` 检查。 | 通过远端 `LIMIT 0` 和最终显式列 `INSERT` 的 `EXPLAIN` 检查。 |
| 写语句断线处理 | 进程内 Spark 执行，不存在 JDBC statement 自动重放。 | `save` statement 永不自动重试；连接异常返回写入状态未知。 |
| `/api/compile` 语义 | local 下编译结果通常可直接执行。 | 若不做 engine-aware 校验，可能出现 Compile 成功但 Run 才发现 Kyuubi 不支持的情况。 |

## 已落地优化

| 优化 | 状态 |
| --- | --- |
| `/api/compile` 做 engine-aware 校验 | 已落地。Compile 会按请求 engine 编译；Kyuubi 支持 `mysql.\`catalog.db.table\``，并会提前拒绝不支持的 `save mysql` adapter。 |
| `/api/config` 暴露 engine capabilities | 已落地。前端可读取 `mysqlAdapter`、`externalCatalogConfiguredBySparkOne` 等执行能力；固定写入矩阵不作为可变 engine capability 暴露。 |
| Kyuubi compile diagnostics | 已落地第一版。Compile 成功时返回 Kyuubi 配置归属提示，说明 catalog/provider jars 需要在 Kyuubi/Spark engine 侧配置。 |
| Kyuubi-safe MySQL catalog load | 已落地。Kyuubi `load mysql.\`catalog.db.table\`` 不读取 SparkOne 本地 MySQL 密钥；大表 provider 只传 catalog/table/where/partition 参数。 |
| Catalog append 3A | 已落地。Hive/Doris 使用 Spark 3.3+ column list 语法，Local/Kyuubi 都做目标、列名和类型检查；Kyuubi 写 statement 禁止断线重放。 |
| MySQL append 3B | 已落地。Local JDBC adapter 与 Kyuubi JDBC Catalog 都做目标、列名和类型预检并按列名写入；SQL 不携带连接密钥，overwrite 永久拒绝。 |
| 受控 HDFS overwrite | 已落地。Spark driver 使用目标级 ZK ephemeral lock、固定同级 staging/backup 和 HDFS rename 发布。 |
| 受控 HDFS load | 已落地。文件 provider 只接受租户相对路径，Spark driver 使用可信 workspace 配置解析并注册临时视图。 |

## 后续优先级

1. 在真实 Kyuubi、HDFS 和 ZooKeeper 环境验证受控 load 的租户隔离，以及 overwrite 的并发拒绝、driver 退出和残留恢复。
2. 文件 append 不进入 MVP 路线；只有出现明确生产案例，并定义 schema、分区、并发与幂等合同后，才单独评估 Parquet/ORC 分区 append 或事务湖表写入。
3. 本地文件、S3、OSS 裸路径写入保持永久拒绝，不增加配置开关。
4. 保持架构原则：SparkOne 不直接替 Kyuubi 管理 Spark/YARN/Hive 执行用户、connector classpath 或 catalog 密钥。
