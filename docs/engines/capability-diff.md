# Local 与 Kyuubi 引擎能力差异

本文记录 QueryOne local 引擎与 Kyuubi 引擎的能力边界。结论是：两者应追求 SQL 主路径和写入决策对等，进程内 DataFrame adapter 等执行差异保持在各自 engine 边界内。

## 完全对等

| 功能 | 说明 |
| --- | --- |
| 原生 Spark SQL 查询/只读检查 | `select/show/describe/explain/use` 等语句除 `hive` 逻辑 catalog 别名外由 compiler 原样交给所选 engine；其他原生 command 在编译阶段统一拒绝。 |
| `view name as select ...` | 两边都编译成 `CREATE OR REPLACE TEMPORARY VIEW`，并在各自 session 中生效。 |
| `set name = literal` | 两边都在 QueryOne runtime 层维护变量 map。 |
| `set name as select ...` | 两边都执行查询取第一行第一列；local 通过 DataFrame，Kyuubi 通过 JDBC ResultSet。 |
| `assert table where ... message ...` | 两边执行同一条违规行查询；零行通过，有违规行返回有限样本并停止后续语句，`NULL` 谓词统一按失败处理。 |
| `load hive/doris ... as t` | 编译 SQL 一致；Kyuubi 需要在 Kyuubi/Spark engine 侧配置好 Hive/Doris catalog。 |
| ODEP JDBC/Doris Catalog | 两边默认使用 `jdbc.<alias>.<table>`、`doris.<alias>.<table>`，并按需调用同一套 index/resolve 接口；普通 SQL 不触发 ODEP。 |
| ODEP MySQL 分区读取 | 两边的 `load jdbc` 都可使用受控 partition 参数并编译为 `queryone_mysql`；provider 在 Engine 内解析 alias，不在 SQL 中暴露连接密钥。 |
| RMS-backed ODEP 鉴权 | 两边使用同一套 LogicalPlan 资源提取与 ODEP authz API；Local subject 来自服务端 `TenantContext`，Kyuubi subject 来自 ECDSA session 签名。 |
| 受控 HDFS 文件 load | 两边都只接受 workspace 相对路径，并使用相同内部命令和 driver extension 注册临时视图；`owner` 可选择其他用户 workspace，跨 owner 读取都走 RMS 鉴权。 |
| 固定写入能力矩阵 | 两边都在提交前使用同一个 `WritePlan` 和矩阵；Hive/Doris/JDBC Catalog overwrite 永久拒绝。 |
| Catalog append | Hive/Doris/JDBC Catalog 都要求目标存在、源和目标列名集合一致，并通过显式目标列清单和源列投影写入；类型不兼容在写入前失败。 |
| 文件路径写入边界 | 两边都永久拒绝文件 append 和 external path 写入；受控 HDFS overwrite 使用相同的内部命令和 driver extension。 |

## 有条件对等

| 功能 | local | Kyuubi | 结论 |
| --- | --- | --- | --- |
| 静态 MySQL catalog SQL | 从 `engines.local.catalogs.mysql_static` 注入同名 Catalog。 | 必须配置在 Kyuubi/Spark engine。 | 配置归属不同；两边均允许 `_static` 三段式访问且不走 RMS。 |
| 静态 Doris catalog SQL | 从 `engines.local.catalogs.doris_static` 注入同名 Catalog。 | 必须配置在 Kyuubi/Spark engine。 | 配置归属不同；两边均允许 `_static` 三段式访问且不走 RMS。 |
| 外部 provider，例如 Excel | 可通过 `engines.local.jars.*` 注入进本地 SparkSession。 | 必须放到远端 engine classpath 或 `spark.jars`。 | connector 分发不对等。 |
| Preview | 通过 DataFrame collect。 | 通过 JDBC ResultSet collect。 | 行为接近，schema 类型名可能不同。 |
| 临时视图生命周期 | 跟 QueryOne 进程内 SparkSession 绑定。 | 跟逻辑租户独立的 Kyuubi JDBC connection/session 绑定。 | Kyuubi server/engine 重启后临时视图会丢。 |
| 原生命令保护 | compiler 使用 `StatementIntent` 和 Spark LogicalPlan 白名单。 | 使用相同策略，拒绝后不会提交 JDBC。 | Compile/Run 策略对等。 |
| 受控 HDFS staging overwrite | QueryOne 进程内直接注册 extension，ZK/HDFS 参数来自 local HOCON。 | extension jar 和 SparkConf 必须部署到 Kyuubi Spark engine。 | 执行语义对等，部署归属不对等。 |

## 明确不对等

| 功能 | local | Kyuubi |
| --- | --- | --- |
| 静态 `load jdbc` | `jdbc.\`catalog_static.db.table\`` 复用 Local Catalog；带分片参数走 `queryone_mysql`。 | SQL 语义相同，配置和 provider JAR 位于 Kyuubi/Spark engine。 |
| 静态 `save jdbc` | 走 Local JDBC Catalog SQL。 | 走远端 JDBC Catalog SQL。两边都不接受 SQL OPTIONS。 |
| Catalog append 预检查 | 通过 `spark.catalog.tableExists`、DataFrame schema 和最终 SQL 的 `EXPLAIN` 检查。 | 通过远端 `LIMIT 0` 和最终显式列 `INSERT` 的 `EXPLAIN` 检查。 |
| 写语句断线处理 | 进程内 Spark 执行，不存在 JDBC statement 自动重放。 | `save` statement 永不自动重试；连接异常返回写入状态未知。 |
| `/api/compile` 语义 | local 下编译结果通常可直接执行。 | 若不做 engine-aware 校验，可能出现 Compile 成功但 Run 才发现 Kyuubi 不支持的情况。 |
| 鉴权 subject 信任来源 | 服务端当前 `TenantContext.username`；适合断点调试，不构成生产安全边界。 | Kyuubi operation 的 ECDSA 签名 session user。 |
| 会话基础设施 | 单 SparkSession、全局执行锁，只支持 `tenant_shared`。 | 支持独立 JDBC session、失效恢复、ZooKeeper 服务发现和 `run_isolated`。 |

## 已落地优化

| 优化 | 状态 |
| --- | --- |
| `/api/compile` 做 engine-aware 校验 | 已落地。Local/Kyuubi 使用统一 resolver，动态 JDBC 与静态 JDBC 的编译语义一致。 |
| `/api/config` 暴露 engine capabilities | 已落地。前端可读取 `externalCatalogConfiguredByQueryOne` 等执行能力；固定写入矩阵不作为可变 engine capability 暴露。 |
| Kyuubi compile diagnostics | 已落地第一版。Compile 成功时返回 Kyuubi 配置归属提示，说明 catalog/provider jars 需要在 Kyuubi/Spark engine 侧配置。 |
| Kyuubi-safe MySQL catalog load | 已落地。Kyuubi `load jdbc.\`catalog_static.db.table\`` 不读取 QueryOne 本地 MySQL 密钥；大表 provider 只传 catalog/table/where/partition 参数。 |
| Catalog append 3A | 已落地。Hive/Doris 使用 Spark 3.3+ column list 语法，Local/Kyuubi 都做目标、列名和类型检查；Kyuubi 写 statement 禁止断线重放。 |
| JDBC Catalog append 3B | 已落地。Local/Kyuubi JDBC Catalog 都做目标、列名和类型预检并按列名写入；SQL 不携带连接密钥，overwrite 永久拒绝。 |
| 受控 HDFS overwrite | 已落地。Spark driver 使用目标级 ZK ephemeral lock、固定同级 staging/backup 和 HDFS rename 发布。 |
| 受控 HDFS load | 已落地。文件 provider 的 `load` 只接受 workspace 相对路径并支持只读 `owner`；原生 relation 只开放经 Engine 鉴权的绝对 HDFS/viewfs 路径读取。 |

## 后续优先级

1. 在真实 Kyuubi、HDFS 和 ZooKeeper 环境验证受控 load 的租户隔离，以及 overwrite 的并发拒绝、driver 退出和残留恢复。
2. 文件 append 不进入 MVP 路线；只有出现明确生产案例，并定义 schema、分区、并发与幂等合同后，才单独评估 Parquet/ORC 分区 append 或事务湖表写入。
3. 本地文件、S3、OSS 裸路径写入保持永久拒绝，不增加配置开关。
4. 保持架构原则：QueryOne 不直接替 Kyuubi 管理 Spark/YARN/Hive 执行用户、connector classpath 或 catalog 密钥。
