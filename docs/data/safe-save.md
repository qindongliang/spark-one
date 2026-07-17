# Write Safety

SparkOne 的写入安全策略由内部 `WritePlan` 和固定能力矩阵统一决定。配置只能进一步收紧能力，不能放开矩阵中永久拒绝的写入类型。

## 固定能力矩阵

| 目标类型 | append | overwrite |
| --- | --- | --- |
| Hive Catalog | 允许 | 永久拒绝 |
| Doris Catalog | 允许 | 永久拒绝 |
| MySQL | 允许 | 永久拒绝 |
| 受控 HDFS workspace | 永久拒绝 | 允许，但必须由 staging executor 执行 |
| 本地文件、S3、OSS 等外部路径 | 永久拒绝 | 永久拒绝 |
| 未识别 provider | 永久拒绝 | 永久拒绝 |

旧的全局 overwrite policy、备份策略、protected paths 以及 MySQL/Doris overwrite 开关已经删除，不兼容旧配置。原生 DDL/DML 没有放行开关。

## WritePlan

SparkOne compiler 不再从 `save` 直接拼接最终 SQL，而是先生成 `WritePlan`：

```text
tenant
mode
sourceTable
target(kind, identifier, provider, connectionOptions)
format
providerOptions
partitionColumns
executionType
```

处理顺序固定为：

```text
SAVE AST -> WritePlanner -> WriteCapabilityMatrix -> engine/runtime schema preflight -> CatalogWriteSqlRenderer
```

`tenant` 来自服务端登录 session。页面用户名登录只是开发测试阶段的逻辑租户选择；Kyuubi/Spark 仍使用启动配置中的固定 keytab 服务账号执行，租户身份只用于 SparkOne 权限决策和 workspace 计算。

## 当前实现状态

第二阶段、3A、3B 及受控 HDFS overwrite 已经实现：

- Hive、Doris、MySQL append 生成对应的 catalog SQL 或 runtime adapter 计划。
- Hive、Doris、MySQL overwrite 在编译阶段永久拒绝。
- 所有文件 append 在编译阶段永久拒绝；裸文件 append 不进入 MVP 路线。
- 绝对路径、带 scheme 的路径以及本地/S3/OSS 等外部路径 append 和 overwrite 都在编译阶段永久拒绝。
- 未识别 provider 的 append 和 overwrite 均在编译阶段拒绝。
- 每条语句携带 `StatementIntent`；原生 SQL 只允许查询和只读检查命令，其他 Spark command 在编译阶段默认拒绝。
- Hive、Doris、Kyuubi MySQL Catalog append 在 runtime 统一生成 `INSERT INTO TABLE target (目标列...) SELECT 源列...`，显式按目标列顺序投影，源列顺序不影响目标映射。
- Local MySQL append 通过 JDBC adapter 读取目标 schema，并在 DataFrame JDBC write 前按相同规则重排和转换源列。
- Hive、Doris、MySQL append 要求目标存在且源和目标列名集合完全一致；缺列、多列、重名列或类型不兼容均在写入前失败。
- Local MySQL 使用 `mysql.\`connection.table\`` 和 HOCON 数据源；Kyuubi MySQL 使用 `mysql.\`catalog.database.table\`` 和远端 JDBC Catalog。Kyuubi save 不接受 SQL `OPTIONS`，两条路径都不会把 URL、用户名或密码放进 SQL。
- Kyuubi 在 Catalog append 前依次检查目标 schema、源 schema，并对最终显式列 `INSERT` 执行 `EXPLAIN`；任何一步失败都不会提交写语句。
- Catalog append 使用 Spark 3.3 已支持的 column list 语法，不做 Spark 版本分支；当前 Kyuubi 远端支持范围是 Spark 3.3.x–3.5.x。
- Kyuubi `save` 写语句遇到连接异常时不会自动重试。错误会明确提示写入状态未知，需要人工核查目标后再决定是否重提。
- 受控 HDFS overwrite 编译为 SparkOne 内部命令，由独立 Spark extension 在 Spark driver 内完成 ZK 排他、staging 写入、发布、回滚和清理。
- 文件 append 以及本地/S3/OSS 写入仍属于固定策略拒绝，不提供 SQL option 或 HOCON 放行开关。

Hive、Doris、MySQL append 的受控执行顺序为：

```text
确认目标存在 -> 比较源/目标列名集合 -> 按目标顺序生成显式列 SQL -> 分析类型兼容 -> INSERT
```

Kyuubi 的前三步都是只读预检，可以在连接失效时重连一次；最后的 `INSERT` 永不自动重试。Local MySQL 最终 JDBC write 同样不做 SparkOne 层自动重放。

## 受控 HDFS workspace

开发阶段每个逻辑租户的 workspace 固定为：

```text
/public/sparkone/user/${username}
```

DSL 文件写入只接受相对路径，例如：

```sql
save overwrite city_stats as parquet.`reports/daily`;
```

Spark extension 会把它解析到：

```text
/public/sparkone/user/${username}/reports/daily
```

相对路径必须逐段校验。空路径、绝对路径、URI scheme、authority、query、fragment、百分号编码、反斜杠、空段、`.` 和 `..` 都不能进入受控 workspace。下面这些目标不会被分类为受控 HDFS：

```text
/public/sparkone/user/alice/reports
hdfs:///public/sparkone/user/alice/reports
file:///tmp/reports
s3a://bucket/reports
../reports
reports/../daily
```

客户端不能直接提交完整 workspace 路径，也不能提交其他用户名。最终绝对路径只能由服务端根据当前 `TenantContext` 计算。

## HDFS overwrite 执行链路

以租户 `alice` 和目标 `reports/daily` 为例，Spark driver 内的路径为：

```text
final   = /public/sparkone/user/alice/reports/daily
work    = /public/sparkone/user/alice/reports/.sparkone-overwrite-<targetHash>
staging = <work>/staging
backup  = <work>/backup
lock    = /sparkone/overwrite/alice/reports~daily--<qualifiedFinalPathSha256>
value   = operationId=<uuid> + qualified target
```

执行顺序固定为：

```text
创建目标级 ZK ephemeral node
-> 恢复或清理同目标上次中断留下的固定 work 目录
-> DataFrameWriter overwrite 到 staging
-> final rename 到 backup（final 已存在时）
-> staging rename 到 final
-> 删除 backup/work
-> 释放 ZK node 和 session
```

锁节点按“租户父节点 + 可读相对路径 + 完整 qualified path SHA-256”组织。可读路径只用于展示，会清理特殊字符并限制长度；锁唯一性仍完全由最终 qualified HDFS path 的完整 hash 保证。临时节点 value 只保存 `operationId` 和完整 qualified target，租户可从节点层级识别。

锁粒度是最终 qualified HDFS path：同一目标并发 overwrite 立即失败，不同目标互不影响。冲突错误会返回现有锁的 `lockPath`、`operationId` 和 `target`。锁覆盖 staging、发布、回滚和清理全过程；Spark driver 退出或 session 丢失后 ephemeral node 由 ZooKeeper 删除。固定 work 目录不会无限增长，下次取得锁的任务会先恢复 backup，再清理残留 staging。

staging 位于正式目标的同级隐藏目录，不会被读取正式 `final` path 的查询命中。发布依赖同一 HDFS FileSystem 内的 rename；`workspaceRoot` 不应跨文件系统或指向 S3/OSS 等对象存储。

ZK 地址、ZK root、workspace root 和 HDFS 认证均由平台配置注入，不能通过 `SAVE OPTIONS` 传入。`path/url/user/password/token/access key/fs.*` 等敏感或重定向类 option 会被拒绝。

## 文件 append 产品边界

SparkOne MVP 不提供 Parquet、ORC、CSV、JSON、Text、Excel 等裸文件或目录 append。增量写入优先落到 Hive、Doris、MySQL 等受治理表；Delta、Iceberg、Hudi 或 Structured Streaming 等需求应按事务表或流式 sink 单独设计，不能复用通用裸路径 append。

只有出现明确生产案例后才重新评估文件 append，且至少需要同时明确：目标格式与分区约束、schema 合同、并发写策略、失败重跑幂等语义和存储系统 committer。即使重新评估，也应优先限定为 Parquet/ORC 分区或事务湖表，不开放 CSV、Excel、本地文件以及任意 S3/OSS 裸路径 append。

## 原生 SQL 旁路

原生 SQL 只允许 `SELECT/WITH` 查询，以及 `SHOW/DESCRIBE/EXPLAIN/USE` 等只读检查命令。用户必须通过 SparkOne `load/view/set/save` 进入受控执行意图；其他命令在 Compile 和 Run 前统一拒绝。

永久拒绝范围包括：

- `INSERT INTO`、`INSERT OVERWRITE`、`INSERT DIRECTORY`
- CTAS、REPLACE TABLE AS SELECT
- DataSource V2 append/overwrite
- `LOAD DATA`、`TRUNCATE TABLE`
- `DELETE`、`UPDATE`、`MERGE`
- `CREATE/DROP/ALTER/RENAME/REPLACE TABLE`
- `CREATE/DROP DATABASE`、namespace、view、function、index
- 分区变更、`ANALYZE TABLE`、`REPAIR TABLE`
- 原生 `SET/RESET` 和其他未明确允许的 Spark command

判断基于 Spark `SparkSqlParser` 的 LogicalPlan 和 compiler 生成的 `StatementIntent`，不依赖 SQL 字符串前缀。SparkOne `load/view` 内部生成的临时视图命令只有携带对应 intent 时才能执行，用户直接提交原生 `CREATE VIEW` 仍会被拒绝。

Hive、Doris、MySQL 目标表必须由平台外的 catalog/数据库治理入口预建。SparkOne 不提供建表、删表或改表结构入口。

## 本阶段验证

允许的 catalog append：

```sql
save append source_view as hive.`default.target_table`;
save append source_view as doris.`app.target_table`;
save append source_view as mysql.`analytics.target_table`;
-- Kyuubi engine 使用三段式远端 Catalog 路径：
save append source_view as mysql.`analytics.app.target_table`;
```

Hive、Doris、Kyuubi MySQL 的 Compile 结果是无副作用的安全占位 SQL；Run 取得 schema 后生成类似下面的最终按列名写入 SQL：

```sql
INSERT INTO TABLE default.target_table (`city`, `cnt`) SELECT `city`, `cnt` FROM source_view;
INSERT INTO TABLE doris.app.target_table (`city`, `cnt`) SELECT `city`, `cnt` FROM source_view;
INSERT INTO TABLE analytics.app.target_table (`city`, `cnt`) SELECT `city`, `cnt` FROM source_view;
```

应在编译阶段永久拒绝：

```sql
save overwrite source_view as hive.`default.target_table`;
save overwrite source_view as doris.`app.target_table`;
save overwrite source_view as mysql.`analytics.target_table`;
save append source_view as parquet.`reports/daily`;
save append source_view as parquet.`s3a://bucket/target`;
save overwrite source_view as parquet.`/tmp/target`;
save overwrite source_view as parquet.`s3a://bucket/target`;
```

应编译并执行为受控 HDFS overwrite：

```sql
save overwrite source_view as parquet.`reports/daily`;
```

应在编译阶段拒绝原生 DDL/DML：

```sql
insert into default.target_table select * from source_view;
insert overwrite directory '/tmp/result' using parquet select * from source_view;
create table default.result using parquet as select * from source_view;
truncate table default.target_table;
create table default.target_table (id int) using parquet;
drop table default.target_table;
alter table default.target_table add columns (name string);
```
