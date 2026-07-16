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

## 当前阶段状态

第二阶段、3A 及 3B 已经实现：

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

当前尚未开放：

- 受控 HDFS staging overwrite executor。

因此，只有受控 HDFS overwrite 属于“矩阵允许但 executor 尚未实现”的能力，当前会 fail closed 并提示 staging executor 尚未就绪。文件 append 以及本地/S3/OSS 写入属于固定策略拒绝，不提供 SQL option 或 HOCON 放行开关。

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

后续 staging executor 会把它解析到：

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

应识别为受控 HDFS，但因 executor 尚未开放而拒绝：

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
