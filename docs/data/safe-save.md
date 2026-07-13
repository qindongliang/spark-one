# Write Safety

SparkOne 的写入安全策略由内部 `WritePlan` 和固定能力矩阵统一决定。配置只能进一步收紧能力，不能放开矩阵中永久拒绝的 overwrite 类型。

## 固定能力矩阵

| 目标类型 | append | overwrite |
| --- | --- | --- |
| Hive Catalog | 允许 | 永久拒绝 |
| Doris Catalog | 允许 | 永久拒绝 |
| MySQL | 允许 | 永久拒绝 |
| 受控 HDFS workspace | 允许 | 允许，但必须由 staging executor 执行 |
| 本地文件、S3、OSS 等外部路径 | 允许 | 永久拒绝 |
| 未识别 provider | 默认拒绝 | 永久拒绝 |

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
SAVE AST -> WritePlanner -> WriteCapabilityMatrix -> WriteSqlRenderer -> engine/runtime
```

`tenant` 来自服务端登录 session。页面用户名登录只是开发测试阶段的逻辑租户选择；Kyuubi/Spark 仍使用启动配置中的固定 keytab 服务账号执行，租户身份只用于 SparkOne 权限决策和 workspace 计算。

## 当前阶段状态

第二阶段已经实现：

- Hive、Doris、MySQL append 生成对应的 catalog SQL 或 runtime adapter 计划。
- Hive、Doris、MySQL overwrite 在编译阶段永久拒绝。
- 绝对路径、带 scheme 的路径以及本地/S3/OSS 等外部路径 overwrite 在编译阶段永久拒绝。
- 未识别 provider 的 append 和 overwrite 均在编译阶段拒绝。
- 每条语句携带 `StatementIntent`；原生 SQL 只允许查询和只读检查命令，其他 Spark command 在编译阶段默认拒绝。

第二阶段尚未开放：

- 文件 append executor。
- 受控 HDFS staging overwrite executor。
- append 的按列名映射和 schema 兼容检查。

因此，矩阵中“允许”的文件 append 和受控 HDFS overwrite 当前仍会 fail closed，并明确提示对应 executor 尚未实现。这不是配置问题，不能通过 SQL option 或 HOCON 放开。

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
```

应在编译阶段永久拒绝：

```sql
save overwrite source_view as hive.`default.target_table`;
save overwrite source_view as doris.`app.target_table`;
save overwrite source_view as mysql.`analytics.target_table`;
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
