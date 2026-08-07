# Save 测试

第二阶段已经统一使用 `WritePlan` 和固定能力矩阵。Hive、Doris、MySQL 只允许 append；这些 catalog/数据库目标的 overwrite 永久拒绝。所有文件 append 以及本地/S3/OSS external path 读写永久拒绝；受控 HDFS load/overwrite 由 Spark driver extension 执行。

测试原生 DDL/DML 是否被拦截。下面每条语句应分别点击 Compile，且都应失败：

```sql
insert overwrite directory '/tmp/queryone_native_insert_overwrite_blocked'
using parquet
select 1 as id;

create table if not exists default.queryone_drop_table_blocked (
  id int
)
using parquet;

drop table default.queryone_drop_table_blocked;

alter table default.queryone_drop_table_blocked add columns (name string);
```

预期：错误包含 `only allows native read-only SQL`。原生 `INSERT INTO`、CTAS、`TRUNCATE`、`DELETE`、`UPDATE`、`MERGE`、数据库/view/function/index DDL 和原生 `SET/RESET` 也永久拒绝，不存在放行配置。

测试 external path overwrite 永久拒绝：

```sql
view city_stats as
select city, count(*) as cnt
from users
group by city;

save overwrite city_stats as parquet.`/tmp/city_stats_parquet`;
```

预期 Compile 失败，错误包含 `external-path` 和 `permanently denied`。

测试文件 append 永久拒绝：

```sql
save append city_stats as parquet.`reports/city_stats`;
save append city_stats as parquet.`s3a://bucket/reports/city_stats`;
```

两条语句都应 Compile 失败并包含 `permanently denied`；第一条目标类型是 `managed-hdfs`，第二条是 `external-path`。文件 append 不存在待开启的 executor 或配置开关。

测试相对路径被识别为受控 HDFS：

```sql
save overwrite city_stats as parquet.`reports/city_stats`;
```

预期 Compile 成功，页面展示以下可读摘要，不应出现内部 Base64 payload：

```text
MANAGED HDFS OVERWRITE
  tenant: <当前登录用户>
  source: city_stats
  format: parquet
  target: reports/city_stats
  options: {}
```

实际执行仍使用版本化内部命令。Run 会把目标解析为 `/public/odep/user/${username}/reports/city_stats`；客户端不能提交绝对 workspace 路径，也不能通过 `options owner` 指定其他用户。Local 必须先配置 `engines.local.overwrite.zkConnect`，Kyuubi 必须部署 extension jar 并配置 `spark.sql.extensions` 和 `spark.queryone.overwrite.*`。

第一次 Run 后通过同一相对路径读取：

```sql
load parquet.`reports/city_stats` as saved_city_stats;

select *
from saved_city_stats
order by city;
```

Compile 应显示 `MANAGED HDFS LOAD` 摘要，Run 后只包含本次结果。修改 `city_stats` 后再次 overwrite，再重新执行 load，路径中应只包含第二次完整结果。执行期间，同目标的第二个 overwrite 应失败并包含 `already running`，同时显示占用锁的 `operationId`、`target` 和包含租户的 ZK `lockPath`；不同目标应可并发。成功或明确失败后，正式目录同级不应残留 `.queryone-overwrite-*`；模拟 driver 中断留下 work 目录时，下次取得锁的 overwrite 应先恢复/清理残留再执行。

绝对 HDFS 路径只读 relation 已开放，Local 和 Kyuubi Engine 都走 ODEP/RMS `hdfs read` 鉴权。Local 使用 `TenantContext` subject，Kyuubi 使用签名 session user：

```sql
select * from parquet.`/public/odep/user/alice/reports/city_stats`;
```

下面两条仍应在 Compile 阶段失败：

```sql
view bypass as select * from parquet.`reports/city_stats`;
load parquet.`../bob/reports/city_stats` as bypass;
```

读取其他用户 workspace 的推荐写法是 `load ... options owner="alice"`；写入始终只能使用当前用户自己的相对路径，`save options owner=...` 会拒绝。原生绝对文件路径写入在 QueryOne 和 Kyuubi Engine 两层都拒绝。

本地文件、S3、OSS 裸路径的 append/overwrite 都保持永久拒绝。未来只有出现明确生产案例并定义 schema、分区、并发和失败重跑幂等合同后，才单独评估 Parquet/ORC 分区 append 或事务湖表写入，不恢复 CSV、Excel 或任意裸路径 append。

Hive/catalog 表 append 要求目标表先存在。请先通过平台外 Hive/catalog 管理入口准备下面三个目标表，不能在 QueryOne 编辑器执行建表语句：

- `default.queryone_save_hive_append(city string, cnt bigint)`
- `default.queryone_save_hive_overwrite_block(id int, name string)`
- `default.queryone_save_hive_partition(city string, cnt bigint)`，按 `dt string` 分区

下面的 case 只包含 QueryOne 允许的语句。

Case 1：append 写入 Hive 表。

```sql
view queryone_hive_append_data as
select * from values
  (3L, 'beijing'),
  (2L, 'shanghai')
as queryone_hive_append_data(cnt, city);

save append queryone_hive_append_data
as hive.`default.queryone_save_hive_append`;

select city, cnt
from default.queryone_save_hive_append
order by city;
```

预期：查询结果有 `beijing=3`、`shanghai=2` 两行。源视图列顺序是 `cnt, city`，与目标表的 `city, cnt` 相反，用于确认 append 按列名而非位置写入。

Case 2：Hive overwrite 永久拒绝。

```sql
view queryone_hive_overwrite_seed as
select * from values
  (1, 'old')
as queryone_hive_overwrite_seed(id, name);

save append queryone_hive_overwrite_seed
as hive.`default.queryone_save_hive_overwrite_block`;

view queryone_hive_overwrite_new as
select * from values
  (2, 'new')
as queryone_hive_overwrite_new(id, name);

save overwrite queryone_hive_overwrite_new
as hive.`default.queryone_save_hive_overwrite_block`;
```

预期：最后一条 Compile/Run 在写入前失败，错误包含 `hive-catalog` 和 `permanently denied`。配置和 SQL option 都不能放开。

Case 3：动态分区 append。

```sql
view queryone_hive_partition_data as
select * from values
  ('beijing', 3L, '2026-06-10'),
  ('shanghai', 2L, '2026-06-10'),
  ('hangzhou', 1L, '2026-06-11')
as queryone_hive_partition_data(city, cnt, dt);

save append queryone_hive_partition_data
as hive.`default.queryone_save_hive_partition`
partitionBy dt;

select city, cnt, dt
from default.queryone_save_hive_partition
order by dt, city;
```

预期：查询结果有 3 行，并按 `dt` 写入两个动态分区。`partitionBy dt` 会编译成 Spark SQL 的 `PARTITION (dt)`，分区列必须在源视图字段中。

Case 4：缺列时在写入前失败。

```sql
view queryone_hive_missing_column as
select 'should-not-write' as city;

save append queryone_hive_missing_column
as hive.`default.queryone_save_hive_append`;
```

预期：错误包含 `must match target columns by name`，目标表不会新增 `should-not-write` 数据。

说明：

- `save append ... as hive` 会先编译成 `WritePlan`，Run 时根据目标 schema 生成 Spark 3.3+ 支持的显式 column list `INSERT INTO TABLE ... (...) SELECT ...`。
- `save append ... as hive` 要求目标表已存在；QueryOne 不会自动创建 Hive 表，建表格式、分区定义和表结构由平台外 catalog 治理入口维护。
- 源和目标列名集合必须完全一致；列顺序可以不同，缺列、多列、重名列或不兼容类型都会在真正写入前失败。
- `partitionBy` 对应 Spark SQL 的动态分区 `PARTITION (...)`，分区列需要出现在源视图字段中。
- Hive、Doris、MySQL overwrite 永久拒绝；完整矩阵见 [../../data/safe-save.md](../../data/safe-save.md)。
