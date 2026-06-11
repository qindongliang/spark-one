# SQL 编辑器测试手册

这个页面是 SparkOne MVP 的本地测试台，用来快速验证 Spark SQL、SparkOne 薄 DSL 转译、HDFS/Hive 配置和数据源读写。

访问地址通常是：

```text
http://127.0.0.1:7070
```

## 页面区域

- 左侧编辑器：输入一段 SQL 脚本，可以包含多条语句，用分号 `;` 分隔。
- `Compile`：只编译，不执行。适合检查 `load/save` 这类 SparkOne DSL 被转成了什么 Spark SQL。
- `Run`：编译后按顺序执行每条 SQL，后面的语句可以使用前面创建的临时视图。
- 选中执行：如果编辑器里有选中的 SQL，`Compile` 和 `Run` 只处理选中部分；没有选区时处理整篇脚本。
- `Run` 默认隐藏每条 statement 的编译后 SQL；如果需要调试转译结果，在 `conf/sparkone.conf` 里配置 `server.showCompiledSql = true`。
- `Rows`：控制每条查询最多展示多少行，服务端会限制在 `1` 到 `1000`。
- 右侧结果区：展示每条语句的编译后 SQL、耗时、schema 和结果数据；失败语句会显示错误信息。

## 基础冒烟测试

最小 SQL：

```sql
select 1 as id;
```

查看当前 catalog：

```sql
show databases;
show tables;
```

如果已经启用 Hive，可以指定库：

```sql
show tables in default;
```

## 多语句上下文

同一次 `Run` 内，多条 SQL 会在同一个 `SparkSession` 中顺序执行。因此可以先创建临时视图，再查询它：

```sql
view users as
select * from values
  ('beijing', 1),
  ('shanghai', 2),
  ('beijing', 3)
as users(city, id);

view city_stats as
select city, count(*) as cnt
from users
group by city;

select * from city_stats order by city;
```

页面服务不重启时，临时视图会留在当前本地 Spark 会话里；服务重启后临时视图会消失。

## Spark SQL 原生能力

普通 Spark SQL 会原样交给 Spark 执行。常用测试：

```sql
select current_date() as dt, current_timestamp() as ts;
```

```sql
select city, count(*) as cnt
from users
group by city
having count(*) > 1;
```

```sql
with city_stats as (
  select city, count(*) as cnt
  from users
  group by city
)
select * from city_stats order by cnt desc;
```

`WITH` 是查询内的临时 CTE，只在当前这一条 SQL 内有效；`CREATE OR REPLACE TEMPORARY VIEW` 会把结果注册到当前 Spark 会话，后续语句可以继续引用。

## View As 语法糖

SparkOne 支持 `view name as select ...` 语法糖，用于把查询结果注册成当前 Spark 会话里的临时视图，避免反复书写 `CREATE OR REPLACE TEMPORARY VIEW`：

```sql
view city_stats as
select city, count(*) as cnt
from users
group by city;

select * from city_stats order by cnt desc;
```

它等价于：

```sql
CREATE OR REPLACE TEMPORARY VIEW city_stats AS
select city, count(*) as cnt
from users
group by city;
```

注意：

- SparkOne 不再支持尾部 `select ... as table` 语法糖，避免跟 Spark 原生列别名、表别名产生歧义。
- `view myview as select current_date() as dt, current_timestamp() as ts` 会注册成 `myview`，其中 `as dt`、`as ts` 都是字段别名。
- `select * from users as u` 这种 Spark 原生表别名会保持原样；`u` 只是本条查询内的别名，不会注册成临时视图。
- 生成的目标统一使用 `CREATE OR REPLACE TEMPORARY VIEW`，表示只在当前 Spark 会话内有效。

原生别名和 `view` 语法糖可以混用：

```sql
view joined_orders as
select u.id, o.order_id
from users as u
join orders as o on u.id = o.user_id;
```

这里 `as u`、`as o` 是 Spark SQL 的表别名，`joined_orders` 才是 SparkOne 临时视图名。

## 使用 SparkOne Load DSL

`load` 是 SparkOne 提供的薄 DSL，目的是让加载数据更接近 MLSQL 写法。推荐先点 `Compile` 看转译结果。

路径说明：

- 类似 `load csv` 使用 `/tmp/users.csv` 这种没有 scheme 的绝对路径时，会交给 Spark/Hadoop 按 `fs.defaultFS` 解析。
- 如果已经加载 Hadoop 配置，且 `fs.defaultFS=hdfs://nameservice1`，`/tmp/users.csv` 默认就是 HDFS 上的 `hdfs://nameservice1/tmp/users.csv`。
- 只有显式写 `file:///tmp/users.csv`，才表示 Spark driver 所在机器的本地文件。
- `hdfs:///tmp/users.csv` 是显式 HDFS 路径，适合在文档或脚本里避免歧义。

CSV（默认按 `fs.defaultFS` 解析；在测试环境里通常是 HDFS）：

```sql
load csv.`/tmp/users.csv`
options header="true" and inferSchema="true"
as users;

select * from users limit 20;
```

它会编译成类似：

```sql
CREATE OR REPLACE TEMPORARY VIEW users
USING csv
OPTIONS (path '/tmp/users.csv', header 'true', inferSchema 'true');
```

本地文件：

```sql
load csv.`file:///tmp/users.csv`
options header="true" and inferSchema="true"
as local_users;

select * from local_users limit 20;
```

Parquet：

```sql
load parquet.`/tmp/users_parquet` as users_parquet;

select * from users_parquet limit 20;
```

JSON：

页面内造 JSON 数据并生成 view：

```sql
view raw_events_json as
select * from values
  ('{"event_id":"e001","event_type":"click","city":"beijing","amount":12.50,"created_at":"2026-06-09 10:00:00"}'),
  ('{"event_id":"e002","event_type":"view","city":"shanghai","amount":0.00,"created_at":"2026-06-09 10:05:00"}'),
  ('{"event_id":"e003","event_type":"click","city":"beijing","amount":18.80,"created_at":"2026-06-09 10:08:00"}'),
  ('{"event_id":"e004","event_type":"pay","city":"shenzhen","amount":99.90,"created_at":"2026-06-09 10:20:00"}')
as raw_events_json(raw_json);

view events_from_json as
select
  event.event_id,
  event.event_type,
  event.city,
  event.amount,
  event.created_at
from (
  select from_json(
    raw_json,
    'event_id STRING, event_type STRING, city STRING, amount DOUBLE, created_at TIMESTAMP'
  ) as event
  from raw_events_json
) parsed;

select city, event_type, count(*) as cnt, sum(amount) as total_amount
from events_from_json
group by city, event_type
order by city, event_type;
```

这个例子不依赖外部文件，适合直接在页面里验证 `view` 语法糖、多语句上下文和 Spark 原生 `from_json` 函数。

HDFS/defaultFS 上的 JSON Lines：

```sql
load json.`/tmp/events.json` as events;

select * from events limit 20;
```

本地 JSON Lines：

```sql
load json.`file:///Users/qindongliang/Downloads/events.json`
as local_events;

select * from local_events limit 20;
```

多行 JSON 文件：

```sql
load json.`/tmp/events_pretty.json`
options multiLine="true"
as pretty_events;

select * from pretty_events limit 20;
```

显式推断 schema：

```sql
load json.`/tmp/events.json`
options inferSchema="true"
as inferred_events;

select event_type, count(*) as cnt
from inferred_events
group by event_type
order by cnt desc;
```

指定 schema 并过滤脏数据：

```sql
load json.`/tmp/events.json`
options schema="event_id STRING, event_type STRING, amount DOUBLE, created_at TIMESTAMP"
and mode="PERMISSIVE"
and timestampFormat="yyyy-MM-dd HH:mm:ss"
as typed_events;

select event_type, sum(amount) as total_amount
from typed_events
group by event_type;
```

Hive 表：

```sql
load hive.`default.some_table` as some_table;

select * from some_table limit 20;
```

`hive` 是特殊 catalog 语义，会编译成：

```sql
CREATE OR REPLACE TEMPORARY VIEW some_table AS
SELECT * FROM default.some_table;
```

MySQL：

先在 `conf/sparkone.conf` 配置连接，SQL 里只引用连接名：

```hocon
datasources.mysql.analytics {
  url = "jdbc:mysql://192.168.1.179:3306/Dworks?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
  driver = "com.mysql.cj.jdbc.Driver"
  user = "root"
  password = "******"

  options {
    fetchsize = 1000
    batchsize = 1000
  }
}
```

运行时还需要 MySQL JDBC driver 在 classpath 中，可以在 HOCON 里选择 `packages` 或本地 JAR：

```hocon
jars {
  packages = "com.mysql:mysql-connector-j:8.4.0"
  # jars = "/Users/qindongliang/.m2/repository/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar"
}
```

先在 MySQL 客户端准备测试表和数据：

```sql
CREATE DATABASE IF NOT EXISTS Dworks
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE Dworks;

DROP TABLE IF EXISTS sparkone_mysql_seed;
CREATE TABLE sparkone_mysql_seed (
  id BIGINT PRIMARY KEY,
  city VARCHAR(64) NOT NULL,
  cnt BIGINT NOT NULL,
  biz_date DATE NOT NULL
);

INSERT INTO sparkone_mysql_seed (id, city, cnt, biz_date) VALUES
  (1, 'beijing', 10, '2026-06-01'),
  (2, 'shanghai', 20, '2026-06-01'),
  (3, 'beijing', 30, '2026-06-02'),
  (4, 'hangzhou', 40, '2026-06-02');

DROP TABLE IF EXISTS sparkone_city_result;
CREATE TABLE sparkone_city_result (
  city VARCHAR(64) NOT NULL,
  row_count BIGINT NOT NULL,
  total_cnt BIGINT NOT NULL
);
```

在 SparkOne 编辑器里读取 MySQL 表：

```sql
load mysql.`analytics.sparkone_mysql_seed` as mysql_seed;

select * from mysql_seed order by id;
```

`load mysql` 会在运行时用 Spark JDBC reader 注册临时视图。`Compile` 只展示安全占位 SQL，不展示 HOCON 里的账号密码：

```sql
SELECT 'LOAD MYSQL' AS sparkone_action, 'sparkone_mysql_seed AS mysql_seed' AS sparkone_target
```

测试 `save append` 写入 MySQL：

```sql
load mysql.`analytics.sparkone_mysql_seed` as mysql_seed;

view mysql_city_stats as
select
  city,
  count(*) as row_count,
  sum(cnt) as total_cnt
from mysql_seed
group by city;

save append mysql_city_stats
as mysql.`analytics.sparkone_city_result`
options batchsize="500";

load mysql.`analytics.sparkone_city_result` as mysql_saved_result;

select * from mysql_saved_result order by city;
```

预期可以看到 `beijing/shanghai/hangzhou` 的聚合结果。重复执行 append 会重复追加数据，这是 Spark JDBC append 的正常语义。

测试 `save overwrite` 覆盖 MySQL：

MySQL overwrite 默认被启动级安全开关拦截。确认要测试覆盖写时，先在 `conf/sparkone.conf` 的 `save` 中打开：

```hocon
save {
  allowMysqlOverwrite = true
}
```

然后在编辑器里执行：

```sql
view mysql_overwrite_result as
select
  "overwrite_city" as city,
  cast(1 as bigint) as row_count,
  cast(999 as bigint) as total_cnt;

save overwrite mysql_overwrite_result
as mysql.`analytics.sparkone_city_result`
options sparkoneOverwrite="allow"
and truncate="true";

load mysql.`analytics.sparkone_city_result` as mysql_overwritten_result;

select * from mysql_overwritten_result order by city;
```

预期 `sparkone_city_result` 中只剩 `overwrite_city` 这一行。`truncate="true"` 表示 Spark JDBC 会尽量复用已有表结构并清空数据；如果 schema 不兼容或 dialect 不支持，Spark JDBC 仍可能退化为 drop/recreate 之类行为，所以 SparkOne 默认禁止 MySQL overwrite。

说明：

- SparkOne DSL 不支持 `load/save jdbc`；MySQL 统一使用 `load/save mysql`。
- `mysql.\`analytics.sparkone_mysql_seed\`` 中 `analytics` 是 HOCON 连接名，`sparkone_mysql_seed` 是 MySQL 表名。
- SQL 里的 `options` 只能补充 `fetchsize`、`batchsize`、`truncate` 等非连接参数，不能覆盖 `url/user/password/driver/dbtable`。
- `save overwrite ... as mysql` 需要先用 HOCON 打开 `save.allowMysqlOverwrite = true`，再用单条 SQL 的 `sparkoneOverwrite="allow"` 显式确认；SparkOne 不会对 MySQL 表做备份。
- `load` 只是注册临时视图，不负责限制结果行数；需要抽样查看时，在后续 `select * from mysql_seed limit 10` 中限制。

## 使用 SparkOne Save DSL

文件类 save 当前只支持 `save overwrite`。它会转成 Spark SQL 的 `INSERT OVERWRITE DIRECTORY`。

为了避免路径写错时覆盖已有目录，默认 `conf/sparkone.conf` 使用：

```hocon
save {
  overwritePolicy = "requireExplicit"
  overwriteBackup = "rename"
  overwriteBackupPath = "/tmp/sparkone_back"
  allowNativeInsertOverwrite = false
  allowNativeDropTable = false
  # 可选：全局保护危险 overwrite 边界路径，命中后不能被 SQL 覆盖。
  # 规则：禁止覆盖这些路径本身以及它们的上级目录；允许覆盖其下更具体的业务目录。
  # 支持整段通配符 "*"：例如 "/*" 保护所有一级目录，"/*/*" 保护所有一级和二级目录。
  # overwriteProtectedPaths = [
  #   "/",
  #   "/user",
  #   "/tmp",
  # ]
}
```

因此覆盖写需要在当前 `save` 语句里显式确认：

```sql
save overwrite city_stats as parquet.`/tmp/city_stats_parquet`
options sparkoneOverwrite="allow";
```

如果目标路径已经存在，`rename` 会先把原目录移动到 `/tmp/sparkone_back` 目录下；如果后续写入失败，SparkOne 会尝试把备份恢复回原路径。

原生 Spark SQL `INSERT OVERWRITE` 默认会被拦截，因为它不会携带 SparkOne save metadata，无法进入 Safe Save 备份流程。需要兼容历史脚本时，才把 `allowNativeInsertOverwrite` 改为 `true`。

测试原生 `INSERT OVERWRITE` 拦截是否生效：

```sql
insert overwrite directory '/tmp/sparkone_native_insert_overwrite_blocked'
using parquet
select 1 as id;
```

预期：执行失败，错误信息包含 `Native Spark SQL INSERT OVERWRITE is disabled`。目标目录不会被写出。

推荐改成 SparkOne DSL：

```sql
view sparkone_save_replacement as
select 1 as id;

save overwrite sparkone_save_replacement
as parquet.`/tmp/sparkone_native_insert_overwrite_blocked`
options sparkoneOverwrite="allow";
```

如果临时需要验证兼容模式，先在 `conf/sparkone.conf` 中配置并重启服务：

```hocon
save {
  allowNativeInsertOverwrite = true
}
```

然后再次执行原生 `INSERT OVERWRITE`，预期可以成功。但此时不会走 SparkOne Safe Save 的备份和保护路径逻辑。

原生 Spark SQL `DROP TABLE` 默认也会被拦截，避免误删 Hive/catalog 表：

```sql
create table if not exists default.sparkone_drop_table_blocked (
  id int
)
using parquet;

drop table default.sparkone_drop_table_blocked;
```

预期：第二条语句失败，错误信息包含 `Native Spark SQL DROP TABLE is disabled`。

下面这种页面内 `SET` 不会放开 `DROP TABLE`，因为危险 DDL 开关只允许启动配置生效：

```sql
set sparkone.save.native.dropTable.enabled=true;

drop table default.sparkone_drop_table_blocked;
```

如果测试环境确实需要执行原生删表，先在 `conf/sparkone.conf` 中配置并重启服务：

```hocon
save {
  allowNativeDropTable = true
}
```

如果配置了 `overwriteProtectedPaths` 且包含 `/tmp`，只会拦截覆盖 `/tmp` 本身；下面这些 `/tmp/...` 子目录示例仍可以作为测试路径。

保存成 Parquet：

```sql
view city_stats as
select city, count(*) as cnt
from users
group by city;

save overwrite city_stats as parquet.`/tmp/city_stats_parquet`
options sparkoneOverwrite="allow";
```

保存成 CSV：

```sql
save overwrite city_stats as csv.`/tmp/city_stats_csv`
options header="true"
and sparkoneOverwrite="allow";
```

单条语句可覆盖全局备份策略：

```sql
save overwrite city_stats as json.`/tmp/city_stats_json`
options sparkoneOverwrite="allow"
and sparkoneOverwriteBackup="trash";
```

`sparkoneOverwriteBackup` 支持：

- `rename`：默认值，先重命名备份，写失败时尝试恢复。
- `trash`：写入前移动到 Hadoop Trash，不做自动恢复。
- `none`：不备份，直接交给 Spark 覆盖，生产环境不建议使用。

更多 Safe Save 测试案例见 [safe-save.md](safe-save.md)。

当前不支持：

```sql
save append city_stats as parquet.`/tmp/city_stats_parquet`;
save ignore city_stats as parquet.`/tmp/city_stats_parquet`;
save errorifexists city_stats as parquet.`/tmp/city_stats_parquet`;
```

这些模式会在编译阶段报错，后续需要时再扩展 compiler。

Hive/catalog 表 save 支持 `append` 和 `overwrite`，目标表需要先存在。下面这些 case 都用页面内联数据构造视图，方便直接复制执行。

Case 1：append 写入 Hive 表。

```sql
drop table if exists default.sparkone_save_hive_append;

create table default.sparkone_save_hive_append (
  city string,
  cnt bigint
) using parquet;

view sparkone_hive_append_data as
select * from values
  ('beijing', 3L),
  ('shanghai', 2L)
as sparkone_hive_append_data(city, cnt);

save append sparkone_hive_append_data
as hive.`default.sparkone_save_hive_append`;

select city, cnt
from default.sparkone_save_hive_append
order by city;
```

预期：查询结果有 `beijing=3`、`shanghai=2` 两行。

Case 2：overwrite 默认会被 Safe Save 拦截。

```sql
drop table if exists default.sparkone_save_hive_overwrite_block;

create table default.sparkone_save_hive_overwrite_block (
  id int,
  name string
) using parquet;

view sparkone_hive_overwrite_seed as
select * from values
  (1, 'old')
as sparkone_hive_overwrite_seed(id, name);

save append sparkone_hive_overwrite_seed
as hive.`default.sparkone_save_hive_overwrite_block`;

view sparkone_hive_overwrite_new as
select * from values
  (2, 'new')
as sparkone_hive_overwrite_new(id, name);

save overwrite sparkone_hive_overwrite_new
as hive.`default.sparkone_save_hive_overwrite_block`;
```

预期：最后一条 `save overwrite` 失败，提示需要添加 `sparkoneOverwrite="allow"`。这是故意的，用来确认 Hive 表覆盖写也进入 Safe Save 策略。

Case 3：显式允许 overwrite Hive 表。

```sql
view sparkone_hive_overwrite_new as
select * from values
  (2, 'new')
as sparkone_hive_overwrite_new(id, name);

save overwrite sparkone_hive_overwrite_new
as hive.`default.sparkone_save_hive_overwrite_block`
options sparkoneOverwrite="allow";

select id, name
from default.sparkone_save_hive_overwrite_block
order by id;
```

预期：查询结果只剩 `2, new`。日志会出现 `catalog overwrite allowed`，说明这是 catalog 表覆盖确认，不做文件目录备份。

Case 4：动态分区 append。

```sql
drop table if exists default.sparkone_save_hive_partition;

create table default.sparkone_save_hive_partition (
  city string,
  cnt bigint
)
using parquet
partitioned by (dt string);

view sparkone_hive_partition_data as
select * from values
  ('beijing', 3L, '2026-06-10'),
  ('shanghai', 2L, '2026-06-10'),
  ('hangzhou', 1L, '2026-06-11')
as sparkone_hive_partition_data(city, cnt, dt);

save append sparkone_hive_partition_data
as hive.`default.sparkone_save_hive_partition`
partitionBy dt;

select city, cnt, dt
from default.sparkone_save_hive_partition
order by dt, city;
```

预期：查询结果有 3 行，并按 `dt` 写入两个动态分区。`partitionBy dt` 会编译成 Spark SQL 的 `PARTITION (dt)`，分区列必须在源视图字段中。

说明：

- `save ... as hive` 会编译成 Spark 原生 `INSERT INTO/OVERWRITE TABLE`。
- `partitionBy` 对应 Spark SQL 的动态分区 `PARTITION (...)`，分区列需要出现在源视图字段中。
- Hive 表 overwrite 不做文件目录 `rename/trash` 备份；它只复用 Safe Save 的显式确认策略。
- `options` 当前只建议放 SparkOne 控制参数，例如 `sparkoneOverwrite="allow"`；建表格式、分区定义和表结构使用 Spark 原生 `CREATE TABLE` 明确声明。

## HDFS 和 Hive 测试

如果使用 `conf/sparkone.conf` 配置了 Hadoop/Hive/Kerberos，页面里可以直接写 HDFS 路径或 Hive 表。裸路径 `/tmp/...` 会按 Hadoop `fs.defaultFS` 解析；为了让脚本更明确，也可以写成 `hdfs:///tmp/...`。

HDFS CSV：

```sql
load csv.`hdfs:///tmp/users.csv`
options header="true" and inferSchema="true"
as users;

select * from users limit 20;
```

Hive：

```sql
show databases;
show tables in default;

load hive.`default.some_table` as t;
select * from t limit 20;
```

如果遇到认证、权限、NameNode 或 Hive metastore 错误，优先检查启动配置，而不是 SQL 编辑器本身。相关配置见 [hadoop-hive.md](hadoop-hive.md) 和 [startup.md](startup.md)。

## Excel 测试

`excel` 当前只是 provider 别名，主包不内置 Excel connector。要测试 Excel，启动时必须提供对应 provider jar 或 Maven package，例如在 `conf/sparkone.conf` 中配置：

```hocon
jars {
  packages = "dev.mauch:spark-excel_2.12:3.5.6_0.31.2"
  # 或者直接指定本地 jar：
  # jars = "/Users/qindongliang/.m2/repository/dev/mauch/spark-excel_2.12/3.5.6_0.31.2/spark-excel_2.12-3.5.6_0.31.2.jar"
  # 如果只是分发普通配置文件，用 files，不要用来放 provider jar：
  # files = "/path/to/app.conf"
}
```

`packages` 使用 Maven 坐标，由 Spark/Ivy 解析依赖；`jars` 对应 Spark 原生 `spark.jars`，可以直接写本地 jar 的绝对路径。`files` 对应 Spark 原生 `spark.files`，只分发普通文件，不会加入 classpath，不能用来加载 Excel provider。

然后页面里可以写：

```sql
load excel.`file:///Users/qindongliang/Downloads/jupyter_tasks.xlsx`
options header="true" and inferSchema="true"
as users_excel;

select * from users_excel limit 20;
```

如果 provider 没加载，`Compile` 可能成功，但 `Run` 会失败，因为真正解析 provider 的是 Spark runtime。

如果启动 SparkContext 时出现 `Failed to connect to /192.168...` 且日志里有 `Added JAR ... at spark://.../jars/...`，通常是本地调试时 Spark driver 广播地址和实际绑定地址不一致。`conf/sparkone.conf` 的 `spark` 建议保留：

```hocon
spark {
  driverHost = "127.0.0.1"
  driverBindAddress = "127.0.0.1"
}
```

这个配置只适合本地 `local[*]` 调试；如果改成 `master = "yarn"`，不要把 `driverHost` 固定为 `127.0.0.1`，应使用 executor 能访问到的 driver 地址，或交给 Spark/YARN 环境决定。

## Compile 和 Run 的使用建议

- 写普通 Spark SQL 时，通常直接点 `Run`。
- 写 `load/save` 时，先点 `Compile` 确认转译出来的 Spark SQL 符合预期，再点 `Run`。
- 多条语句调试时，先把前置建表/建视图语句和最后查询语句放在同一次脚本里。
- 查询大表时先加 `limit`，并把页面右上角 `Rows` 控制在较小值。
- 保存数据前先用 `select count(*)` 或抽样查询确认临时视图内容。

## 常见问题

`Run` 成功但没有表格结果：

- DDL、`CREATE VIEW`、`INSERT OVERWRITE DIRECTORY` 等语句本身可能没有业务行数据返回，这是正常现象。

`Compile` 成功但 `Run` 失败：

- 常见原因是文件路径不存在、HDFS/Hive 权限不足、provider jar 未加载，或 Spark runtime 不支持对应数据源。

`load hive...` 带 options 报错：

- 当前 `hive` 是 catalog 表语义，不支持 `options` 参数。

临时视图查不到：

- 确认创建视图和查询视图在同一个服务进程内执行。
- 如果刚重启过服务，需要重新执行创建视图语句。

SQL 里有注释：

```sql
-- 单行注释
select 1;

/* 块注释 */
select 2;
```

注释会被 SparkOne DSL parser 忽略，不会作为独立语句执行。
