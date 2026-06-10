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
- `Run` 默认隐藏每条 statement 的编译后 SQL；如果需要调试转译结果，在 `conf/sparkone.toml` 里配置 `[server] showCompiledSql = true`。
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
where header="true" and inferSchema="true"
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
where header="true" and inferSchema="true"
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
where multiLine="true"
as pretty_events;

select * from pretty_events limit 20;
```

显式推断 schema：

```sql
load json.`/tmp/events.json`
where inferSchema="true"
as inferred_events;

select event_type, count(*) as cnt
from inferred_events
group by event_type
order by cnt desc;
```

指定 schema 并过滤脏数据：

```sql
load json.`/tmp/events.json`
where schema="event_id STRING, event_type STRING, amount DOUBLE, created_at TIMESTAMP"
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

MySQL / JDBC：

```sql
load jdbc.`mysql_users`
where url="jdbc:mysql://192.168.1.179:3306/Dworks"
and dbtable="cloud_host_info"
and user="root"
and password="******"
and driver="com.mysql.cj.jdbc.Driver"
as users_mysql;

select * from users_mysql limit 10;
```

它会编译成 Spark 原生 JDBC 临时视图：

```sql
CREATE OR REPLACE TEMPORARY VIEW users_mysql
USING jdbc
OPTIONS (
  path 'mysql_users',
  url 'jdbc:mysql://192.168.1.179:3306/Dworks',
  dbtable 'cloud_host_info',
  user 'root',
  password '******',
  driver 'com.mysql.cj.jdbc.Driver'
);
```

说明：

- `load jdbc` 走 Spark 内置 JDBC provider。
- `dbtable` 可以是表名，也可以按 Spark JDBC 规则写成带别名的子查询，例如 `"(select * from cloud_host_info limit 10) t"`。
- 运行时需要 MySQL JDBC driver 在 classpath 中；本地可通过 `[jars] jars = "/path/to/mysql-connector-j.jar"` 或 Maven package 提供。
- `load` 只是注册临时视图，不负责限制结果行数；需要抽样查看时，在后续 `select * from users_mysql limit 10` 中限制。

## 使用 SparkOne Save DSL

当前 MVP 只支持 `save overwrite`。它会转成 Spark SQL 的 `INSERT OVERWRITE DIRECTORY`。

保存成 Parquet：

```sql
view city_stats as
select city, count(*) as cnt
from users
group by city;

save overwrite city_stats as parquet.`/tmp/city_stats_parquet`;
```

保存成 CSV：

```sql
save overwrite city_stats as csv.`/tmp/city_stats_csv`
where header="true";
```

当前不支持：

```sql
save append city_stats as parquet.`/tmp/city_stats_parquet`;
save ignore city_stats as parquet.`/tmp/city_stats_parquet`;
save errorifexists city_stats as parquet.`/tmp/city_stats_parquet`;
```

这些模式会在编译阶段报错，后续需要时再扩展 compiler。

## HDFS 和 Hive 测试

如果使用 `conf/sparkone.toml` 配置了 Hadoop/Hive/Kerberos，页面里可以直接写 HDFS 路径或 Hive 表。裸路径 `/tmp/...` 会按 Hadoop `fs.defaultFS` 解析；为了让脚本更明确，也可以写成 `hdfs:///tmp/...`。

HDFS CSV：

```sql
load csv.`hdfs:///tmp/users.csv`
where header="true" and inferSchema="true"
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

`excel` 当前只是 provider 别名，主包不内置 Excel connector。要测试 Excel，启动时必须提供对应 provider jar 或 Maven package，例如在 `conf/sparkone.toml` 中配置：

```toml
[jars]
packages = "dev.mauch:spark-excel_2.12:3.5.6_0.31.2"
# 或者直接指定本地 jar：
# jars = "/Users/qindongliang/.m2/repository/dev/mauch/spark-excel_2.12/3.5.6_0.31.2/spark-excel_2.12-3.5.6_0.31.2.jar"
# 如果只是分发普通配置文件，用 files，不要用来放 provider jar：
# files = "/path/to/app.conf"
```

`packages` 使用 Maven 坐标，由 Spark/Ivy 解析依赖；`jars` 对应 Spark 原生 `spark.jars`，可以直接写本地 jar 的绝对路径。`files` 对应 Spark 原生 `spark.files`，只分发普通文件，不会加入 classpath，不能用来加载 Excel provider。

然后页面里可以写：

```sql
load excel.`file:///Users/qindongliang/Downloads/jupyter_tasks.xlsx`
where header="true" and inferSchema="true"
as users_excel;

select * from users_excel limit 20;
```

如果 provider 没加载，`Compile` 可能成功，但 `Run` 会失败，因为真正解析 provider 的是 Spark runtime。

如果启动 SparkContext 时出现 `Failed to connect to /192.168...` 且日志里有 `Added JAR ... at spark://.../jars/...`，通常是本地调试时 Spark driver 广播地址和实际绑定地址不一致。`conf/sparkone.toml` 的 `[spark]` 建议保留：

```toml
driverHost = "127.0.0.1"
driverBindAddress = "127.0.0.1"
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

- 当前 `hive` 是 catalog 表语义，不支持 `where/options` 参数。

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
