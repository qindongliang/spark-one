# Load 文件与 Hive 测试

`load` 是 SparkOne 提供的薄 DSL，目的是让加载数据更接近 MLSQL 写法。推荐先点 `Compile` 看转译结果。

路径说明：

- 文件 `load` 只接受当前登录租户 workspace 下的相对路径。
- `load csv.\`imports/users.csv\`` 会在 Spark driver 内解析为 `/public/sparkone/user/${username}/imports/users.csv`。
- 绝对路径、`file://`、`hdfs://`、`s3a://`、`oss://`、`..` 和内部 `.sparkone-overwrite-*` 目录都会被拒绝。
- 原生 SQL/`view` 不能直接写 `csv.\`path\``、`parquet.\`path\`` 等文件 relation，必须先通过 `load ... as view` 注册临时视图。

CSV：

```sql
load csv.`imports/users.csv`
options header="true" and inferSchema="true"
as users;

select * from users limit 20;
```

Compile 页面会显示可读摘要，实际执行仍使用内部命令：

```text
MANAGED HDFS LOAD
  tenant: <当前登录用户>
  view: users
  format: csv
  source: imports/users.csv
  options: {header='true', inferSchema='true'}
```

Parquet：

```sql
load parquet.`datasets/users_parquet` as users_parquet;

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
load json.`events/events.json` as events;

select * from events limit 20;
```

多行 JSON 文件：

```sql
load json.`events/events_pretty.json`
options multiLine="true"
as pretty_events;

select * from pretty_events limit 20;
```

显式推断 schema：

```sql
load json.`events/events.json`
options inferSchema="true"
as inferred_events;

select event_type, count(*) as cnt
from inferred_events
group by event_type
order by cnt desc;
```

指定 schema 并过滤脏数据：

```sql
load json.`events/events.json`
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
SELECT * FROM spark_catalog.default.some_table;
```

Hive 也支持在 `load` 语法糖里追加过滤：

```sql
load hive.`default.some_table`
where "dt = date '2026-06-17'"
as some_table_0617;

select *
from some_table_0617
limit 20;
```

编译结果应是标准 Spark SQL：

```sql
CREATE OR REPLACE TEMPORARY VIEW some_table_0617 AS
SELECT * FROM spark_catalog.default.some_table WHERE dt = date '2026-06-17';
```

预期：只返回满足条件的数据。是否触发 Hive 分区裁剪或 Parquet/ORC 谓词下推由 Spark/Hive 自身优化能力决定。

原生只读 SQL 也支持同一个 Hive 逻辑别名：

```sql
show namespaces in hive;
show tables in hive.default;
select * from hive.default.some_table limit 20;
```

预期：Compile 分别显示 `show tables in spark_catalog.default` 和 `select * from spark_catalog.default.some_table limit 20`，Run 通过 Kyuubi/Spark 内置 session catalog 查询 Hive。
