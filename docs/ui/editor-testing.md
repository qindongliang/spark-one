# SQL 编辑器测试手册

这个页面是 SparkOne MVP 的本地测试台，用来快速验证 Spark SQL、SparkOne 薄 DSL 转译、HDFS/Hive 配置和数据源读写。

数据质量 `assert` 的完整用例单独见 [Assert 测试用例](assertion-testing.md)。

访问地址通常是：

```text
http://127.0.0.1:7070
```

首次打开页面需要输入用户名。这个页面只在开发测试环境选择逻辑租户，不校验密码，也不代表生产身份认证。登录后刷新页面应保持当前 session；点击 `Log out` 后，编译、执行和预览接口都应返回未登录状态。

页面选择 Kyuubi 引擎时会显示 `Session` 下拉框，可以直接切换 `Tenant shared` 和 `Run isolated` 测试两种会话模式。Local 引擎不显示该控件。

## 页面区域

- 左侧编辑器：输入一段 SQL 脚本，可以包含多条语句，用分号 `;` 分隔。
- `Session`：只在 Kyuubi 引擎下显示。`Tenant shared` 表示同一租户共享会话、支持跨 Run 临时视图；`Run isolated` 表示每次 Run 使用独立会话，临时视图不能跨 Run，适合模拟定时任务。
- `Compile`：只编译，不执行。只有 `server.showCompiledSql = true` 时才显示，适合检查 `load/save` 这类 SparkOne DSL 被转成了什么 Spark SQL。
- `Run`：编译后按顺序执行每条 SQL，后面的语句可以使用前面创建的临时视图。
- `Preview`：在结果区的 Preview tab 里显示；对 `load ... as t`，先 `Run` 注册临时视图并展示 schema，再点该结果里的 `Preview` tab 显式拉取 `t` 的预览数据。
- 选中执行：如果编辑器里有选中的 SQL，`Compile` 和 `Run` 只处理选中部分；没有选区时处理整篇脚本。
- `Run` 默认隐藏每条 statement 的编译后 SQL；如果需要调试转译结果，在 `conf/sparkone.conf` 里配置 `server.showCompiledSql = true`。
- `Rows`：控制每条 statement 最多预览多少行；默认上限是 `preview.maxRows = 10`，页面输入只能小于或等于该 HOCON 上限。
- 默认结果 tab 由 `preview.defaultTab` 控制，可选 `schema` 或 `preview`；默认是 `schema`。
- 右侧结果区：展示每条语句的编译后 SQL、耗时、schema 和预览数据；schema 和预览数据通过 tab 切换，失败语句会显示错误信息。

## 基础冒烟测试

最小 SQL：

```sql
select 1 as id;
```

查看当前 Catalog 和 Hive namespace：

```sql
show catalogs;
show namespaces in hive;
show tables in hive.default;
```

`hive` 是 SparkOne 对内置 `spark_catalog` 的逻辑别名。注意 Spark SQL 使用复数
`SHOW DATABASES`，不支持 `show database in hive`；测试手册统一使用等价且更清晰的
`SHOW NAMESPACES IN hive`。

## 默认结果 Tab

在 `conf/sparkone.conf` 中可以控制运行结果默认展示 schema 还是 preview：

```hocon
preview {
  maxRows = 10
  defaultTab = "preview"
}
```

可选值：

- `schema`：默认展示字段结构，适合检查表结构和类型。
- `preview`：默认展示结果行；对 `load ... as t` 会自动加载一次 `t` 的预览数据。

改完配置后需要重启服务。可以用下面的 SQL 快速验证：

```sql
select 1 as id, 'beijing' as city;
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

## Kyuubi Session 隔离

页面选择 Kyuubi 引擎后，可以用 `Session` 下拉框验证两种运行方式。

选择 `Tenant shared`，先单独 Run：

```sql
view editor_session_probe as
select 1 as id, 'shared' as mode;
```

再单独 Run：

```sql
select * from editor_session_probe;
```

第二次应查询成功，因为同一登录租户复用了 Kyuubi session。该模式仍允许同租户并发 Run，共享的是 connection/session，不是串行执行锁。

切换为 `Run isolated` 后，仍分两次执行上面的 SQL。第二次应提示找不到 `editor_session_probe`，因为每次 Run 都创建并关闭独立 Kyuubi session。把创建和查询放进同一次 Run 则应成功：

```sql
view isolated_session_probe as
select 1 as id, 'isolated' as mode;

select * from isolated_session_probe;
```

切回 Local 引擎后，`Session` 控件应隐藏，并按默认的 `tenant_shared` 请求值提交；该值不会改变 Local 引擎已有的 SparkSession 行为。

## 脚本变量 Set

SparkOne 支持脚本内变量，变量只在同一次 `Run` 内按顺序生效，后续语句用 `${name}` 引用。

普通字面量变量：

```sql
set biz_date = "2026-03-14";

select '${biz_date}' as dt;
```

SQL 变量使用 `set name as select ...`，执行时取查询结果第一行第一列作为变量值：

```sql
set start_date as select date_sub(date '2026-03-15', 1) as dt;
set end_date as select date '2026-03-15' as dt;

view source_events as
select * from values
  (1, timestamp '2026-03-14 10:00:00'),
  (2, timestamp '2026-03-15 00:00:00')
as source_events(id, create_time);

select *
from source_events
where create_time >= timestamp '${start_date}'
  and create_time < timestamp '${end_date}';
```

MySQL 增量加载可按同样方式拼接 `load mysql ... where` 的过滤条件：

```sql
set start_date as select date_sub(current_date(), 1) as dt;
set end_date as select current_date() as dt;

load mysql.`analytics.orders`
where "createTime >= '${start_date}' and createTime < '${end_date}'"
as orders_delta;
```

注意：

- `Compile` 只展示占位动作，不执行 SQL 变量查询；要看到 `${name}` 的运行时替换效果，请用 `Run`。
- `Run` 时 `set` 语句只更新变量，不展示内部 schema 和 preview；重点看后续业务查询语句的结果。
- SparkOne 不支持 MLSQL 的 `set name = \`select ...\` where type = "sql"` 写法；请使用 `set name as select ...`。

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

Doris：

先在 `conf/sparkone.conf` 配置 Spark Doris Catalog。SparkOne 本地运行时会把它转成 `spark.sql.catalog.doris.*`；非 ODEP Kyuubi 环境也可以使用同样的静态配置。ODEP 模式使用后文单独的 alias 测试，不重复配置 `spark.sql.catalog.doris.*`：

```hocon
engines {
  local {
    type = "local"

    catalogs.doris {
      fenodes = "fe-1:8030,fe-2:8030"
      queryPort = 9030
      user = "root"
      password = "******"

      options {
        doris.request.retries = 3
      }
    }

    jars {
      packages = "org.apache.doris:spark-doris-connector-spark-3.5:25.2.0"
    }
  }
}
```

验证 Hive 仍是默认 catalog：

```sql
show databases;
```

查看默认 Doris catalog 的库列表，两种写法都可以：

```sql
show namespaces in doris;
```

```sql
show databases in doris;
```

查看 Local `catalogs.doris` 中某个 Doris 库下的表列表：

```sql
show tables in doris.dataagent;
```

Doris 测试表可以先用 Doris/MySQL 协议客户端准备。下面的 `replication_num` 按本地单副本测试环境写，生产集群按实际副本数调整：

```sql
CREATE DATABASE IF NOT EXISTS app;

DROP TABLE IF EXISTS app.sparkone_doris_seed;
CREATE TABLE app.sparkone_doris_seed (
  id BIGINT NOT NULL,
  city VARCHAR(64) NOT NULL,
  cnt BIGINT NOT NULL,
  biz_date DATE NOT NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);

INSERT INTO app.sparkone_doris_seed VALUES
  (1, 'beijing', 10, '2026-06-01'),
  (2, 'shanghai', 20, '2026-06-01'),
  (3, 'beijing', 30, '2026-06-02'),
  (4, 'hangzhou', 40, '2026-06-02');
```

不要把上面的 DDL 或 `INSERT` 复制到 SparkOne 编辑器；原生 catalog 修改命令会在 Compile 阶段拒绝。种子数据也应通过 Doris 管理客户端准备。

在 SparkOne 编辑器里读取 Doris 表：

```sql
select *
from doris.app.sparkone_doris_seed
order by id;
```

预期：返回 4 行，`city/cnt/biz_date` 与上面插入的数据一致。过滤直接写 Spark SQL：

```sql
select id, city, cnt
from doris.app.sparkone_doris_seed
where biz_date = date '2026-06-02'
order by id;
```

预期：返回 `id=3` 和 `id=4` 两行。过滤下推由 Spark Doris Catalog / Connector 的 DataSource V2 能力决定。

`load doris` 只是临时视图语法糖：

```sql
load doris.`app.sparkone_doris_seed` as doris_seed;

select *
from doris_seed
order by id;
```

编译结果应是标准 Spark SQL：

```sql
CREATE OR REPLACE TEMPORARY VIEW doris_seed AS SELECT * FROM doris.app.sparkone_doris_seed
```

`load doris` 也可以追加 `where "..."`，编译结果仍是标准 Spark SQL，过滤是否源端下推由 Spark Doris Catalog / Connector 决定：

```sql
load doris.`app.sparkone_doris_seed`
where "biz_date = date '2026-06-02'"
as doris_seed_0602;

select id, city, cnt
from doris_seed_0602
order by id;
```

预期：返回 `id=3` 和 `id=4` 两行。编译结果应包含：

```sql
CREATE OR REPLACE TEMPORARY VIEW doris_seed_0602 AS SELECT * FROM doris.app.sparkone_doris_seed WHERE biz_date = date '2026-06-02'
```

用 Doris 临时视图继续做 SQL 分析：

```sql
load doris.`app.sparkone_doris_seed` as doris_seed;

view doris_city_stats as
select city, count(*) as row_count, sum(cnt) as total_cnt
from doris_seed
group by city;

select *
from doris_city_stats
order by city;
```

预期：`beijing` 的 `row_count=2`、`total_cnt=40`，`hangzhou` 和 `shanghai` 各 1 行。

`load doris` 不支持在 DSL 里写连接 options；连接能力回到 Spark catalog 配置：

```sql
load doris.`app.sparkone_doris_seed`
options password="bad"
as doris_seed;
```

预期：编译失败，提示 `LOAD doris does not support SQL OPTIONS`。

测试 `save append` 写入 Doris：

先用 Doris/MySQL 协议客户端准备目标表：

```sql
DROP TABLE IF EXISTS app.sparkone_doris_city_result;
CREATE TABLE app.sparkone_doris_city_result (
  city VARCHAR(64) NOT NULL,
  row_count BIGINT NOT NULL,
  total_cnt BIGINT NOT NULL
)
DUPLICATE KEY(city)
DISTRIBUTED BY HASH(city) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);
```

然后在 SparkOne 编辑器里执行：

```sql
load doris.`app.sparkone_doris_seed` as doris_seed;

view doris_city_result as
select
  sum(cnt) as total_cnt,
  city,
  count(*) as row_count
from doris_seed
group by city;

save append doris_city_result
as doris.`app.sparkone_doris_city_result`;

select *
from doris.app.sparkone_doris_city_result
order by city;
```

预期可以看到 `beijing/shanghai/hangzhou` 的聚合结果。源视图列顺序为 `total_cnt, city, row_count`，与 Doris 目标表不同，用于验证按列名写入。重复执行 append 会重复追加数据，这是 Doris 目标表和 Spark Doris Catalog append 写入的正常语义。

### Doris 表模型对 save append 结果的影响

SparkOne 的 `save append ... as doris` 会在 schema 预检后向目标表提交带显式目标列清单和源列投影的 `INSERT INTO TABLE doris.db.table (...) SELECT ...`。最终查询效果由 Doris 目标表模型决定：

- `DUPLICATE KEY`：保留所有写入行，不去重、不聚合。
- `AGGREGATE KEY`：按 Key 列聚合 Value 列，适合固定汇总指标。
- `UNIQUE KEY`：按 Key 列 UPSERT，同 Key 新数据覆盖旧数据。

可以用同一份 `doris_city_result` 写入三种 Doris 目标表观察差异。

准备 Duplicate Key 目标表：

```sql
DROP TABLE IF EXISTS app.sparkone_doris_city_duplicate;
CREATE TABLE app.sparkone_doris_city_duplicate (
  city VARCHAR(64) NOT NULL,
  row_count BIGINT NOT NULL,
  total_cnt BIGINT NOT NULL
)
DUPLICATE KEY(city)
DISTRIBUTED BY HASH(city) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);
```

准备 Aggregate Key 目标表：

```sql
DROP TABLE IF EXISTS app.sparkone_doris_city_aggregate;
CREATE TABLE app.sparkone_doris_city_aggregate (
  city VARCHAR(64) NOT NULL,
  row_count BIGINT SUM NOT NULL,
  total_cnt BIGINT SUM NOT NULL
)
AGGREGATE KEY(city)
DISTRIBUTED BY HASH(city) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);
```

准备 Unique Key 目标表：

```sql
DROP TABLE IF EXISTS app.sparkone_doris_city_unique;
CREATE TABLE app.sparkone_doris_city_unique (
  city VARCHAR(64) NOT NULL,
  row_count BIGINT NOT NULL,
  total_cnt BIGINT NOT NULL
)
UNIQUE KEY(city)
DISTRIBUTED BY HASH(city) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);
```

在 SparkOne 编辑器里生成同一份待写入结果：

```sql
load doris.`app.sparkone_doris_seed` as doris_seed;

view doris_city_result as
select
  city,
  count(*) as row_count,
  sum(cnt) as total_cnt
from doris_seed
group by city;
```

分别写入三张表。为了观察差异，可以把下面三条 `save append` 重复执行两次：

```sql
save append doris_city_result
as doris.`app.sparkone_doris_city_duplicate`;

save append doris_city_result
as doris.`app.sparkone_doris_city_aggregate`;

save append doris_city_result
as doris.`app.sparkone_doris_city_unique`;
```

查看 Duplicate Key 表：

```sql
select city, count(*) as physical_rows, sum(row_count) as sum_row_count, sum(total_cnt) as sum_total_cnt
from doris.app.sparkone_doris_city_duplicate
group by city
order by city;
```

预期：重复 append 两次后，每个 `city` 会有两条物理行；例如 `beijing` 的 `physical_rows=2`、`sum_row_count=4`、`sum_total_cnt=80`。

查看 Aggregate Key 表：

```sql
select city, row_count, total_cnt
from doris.app.sparkone_doris_city_aggregate
order by city;
```

预期：重复 append 两次后，同 Key 指标会按 `SUM` 聚合；例如 `beijing` 的 `row_count=4`、`total_cnt=80`。

查看 Unique Key 表：

```sql
select city, row_count, total_cnt
from doris.app.sparkone_doris_city_unique
order by city;
```

预期：重复 append 两次后，同 Key 只保留最新一行；如果两次写入的数据相同，`beijing` 仍是 `row_count=2`、`total_cnt=40`。

测试 Doris overwrite 永久拒绝：

```sql
view doris_overwrite_result as
select
  "overwrite_city" as city,
  cast(1 as bigint) as row_count,
  cast(999 as bigint) as total_cnt;

save overwrite doris_overwrite_result
as doris.`app.sparkone_doris_city_result`;
```

预期编译失败，错误包含 `doris-catalog` 和 `permanently denied`。不存在可放开 Doris overwrite 的 SQL option 或 HOCON 配置。

说明：

- `doris.\`app.sparkone_doris_city_result\`` 中 `app` 是 Doris database，`sparkone_doris_city_result` 是 Doris 表名；SparkOne 会补成 Spark Catalog 表名 `doris.app.sparkone_doris_city_result`。
- `save append ... as doris` 要求目标表已存在；SparkOne 不会自动创建 Doris 表。表结构、key、distribution、分区等用 Doris DDL 先建好。
- `save doris` 不支持在 SQL 里写 `fenodes/user/password` 等连接 options，这些统一放在 `engines.local.catalogs.doris` 或 Kyuubi/Spark engine 配置。
- `save doris` 不支持 `partitionBy`，Doris 的分布、分区和表结构应由 Doris DDL 管理。

MySQL：

先在 `conf/sparkone.conf` 配置连接，SQL 里只引用连接名：

```hocon
engines {
  local {
    type = "local"

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

    catalogs.mysql {
      url = "jdbc:mysql://192.168.1.179:3306/?databaseTerm=SCHEMA&useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
      driver = "com.mysql.cj.jdbc.Driver"
      user = "root"
      password = "******"

      options {
        fetchsize = 1000
      }
    }
  }
}
```

运行时还需要 MySQL JDBC driver 在 classpath 中，可以在 HOCON 里选择 `packages` 或本地 JAR：

```hocon
engines {
  local {
    type = "local"

    jars {
      packages = "com.mysql:mysql-connector-j:8.4.0"
      # jars = "/Users/qindongliang/.m2/repository/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar"
    }
  }
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

DROP TABLE IF EXISTS sparkone_mysql_orders_demo;
CREATE TABLE sparkone_mysql_orders_demo (
  id BIGINT NOT NULL AUTO_INCREMENT,
  user_id BIGINT NOT NULL,
  city VARCHAR(64) NOT NULL,
  amount DECIMAL(12, 2) NOT NULL,
  status VARCHAR(32) NOT NULL,
  biz_date DATE NOT NULL,
  PRIMARY KEY (id),
  KEY idx_orders_demo_biz_status (biz_date, status),
  KEY idx_orders_demo_status_id (status, id)
);

INSERT INTO sparkone_mysql_orders_demo
  (user_id, city, amount, status, biz_date)
VALUES
  (101, 'beijing',  99.90, 'PAID',    '2026-06-09'),
  (102, 'shanghai', 35.50, 'PAID',    '2026-06-10'),
  (103, 'beijing',  18.00, 'CREATED', '2026-06-10'),
  (104, 'hangzhou', 66.60, 'PAID',    '2026-06-10'),
  (105, 'shenzhen', 22.20, 'CANCEL',  '2026-06-10'),
  (106, 'shanghai', 88.80, 'PAID',    '2026-06-11'),
  (107, 'beijing',  42.00, 'PAID',    '2026-06-11'),
  (108, 'hangzhou', 13.14, 'CREATED', '2026-06-11');
```

在 SparkOne 编辑器里读取 MySQL 表：

```sql
load mysql.`analytics.sparkone_mysql_seed` as mysql_seed;

select * from mysql_seed order by id;
```

查看 MySQL catalog：

```sql
show namespaces in mysql;
show tables in mysql.Dworks;
select * from mysql.Dworks.sparkone_mysql_seed limit 10;
```

说明：`mysql` 由 HOCON 的 `engines.local.catalogs.mysql` 显式注册。它适合浏览和原生查询；`load/save mysql` 仍走 local 引擎的 `datasources.mysql.*` adapter，用于隐藏连接信息、控制 `dbtable/query` 和执行 save 安全策略。

`engines.local.catalogs.mysql.url` 需要带 `databaseTerm=SCHEMA`。Spark 原生 JDBC Catalog 会把 `mysql.Dworks` 中的 `Dworks` 当 JDBC `schemaPattern` 查询元数据；MySQL Connector/J 默认把 MySQL database 当 JDBC catalog。加上该参数后，`show tables in mysql.Dworks` 才会按 `Dworks` 这个库过滤。

`load mysql` 会在运行时用 Spark JDBC reader 注册临时视图。`Compile` 只展示安全占位 SQL，不展示 HOCON 里的账号密码：

```sql
SELECT 'LOAD MYSQL' AS sparkone_action, 'sparkone_mysql_seed AS mysql_seed' AS sparkone_target
```

大表读取最佳实践：

如果 MySQL 表有自增主键 `id`，并且数据量约 3000 万行，不要使用默认单分区 JDBC 读取。默认读取通常只有一个 JDBC 任务在拉全表，吞吐低，也容易把单个 executor task、MySQL 连接或网络打满。推荐用自增主键做 Spark JDBC 分区列，让 Spark 并发发起多个范围查询。

先在 MySQL 侧确认分区列有索引：

```sql
show index from big_orders;
```

在 SparkOne 编辑器里读取大表，推荐只提供 `partitionColumn`：

```sql
load mysql.`analytics.big_orders`
options partitionColumn="id"
as big_orders_30m;

select count(*) as row_count from big_orders_30m;
```

SparkOne 会在执行侧自动查询：

- 不带 `where` 时：`SELECT MIN(id), MAX(id) FROM big_orders`
- 带 `where` 时：`SELECT MIN(id), MAX(id) FROM (select * from big_orders where ...) as sparkone_mysql_load`

如果确实需要覆盖默认并发，可以显式写 `numPartitions`；默认是 `10`。`fetchsize` 默认是 `10000`，通常不需要每次写。

参数含义：

- `partitionColumn="id"`：用 `id` 这一列切分 JDBC 读取任务。Spark 要求它是 numeric、date 或 timestamp 类型；MySQL 自增主键最适合，前提是有索引。
- `lowerBound` / `upperBound`：默认自动查询；它们只用于计算每个分区的步长，不是过滤条件。
- `numPartitions`：并发 JDBC 分区数，也近似等于同时打到 MySQL 的查询连接数上限。默认 `10`，大表可结合源库压力显式调到 `16`、`24` 或 `32`。
- `fetchsize`：每次 JDBC round trip 拉取的行数。默认 `10000`；太小会增加网络往返，太大可能增加单 task 内存压力。

调参建议：

- 先估算单分区数据量：默认 `30000000 / 10` 约 300 万行/分区；如果单行很宽，优先只选择必要列，或结合源库压力调整 `numPartitions`。
- 用 Spark UI 看 task 是否均衡；如果少数 task 特别慢，通常是 `id` 分布有大空洞或热点范围，需要重新选择更均匀的分区列，或按日期/主键范围分批读取。
- 用 MySQL 监控观察并发连接数、慢查询、磁盘 IO 和 buffer pool 命中率；Spark 侧更快不代表源库能承受。
- 如果 MySQL Connector/J 实际没有按 `fetchsize` 流式拉取，可在 HOCON 的 JDBC URL 中评估加入 `useCursorFetch=true`，并用 executor 内存和 MySQL 连接状态验证效果。
- 大表抽样查看时不要直接 `select *`；先用 `select * from big_orders_30m where id between 1 and 1000 order by id` 或 `limit 100`。

Spark JDBC 底层仍要求 `partitionColumn/lowerBound/upperBound/numPartitions` 成组传入。SparkOne 的优化是在执行侧自动补齐 `lowerBound/upperBound`，并提供默认 `numPartitions=10`。

带 `where` 过滤条件的读取：

Spark JDBC 官方支持 `query` 参数，例如在 DataFrameReader 里写 `.option("query", "select c1, c2 from t1")`。但官方同时规定：

- `query` 不能和 `dbtable` 同时指定。
- `query` 不能和 `partitionColumn` 同时指定；如果既要查询过滤又要分区读取，官方建议把子查询写到 `dbtable`，例如 `"(select c1, c2 from t1) as subq"`，再指定 `partitionColumn`。

SparkOne 因此不开放 SQL 侧 `query=`。`load mysql.\`analytics.table\`` 仍然从路径里的 `table` 自动生成 JDBC `dbtable`，并禁止 SQL 侧覆盖 `dbtable` 连接目标。需要过滤时，使用 SparkOne 扩展的 `where "..."`，编译器会把它变成 Spark JDBC 官方推荐的 `dbtable` 子查询：

```sql
load mysql.`analytics.big_orders`
where "biz_date = '2026-06-10' and status = 'PAID'"
options partitionColumn="id"
as big_orders_paid;
```

它等价于给 Spark JDBC reader 传入类似下面的 `dbtable`：

```text
(select * from big_orders where biz_date = '2026-06-10' and status = 'PAID') as sparkone_mysql_load
```

SparkOne 会基于这个子查询自动查询 `MIN(id), MAX(id)`，再传给 Spark JDBC reader。这样既能让 MySQL 先做业务过滤，又保留 Spark JDBC 的并行范围读取能力。

如果过滤后还要做列裁剪或二次加工，继续接 `view`：

```sql
view paid_order_amounts as
select
  id,
  user_id,
  amount
from big_orders_paid
where amount > 0;

select count(*) as paid_order_count
from paid_order_amounts;
```

过滤写法建议：

- `load mysql ... where "..."` 只支持 MySQL 特殊 source；Hive/Doris catalog 也支持 `load ... where "..."`，文件类 provider 暂不支持这个扩展。
- `where` 后必须是引号包住的一段 MySQL 条件表达式，不写 `where` 关键字本身，例如 `where "status = 'PAID'"`。
- `partitionColumn/lowerBound/upperBound/numPartitions` 只负责并行切分；业务过滤写在 `where "..."` 里。通常只需要手写 `partitionColumn`。
- 尽量写简单谓词，例如 `=`、`between`、`>=`、`<`、`in (...)`，并使用 MySQL 表上已有索引列，例如 `id`、`biz_date`、`status`。
- 避免在过滤列上写复杂函数或表达式，例如 `date_format(biz_date, ...) = ...`，这类条件更难下推，也更难命中 MySQL 索引。
- 如果过滤逻辑非常复杂，优先在 MySQL 侧建 view，或在源表侧准备按业务条件切好的中间表，再用 `load mysql.\`analytics.some_view_or_table\`` 读取。

`load mysql` 组合用法示例：

下面几组 SQL 都基于前面创建的 `sparkone_mysql_orders_demo` 小表，用来验证不同读取组合。生产大表时把表名、上下界和分区数换成真实值即可。

1. 普通读取：不写 `where`，不写分区参数。

```sql
load mysql.`analytics.sparkone_mysql_orders_demo` as orders_all;

select id, user_id, city, amount, status, biz_date
from orders_all
order by id;
```

预期：读取 8 行。底层 JDBC `dbtable` 就是 `sparkone_mysql_orders_demo`。

2. 只写 `where`：MySQL 侧先过滤，不做 Spark JDBC 分区读取。

```sql
load mysql.`analytics.sparkone_mysql_orders_demo`
where "biz_date = '2026-06-10' and status = 'PAID'"
as orders_paid_0610;

select id, city, amount, status, biz_date
from orders_paid_0610
order by id;
```

预期：只返回 `2026-06-10` 且 `status='PAID'` 的 2 行。底层 JDBC `dbtable` 是：

```text
(select * from sparkone_mysql_orders_demo where biz_date = '2026-06-10' and status = 'PAID') as sparkone_mysql_load
```

这意味着过滤发生在 MySQL 层；Spark 读取的是这个子查询的结果。

3. 只写分区列：不做业务过滤，但按 `id` 范围并发读取。

```sql
load mysql.`analytics.sparkone_mysql_orders_demo`
options partitionColumn="id"
as orders_partitioned;

select count(*) as row_count
from orders_partitioned;
```

预期：读取 8 行。执行侧会自动查询 `id` 的上下界，并使用默认 `numPartitions=10`、`fetchsize=10000`；业务过滤必须写 `where "..."`。

4. 同时写 `where` 和分区参数：MySQL 侧先按子查询过滤，Spark JDBC 再按 `id` 并发读取子查询结果。

```sql
load mysql.`analytics.sparkone_mysql_orders_demo`
where "status = 'PAID'"
options partitionColumn="id"
as orders_paid_partitioned;

select id, city, amount, status, biz_date
from orders_paid_partitioned
order by id;
```

预期：读取 5 行 `PAID` 订单。底层 JDBC `dbtable` 是：

```text
(select * from sparkone_mysql_orders_demo where status = 'PAID') as sparkone_mysql_load
```

同时仍会传 `partitionColumn=id`，并在执行侧按过滤后的子查询自动补齐上下界和默认分区参数。

选择 local engine，测试 `save append` 写入 MySQL：

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

预期可以看到 `beijing/shanghai/hangzhou` 的聚合结果。目标必须已存在，源/目标列名集合必须完全一致；即使源列顺序不同，写入也会按目标列名重排。重复执行 append 会重复追加数据，这是 Spark JDBC append 的正常语义。

测试 MySQL overwrite 永久拒绝：

```sql
view mysql_overwrite_result as
select
  "overwrite_city" as city,
  cast(1 as bigint) as row_count,
  cast(999 as bigint) as total_cnt;

save overwrite mysql_overwrite_result
as mysql.`analytics.sparkone_city_result`;
```

预期编译失败，错误包含 `mysql` 和 `permanently denied`。不存在可放开 MySQL overwrite 的 SQL option 或 HOCON 配置。

说明：

- `load jdbc` 是 Kyuubi/ODEP 路由读取语法，不读取 local engine 的 MySQL HOCON；ODEP MySQL alias 可以使用 `partitionColumn/lowerBound/upperBound/numPartitions/fetchsize`，连接参数仍禁止出现在 SQL。`save jdbc` 当前不支持。本节 local MySQL adapter 仍统一使用 `load/save mysql`。
- `mysql.\`analytics.sparkone_mysql_seed\`` 中 `analytics` 是 HOCON 连接名，`sparkone_mysql_seed` 是 MySQL 表名。
- `save append ... as mysql` 要求目标表已存在；SparkOne 不会自动创建 MySQL 表。表结构、主键、索引等用 MySQL DDL 先建好。
- SQL 里的 `options` 只能补充 `fetchsize`、`batchsize` 等非连接参数，不能覆盖 `url/user/password/driver/dbtable`。
- 缺列、多列或类型不兼容会在 JDBC write 前失败，目标表不会因为 schema 预检失败而新增数据。
- `load` 会注册临时视图；普通 `Run` 默认只展示 schema，不自动拉取数据。需要看样例数据时，在该结果的 Preview tab 里点 `Preview`，预览行数受 `preview.maxRows` 和页面 `Rows` 共同限制。需要更精确抽样时，仍建议在后续 `select * from mysql_seed limit 10` 中显式限制。

## 测试 Kyuubi 三段式 Catalog

Kyuubi 页面测试统一使用下面五组用户可见标识：

| 数据源 | 三段式表名 | `load` 写法 |
| --- | --- | --- |
| Hive | `hive.<database>.<table>` | `load hive.\`database.table\`` |
| ODEP JDBC | `jdbc.<alias>.<table>` | `load jdbc.\`alias.table\`` |
| ODEP Doris | `doris.<alias>.<table>` | `load doris.\`alias.table\`` |
| 静态 MySQL | `mysql_static.<database>.<table>` | `load mysql.\`mysql_static.database.table\`` |
| 静态 Doris | `doris_static.<database>.<table>` | `load doris.\`doris_static.database.table\`` |

其中 ODEP 的第二段是注册别名，由路由 Catalog 映射到 `physicalNamespace`；静态数据源的第二段就是真实数据库名。`hive` 会由 SparkOne 改写为 Spark 内置的 `spark_catalog`。

### Hive 三段式

```sql
show namespaces in hive;
show tables in hive.default;
select * from hive.default.some_table limit 10;

load hive.`default.some_table` as hive_table;
select * from hive_table limit 10;
```

不要写 `show database in hive`。Spark SQL 没有单数 `SHOW DATABASE` 语法；使用上面的 `SHOW NAMESPACES`，或者使用复数 `SHOW DATABASES IN hive`。

### ODEP 数据源

前置检查：

- ODEP snapshot 中待测试的 JDBC/Doris 数据源已有非空 `physicalNamespace`。
- `sparkone-kyuubi-odep-plugin` JAR 同时位于 Kyuubi Server `$KYUUBI_HOME/jars` 和 Spark Engine 的 `spark.jars`。
- `sparkone-mysql-provider` JAR 位于 Spark Engine 的 `spark.jars`，用于 ODEP MySQL alias 的分区读取。
- JDBC driver、Doris connector 已放入 Spark Engine classpath。
- `kyuubi-defaults.conf` 和 session profile 中没有静态 `spark.sql.catalog.jdbc.*`、`spark.sql.catalog.doris.*`。

重启 Kyuubi Server；共享 Engine 还需人工停止旧 Engine，确保后续连接创建新 Engine。然后在 SparkOne 页面选择 Kyuubi engine，依次执行：

```sql
show catalogs;

show namespaces in jdbc;
show tables in jdbc.sync_search;
select * from jdbc.sync_search.drug_ai_drug_decision limit 10;

load jdbc.`sync_search.drug_ai_drug_decision` as odep_drugs;
select * from odep_drugs limit 10;

load jdbc.`sync_search.drug_ai_drug_decision`
where "menu_id = '1_0' AND section_id = 5"
options partitionColumn="modify_time"
as odep_drugs_parallel;

select count(*) from odep_drugs_parallel;
```

Doris 使用相同结构：

```sql
show namespaces in doris;
show tables in doris.recommend_prod;
select * from doris.recommend_prod.r_qa_log limit 10;

load doris.`recommend_prod.r_qa_log` as qa_log;
select * from qa_log limit 10;
```

`show namespaces` 返回 ODEP alias，不返回真实数据库；路由 Catalog 在 Engine 内将 alias 转成 `physicalNamespace`。如果 `show catalogs` 有 `jdbc/doris`，但首次 `show namespaces` 报静态配置冲突，删除同名前缀的旧 Catalog 配置后重建 Engine。

### 静态数据源

当前 `kyuubi-defaults.conf` 中静态 Catalog 使用 `mysql_static` 和 `doris_static`，与 ODEP 独占的 `jdbc`、`doris` 前缀不冲突。验证 MySQL：

```sql
show namespaces in mysql_static;
show tables in mysql_static.Dworks;
select * from mysql_static.Dworks.cloud_host_info limit 10;

load mysql.`mysql_static.Dworks.cloud_host_info` as static_mysql_hosts;
select * from static_mysql_hosts limit 10;
```

验证 Doris：

```sql
show namespaces in doris_static;
show tables in doris_static.dataagent;
select * from doris_static.dataagent.r_qa_log limit 10;

load doris.`doris_static.dataagent.r_qa_log` as static_qa_log;
select * from static_qa_log limit 10;
```

## 测试 Kyuubi sparkone_mysql Provider

这组用例只用于 SparkOne 选择 Kyuubi engine 时。它不读取 `engines.local.datasources.mysql`，MySQL 连接信息来自 Kyuubi/Spark engine 侧的 `spark.sql.catalog.<catalog>.*`。

前置检查：

- Kyuubi/Spark engine 已配置 `spark.sql.catalog.mysql_static=org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog` 以及 `spark.sql.catalog.mysql_static.url/user/password/driver`。
- `sparkone-mysql-provider_2.12-0.1.0-SNAPSHOT.jar` 已放入 Kyuubi/Spark engine classpath，例如 `spark.jars`。
- SparkOne 页面右上角选择 Kyuubi engine。

在编辑器里执行：

```sql
load mysql.`mysql_static.Dworks.cloud_host_info`
options partitionColumn="id"
as orders_big;

select count(*) from orders_big;
```

预期 Kyuubi operation log 中先出现临时视图注册：

```sql
CREATE OR REPLACE TEMPORARY VIEW orders_big
USING sparkone_mysql
OPTIONS (
  catalog 'mysql_static',
  dbtable 'Dworks.cloud_host_info',
  partitionColumn 'id',
  numPartitions '10',
  fetchsize '10000'
)
```

然后 `select count(*) from orders_big` 触发真正读取。验证分区参数是否生效：

```sql
EXPLAIN FORMATTED
SELECT count(*) FROM orders_big;
```

物理计划里应能看到：

```text
Scan JDBCRelation(Dworks.cloud_host_info) [numPartitions=10]
```

再看 Spark engine 的 Spark UI。local/client 模式通常是 `http://127.0.0.1:4040/jobs/`；YARN/Kubernetes/cluster 模式看 Spark application tracking URL。对 `select count(*) from orders_big`，负责 JDBC scan 的 stage 应有 `10/10` tasks。看到 `1/1 skipped` 的辅助 job 不代表分区没生效，通常是 AQE、聚合收尾或结果复用。

如果执行 `load` 后立刻在 Kyuubi log 里看到：

```sql
SELECT * FROM `orders_big` LIMIT 101
```

这是 SparkOne 页面预览，不是 `load` 自己全量查询。原因通常是 `preview.defaultTab = "preview"`，或者点击了结果区的 Preview tab。`101` 表示请求 100 行再多取 1 行判断是否截断；实际上限受 `preview.maxRows` 和页面 `Rows` 控制。

注意：

- `load mysql.\`mysql_static.Dworks.cloud_host_info\`` 中的 `mysql_static` 是 Kyuubi/Spark engine 侧静态 Catalog 名，不是 SparkOne local HOCON 的连接名。
- 不带大表参数时，Kyuubi `load mysql.\`catalog.db.table\`` 编译成 catalog SQL；带 `partitionColumn` 或其他大表读取参数时编译成 `USING sparkone_mysql`。
- SQL options 禁止传 `url/user/password/driver/dbtable/query`；这些敏感配置留在 Kyuubi/Spark engine 侧。
- `lowerBound/upperBound` 只决定 Spark JDBC 分区步长，不做业务过滤。业务过滤写 `where "..."`，例如 `where "biz_date = '2026-07-07'"`。
- `numPartitions` 会增加对 MySQL 的并发连接和 IO 压力。验证通过后再按 MySQL 监控、Spark task 耗时和源表索引情况调大。
- `load jdbc/mysql ... options partitionColumn=...` 会并行读取完整的过滤结果，不支持源端全局 `LIMIT`。后续 `select * from orders_big limit 10` 只限制 Spark 输出；需要 MySQL 端下推 `LIMIT` 时直接查询三段式 Catalog 表，不要同时使用分区 LOAD。

### 测试 Kyuubi MySQL append

先通过 MySQL 管理入口预建测试目标表，SparkOne 内禁止执行 `CREATE TABLE`。以下示例假设远端 JDBC Catalog 中已存在 `mysql_static.Dworks.sparkone_3b_target`，列为 `name string, id int`，且页面选择 Kyuubi engine：

```sql
view stage3b_mysql_source as
select * from values
  (101, 'stage3b-alice'),
  (102, 'stage3b-bob')
as stage3b_mysql_source(id, name);

save append stage3b_mysql_source
as mysql.`mysql_static.Dworks.sparkone_3b_target`;

select name, id
from mysql_static.Dworks.sparkone_3b_target
where id in (101, 102)
order by id;
```

Run 结果中的最终写入 SQL 应为显式列写入，源列顺序不会决定目标映射：

```sql
INSERT INTO TABLE mysql_static.Dworks.sparkone_3b_target (`name`, `id`)
SELECT `name`, `id` FROM stage3b_mysql_source
```

该 SQL 不应包含 JDBC URL、用户名或密码。Kyuubi MySQL save 只接受 `mysql.\`catalog.database.table\`` 三段式路径，不接受任何 SQL `OPTIONS`；两段式路径、缺列、多列和类型不兼容都应在真正写入前失败。`save overwrite` 永久拒绝。最终写 statement 如果连接中断，SparkOne 返回写入状态未知且不会自动重放，需先核查目标表再决定是否重新提交。

## 使用 SparkOne Save DSL

第二阶段已经统一使用 `WritePlan` 和固定能力矩阵。Hive、Doris、MySQL 只允许 append；这些 catalog/数据库目标的 overwrite 永久拒绝。所有文件 append 以及本地/S3/OSS external path 读写永久拒绝；受控 HDFS load/overwrite 由 Spark driver extension 执行。

测试原生 DDL/DML 是否被拦截。下面每条语句应分别点击 Compile，且都应失败：

```sql
insert overwrite directory '/tmp/sparkone_native_insert_overwrite_blocked'
using parquet
select 1 as id;

create table if not exists default.sparkone_drop_table_blocked (
  id int
)
using parquet;

drop table default.sparkone_drop_table_blocked;

alter table default.sparkone_drop_table_blocked add columns (name string);
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

实际执行仍使用版本化内部命令。Run 会把目标解析为 `/public/sparkone/user/${username}/reports/city_stats`；客户端不能提交绝对 workspace 路径。Local 必须先配置 `engines.local.overwrite.zkConnect`，Kyuubi 必须部署 extension jar 并配置 `spark.sql.extensions` 和 `spark.sparkone.overwrite.*`。

第一次 Run 后通过同一相对路径读取：

```sql
load parquet.`reports/city_stats` as saved_city_stats;

select *
from saved_city_stats
order by city;
```

Compile 应显示 `MANAGED HDFS LOAD` 摘要，Run 后只包含本次结果。修改 `city_stats` 后再次 overwrite，再重新执行 load，路径中应只包含第二次完整结果。执行期间，同目标的第二个 overwrite 应失败并包含 `already running`，同时显示占用锁的 `operationId`、`target` 和包含租户的 ZK `lockPath`；不同目标应可并发。成功或明确失败后，正式目录同级不应残留 `.sparkone-overwrite-*`；模拟 driver 中断留下 work 目录时，下次取得锁的 overwrite 应先恢复/清理残留再执行。

下面的直接路径读取都应在 Compile 阶段失败，并提示使用 SparkOne `LOAD`：

```sql
select * from parquet.`/public/sparkone/user/alice/reports/city_stats`;
view bypass as select * from parquet.`reports/city_stats`;
load parquet.`../bob/reports/city_stats` as bypass;
```

本地文件、S3、OSS 裸路径的 append/overwrite 都保持永久拒绝。未来只有出现明确生产案例并定义 schema、分区、并发和失败重跑幂等合同后，才单独评估 Parquet/ORC 分区 append 或事务湖表写入，不恢复 CSV、Excel 或任意裸路径 append。

Hive/catalog 表 append 要求目标表先存在。请先通过平台外 Hive/catalog 管理入口准备下面三个目标表，不能在 SparkOne 编辑器执行建表语句：

- `default.sparkone_save_hive_append(city string, cnt bigint)`
- `default.sparkone_save_hive_overwrite_block(id int, name string)`
- `default.sparkone_save_hive_partition(city string, cnt bigint)`，按 `dt string` 分区

下面的 case 只包含 SparkOne 允许的语句。

Case 1：append 写入 Hive 表。

```sql
view sparkone_hive_append_data as
select * from values
  (3L, 'beijing'),
  (2L, 'shanghai')
as sparkone_hive_append_data(cnt, city);

save append sparkone_hive_append_data
as hive.`default.sparkone_save_hive_append`;

select city, cnt
from default.sparkone_save_hive_append
order by city;
```

预期：查询结果有 `beijing=3`、`shanghai=2` 两行。源视图列顺序是 `cnt, city`，与目标表的 `city, cnt` 相反，用于确认 append 按列名而非位置写入。

Case 2：Hive overwrite 永久拒绝。

```sql
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

预期：最后一条 Compile/Run 在写入前失败，错误包含 `hive-catalog` 和 `permanently denied`。配置和 SQL option 都不能放开。

Case 3：动态分区 append。

```sql
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

Case 4：缺列时在写入前失败。

```sql
view sparkone_hive_missing_column as
select 'should-not-write' as city;

save append sparkone_hive_missing_column
as hive.`default.sparkone_save_hive_append`;
```

预期：错误包含 `must match target columns by name`，目标表不会新增 `should-not-write` 数据。

说明：

- `save append ... as hive` 会先编译成 `WritePlan`，Run 时根据目标 schema 生成 Spark 3.3+ 支持的显式 column list `INSERT INTO TABLE ... (...) SELECT ...`。
- `save append ... as hive` 要求目标表已存在；SparkOne 不会自动创建 Hive 表，建表格式、分区定义和表结构由平台外 catalog 治理入口维护。
- 源和目标列名集合必须完全一致；列顺序可以不同，缺列、多列、重名列或不兼容类型都会在真正写入前失败。
- `partitionBy` 对应 Spark SQL 的动态分区 `PARTITION (...)`，分区列需要出现在源视图字段中。
- Hive、Doris、MySQL overwrite 永久拒绝；完整矩阵见 [../data/safe-save.md](../data/safe-save.md)。

## HDFS 和 Hive 测试

如果使用 `conf/sparkone.conf` 配置了 Hadoop/Hive/Kerberos，Hive 表仍按 catalog 标识读取；HDFS 文件必须先放到当前租户的 `/public/sparkone/user/${username}` workspace，再在页面使用相对路径。

HDFS CSV：

```sql
load csv.`imports/users.csv`
options header="true" and inferSchema="true"
as users;

select * from users limit 20;
```

Hive：

```sql
show namespaces in hive;
show tables in hive.default;
select * from hive.default.some_table limit 10;

load hive.`default.some_table` as t;
select * from t limit 20;
```

如果遇到认证、权限、NameNode 或 Hive metastore 错误，优先检查启动配置，而不是 SQL 编辑器本身。相关配置见 [../data/hadoop-hive.md](../data/hadoop-hive.md) 和 [../ops/startup.md](../ops/startup.md)。

## Excel 测试

`excel` 当前只是 provider 别名，主包不内置 Excel connector。要测试 Excel，启动时必须提供对应 provider jar 或 Maven package，例如在 `conf/sparkone.conf` 中配置：

```hocon
engines {
  local {
    type = "local"

    jars {
      packages = "dev.mauch:spark-excel_2.12:3.5.6_0.31.2"
      # 或者直接指定本地 jar：
      # jars = "/Users/qindongliang/.m2/repository/dev/mauch/spark-excel_2.12/3.5.6_0.31.2/spark-excel_2.12-3.5.6_0.31.2.jar"
      # 如果只是分发普通配置文件，用 files，不要用来放 provider jar：
      # files = "/path/to/app.conf"
    }
  }
}
```

`packages` 使用 Maven 坐标，由 Spark/Ivy 解析依赖；`jars` 对应 Spark 原生 `spark.jars`，可以直接写本地 jar 的绝对路径。`files` 对应 Spark 原生 `spark.files`，只分发普通文件，不会加入 classpath，不能用来加载 Excel provider。

然后页面里可以写：

```sql
load excel.`imports/jupyter_tasks.xlsx`
options header="true" and inferSchema="true"
as users_excel;

select * from users_excel limit 20;
```

如果 provider 没加载，`Compile` 可能成功，但 `Run` 会失败，因为真正解析 provider 的是 Spark runtime。

如果启动 SparkContext 时出现 `Failed to connect to /192.168...` 且日志里有 `Added JAR ... at spark://.../jars/...`，通常是本地调试时 Spark driver 广播地址和实际绑定地址不一致。`conf/sparkone.conf` 的 `engines.local.spark` 建议保留：

```hocon
engines {
  local {
    type = "local"

    spark {
      driverHost = "127.0.0.1"
      driverBindAddress = "127.0.0.1"
    }
  }
}
```

这个配置只适合本地 `local[*]` 调试；如果改成 `master = "yarn"`，不要把 `driverHost` 固定为 `127.0.0.1`，应使用 executor 能访问到的 driver 地址，或交给 Spark/YARN 环境决定。

## Compile 和 Run 的使用建议

- 写普通 Spark SQL 时，通常直接点 `Run`。
- 写 `load/save` 时，如已打开 `server.showCompiledSql = true`，可先点 `Compile` 确认转译出来的 Spark SQL 符合预期，再点 `Run`。
- 多条语句调试时，先把前置建表/建视图语句和最后查询语句放在同一次脚本里。
- 查询大表时先加 `limit`，并保持 `preview.maxRows` 和结果区 `Rows` 在较小值；默认是 10 行。
- 保存数据前先用 `select count(*)` 或抽样查询确认临时视图内容。

## 常见问题

`Run` 成功但没有表格结果：

- 原生 DDL/DML 会在 Compile 阶段拒绝；SparkOne 页面只允许查询、只读检查命令以及受控 `load/view/set/save`。

`Compile` 成功但 `Run` 失败：

- 常见原因是文件路径不存在、HDFS/Hive 权限不足、provider jar 未加载，或 Spark runtime 不支持对应数据源。

`load hive...` 带 options 报错：

- 当前 `hive` 是 catalog 表语义，不支持 `options` 参数。

`show database in hive` 报 Spark SQL 解析错误：

- `SHOW DATABASE` 没有单数形式。使用 `show namespaces in hive`，或者使用 `show databases in hive`。

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
