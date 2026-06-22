# SQL 编辑器测试手册

这个页面是 SparkOne MVP 的本地测试台，用来快速验证 Spark SQL、SparkOne 薄 DSL 转译、HDFS/Hive 配置和数据源读写。

访问地址通常是：

```text
http://127.0.0.1:7070
```

## 页面区域

- 左侧编辑器：输入一段 SQL 脚本，可以包含多条语句，用分号 `;` 分隔。
- `Compile`：只编译，不执行。只有 `server.showCompiledSql = true` 时才显示，适合检查 `load/save` 这类 SparkOne DSL 被转成了什么 Spark SQL。
- `Run`：编译后按顺序执行每条 SQL，后面的语句可以使用前面创建的临时视图。
- `Preview`：在结果区的 Preview tab 里显示；对 `load ... as t`，先 `Run` 注册临时视图并展示 schema，再点该结果里的 `Preview` tab 显式拉取 `t` 的预览数据。
- 选中执行：如果编辑器里有选中的 SQL，`Compile` 和 `Run` 只处理选中部分；没有选区时处理整篇脚本。
- `Run` 默认隐藏每条 statement 的编译后 SQL；如果需要调试转译结果，在 `conf/sparkone.conf` 里配置 `server.showCompiledSql = true`。
- `Rows`：控制每条 statement 最多预览多少行；默认上限是 `preview.maxRows = 10`，页面输入只能小于或等于该 HOCON 上限。
- 右侧结果区：展示每条语句的编译后 SQL、耗时、schema 和预览数据；schema 和预览数据通过 tab 切换，失败语句会显示错误信息。

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
SELECT * FROM default.some_table WHERE dt = date '2026-06-17';
```

预期：只返回满足条件的数据。是否触发 Hive 分区裁剪或 Parquet/ORC 谓词下推由 Spark/Hive 自身优化能力决定。

Doris：

先在 `conf/sparkone.conf` 配置 Spark Doris Catalog。SparkOne 本地运行时会把它转成 `spark.sql.catalog.doris.*`；接 Kyuubi 时，把同样的 Spark 配置放到 Kyuubi/Spark engine：

```hocon
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

save {
  # 只有测试 Doris 覆盖写时才打开；append 不需要。
  allowDorisOverwrite = false
}
```

验证 Hive 仍是默认 catalog：

```sql
show databases;
```

查看 Doris 库列表，两种写法都可以：

```sql
show namespaces in doris;
```

```sql
show databases in doris;
```

查看某个 Doris 库下的表列表：

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

如果在 SparkOne 编辑器里通过 Spark Doris Catalog 插入种子数据，目标表要写成三段 catalog 表名，并且 `DATE` 列建议使用 Spark SQL 的 date literal，避免 Spark 把字符串写入 `DATE` 列时报类型不兼容：

```sql
INSERT INTO doris.app.sparkone_doris_seed VALUES
  (1, 'beijing', 10, DATE '2026-06-01'),
  (2, 'shanghai', 20, DATE '2026-06-01'),
  (3, 'beijing', 30, DATE '2026-06-02'),
  (4, 'hangzhou', 40, DATE '2026-06-02');
```

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
  city,
  count(*) as row_count,
  sum(cnt) as total_cnt
from doris_seed
group by city;

save append doris_city_result
as doris.`app.sparkone_doris_city_result`;

select *
from doris.app.sparkone_doris_city_result
order by city;
```

预期可以看到 `beijing/shanghai/hangzhou` 的聚合结果。重复执行 append 会重复追加数据，这是 Doris 目标表和 Spark Doris Catalog append 写入的正常语义。

### Doris 表模型对 save append 结果的影响

SparkOne 的 `save append ... as doris` 只负责向目标表提交 `INSERT INTO TABLE doris.db.table SELECT ...`。最终查询效果由 Doris 目标表模型决定：

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

`save overwrite ... as doris` 会先走 Doris Catalog 的覆盖写语义，再由目标表模型处理本次写入数据。也就是说，SparkOne 不改变 Doris 表模型；是否保留明细、聚合指标或按 Key upsert，始终由目标表 DDL 决定。

测试 `save overwrite` 覆盖 Doris：

Doris overwrite 默认被启动级安全开关拦截。确认要测试覆盖写时，先在 `conf/sparkone.conf` 的 `save` 中打开并重启：

```hocon
save {
  allowDorisOverwrite = true
}
```

然后在编辑器里执行：

```sql
view doris_overwrite_result as
select
  "overwrite_city" as city,
  cast(1 as bigint) as row_count,
  cast(999 as bigint) as total_cnt;

save overwrite doris_overwrite_result
as doris.`app.sparkone_doris_city_result`
options sparkoneOverwrite="allow";

select *
from doris.app.sparkone_doris_city_result
order by city;
```

预期 `sparkone_doris_city_result` 中只剩 `overwrite_city` 这一行。`save overwrite ... as doris` 会编译成 `INSERT OVERWRITE TABLE doris.app.sparkone_doris_city_result SELECT ...`；SparkOne 不对 Doris 表做备份或回滚，提交语义由 Spark Doris Connector / Doris 负责。

说明：

- `doris.\`app.sparkone_doris_city_result\`` 中 `app` 是 Doris database，`sparkone_doris_city_result` 是 Doris 表名；SparkOne 会补成 Spark Catalog 表名 `doris.app.sparkone_doris_city_result`。
- `save append/overwrite ... as doris` 都要求目标表已存在；SparkOne 不会自动创建 Doris 表。表结构、key、distribution、分区等用 Doris DDL 先建好。
- `save doris` 不支持在 SQL 里写 `fenodes/user/password` 等连接 options，这些统一放在 `catalogs.doris` 或 Kyuubi/Spark engine 配置。
- `save doris` 不支持 `partitionBy`，Doris 的分布、分区和表结构应由 Doris DDL 管理。
- `save overwrite ... as doris` 需要先用 HOCON 打开 `save.allowDorisOverwrite = true`，再用单条 SQL 的 `sparkoneOverwrite="allow"` 显式确认；`allowNativeInsertOverwrite` 不能替代这个开关。

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

`load mysql` 会在运行时用 Spark JDBC reader 注册临时视图。`Compile` 只展示安全占位 SQL，不展示 HOCON 里的账号密码：

```sql
SELECT 'LOAD MYSQL' AS sparkone_action, 'sparkone_mysql_seed AS mysql_seed' AS sparkone_target
```

大表读取最佳实践：

如果 MySQL 表有自增主键 `id`，并且数据量约 3000 万行，不要使用默认单分区 JDBC 读取。默认读取通常只有一个 JDBC 任务在拉全表，吞吐低，也容易把单个 executor task、MySQL 连接或网络打满。推荐用自增主键做 Spark JDBC 分区列，让 Spark 并发发起多个范围查询。

先在 MySQL 侧确认主键范围和数据量：

```sql
select min(id) as min_id, max(id) as max_id, count(*) as row_count
from big_orders;
```

确认 `id` 是主键或至少有索引：

```sql
show index from big_orders;
```

在 SparkOne 编辑器里读取大表：

```sql
load mysql.`analytics.big_orders`
options partitionColumn="id"
and lowerBound="1"
and upperBound="30000000"
and numPartitions="24"
and fetchsize="10000"
as big_orders_30m;

select count(*) as row_count from big_orders_30m;
```

如果 `id` 最大值已经明显超过 3000 万，就把 `upperBound` 改成上面 `max(id)` 查到的值，例如：

```sql
load mysql.`analytics.big_orders`
options partitionColumn="id"
and lowerBound="1"
and upperBound="48291377"
and numPartitions="32"
and fetchsize="10000"
as big_orders_30m;
```

参数含义：

- `partitionColumn="id"`：用 `id` 这一列切分 JDBC 读取任务。Spark 要求它是 numeric、date 或 timestamp 类型；MySQL 自增主键最适合，前提是有索引。
- `lowerBound` / `upperBound`：用于计算每个分区的步长。它们不是过滤条件，不会只读取这个范围内的数据；如果要限制业务数据范围，需要在后续 SQL 里显式写 `where id between ...`。
- `numPartitions`：并发 JDBC 分区数，也近似等于同时打到 MySQL 的查询连接数上限。3000 万行可以从 `16` 或 `24` 起步；如果 MySQL、网络和 Spark executor 都有余量，再试 `32`。不要盲目开到很大，否则会把 MySQL 连接池、buffer pool 或磁盘 IO 打满。
- `fetchsize`：每次 JDBC round trip 拉取的行数。大表读取建议从 `5000` 到 `10000` 试起；太小会增加网络往返，太大可能增加单 task 内存压力。

调参建议：

- 先估算单分区数据量：`30000000 / 24` 约 125 万行/分区；如果单行很宽，优先降低 `numPartitions` 或只选择必要列。
- 用 Spark UI 看 task 是否均衡；如果少数 task 特别慢，通常是 `id` 分布有大空洞或热点范围，需要重新选择更均匀的分区列，或按日期/主键范围分批读取。
- 用 MySQL 监控观察并发连接数、慢查询、磁盘 IO 和 buffer pool 命中率；Spark 侧更快不代表源库能承受。
- 如果 MySQL Connector/J 实际没有按 `fetchsize` 流式拉取，可在 HOCON 的 JDBC URL 中评估加入 `useCursorFetch=true`，并用 executor 内存和 MySQL 连接状态验证效果。
- 大表抽样查看时不要直接 `select *`；先用 `select * from big_orders_30m where id between 1 and 1000 order by id` 或 `limit 100`。

Spark JDBC 参数语义以 [Spark 3.5.7 JDBC 官方文档](https://spark.apache.org/docs/3.5.7/sql-data-sources-jdbc.html) 为准：`partitionColumn`、`lowerBound`、`upperBound`、`numPartitions` 必须成组使用；上下界只用于分区步长，不负责过滤数据。

带 `where` 过滤条件的读取：

Spark JDBC 官方支持 `query` 参数，例如在 DataFrameReader 里写 `.option("query", "select c1, c2 from t1")`。但官方同时规定：

- `query` 不能和 `dbtable` 同时指定。
- `query` 不能和 `partitionColumn` 同时指定；如果既要查询过滤又要分区读取，官方建议把子查询写到 `dbtable`，例如 `"(select c1, c2 from t1) as subq"`，再指定 `partitionColumn`。

SparkOne 因此不开放 SQL 侧 `query=`。`load mysql.\`analytics.table\`` 仍然从路径里的 `table` 自动生成 JDBC `dbtable`，并禁止 SQL 侧覆盖 `dbtable` 连接目标。需要过滤时，使用 SparkOne 扩展的 `where "..."`，编译器会把它变成 Spark JDBC 官方推荐的 `dbtable` 子查询：

```sql
load mysql.`analytics.big_orders`
where "biz_date = '2026-06-10' and status = 'PAID'"
options partitionColumn="id"
and lowerBound="1"
and upperBound="30000000"
and numPartitions="24"
and fetchsize="10000"
as big_orders_paid;
```

它等价于给 Spark JDBC reader 传入类似下面的 `dbtable`：

```text
(select * from big_orders where biz_date = '2026-06-10' and status = 'PAID') as sparkone_mysql_load
```

再继续传 `partitionColumn/lowerBound/upperBound/numPartitions/fetchsize`。这样既能让 MySQL 先做业务过滤，又保留 Spark JDBC 的并行范围读取能力。

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
- `partitionColumn/lowerBound/upperBound/numPartitions` 只负责并行切分；业务过滤写在 `where "..."` 里。
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

3. 只写分区参数：不做业务过滤，但按 `id` 范围并发读取。

```sql
load mysql.`analytics.sparkone_mysql_orders_demo`
options partitionColumn="id"
and lowerBound="1"
and upperBound="8"
and numPartitions="4"
and fetchsize="1000"
as orders_partitioned;

select count(*) as row_count
from orders_partitioned;
```

预期：读取 8 行。`lowerBound/upperBound` 只用于计算 Spark JDBC 分区步长，不会过滤掉边界外的数据；业务过滤必须写 `where "..."`。

4. 同时写 `where` 和分区参数：MySQL 侧先按子查询过滤，Spark JDBC 再按 `id` 并发读取子查询结果。

```sql
load mysql.`analytics.sparkone_mysql_orders_demo`
where "status = 'PAID'"
options partitionColumn="id"
and lowerBound="1"
and upperBound="8"
and numPartitions="4"
and fetchsize="1000"
as orders_paid_partitioned;

select id, city, amount, status, biz_date
from orders_paid_partitioned
order by id;
```

预期：读取 5 行 `PAID` 订单。底层 JDBC `dbtable` 是：

```text
(select * from sparkone_mysql_orders_demo where status = 'PAID') as sparkone_mysql_load
```

同时仍会传 `partitionColumn=id`、`lowerBound=1`、`upperBound=8`、`numPartitions=4` 给 Spark JDBC reader。

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
- `save append/overwrite ... as mysql` 都要求目标表已存在；SparkOne 不会自动创建 MySQL 表。表结构、主键、索引等用 MySQL DDL 先建好。
- SQL 里的 `options` 只能补充 `fetchsize`、`batchsize`、`truncate` 等非连接参数，不能覆盖 `url/user/password/driver/dbtable`。
- `save overwrite ... as mysql` 需要先用 HOCON 打开 `save.allowMysqlOverwrite = true`，再用单条 SQL 的 `sparkoneOverwrite="allow"` 显式确认；SparkOne 不会对 MySQL 表做备份。
- `load` 会注册临时视图；普通 `Run` 默认只展示 schema，不自动拉取数据。需要看样例数据时，在该结果的 Preview tab 里点 `Preview`，预览行数受 `preview.maxRows` 和页面 `Rows` 共同限制。需要更精确抽样时，仍建议在后续 `select * from mysql_seed limit 10` 中显式限制。

## 使用 SparkOne Save DSL

文件类 save 当前只支持 `save overwrite`。它会转成 Spark SQL 的 `INSERT OVERWRITE DIRECTORY`。

为了避免路径写错时覆盖已有目录，默认 `conf/sparkone.conf` 使用：

```hocon
save {
  overwritePolicy = "requireExplicit"
  overwriteBackup = "rename"
  overwriteBackupPath = "/tmp/sparkone_back"
  allowDorisOverwrite = false
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

备份策略只能在 `conf/sparkone.conf` 的 `save` 中配置，不能用单条 SQL 或 `SET` 覆盖。例如要改成 Hadoop Trash：

```hocon
save {
  overwriteBackup = "trash"
}
```

然后执行覆盖写时仍只写确认信号：

```sql
save overwrite city_stats as json.`/tmp/city_stats_json`
options sparkoneOverwrite="allow";
```

`overwriteBackup` 支持：

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
- `save append/overwrite ... as hive` 都要求目标表已存在；SparkOne 不会自动创建 Hive 表，建表格式、分区定义和表结构用 Spark 原生 `CREATE TABLE` 明确声明。
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
- 写 `load/save` 时，如已打开 `server.showCompiledSql = true`，可先点 `Compile` 确认转译出来的 Spark SQL 符合预期，再点 `Run`。
- 多条语句调试时，先把前置建表/建视图语句和最后查询语句放在同一次脚本里。
- 查询大表时先加 `limit`，并保持 `preview.maxRows` 和结果区 `Rows` 在较小值；默认是 10 行。
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
