# Doris 测试

先在 Local 的 `conf/queryone.conf` 配置静态 Spark Doris Catalog；Kyuubi 测试则把同等参数放在
远端 Spark Engine。QueryOne 本地运行时会把它转成 `spark.sql.catalog.doris_static.*`；默认的
`doris` 名称保留给 ODEP 路由 Catalog：

```hocon
engines {
  local {
    type = "local"

    catalogs.doris_static {
      fenodes = "fe-1:8030,fe-2:8030"
      queryPort = 9030
      user = "root"
      password = "******"

      options {
        doris.request.retries = 3
      }
    }

    jars {
      packages = "org.apache.doris:spark-doris-connector-spark-3.3:25.2.0"
    }
  }
}
```

验证 Hive 仍是默认 catalog：

```sql
show databases;
```

查看静态 Doris catalog 的库列表，两种写法都可以：

```sql
show namespaces in doris_static;
```

```sql
show databases in doris_static;
```

查看 Local `catalogs.doris_static` 中某个 Doris 库下的表列表：

```sql
show tables in doris_static.dataagent;
```

静态 Catalog 的元数据可用于确认 connector 配置，但当前 RMS 资源模型不识别 `doris_static`，表读取会 fail closed。下文 `doris.app.<table>` 的读写用例要求 ODEP 中存在逻辑 alias `app`；Local 和 Kyuubi 都通过同一个 ODEP 路由与 RMS 鉴权链路执行。

Doris 测试表可以先用 Doris/MySQL 协议客户端准备。下面的 `replication_num` 按本地单副本测试环境写，生产集群按实际副本数调整：

```sql
CREATE DATABASE IF NOT EXISTS app;

DROP TABLE IF EXISTS app.queryone_doris_seed;
CREATE TABLE app.queryone_doris_seed (
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

INSERT INTO app.queryone_doris_seed VALUES
  (1, 'beijing', 10, '2026-06-01'),
  (2, 'shanghai', 20, '2026-06-01'),
  (3, 'beijing', 30, '2026-06-02'),
  (4, 'hangzhou', 40, '2026-06-02');
```

不要把上面的 DDL 或 `INSERT` 复制到 QueryOne 编辑器；原生 catalog 修改命令会在 Compile 阶段拒绝。种子数据也应通过 Doris 管理客户端准备。

在 QueryOne 编辑器里读取 Doris 表：

```sql
select *
from doris.app.queryone_doris_seed
order by id;
```

预期：返回 4 行，`city/cnt/biz_date` 与上面插入的数据一致。过滤直接写 Spark SQL：

```sql
select id, city, cnt
from doris.app.queryone_doris_seed
where biz_date = date '2026-06-02'
order by id;
```

预期：返回 `id=3` 和 `id=4` 两行。过滤下推由 Spark Doris Catalog / Connector 的 DataSource V2 能力决定。

`load doris` 只是临时视图语法糖：

```sql
load doris.`app.queryone_doris_seed` as doris_seed;

select *
from doris_seed
order by id;
```

编译结果应是标准 Spark SQL：

```sql
CREATE OR REPLACE TEMPORARY VIEW doris_seed AS SELECT * FROM doris.app.queryone_doris_seed
```

`load doris` 也可以追加 `where "..."`，编译结果仍是标准 Spark SQL，过滤是否源端下推由 Spark Doris Catalog / Connector 决定：

```sql
load doris.`app.queryone_doris_seed`
where "biz_date = date '2026-06-02'"
as doris_seed_0602;

select id, city, cnt
from doris_seed_0602
order by id;
```

预期：返回 `id=3` 和 `id=4` 两行。编译结果应包含：

```sql
CREATE OR REPLACE TEMPORARY VIEW doris_seed_0602 AS SELECT * FROM doris.app.queryone_doris_seed WHERE biz_date = date '2026-06-02'
```

用 Doris 临时视图继续做 SQL 分析：

```sql
load doris.`app.queryone_doris_seed` as doris_seed;

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
load doris.`app.queryone_doris_seed`
options password="bad"
as doris_seed;
```

预期：编译失败，提示 `LOAD doris does not support SQL OPTIONS`。

测试 `save append` 写入 Doris：

先用 Doris/MySQL 协议客户端准备目标表：

```sql
DROP TABLE IF EXISTS app.queryone_doris_city_result;
CREATE TABLE app.queryone_doris_city_result (
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

然后在 QueryOne 编辑器里执行：

```sql
load doris.`app.queryone_doris_seed` as doris_seed;

view doris_city_result as
select
  sum(cnt) as total_cnt,
  city,
  count(*) as row_count
from doris_seed
group by city;

save append doris_city_result
as doris.`app.queryone_doris_city_result`;

select *
from doris.app.queryone_doris_city_result
order by city;
```

预期可以看到 `beijing/shanghai/hangzhou` 的聚合结果。源视图列顺序为 `total_cnt, city, row_count`，与 Doris 目标表不同，用于验证按列名写入。重复执行 append 会重复追加数据，这是 Doris 目标表和 Spark Doris Catalog append 写入的正常语义。

## Doris 表模型对 save append 结果的影响

QueryOne 的 `save append ... as doris` 会在 schema 预检后向目标表提交带显式目标列清单和源列投影的 `INSERT INTO TABLE doris.db.table (...) SELECT ...`。最终查询效果由 Doris 目标表模型决定：

- `DUPLICATE KEY`：保留所有写入行，不去重、不聚合。
- `AGGREGATE KEY`：按 Key 列聚合 Value 列，适合固定汇总指标。
- `UNIQUE KEY`：按 Key 列 UPSERT，同 Key 新数据覆盖旧数据。

可以用同一份 `doris_city_result` 写入三种 Doris 目标表观察差异。

准备 Duplicate Key 目标表：

```sql
DROP TABLE IF EXISTS app.queryone_doris_city_duplicate;
CREATE TABLE app.queryone_doris_city_duplicate (
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
DROP TABLE IF EXISTS app.queryone_doris_city_aggregate;
CREATE TABLE app.queryone_doris_city_aggregate (
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
DROP TABLE IF EXISTS app.queryone_doris_city_unique;
CREATE TABLE app.queryone_doris_city_unique (
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

在 QueryOne 编辑器里生成同一份待写入结果：

```sql
load doris.`app.queryone_doris_seed` as doris_seed;

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
as doris.`app.queryone_doris_city_duplicate`;

save append doris_city_result
as doris.`app.queryone_doris_city_aggregate`;

save append doris_city_result
as doris.`app.queryone_doris_city_unique`;
```

查看 Duplicate Key 表：

```sql
select city, count(*) as physical_rows, sum(row_count) as sum_row_count, sum(total_cnt) as sum_total_cnt
from doris.app.queryone_doris_city_duplicate
group by city
order by city;
```

预期：重复 append 两次后，每个 `city` 会有两条物理行；例如 `beijing` 的 `physical_rows=2`、`sum_row_count=4`、`sum_total_cnt=80`。

查看 Aggregate Key 表：

```sql
select city, row_count, total_cnt
from doris.app.queryone_doris_city_aggregate
order by city;
```

预期：重复 append 两次后，同 Key 指标会按 `SUM` 聚合；例如 `beijing` 的 `row_count=4`、`total_cnt=80`。

查看 Unique Key 表：

```sql
select city, row_count, total_cnt
from doris.app.queryone_doris_city_unique
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
as doris.`app.queryone_doris_city_result`;
```

预期编译失败，错误包含 `doris-catalog` 和 `permanently denied`。不存在可放开 Doris overwrite 的 SQL option 或 HOCON 配置。

说明：

- `doris.\`app.queryone_doris_city_result\`` 中 `app` 是 Doris database，`queryone_doris_city_result` 是 Doris 表名；QueryOne 会补成 Spark Catalog 表名 `doris.app.queryone_doris_city_result`。
- `save append ... as doris` 要求目标表已存在；QueryOne 不会自动创建 Doris 表。表结构、key、distribution、分区等用 Doris DDL 先建好。
- `save doris` 不支持在 SQL 里写 `fenodes/user/password` 等连接 options，这些统一放在 `engines.local.catalogs.doris_static` 或 Kyuubi/Spark engine 配置。
- `save doris` 不支持 `partitionBy`，Doris 的分布、分区和表结构应由 Doris DDL 管理。
