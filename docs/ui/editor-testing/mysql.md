# MySQL 测试

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
