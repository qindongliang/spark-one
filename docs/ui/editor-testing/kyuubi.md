# Kyuubi 数据源测试

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

### Catalog 配置与 `SHOW CATALOGS` 的差异

Spark 对 Catalog 使用懒加载。Kyuubi Session 配置中已经存在
`spark.sql.catalog.<name>=<plugin class>`，只表示该 Catalog 已配置、可以按名称访问；
Spark 仍会等到 `SHOW NAMESPACES IN <name>`、`SHOW TABLES IN <name>.<namespace>`、
`SELECT ... FROM <name>.<namespace>.<table>` 等语句首次引用它时，才创建 Catalog 实例。

`SHOW CATALOGS` 读取当前 Spark Session 的 `CatalogManager` 实例缓存，不会扫描全部
`spark.sql.catalog.*` 配置。因此新 Session 第一次执行时，结果通常只有
`spark_catalog`，尚未出现 `jdbc`、`doris`、`mysql_static` 或 `doris_static`，不能据此判断
ODEP 路由 Catalog 或静态 Catalog 配置未生效。应先访问目标 Catalog，再查看列表：

```sql
show catalogs;
show namespaces in jdbc;
show catalogs;
```

第二次 `show catalogs` 才应包含 `jdbc`。其他 Catalog 同理。`Tenant shared` 模式可以在后续
Run 中观察同一 Session 已实例化的 Catalog；`Run isolated` 每次都会创建新 Session，必须把
初始化语句和后面的 `show catalogs` 放在同一次 Run 中。

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

- ODEP `/index` 中待测试的 JDBC/Doris 数据源已有非空 `physicalNamespace`。
- `sparkone-kyuubi-odep-plugin` JAR 位于 Spark Engine 的 `spark.jars`；Kyuubi Server classpath 不需要该 JAR。
- `sparkone-mysql-provider` JAR 位于 Spark Engine 的 `spark.jars`，用于 ODEP MySQL alias 的分区读取。
- JDBC driver、Doris connector 已放入 Spark Engine classpath。
- Engine driver 能读取 `ODEP_API_URL`、`ODEP_KYUUBI_APP_ID`、`ODEP_KYUUBI_SIGN_KEY`。
- `kyuubi-defaults.conf` 配置了 `spark.sql.catalog.jdbc`、`spark.sql.catalog.doris` 路由类，且这两个前缀下没有静态连接参数。

首次修改 `kyuubi-defaults.conf` 或升级 JAR 后，按正常部署流程重启 Kyuubi Server 并停止旧 Engine。此后只修改 ODEP 注册信息时无需重启 Server，只需停止缓存旧索引/详情的 Engine。然后在 SparkOne 页面选择 Kyuubi engine，依次执行：

```sql
show namespaces in jdbc;
show catalogs;
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
show catalogs;
show tables in doris.recommend_prod;
select * from doris.recommend_prod.r_qa_log limit 10;

load doris.`recommend_prod.r_qa_log` as qa_log;
select * from qa_log limit 10;
```

`show namespaces` 首次调用 ODEP `POST /api/datasource/index`，返回 alias 而不返回真实数据库；首次访问某个 alias 的表时才调用 SparkOne/Kyuubi 专用的 `POST /api/datasource/resolve`，路由 Catalog 在 Engine 内将 alias 转成 `physicalNamespace`。`/resolve` 使用 ODEP 当前环境的 `common-url.rms.api` 和 `pk.name` 解析 PK 占位符，MLSQL 旧链路的 `/detail` 不受影响。两级结果都会缓存到当前 Engine 退出。如果目标 Catalog 已经出现在 `show catalogs` 中，但 `show namespaces` 报静态配置冲突，删除同名前缀的旧 Catalog 参数后重建 Engine。

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
