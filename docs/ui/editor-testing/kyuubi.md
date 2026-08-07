# Catalog 与远程 Engine 数据源测试

ODEP 路由 Catalog 和 `queryone_mysql` 分区读取属于 Local/Kyuubi 共用的页面能力：同一组 SQL
应先在 Local 调试，再在 Kyuubi 的 `Tenant shared` 模式验收。Local 默认从 QueryOne HOCON
注册路由和 provider，使用 `TenantContext.username` 作为 Local subject；Kyuubi 从远端 Spark
Engine 配置加载 Catalog、provider JAR 和连接信息，使用签名 session user。本文后半部分的
ECDSA、远端 Engine 配置、连接恢复和 Session 隔离仍是 Kyuubi 专属。

## 测试三段式 Catalog

页面测试统一使用下面五组用户可见标识：

| 数据源 | 三段式表名 | `load` 写法 |
| --- | --- | --- |
| Hive | `hive.<database>.<table>` | `load hive.\`database.table\`` |
| ODEP JDBC | `jdbc.<alias>.<table>` | `load jdbc.\`alias.table\`` |
| ODEP Doris | `doris.<alias>.<table>` | `load doris.\`alias.table\`` |
| 静态 MySQL | `mysql_static.<database>.<table>` | 仅 `SHOW NAMESPACES/TABLES`；表访问应拒绝 |
| 静态 Doris | `doris_static.<database>.<table>` | 仅 `SHOW NAMESPACES/TABLES`；表访问应拒绝 |

其中 ODEP 的第二段是注册别名，由路由 Catalog 映射到 `physicalNamespace`；静态数据源的第二段就是真实数据库名。`hive` 会由 QueryOne 改写为 Spark 内置的 `spark_catalog`。

静态 `mysql_static` / `doris_static` 只用于确认外部 connector 的元数据配置。当前 RMS 资源模型不识别这两个 Catalog，表数据访问必须 fail closed；不要把静态 Catalog 的表查询成功当成 Local/Kyuubi 公共能力。需要验证 ODEP 数据时，使用 `jdbc.<alias>.<table>` 或 `doris.<alias>.<table>`。

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

这组用例在 Local 和 Kyuubi 上执行同一份 SQL。Local 需要配置 `engines.local.odep` 或
`ODEP_*` 环境变量并使用默认的 `jdbc` / `doris` 路由 Catalog；Kyuubi 需要在 Spark Engine 侧部署对应 Catalog 模块、扩展和
connector JAR。Local 用于断点查看 alias resolve、LogicalPlan 资源提取和 RMS 请求，Kyuubi
用于验收远程 Engine 的签名 subject、物理 classpath 和真实网络链路。

前置检查：

- ODEP `/index` 中待测试的 JDBC/Doris 数据源已有非空 `physicalNamespace`。
- `queryone-odep-catalog` JAR 位于 Spark Engine 的 `spark.jars`；Kyuubi Server classpath 不需要该 JAR。
- `queryone-mysql-provider` JAR 位于 Spark Engine 的 `spark.jars`，用于 ODEP MySQL alias 的分区读取。
- JDBC driver、Doris connector 已放入 Spark Engine classpath。
- Local Server 能读取 `engines.local.odep` 或同名环境变量；Kyuubi Engine driver 能读取 `ODEP_API_URL`、`ODEP_KYUUBI_APP_ID`、`ODEP_KYUUBI_SIGN_KEY`。
- `kyuubi-defaults.conf` 配置了 `spark.sql.catalog.jdbc`、`spark.sql.catalog.doris` 路由类，且这两个前缀下没有静态连接参数。

首次修改 Kyuubi 的 `kyuubi-defaults.conf` 或升级远端 JAR 后，按正常部署流程重启 Kyuubi Server 并停止旧 Engine。Local 只需重启 QueryOne 服务以重建 SparkSession；此后只修改 ODEP 注册信息时两边都应重建/停止持有旧索引的 Engine。然后分别在 QueryOne 页面选择 Local 和 Kyuubi，依次执行：

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

`show namespaces` 首次调用 ODEP `POST /api/datasource/index`，返回 alias 而不返回真实数据库；首次访问某个 alias 的表时才调用 QueryOne/Kyuubi 专用的 `POST /api/datasource/resolve`，路由 Catalog 在 Engine 内将 alias 转成 `physicalNamespace`。`/resolve` 使用 ODEP 当前环境的 `common-url.rms.api` 和 `pk.name` 解析 PK 占位符，MLSQL 旧链路的 `/detail` 不受影响。两级结果都会缓存到当前 Engine 退出。如果目标 Catalog 已经出现在 `show catalogs` 中，但 `show namespaces` 报静态配置冲突，删除同名前缀的旧 Catalog 参数后重建 Engine。

### 静态数据源

当前 Local/Kyuubi 配置中的静态 Catalog 使用 `mysql_static` 和 `doris_static`，与 ODEP 独占的
`jdbc`、`doris` 前缀不冲突。静态 Catalog 只做元数据检查，验证 MySQL：

```sql
show namespaces in mysql_static;
show tables in mysql_static.Dworks;
select * from mysql_static.Dworks.cloud_host_info limit 10;
```

最后一条查询应在分析阶段被拒绝，并包含 `Unsupported catalog for authorization: mysql_static`。
表数据访问请改用 ODEP 路由，例如 `select * from jdbc.<alias>.<table>`。

验证 Doris：

```sql
show namespaces in doris_static;
show tables in doris_static.dataagent;
select * from doris_static.dataagent.r_qa_log limit 10;
```

最后一条查询应在分析阶段被拒绝，并包含 `Unsupported catalog for authorization: doris_static`。
表数据访问请改用 `doris.<alias>.<table>`。

## 测试 `queryone_mysql` Provider（Local/Kyuubi）

这组用例验证 ODEP JDBC alias 的并行读取，Local 和 Kyuubi 应执行同一份 SQL。Local 使用
QueryOne fat jar 内置的路由 Catalog、provider 和 HOCON/`ODEP_*` 配置；Kyuubi 使用远端 Spark
Engine 的对应 JAR、JDBC driver 和 ODEP 配置。静态 `mysql_static` 只用于元数据检查，不能
作为这组表数据读取的入口。

前置检查：

- ODEP `/index` 中待测试的 JDBC alias 已有非空 `physicalNamespace`，且对应数据源是 MySQL。
- Local 服务已加载 `queryone_mysql` provider；Kyuubi/Spark engine 还需显式部署
  `queryone-mysql-provider_2.12-0.1.0-SNAPSHOT.jar` 和 MySQL JDBC driver。
- Local 能读取 `engines.local.odep` 或同名环境变量；Kyuubi Engine 能读取 `ODEP_API_URL`、`ODEP_KYUUBI_APP_ID`、`ODEP_KYUUBI_SIGN_KEY`。

在编辑器里分别选择 Local 和 Kyuubi，执行：

```sql
load jdbc.`sync_search.drug_ai_drug_decision`
where "menu_id = '1_0' AND section_id = 5"
options partitionColumn="modify_time"
as orders_big;

select count(*) from orders_big;
```

预期两边的编译结果都包含临时视图注册：

```sql
CREATE OR REPLACE TEMPORARY VIEW orders_big
USING queryone_mysql
OPTIONS (
  catalog 'jdbc',
  alias 'sync_search',
  dbtable 'drug_ai_drug_decision',
  whereClauseBase64 '<过滤条件>',
  partitionColumn 'modify_time',
  numPartitions '10',
  fetchsize '10000'
)
```

实际的 `whereClauseBase64` 值由编译器生成，页面不需要手写。上面的示意中
`partitionColumn` 应为真实表中的分区列，例如 `modify_time`；日志中应以实际值为准。

然后 `select count(*) from orders_big` 触发真正读取。验证分区参数是否生效：

```sql
EXPLAIN FORMATTED
SELECT count(*) FROM orders_big;
```

物理计划里应能看到：

```text
Scan JDBCRelation(<physical_namespace>.drug_ai_drug_decision) [numPartitions=10]
```

再看 Spark engine 的 Spark UI。local/client 模式通常是 `http://127.0.0.1:4040/jobs/`；YARN/Kubernetes/cluster 模式看 Spark application tracking URL。对 `select count(*) from orders_big`，负责 JDBC scan 的 stage 应有 `10/10` tasks。看到 `1/1 skipped` 的辅助 job 不代表分区没生效，通常是 AQE、聚合收尾或结果复用。

如果执行 `load` 后立刻在 Local server log 或 Kyuubi operation log 里看到：

```sql
SELECT * FROM `orders_big` LIMIT 101
```

这是 QueryOne 页面预览，不是 `load` 自己全量查询。原因通常是 `preview.defaultTab = "preview"`，或者点击了结果区的 Preview tab。`101` 表示请求 100 行再多取 1 行判断是否截断；实际上限受 `preview.maxRows` 和页面 `Rows` 控制。

注意：

- `load jdbc.\`alias.table\`` 无大表参数时编译成 ODEP 路由 Catalog SQL；带 `partitionColumn` 或其他大表读取参数时编译成 `USING queryone_mysql`。Local 和 Kyuubi 的编译形态相同。
- SQL options 禁止传 `url/user/password/driver/dbtable/query`；Local 的连接信息来自 ODEP resolve，Kyuubi 的连接信息来自远端 Spark Engine/ODEP。
- `lowerBound/upperBound` 只决定 Spark JDBC 分区步长，不做业务过滤。业务过滤写 `where "..."`，例如 `where "biz_date = '2026-07-07'"`。
- `numPartitions` 会增加对 MySQL 的并发连接和 IO 压力。验证通过后再按 MySQL 监控、Spark task 耗时和源表索引情况调大。
- `load jdbc/mysql ... options partitionColumn=...` 会并行读取完整的过滤结果，不支持源端全局 `LIMIT`。后续 `select * from orders_big limit 10` 只限制 Spark 输出；需要 MySQL 端下推 `LIMIT` 时直接查询三段式 Catalog 表，不要同时使用分区 LOAD。

### Kyuubi 静态 MySQL 写入边界

静态 `_static` Catalog 不走 RMS；未知 Catalog 仍 fail closed。验证静态 MySQL append：

```sql
view stage3b_mysql_source as
select * from values
  (101, 'stage3b-alice'),
  (102, 'stage3b-bob')
as stage3b_mysql_source(id, name);

save append stage3b_mysql_source
as jdbc.`mysql_static.Dworks.queryone_3b_target`;
```

预期写入成功，随后可用 `select * from mysql_static.Dworks.queryone_3b_target` 验证。ODEP
MySQL alias 仍只开放读取和分区读取，`save jdbc.\`alias.table\`` 会在 Compile 阶段拒绝。
