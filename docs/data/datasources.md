# Data Sources

QueryOne 的数据源策略是：compiler 负责把 SQL 友好的 `load/save` 薄 DSL 编译成 Spark SQL 或极薄 runtime adapter，connector jar 由 Spark/Kyuubi 运行环境提供。Doris 这类支持 Spark Catalog 的系统优先走 Catalog，便于 QueryOne 逐步退化成“DSL 编译器 + SQL 提交器”。

默认主包不内置第三方 provider。这样可以避免 Excel、Mongo、ES、Kafka 等 connector 和 QueryOne 主应用强耦合，也减少 shade 冲突。

内置 Spark provider：

```text
csv
json
parquet
orc
text
libsvm
```

文件 provider 的 `load` 接受 workspace 相对路径，例如 `load parquet.\`extension-test/result\` as result`。默认解析到 `/public/odep/user/${username}/extension-test/result`；通过 `options owner="bob"` 可以读取其他用户 workspace，并在 Local 或 Kyuubi Spark Engine 中按绝对路径调用 ODEP/RMS `hdfs read` 鉴权。原生 SQL/`view` 也允许受支持文件 provider 使用无 authority 的绝对 HDFS 路径；Engine 在纯语法解析后先批量鉴权一次，允许后才进入目录枚举和 schema inference。允许结果绑定当前计划，analysis 完成后在本地核对最终 relation，不重复请求 ODEP。相对原生 relation、本地文件、S3/OSS、glob、百分号编码、路径穿越、重复分隔符和内部 overwrite 工作目录仍拒绝。

Catalog 表统一使用 Spark 原生三段式。Local/Kyuubi 静态 Catalog 使用 `mysql_static.<database>.<table>` 和 `doris_static.<database>.<table>`；ODEP 路由 Catalog 使用 `jdbc.<alias>.<table>` 和 `doris.<alias>.<table>`，由 alias 绑定连接和真实数据库。Hive 使用逻辑别名 `hive.<database>.<table>`，compiler 会改写为 Spark 内置 `spark_catalog.<database>.<table>`。平台协议不增加四段式。

特殊 source：

- `hive` 是 `spark_catalog` 的逻辑别名：`show namespaces in hive`、`show tables in hive.db` 和 `select * from hive.db.table` 会在对应 catalog 语义位置改写为 `spark_catalog`。Spark SQL 不支持单数 `show database in hive`。
- `load hive.\`db.table\` as t` 编译成 `CREATE OR REPLACE TEMPORARY VIEW t AS SELECT * FROM spark_catalog.db.table`。
- `load hive.\`db.table\` where "dt = date '2026-06-17'" as t` 会编译成 `SELECT * FROM spark_catalog.db.table WHERE ...`；分区裁剪和谓词下推由 Spark/Hive 自身优化能力决定。
- `save append t as hive.\`db.table\`` 先生成 `WritePlan`，执行时按目标列顺序生成 `INSERT INTO TABLE spark_catalog.db.table (目标列...) SELECT 源列... FROM t`；要求目标表已存在且列名集合一致。
- `save overwrite t as hive.\`db.table\`` 由固定能力矩阵永久拒绝，不存在可放开的配置。
- `partitionBy` 仅用于 catalog 表写入：`save append t as hive.\`db.table\` partitionBy dt` 编译成动态分区插入。
- QueryOne 不复刻 MLSQL 的 `storage="hive"` / 数据湖替换逻辑，也不开放原生建表/改表语句；目标表由平台外 catalog 治理入口创建和维护。
- 静态 JDBC Catalog 使用以 `_static` 结尾的顶层名，例如 `mysql_static.database.table`。Local 从 `engines.local.catalogs.mysql_static` 注册；Kyuubi 从远端 Spark Engine 配置注册。
- 静态表可直接 `select * from mysql_static.app.users`，也可用 `load jdbc.\`mysql_static.app.users\` as users` 创建临时视图。静态 Catalog 不走 RMS，但必须来自管理员可信配置。
- ODEP 动态 JDBC 仍使用 `jdbc.alias.table` 或 `load jdbc.\`alias.table\``，由 alias 定位连接与真实库，并走 RMS 鉴权。
- `save append t as jdbc.\`mysql_static.app.target_table\`` 复用 JDBC Catalog；目标必须存在，路径必须为三段式且不接受 SQL `OPTIONS`。动态 ODEP alias 不开放写入。
- 静态 JDBC append 要求源和目标列名集合完全一致，写入前校验类型兼容，并按目标列顺序投影；overwrite 由固定能力矩阵永久拒绝。
- ODEP 模式使用稳定顶层 `doris` Catalog；`select * from doris.alias.users` 由 alias 路由到对应连接和 `physicalNamespace`。当前 Kyuubi 静态 Catalog 使用 `doris_static`。
- `show namespaces in doris` 查看 ODEP alias；`show namespaces in doris_static` 查看静态 Catalog 的真实 database。裸写 `show databases` 仍查看默认 Hive catalog。
- `load doris.\`alias.users\` as users` 编译成 `CREATE OR REPLACE TEMPORARY VIEW users AS SELECT * FROM doris.alias.users`；静态 Catalog 写法为 `load doris.\`doris_static.db.users\``。
- `load doris.\`alias.users\` where "dt = date '2026-06-17'" as users` 会编译成 `SELECT * FROM doris.alias.users WHERE ...`；是否源端下推由 Spark Doris Catalog / Connector 的谓词下推能力决定。
- `save append t as doris.\`alias.target\`` 和 Hive 共用显式目标列清单与源列投影；静态目标使用 `save append t as doris.\`doris_static.db.target\``。两种写法都要求目标表已存在且列名集合一致。
- `save overwrite t as doris.\`db.target\`` 由固定能力矩阵永久拒绝。
- `save doris` 不支持 SQL 里的 Doris 连接 options，也不支持 `partitionBy`；连接和写入参数应放在 Spark Doris Catalog 配置中。
- `save doris` 不改变 Doris 表模型语义：`DUPLICATE KEY` 会保留所有写入行，`AGGREGATE KEY` 会按 Key 聚合 Value 列，`UNIQUE KEY` 会按 Key UPSERT。重复 append 的最终查询效果由目标表 DDL 决定。
- Hive/MySQL/Doris 的 `save append` 不会自动创建目标表。目标表、分区、索引、Doris key/distribution 等应由平台外 DDL 流程先创建和治理。
- Doris 查询和聚合使用 Spark 标准 SQL，例如 `select city, count(*) from doris.alias.users group by city`；写入必须使用 `save append ... as doris`，原生 `insert` 会被写入旁路保护拒绝。

MySQL 连接统一配置为静态 Catalog，不再维护 `datasources.mysql` adapter：

```hocon
engines {
  local {
    type = "local"

    catalogs.mysql_static {
      url = "jdbc:mysql://127.0.0.1:3306/?databaseTerm=SCHEMA"
      user = "reader"
      password = ${?QUERYONE_MYSQL_CATALOG_PASSWORD}

      options {
        fetchsize = 1000
      }
    }
  }
}
```

这里的 `databaseTerm=SCHEMA` 只用于 Spark 原生 JDBC Catalog。原因是 Spark 执行 `show tables in mysql_static.app` 时，会把 `app` 作为 JDBC `DatabaseMetaData.getTables` 的 `schemaPattern` 参数传给驱动；而 MySQL Connector/J 默认把 MySQL database 解释为 JDBC catalog，不解释为 schema。加上该参数后，MySQL database 会按 JDBC schema 暴露，`show tables in mysql_static.app` 才会过滤到 `app` 这个库。

### ODEP JDBC/Doris 路由 Catalog

ODEP 模式由 `queryone-odep-catalog` 提供两个稳定的顶层 Catalog；Local server 默认内置并注册，Kyuubi 由 Engine 侧部署：

```text
jdbc.<alias>.<table>
doris.<alias>.<table>
```

alias 是 ODEP 注册名，必须符合 `[A-Za-z_][A-Za-z0-9_]*`；`physicalNamespace` 是底层真实数据库。Engine 首次枚举某个 type 时通过 `POST /api/datasource/index` 获取 alias，首次访问具体 alias 时通过 QueryOne/Kyuubi 专用的 `POST /api/datasource/resolve` 获取连接配置；连接配置只存在于 ODEP 和 Spark Engine 内存，不进入 QueryOne SQL、Kyuubi session overlay 或 `spark-submit --conf`。`/resolve` 使用 ODEP 当前环境已有的 `common-url.rms.api` 和 `pk.name` 获取并解密占位符，MLSQL 旧客户端使用的 `/detail` 保持原行为不变：

```sql
show namespaces in jdbc;
show tables in jdbc.search_prod;
select * from jdbc.search_prod.orders limit 10;
load jdbc.`search_prod.orders` as orders;

load jdbc.`search_prod.big_orders`
where "status = 'ACTIVE'"
options partitionColumn="id"
as big_orders;

show namespaces in doris;
show tables in doris.recommend_prod;
select * from doris.recommend_prod.r_qa_log limit 10;
load doris.`recommend_prod.r_qa_log` as qa_log;
```

`load jdbc` 接受两种路径：ODEP 动态路由为 `alias.table`，静态 Catalog 为 `catalog_static.database.table`。无 `OPTIONS` 时直接读取对应 Catalog；MySQL 路径可以使用受控的 `partitionColumn/lowerBound/upperBound/numPartitions/fetchsize`，Spark Engine 内的 `queryone_mysql` provider 会复用 ODEP resolver 或静态 Catalog 配置。只写 `partitionColumn` 时自动查询过滤后数据的边界，默认 `numPartitions=10`、`fetchsize=10000`。SQL 仍禁止传 `url/user/password/driver/dbtable/query`。`save jdbc` 只接受静态三段式目标，动态 alias 写入明确拒绝。缺少 `physicalNamespace` 的 ODEP JDBC/Doris 数据源不会由 `/index` 发布。

Local 不需要 ODEP 功能开关，默认配置 `spark.sql.catalog.jdbc`、`spark.sql.catalog.doris` 两个路由类；Kyuubi 侧需要显式部署和配置。不要在这两个前缀下混入静态连接参数。当前静态 Catalog 统一命名为 `mysql_static`、`doris_static`，与 ODEP 的 `jdbc`、`doris` 完全分离。索引和已解析 alias 在 Engine JVM 生命周期内缓存；ODEP 信息变化后重启 Local SparkSession 或停止旧 Kyuubi Engine 即可生效。

### 静态 JDBC Catalog 与分区读取

Local 和 Kyuubi 使用同一套静态 JDBC 语义。当前静态 MySQL Catalog 名为 `mysql_static`；QueryOne SQL 使用 `jdbc.\`mysql_static.db.table\``，不在 SQL 或编译结果中保存 MySQL 密钥：

```properties
spark.sql.catalog.mysql_static=org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
spark.sql.catalog.mysql_static.url=jdbc:mysql://mysql.example:3306/?databaseTerm=SCHEMA
spark.sql.catalog.mysql_static.driver=com.mysql.cj.jdbc.Driver
spark.sql.catalog.mysql_static.user=reader
spark.sql.catalog.mysql_static.password=change-me
```

示例 SQL：

```sql
show namespaces in mysql_static;
show tables in mysql_static.Dworks;
select * from mysql_static.Dworks.orders limit 10;

load jdbc.`mysql_static.Dworks.orders`
where "biz_date = '2026-07-07'"
as orders;

load jdbc.`mysql_static.Dworks.big_orders`
where "biz_date = '2026-06-10' and status = 'PAID'"
options partitionColumn="id"
as orders_big;

save append source_view
as jdbc.`mysql_static.Dworks.target_table`;
```

`save jdbc` 不使用 `queryone_mysql` provider，也不接受 SQL `OPTIONS`。执行前读取源/目标 schema，按目标列顺序生成显式 column list `INSERT` 并执行 `EXPLAIN`；Kyuubi 最终写 statement 连接中断时不会自动重试。

无 `partitionColumn/lowerBound/upperBound/numPartitions/fetchsize` 时，`load jdbc.\`mysql_static.Dworks.orders\`` 编译成 catalog SQL：

```sql
CREATE OR REPLACE TEMPORARY VIEW orders AS
SELECT * FROM mysql_static.Dworks.orders WHERE biz_date = '2026-07-07'
```

带大表读取参数时，编译成 `queryone_mysql` provider SQL：

```sql
CREATE OR REPLACE TEMPORARY VIEW orders_big
USING queryone_mysql
OPTIONS (
  catalog 'mysql_static',
  dbtable 'Dworks.big_orders',
  whereClauseBase64 '...',
  partitionColumn 'id',
  numPartitions '10',
  fetchsize '10000'
)
```

只写 `partitionColumn` 时，`lowerBound` 和 `upperBound` 会在真正创建 JDBC relation 前自动查询：

- 带 `where` 时，对过滤后的子查询执行 `MIN(partitionColumn), MAX(partitionColumn)`。
- 不带 `where` 时，对原表执行 `MIN(partitionColumn), MAX(partitionColumn)`。
- `numPartitions` 默认 `10`，`fetchsize` 默认 `10000`；SQL 里仍可显式覆盖。
- 如果过滤后没有数据，QueryOne 会降级为单分区 JDBC 读取，不再传 `partitionColumn/lowerBound/upperBound/numPartitions`。
- local engine 会在 QueryOne 服务端日志记录 bounds 查询 SQL 和最终 JDBC 读取参数；Kyuubi engine 会由远端 `queryone_mysql` provider 记录 `queryone_mysql diagnostic: bounds query sql=...` 和 `queryone_mysql diagnostic: effective jdbc options...`。日志不包含 `url/user/password`。如果 Kyuubi operation log 仍只显示 `CREATE TEMPORARY VIEW ...`，请确认 Kyuubi/Spark engine 已部署重新打包后的 `queryone-mysql-provider` jar，并查看 Spark engine driver 日志。

`queryone_mysql` provider jar 由 `queryone-mysql-provider` 模块生成，应部署到 Kyuubi/Spark engine classpath，例如：

```properties
spark.jars=/path/to/queryone-odep-catalog-0.1.0-SNAPSHOT.jar,\
  /path/to/queryone-mysql-provider_2.12-0.1.0-SNAPSHOT.jar
```

provider 对静态 Catalog 读取 `spark.sql.catalog.mysql_static.*`；对 ODEP JDBC alias 则复用 Catalog resolver 按需获取详情。QueryOne 不读取 `kyuubi-defaults.conf`，也不会把 `url/user/password` 编进 SQL。

### queryone_mysql 测试

测试前确认三件事：

- Kyuubi/Spark engine 已能加载 `queryone-mysql-provider_2.12-0.1.0-SNAPSHOT.jar`。推荐放在 Kyuubi engine 的 `spark.jars` 或等价 classpath 配置中，不放在 QueryOne server 主包里。
- Kyuubi/Spark engine 已注册静态 MySQL JDBC Catalog `spark.sql.catalog.mysql_static.*`；其中 `url/user/password/driver` 都在 Kyuubi/Spark engine 侧。
- QueryOne 的 Kyuubi engine 已启用，并连接同一个 Kyuubi Server。

最小验证 SQL：

```sql
load jdbc.`mysql_static.Dworks.cloud_host_info`
options partitionColumn="id"
as orders_big;

select count(*) from orders_big;
```

如果 `load` 能成功，Kyuubi operation log 中应能看到 QueryOne 编译出的 provider SQL：

```sql
CREATE OR REPLACE TEMPORARY VIEW orders_big
USING queryone_mysql
OPTIONS (
  catalog 'mysql_static',
  dbtable 'Dworks.cloud_host_info',
  partitionColumn 'id',
  numPartitions '10',
  fetchsize '10000'
)
```

这一步只是注册远端 Spark 临时视图，不代表已经全量读取 MySQL。真正触发读取的是后续 action，例如 `select count(*) from orders_big`、`select * from orders_big limit 10`，或者 QueryOne 页面 Preview tab 发起的 `/api/preview`。

如果看到 Kyuubi log 里紧跟着出现：

```sql
SELECT * FROM `orders_big` LIMIT 101
```

这通常是 QueryOne 的结果预览，不是 `load` 语句自身又执行了一次业务查询。触发条件有两个：页面结果默认 tab 配成 `preview`，或者用户点击了结果区的 Preview tab。`101` 是页面请求 limit 加 1，用来判断结果是否被截断；上限由 `preview.maxRows` 控制。

判断大表分区参数是否生效，优先用 Spark engine 的 Spark UI 和 Spark SQL explain，而不是只看 Kyuubi Server Web UI：

```sql
EXPLAIN FORMATTED
SELECT count(*) FROM orders_big;
```

生效时物理计划应包含类似信息：

```text
Scan JDBCRelation(Dworks.cloud_host_info) [numPartitions=10]
```

Spark UI 中执行 `select count(*) from orders_big` 后，应在 Jobs/Stages 里看到负责 JDBC scan 的 stage 有 `10/10` 个 tasks。看到 `10/10` task 的 job，就能说明这次 count 触发了 10 个 JDBC 分区读取任务。另一个 `1/1 skipped` 的 job 多半是 AQE、聚合收尾或复用结果造成的辅助 job，不用把它当成分区数失效。

更多排查入口：

- Spark UI 默认是 Spark engine 的 UI。local/client 模式常见为 `http://127.0.0.1:4040`；YARN/Kubernetes/cluster 模式应看 Spark application tracking URL。
- Kyuubi Server Web UI 只展示 Kyuubi Server 侧信息。源码或本地包没有带 Web UI 时会显示 `The Web UI is currently unavailable`；这不影响 Spark engine 的 Spark UI，也不影响 `queryone_mysql` 测试。
- Kyuubi REST 端口默认 `10099`，用于 Kyuubi REST API，不等同于 Spark UI。

常见注意事项：

- 推荐只写 `partitionColumn`，让 QueryOne 自动查询 `lowerBound/upperBound`，并使用默认 `numPartitions=10`、`fetchsize=10000`。如果手工写边界，`lowerBound/upperBound` 必须成对出现。
- `lowerBound` 和 `upperBound` 只用于计算分区步长，不是业务过滤条件。需要过滤数据时用 `load jdbc ... where "..."` 或后续 SQL 的 `where`。
- `numPartitions` 近似等于并发 JDBC 读取任务数，也意味着对 MySQL 的并发压力会上升。先从较小值验证，再结合 MySQL 连接数、慢查询、IO 和 Spark task 耗时调大。
- `partitionColumn` 应选择 numeric/date/timestamp 类型，最好是有索引且分布相对均匀的列。自增主键适合起步验证，但如果主键范围空洞很大，task 耗时可能明显不均衡。
- `fetchsize` 是否真正流式生效还取决于 MySQL Connector/J 行为；必要时在 catalog JDBC URL 中评估 `useCursorFetch=true`，再结合 executor 内存和 MySQL 连接状态验证。
- 分区 LOAD 会读取完整的过滤结果，不支持源端全局 `LIMIT`。后续对临时视图执行 `LIMIT` 只限制 Spark 输出；需要 MySQL 端下推 `LIMIT` 时直接查询 `jdbc.alias.table` 或 `mysql_static.db.table`，不要同时使用 `partitionColumn`。
- 不要在 SQL options 中传 `url/user/password/driver/dbtable/query`。这些连接目标和密钥必须留在可信 Spark Catalog 配置或 ODEP resolver 中。

Doris 按 Spark Catalog 配置。QueryOne local 运行时会把下面的 HOCON 转成 `spark.sql.catalog.doris_static.*`；`doris` 前缀由默认 ODEP 路由 Catalog 独占，静态 Doris 必须使用独立的 `doris_static` 前缀：

```hocon
engines {
  local {
    type = "local"

    catalogs.doris_static {
      fenodes = "fe-1:8030,fe-2:8030"
      queryPort = 9030
      user = "reader"
      password = ${?QUERYONE_DORIS_PASSWORD}

      options {
        doris.request.retries = 3
        # 需要 Arrow Flight SQL 读取时，可按 Doris 服务端配置打开：
        # doris.read.mode = "arrow"
        # doris.read.arrow-flight-sql.port = 12345
      }
    }
  }
}
```

数据源增多时，不建议把所有连接硬塞进一个大文件。HOCON 支持 `include`、对象合并和环境变量替换，可以按环境或团队拆分，例如主配置只保留：

```hocon
include "datasources/mysql.conf"
include "catalogs/doris.conf"
include "datasources/hive.conf"
```
- `load jdbc` 无 OPTIONS 时读取 ODEP 路由或静态 Catalog；两种 MySQL 路径都可使用受控 JDBC 分区参数。`save jdbc` 只允许 `catalog_static.database.table`，静态读写不走 RMS，动态 ODEP 读取继续走 RMS。
- QueryOne DSL 不支持在 SQL 里写 Doris `fenodes/user/password`；这些连接目标和密钥统一放在 HOCON 或 Kyuubi/Spark engine 配置。

文件类 save：

- 已识别文件 provider 的相对路径会分类为受控 HDFS workspace，默认基准目录是 `/public/odep/user/${username}`。
- 文件 append 不进入 MVP 路线，受控 HDFS 相对路径和 external path 的 append 都由固定能力矩阵永久拒绝。
- 绝对路径或包含 URI scheme/authority 的路径分类为 external path；本地文件、S3、OSS 等 external path 的 append 和 overwrite 都永久拒绝。
- 受控 HDFS overwrite 已由 Spark driver extension 开放，使用目标级 ZK ephemeral lock、固定同级 staging/backup 和 HDFS rename 发布；缺少可信 engine 配置时 fail closed。
- 未来只有出现明确生产案例，并具备格式/分区约束、schema 合同、并发控制和重跑幂等语义时，才单独评估 Parquet/ORC 分区 append 或事务湖表写入；不恢复通用裸路径 append。
- 完整矩阵、路径校验和本阶段测试见 [safe-save.md](safe-save.md)。

文件类 load：

- 已识别文件 provider 的 `load` 只接受 workspace 相对路径；省略 `owner` 时使用当前用户，`options owner="bob"` 时使用指定用户。
- Local/Kyuubi 都将 load 编译为内部命令，由 Spark driver extension 使用可信 `workspaceRoot` 解析最终路径并注册临时视图，并使用同一套 ownership 与 ODEP/RMS 判定。
- 两种 Engine 对当前用户自己的 managed load 直接按 workspace ownership 放行；跨 owner load 使用解析后的绝对路径调用 ODEP/RMS `hdfs read`。Local subject 只适合开发调试，生产仍使用 Kyuubi 签名 subject。
- 原生 SQL 或 `view` 允许 `parquet/csv/json/orc/text/libsvm/binaryfile/excel` 使用 `/absolute/path`、`hdfs:///absolute/path` 或 `viewfs:///absolute/path`，并统一调用 ODEP/RMS `hdfs read`。带 authority 的 URI、相对路径、`file://`、S3/OSS 和未识别 provider 仍拒绝。
- load 不使用 ZooKeeper、staging 或 backup；目标路径不存在时立即失败。
- 未识别 provider 默认拒绝；新增文件格式必须先纳入受控 provider 清单和 workspace 测试，不能回退到通用 `USING provider OPTIONS(path ...)`。

外部 provider：

- `excel` 编译成 `USING excel`，provider jar 需要通过运行环境提供。
- 本地 MVP 可用 `engines.local.jars.packages = "dev.mauch:spark-excel_2.12:3.3.4_0.31.2"`。
- Doris 4.x / Spark 3.3 读写需要 Spark Doris Connector，例如 `org.apache.doris:spark-doris-connector-spark-3.3:25.2.0`。QueryOne 不把该 connector 默认打进主包，由 local 引擎的 `engines.local.jars.packages`、`engines.local.jars.jars` 或运行环境 classpath 提供。
- 未来接 Kyuubi 时，在 Kyuubi/Spark engine 配置 `spark.jars.packages` 或 engine classpath。

新增数据源时：

- 能用 Spark SQL provider 表达的，只在 `DataSourceResolver` 增加别名。
- 需要特殊 catalog 语义的，优先编译成 Spark 多级 catalog SQL，例如当前 `doris`。
- 需要隐藏密钥或运行时 API 的，增加薄 runtime adapter，例如当前 `mysql`。
- 不把 connector 依赖默认加进 `queryone-server/pom.xml`，除非它成为 QueryOne server 自身运行所必需的核心依赖。
