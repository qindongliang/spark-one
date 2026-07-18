# Data Sources

SparkOne 的数据源策略是：compiler 负责把 SQL 友好的 `load/save` 薄 DSL 编译成 Spark SQL 或极薄 runtime adapter，connector jar 由 Spark/Kyuubi 运行环境提供。Doris 这类支持 Spark Catalog 的系统优先走 Catalog，便于 SparkOne 逐步退化成“DSL 编译器 + SQL 提交器”。

默认主包不内置第三方 provider。这样可以避免 Excel、Mongo、ES、Kafka 等 connector 和 SparkOne 主应用强耦合，也减少 shade 冲突。

内置 Spark provider：

```text
csv
json
parquet
orc
text
libsvm
```

文件 provider 的 `load` 只接受当前租户 workspace 下的相对路径，例如 `load parquet.\`extension-test/result\` as result`。Spark extension 会在 driver 内解析为 `/public/sparkone/user/${username}/extension-test/result`；绝对路径、HDFS/S3/OSS URI、路径穿越和内部 overwrite 工作目录都会在编译或执行阶段拒绝。原生 SQL/`view` 不能直接使用文件 provider path relation，必须先通过受控 `load` 注册临时视图。

Catalog 表统一使用 Spark 原生三段式 `<catalog>.<database>.<table>`。一个 catalog 对应一个真实连接实例；多实例名称使用 `<source>_<instance>`，例如 `mysql_dworks.Dworks.cloud_host_info`、`mysql_crm.crm.customer`、`doris_prod.dataagent.r_qa_log`、`doris_ads.dataagent.r_qa_log`。Hive 是唯一例外：用户可以写 `hive.default.some_table`，compiler 会将逻辑别名 `hive` 改写为 Spark 内置 `spark_catalog`。不要引入 `<source>.<instance>.<database>.<table>` 四段式平台协议。

特殊 source：

- `hive` 是 `spark_catalog` 的逻辑别名：`show databases in hive`、`show tables in hive.db` 和 `select * from hive.db.table` 会在对应 catalog 语义位置改写为 `spark_catalog`。
- `load hive.\`db.table\` as t` 编译成 `CREATE OR REPLACE TEMPORARY VIEW t AS SELECT * FROM spark_catalog.db.table`。
- `load hive.\`db.table\` where "dt = date '2026-06-17'" as t` 会编译成 `SELECT * FROM spark_catalog.db.table WHERE ...`；分区裁剪和谓词下推由 Spark/Hive 自身优化能力决定。
- `save append t as hive.\`db.table\`` 先生成 `WritePlan`，执行时按目标列顺序生成 `INSERT INTO TABLE spark_catalog.db.table (目标列...) SELECT 源列... FROM t`；要求目标表已存在且列名集合一致。
- `save overwrite t as hive.\`db.table\`` 由固定能力矩阵永久拒绝，不存在可放开的配置。
- `partitionBy` 仅用于 catalog 表写入：`save append t as hive.\`db.table\` partitionBy dt` 编译成动态分区插入。
- SparkOne 不复刻 MLSQL 的 `storage="hive"` / 数据湖替换逻辑，也不开放原生建表/改表语句；目标表由平台外 catalog 治理入口创建和维护。
- `mysql` 是关系库特殊 source：`load mysql.\`analytics.users\` as users` 从 local 引擎的 `datasources.mysql.analytics` 读取连接，再用 Spark JDBC reader 注册临时视图。
- `engines.local.catalogs.mysql` 是显式 Spark JDBC Catalog 配置：可用 `show namespaces in mysql` 查看 MySQL database，用 `show tables in mysql.app` 查看表，用 `select * from mysql.app.users` 做原生查询。MySQL JDBC URL 需要带 `databaseTerm=SCHEMA`，让 Spark 传入的 JDBC schemaPattern 对应 MySQL database。
- MySQL catalog 可用于原生 SQL 浏览/查询；local 引擎下的 `datasources.mysql` 和 `catalogs.mysql` 不互相隐式复用。local 的 `load/save mysql` 仍走 SparkOne adapter，因此 SQL 侧不能覆盖 `url/user/password/driver/dbtable/query`。
- Kyuubi 引擎下的 `load mysql.\`catalog.db.table\`` 复用远端 Spark JDBC Catalog 配置，不读取 `engines.local.datasources.mysql`，也不允许 SQL 传 `url/user/password/driver/dbtable/query`。SparkOne 只传 catalog、表名、可选 `where` 和受控 partition 参数；真实 MySQL 连接信息必须放在 Kyuubi/Spark engine 侧。
- local 的 `save append t as mysql.\`analytics.target_table\`` 用 Spark JDBC writer 追加写入 MySQL；`analytics` 是 HOCON 连接名。
- Kyuubi 的 `save append t as mysql.\`analytics.app.target_table\`` 复用远端 Spark JDBC Catalog；`analytics` 是 catalog 名，路径必须为三段式且不接受 SQL `OPTIONS`。
- 两条 MySQL append 路径都要求目标表已存在、源和目标列名集合完全一致，并在写入前校验类型兼容；源数据按目标列顺序投影，不依赖源列位置。
- `save overwrite t as mysql.\`analytics.target_table\`` 由固定能力矩阵永久拒绝。
- Doris 多集群分别注册为 Spark Catalog，例如 `doris_prod`、`doris_ads`；`select * from doris_prod.db.users` 直接走 Spark 原生 catalog 解析。
- `show namespaces in doris_prod` 查看对应 Doris 集群的 database；裸写 `show databases` 仍查看默认 Hive catalog。
- `load doris.\`db.users\` as users` 保留为默认 `doris` catalog 的兼容语法；多集群使用 `load doris.\`doris_prod.db.users\` as users`，编译成 `CREATE OR REPLACE TEMPORARY VIEW users AS SELECT * FROM doris_prod.db.users`。
- `load doris.\`db.users\` where "dt = date '2026-06-17'" as users` 会编译成 `SELECT * FROM doris.db.users WHERE ...`；是否源端下推由 Spark Doris Catalog / Connector 的谓词下推能力决定。
- `save append t as doris.\`db.target\`` 和 Hive 共用显式目标列清单与源列投影；多集群目标使用 `save append t as doris.\`doris_prod.db.target\``。两种写法都要求目标表已存在且列名集合一致。
- `save overwrite t as doris.\`db.target\`` 由固定能力矩阵永久拒绝。
- `save doris` 不支持 SQL 里的 Doris 连接 options，也不支持 `partitionBy`；连接和写入参数应放在 Spark Doris Catalog 配置中。
- `save doris` 不改变 Doris 表模型语义：`DUPLICATE KEY` 会保留所有写入行，`AGGREGATE KEY` 会按 Key 聚合 Value 列，`UNIQUE KEY` 会按 Key UPSERT。重复 append 的最终查询效果由目标表 DDL 决定。
- Hive/MySQL/Doris 的 `save append` 不会自动创建目标表。目标表、分区、索引、Doris key/distribution 等应由平台外 DDL 流程先创建和治理。
- Doris 查询和聚合使用 Spark 标准 SQL，例如 `select city, count(*) from doris.db.users group by city`；写入必须使用 `save append ... as doris`，原生 `insert` 会被写入旁路保护拒绝。

HOCON 数据源推荐按类型和连接名分层。local 进程内引擎的连接信息放在 `engines.local` 下，SQL 只引用连接名：

```hocon
engines {
  local {
    type = "local"

    datasources.mysql {
      analytics {
        url = "jdbc:mysql://127.0.0.1:3306/app"
        user = "reader"
        password = ${?SPARKONE_MYSQL_ANALYTICS_PASSWORD}

        options {
          fetchsize = 1000
          batchsize = 1000
        }
      }

      reporting = ${engines.local.datasources.mysql.analytics}
      reporting.url = "jdbc:mysql://127.0.0.1:3306/reporting"
    }
  }
}
```

MySQL 原生 catalog 单独配置在 `engines.local.catalogs.mysql`，不要和 adapter 配置揉在一起：

```hocon
engines {
  local {
    type = "local"

    catalogs.mysql {
      url = "jdbc:mysql://127.0.0.1:3306/?databaseTerm=SCHEMA"
      user = "reader"
      password = ${?SPARKONE_MYSQL_CATALOG_PASSWORD}

      options {
        fetchsize = 1000
      }
    }
  }
}
```

这里的 `databaseTerm=SCHEMA` 只用于 Spark 原生 JDBC Catalog。原因是 Spark 执行 `show tables in mysql.app` 时，会把 `app` 作为 JDBC `DatabaseMetaData.getTables` 的 `schemaPattern` 参数传给驱动；而 MySQL Connector/J 默认把 MySQL database 解释为 JDBC catalog，不解释为 schema。加上该参数后，MySQL database 会按 JDBC schema 暴露，`show tables in mysql.app` 才会过滤到 `app` 这个库。

Kyuubi MySQL load/save 首选直接复用远端 Spark JDBC Catalog。Kyuubi/Spark engine 侧为每个实例注册独立 catalog，例如 `spark.sql.catalog.mysql_dworks`；SparkOne SQL 使用 `mysql.\`catalog.db.table\``，不在 SparkOne 侧保存 MySQL 密钥：

```properties
spark.sql.catalog.mysql_dworks=org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
spark.sql.catalog.mysql_dworks.url=jdbc:mysql://192.168.202.187:3306/?databaseTerm=SCHEMA&useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false
spark.sql.catalog.mysql_dworks.driver=com.mysql.cj.jdbc.Driver
spark.sql.catalog.mysql_dworks.user=reader
spark.sql.catalog.mysql_dworks.password=change-me
```

示例 SQL：

```sql
load mysql.`mysql_dworks.Dworks.orders`
where "biz_date = '2026-07-07'"
as orders;

load mysql.`mysql_dworks.Dworks.big_orders`
where "biz_date = '2026-06-10' and status = 'PAID'"
options partitionColumn="id"
as orders_big;

save append source_view
as mysql.`mysql_dworks.Dworks.target_table`;
```

Kyuubi `save mysql` 不使用 `sparkone_mysql` provider，也不接受 SQL `OPTIONS`。执行前通过远端 `LIMIT 0` 读取源/目标 schema，按目标列顺序生成显式 column list `INSERT` 并执行 `EXPLAIN`；最终写 statement 连接中断时不会自动重试。

无 `partitionColumn/lowerBound/upperBound/numPartitions/fetchsize` 时，Kyuubi `load mysql.\`mysql_dworks.Dworks.orders\`` 编译成远端 catalog SQL：

```sql
CREATE OR REPLACE TEMPORARY VIEW orders AS
SELECT * FROM mysql_dworks.Dworks.orders WHERE biz_date = '2026-07-07'
```

带大表读取参数时，编译成 `sparkone_mysql` provider SQL：

```sql
CREATE OR REPLACE TEMPORARY VIEW orders_big
USING sparkone_mysql
OPTIONS (
  catalog 'mysql_dworks',
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
- 如果过滤后没有数据，SparkOne 会降级为单分区 JDBC 读取，不再传 `partitionColumn/lowerBound/upperBound/numPartitions`。
- local engine 会在 SparkOne 服务端日志记录 bounds 查询 SQL 和最终 JDBC 读取参数；Kyuubi engine 会由远端 `sparkone_mysql` provider 记录 `sparkone_mysql diagnostic: bounds query sql=...` 和 `sparkone_mysql diagnostic: effective jdbc options...`。日志不包含 `url/user/password`。如果 Kyuubi operation log 仍只显示 `CREATE TEMPORARY VIEW ...`，请确认 Kyuubi/Spark engine 已部署重新打包后的 `sparkone-mysql-provider` jar，并查看 Spark engine driver 日志。

`sparkone_mysql` provider jar 由 `sparkone-mysql-provider` 模块生成，应部署到 Kyuubi/Spark engine classpath，例如：

```properties
spark.jars=/path/to/sparkone-mysql-provider_2.12-0.1.0-SNAPSHOT.jar
```

provider 在 Spark engine 内读取 `spark.sql.catalog.mysql_dworks.*`，再转成 Spark JDBC reader options。SparkOne 不读取 `kyuubi-defaults.conf`，也不会把 `url/user/password` 编进 SQL。

`mysqlLoadProfiles` 仍可作为兼容/治理增强使用，例如给 catalog 起业务别名、设置 `allowedTables` 或 `maxNumPartitions`。第一阶段如果只追求 catalog 使用方式不变，可以直接使用 `mysql.\`catalog.db.table\``，不需要配置 SparkOne 侧 profile。

### Kyuubi sparkone_mysql 测试

测试前确认三件事：

- Kyuubi/Spark engine 已能加载 `sparkone-mysql-provider_2.12-0.1.0-SNAPSHOT.jar`。推荐放在 Kyuubi engine 的 `spark.jars` 或等价 classpath 配置中，不放在 SparkOne server 主包里。
- Kyuubi/Spark engine 已注册 MySQL JDBC Catalog，例如 `spark.sql.catalog.mysql.*`；其中 `url/user/password/driver` 都在 Kyuubi/Spark engine 侧。
- SparkOne 的 Kyuubi engine 已启用，并连接同一个 Kyuubi Server。

最小验证 SQL：

```sql
load mysql.`mysql.Dworks.cloud_host_info`
options partitionColumn="id"
as orders_big;

select count(*) from orders_big;
```

如果 `load` 能成功，Kyuubi operation log 中应能看到 SparkOne 编译出的 provider SQL：

```sql
CREATE OR REPLACE TEMPORARY VIEW orders_big
USING sparkone_mysql
OPTIONS (
  catalog 'mysql',
  dbtable 'Dworks.cloud_host_info',
  partitionColumn 'id',
  numPartitions '10',
  fetchsize '10000'
)
```

这一步只是注册远端 Spark 临时视图，不代表已经全量读取 MySQL。真正触发读取的是后续 action，例如 `select count(*) from orders_big`、`select * from orders_big limit 10`，或者 SparkOne 页面 Preview tab 发起的 `/api/preview`。

如果看到 Kyuubi log 里紧跟着出现：

```sql
SELECT * FROM `orders_big` LIMIT 101
```

这通常是 SparkOne 的结果预览，不是 `load` 语句自身又执行了一次业务查询。触发条件有两个：页面结果默认 tab 配成 `preview`，或者用户点击了结果区的 Preview tab。`101` 是页面请求 limit 加 1，用来判断结果是否被截断；上限由 `preview.maxRows` 控制。

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
- Kyuubi Server Web UI 只展示 Kyuubi Server 侧信息。源码或本地包没有带 Web UI 时会显示 `The Web UI is currently unavailable`；这不影响 Spark engine 的 Spark UI，也不影响 `sparkone_mysql` 测试。
- Kyuubi REST 端口默认 `10099`，用于 Kyuubi REST API，不等同于 Spark UI。

常见注意事项：

- 推荐只写 `partitionColumn`，让 SparkOne 自动查询 `lowerBound/upperBound`，并使用默认 `numPartitions=10`、`fetchsize=10000`。如果手工写边界，`lowerBound/upperBound` 必须成对出现。
- `lowerBound` 和 `upperBound` 只用于计算分区步长，不是业务过滤条件。需要过滤数据时用 `load mysql ... where "..."` 或后续 SQL 的 `where`。
- `numPartitions` 近似等于并发 JDBC 读取任务数，也意味着对 MySQL 的并发压力会上升。先从较小值验证，再结合 MySQL 连接数、慢查询、IO 和 Spark task 耗时调大。
- `partitionColumn` 应选择 numeric/date/timestamp 类型，最好是有索引且分布相对均匀的列。自增主键适合起步验证，但如果主键范围空洞很大，task 耗时可能明显不均衡。
- `fetchsize` 是否真正流式生效还取决于 MySQL Connector/J 行为；必要时在 catalog JDBC URL 中评估 `useCursorFetch=true`，再结合 executor 内存和 MySQL 连接状态验证。
- 不要在 SQL options 中传 `url/user/password/driver/dbtable/query`。这些连接目标和密钥必须留在 Kyuubi/Spark engine 配置或受控 profile 中。

Doris 按 Spark Catalog 配置。SparkOne local 运行时会把下面的 HOCON 转成 `spark.sql.catalog.doris.*`；接 Kyuubi 时，把同样的 Spark 配置放到 Kyuubi/Spark engine 即可：

```hocon
engines {
  local {
    type = "local"

    catalogs.doris {
      fenodes = "fe-1:8030,fe-2:8030"
      queryPort = 9030
      user = "reader"
      password = ${?SPARKONE_DORIS_PASSWORD}

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
- SparkOne DSL 不支持 `load/save jdbc`，避免连接串、账号、密码散落在 SQL 中。需要 MySQL 时统一使用 `mysql`。
- SparkOne DSL 不支持在 SQL 里写 Doris `fenodes/user/password`；这些连接目标和密钥统一放在 HOCON 或 Kyuubi/Spark engine 配置。

文件类 save：

- 已识别文件 provider 的相对路径会分类为当前逻辑租户的受控 HDFS workspace，基准目录是 `/public/sparkone/user/${username}`。
- 文件 append 不进入 MVP 路线，受控 HDFS 相对路径和 external path 的 append 都由固定能力矩阵永久拒绝。
- 绝对路径或包含 URI scheme/authority 的路径分类为 external path；本地文件、S3、OSS 等 external path 的 append 和 overwrite 都永久拒绝。
- 受控 HDFS overwrite 已由 Spark driver extension 开放，使用目标级 ZK ephemeral lock、固定同级 staging/backup 和 HDFS rename 发布；缺少可信 engine 配置时 fail closed。
- 未来只有出现明确生产案例，并具备格式/分区约束、schema 合同、并发控制和重跑幂等语义时，才单独评估 Parquet/ORC 分区 append 或事务湖表写入；不恢复通用裸路径 append。
- 完整矩阵、路径校验和本阶段测试见 [safe-save.md](safe-save.md)。

文件类 load：

- 已识别文件 provider 只接受当前逻辑租户 workspace 的相对路径，绝对路径和 URI 在编译阶段拒绝。
- Local/Kyuubi 都将 load 编译为内部命令，由 Spark driver extension 使用可信 `workspaceRoot` 解析最终路径并注册临时视图。
- 原生 SQL 或 `view` 中直接访问文件 provider path relation 会被拒绝，避免固定 keytab 执行身份绕过逻辑租户。
- load 不使用 ZooKeeper、staging 或 backup；目标路径不存在时立即失败。
- 未识别 provider 默认拒绝；新增文件格式必须先纳入受控 provider 清单和 workspace 测试，不能回退到通用 `USING provider OPTIONS(path ...)`。

外部 provider：

- `excel` 编译成 `USING excel`，provider jar 需要通过运行环境提供。
- 本地 MVP 可用 `engines.local.jars.packages = "dev.mauch:spark-excel_2.12:3.5.6_0.31.2"`。
- Doris 4.x / Spark 3.5 读写需要 Spark Doris Connector，例如 `org.apache.doris:spark-doris-connector-spark-3.5:25.2.0`。SparkOne 不把该 connector 默认打进主包，由 local 引擎的 `engines.local.jars.packages`、`engines.local.jars.jars` 或运行环境 classpath 提供。
- 未来接 Kyuubi 时，在 Kyuubi/Spark engine 配置 `spark.jars.packages` 或 engine classpath。

新增数据源时：

- 能用 Spark SQL provider 表达的，只在 `DataSourceResolver` 增加别名。
- 需要特殊 catalog 语义的，优先编译成 Spark 多级 catalog SQL，例如当前 `doris`。
- 需要隐藏密钥或运行时 API 的，增加薄 runtime adapter，例如当前 `mysql`。
- 不把 connector 依赖默认加进 `sparkone-server/pom.xml`，除非它成为 SparkOne server 自身运行所必需的核心依赖。
