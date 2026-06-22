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

特殊 source：

- `hive` 是 catalog 表语义：`load hive.\`db.table\` as t` 编译成 `CREATE OR REPLACE TEMPORARY VIEW t AS SELECT * FROM db.table`。
- `load hive.\`db.table\` where "dt = date '2026-06-17'" as t` 会编译成 `SELECT * FROM db.table WHERE ...`；分区裁剪和谓词下推由 Spark/Hive 自身优化能力决定。
- `save append t as hive.\`db.table\`` 编译成 `INSERT INTO TABLE db.table SELECT * FROM t`，要求目标表已存在。
- `save overwrite t as hive.\`db.table\`` 编译成 `INSERT OVERWRITE TABLE db.table SELECT * FROM t`，要求目标表已存在，默认仍需要 `options sparkoneOverwrite="allow"` 显式确认。
- `partitionBy` 仅用于 catalog 表写入：`save append t as hive.\`db.table\` partitionBy dt` 编译成动态分区插入。
- SparkOne 不复刻 MLSQL 的 `storage="hive"` / 数据湖替换逻辑；如果要创建表、指定存储格式或改表结构，优先使用 Spark 原生 `CREATE TABLE` / `ALTER TABLE`。
- `mysql` 是关系库特殊 source：`load mysql.\`analytics.users\` as users` 从 HOCON 的 `datasources.mysql.analytics` 读取连接，再用 Spark JDBC reader 注册临时视图。
- `catalogs.mysql` 是显式 Spark JDBC Catalog 配置：可用 `show namespaces in mysql` 查看 MySQL database，用 `show tables in mysql.app` 查看表，用 `select * from mysql.app.users` 做原生查询。MySQL JDBC URL 需要带 `databaseTerm=SCHEMA`，让 Spark 传入的 JDBC schemaPattern 对应 MySQL database。
- MySQL catalog 只用于原生 SQL 浏览/查询；`datasources.mysql` 和 `catalogs.mysql` 不互相隐式复用。`load/save mysql` 仍走 SparkOne adapter，因此 SQL 侧仍不能覆盖 `url/user/password/driver/dbtable/query`，`load mysql ... where "..."` 的 JDBC 子查询和 `save overwrite` 安全策略也保持不变。
- `save append t as mysql.\`analytics.target_table\`` 用 Spark JDBC writer 追加写入 MySQL，要求目标表已存在。
- `save overwrite t as mysql.\`analytics.target_table\`` 默认被 `save.allowMysqlOverwrite = false` 拦截。确需覆盖时，必须先在 HOCON 打开 `save.allowMysqlOverwrite = true`，再在单条语句里显式写 `options sparkoneOverwrite="allow"`；SparkOne 不对 MySQL 表做备份。
- `doris` 是 Spark Doris Catalog 名：`select * from doris.db.users` 直接走 Spark 原生 catalog 解析。
- `show namespaces in doris` 查看 Doris database；裸写 `show databases` 仍查看默认 Hive catalog。
- `load doris.\`db.users\` as users` 是语法糖，编译成 `CREATE OR REPLACE TEMPORARY VIEW users AS SELECT * FROM doris.db.users`。
- `load doris.\`db.users\` where "dt = date '2026-06-17'" as users` 会编译成 `SELECT * FROM doris.db.users WHERE ...`；是否源端下推由 Spark Doris Catalog / Connector 的谓词下推能力决定。
- `save append t as doris.\`db.target\`` 编译成 `INSERT INTO TABLE doris.db.target SELECT * FROM t`，要求目标表已存在。
- `save overwrite t as doris.\`db.target\`` 编译成 `INSERT OVERWRITE TABLE doris.db.target SELECT * FROM t`，要求目标表已存在，默认被 `save.allowDorisOverwrite = false` 拦截。确需覆盖时，必须先在 HOCON 打开 `save.allowDorisOverwrite = true`，再在单条语句里显式写 `options sparkoneOverwrite="allow"`；SparkOne 不对 Doris 表做备份。
- `save doris` 不支持 SQL 里的 Doris 连接 options，也不支持 `partitionBy`；连接和写入参数应放在 Spark Doris Catalog 配置中。
- `save doris` 不改变 Doris 表模型语义：`DUPLICATE KEY` 会保留所有写入行，`AGGREGATE KEY` 会按 Key 聚合 Value 列，`UNIQUE KEY` 会按 Key UPSERT。重复 append 的最终查询效果由目标表 DDL 决定。
- Hive/MySQL/Doris 的 `save append/overwrite` 都不会自动创建目标表。目标表、分区、索引、Doris key/distribution 等应由明确 DDL 先创建和治理。
- Doris 聚合、写入优先使用 Spark 标准 SQL，例如 `select city, count(*) from doris.db.users group by city`、`insert into doris.db.target select ...`。

HOCON 数据源推荐按类型和连接名分层，连接信息仍留在启动配置中，SQL 只引用连接名：

```hocon
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

  reporting = ${datasources.mysql.analytics}
  reporting.url = "jdbc:mysql://127.0.0.1:3306/reporting"
}
```

MySQL 原生 catalog 单独配置在 `catalogs.mysql`，不要和 adapter 配置揉在一起：

```hocon
catalogs.mysql {
  url = "jdbc:mysql://127.0.0.1:3306/?databaseTerm=SCHEMA"
  user = "reader"
  password = ${?SPARKONE_MYSQL_CATALOG_PASSWORD}

  options {
    fetchsize = 1000
  }
}
```

这里的 `databaseTerm=SCHEMA` 只用于 Spark 原生 JDBC Catalog。原因是 Spark 执行 `show tables in mysql.app` 时，会把 `app` 作为 JDBC `DatabaseMetaData.getTables` 的 `schemaPattern` 参数传给驱动；而 MySQL Connector/J 默认把 MySQL database 解释为 JDBC catalog，不解释为 schema。加上该参数后，MySQL database 会按 JDBC schema 暴露，`show tables in mysql.app` 才会过滤到 `app` 这个库。

Doris 按 Spark Catalog 配置。SparkOne 本地运行时会把下面的 HOCON 转成 `spark.sql.catalog.doris.*`；接 Kyuubi 时，把同样的 Spark 配置放到 Kyuubi/Spark engine 即可：

```hocon
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

- 当前 MVP 的 `save overwrite table as provider.\`path\`` 仍编译成 Spark SQL `INSERT OVERWRITE DIRECTORY`。
- 覆盖写由 SparkOne runtime 做统一保护，默认需要语句显式写 `sparkoneOverwrite="allow"`。
- `sparkoneOverwrite` 是 `overwritePolicy = "requireExplicit"` 下的单条确认信号，会从 provider options 中剥离，不传给底层数据源；其他 `save { ... }` 策略参数必须写在 HOCON 中，不能用 SQL `options` 或 `SET` 覆盖。
- 目标路径存在时默认采用 `rename` 备份到 `/tmp/sparkone_back`；失败时会尝试恢复备份。
- 测试案例和全局开关说明见 [safe-save.md](safe-save.md)。

外部 provider：

- `excel` 编译成 `USING excel`，provider jar 需要通过运行环境提供。
- 本地 MVP 可用 `-Dspark.jars.packages=dev.mauch:spark-excel_2.12:3.5.6_0.31.2`。
- Doris 4.x / Spark 3.5 读写需要 Spark Doris Connector，例如 `org.apache.doris:spark-doris-connector-spark-3.5:25.2.0`。SparkOne 不把该 connector 默认打进主包，由运行环境通过 `spark.jars.packages` 或 classpath 提供。
- 未来接 Kyuubi 时，在 Kyuubi/Spark engine 配置 `spark.jars.packages` 或 engine classpath。

新增数据源时：

- 能用 Spark SQL provider 表达的，只在 `DataSourceResolver` 增加别名。
- 需要特殊 catalog 语义的，优先编译成 Spark 多级 catalog SQL，例如当前 `doris`。
- 需要隐藏密钥或运行时 API 的，增加薄 runtime adapter，例如当前 `mysql`。
- 不把 connector 依赖默认加进主 `pom.xml`，除非它成为 SparkOne 自身运行所必需的核心依赖。
