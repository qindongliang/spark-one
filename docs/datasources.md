# Data Sources

SparkOne 的数据源策略是：compiler 负责把 SQL 友好的 `load/save` 薄 DSL 编译成 Spark SQL 或极薄 runtime adapter，connector jar 由 Spark/Kyuubi 运行环境提供。

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
- `save append t as hive.\`db.table\`` 编译成 `INSERT INTO TABLE db.table SELECT * FROM t`，要求目标表已存在。
- `save overwrite t as hive.\`db.table\`` 编译成 `INSERT OVERWRITE TABLE db.table SELECT * FROM t`，默认仍需要 `options sparkoneOverwrite="allow"` 显式确认。
- `partitionBy` 仅用于 catalog 表写入：`save append t as hive.\`db.table\` partitionBy dt` 编译成动态分区插入。
- SparkOne 不复刻 MLSQL 的 `storage="hive"` / 数据湖替换逻辑；如果要创建表、指定存储格式或改表结构，优先使用 Spark 原生 `CREATE TABLE` / `ALTER TABLE`。
- `mysql` 是关系库特殊 source：`load mysql.\`analytics.users\` as users` 从 TOML 的 `[datasources.mysql.analytics]` 读取连接，再用 Spark JDBC reader 注册临时视图。
- `save append t as mysql.\`analytics.target_table\`` 用 Spark JDBC writer 追加写入 MySQL。
- `save overwrite t as mysql.\`analytics.target_table\`` 默认被 `[save] allowMysqlOverwrite = false` 拦截。确需覆盖时，必须先在 TOML 打开 `allowMysqlOverwrite = true`，再在单条语句里显式写 `options sparkoneOverwrite="allow"`；SparkOne 不对 MySQL 表做备份。
- SparkOne DSL 不支持 `load/save jdbc`，避免连接串、账号、密码散落在 SQL 中。需要 MySQL 时统一使用 `mysql`。

文件类 save：

- 当前 MVP 的 `save overwrite table as provider.\`path\`` 仍编译成 Spark SQL `INSERT OVERWRITE DIRECTORY`。
- 覆盖写由 SparkOne runtime 做统一保护，默认需要语句显式写 `sparkoneOverwrite="allow"`。
- `sparkoneOverwrite`、`sparkoneOverwriteBackup` 是 SparkOne 控制参数，会从 provider options 中剥离，不传给底层数据源。
- 目标路径存在时默认采用 `rename` 备份到 `/tmp/sparkone_back`；失败时会尝试恢复备份。
- 测试案例和全局开关说明见 [safe-save.md](safe-save.md)。

外部 provider：

- `excel` 编译成 `USING excel`，provider jar 需要通过运行环境提供。
- 本地 MVP 可用 `-Dspark.jars.packages=dev.mauch:spark-excel_2.12:3.5.6_0.31.2`。
- 未来接 Kyuubi 时，在 Kyuubi/Spark engine 配置 `spark.jars.packages` 或 engine classpath。

新增数据源时：

- 能用 Spark SQL provider 表达的，只在 `DataSourceResolver` 增加别名。
- 需要特殊 catalog 语义的，增加特殊 source 分支。
- 需要隐藏密钥或运行时 API 的，增加薄 runtime adapter，例如当前 `mysql`。
- 不把 connector 依赖默认加进主 `pom.xml`，除非它成为 SparkOne 自身运行所必需的核心依赖。
