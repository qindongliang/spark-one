# Data Sources

SparkOne 的数据源策略是：compiler 负责把 SQL 友好的 `load/save` 薄 DSL 编译成 Spark SQL，connector jar 由 Spark/Kyuubi 运行环境提供。

默认主包不内置第三方 provider。这样可以避免 Excel、Mongo、ES、Kafka 等 connector 和 SparkOne 主应用强耦合，也减少 shade 冲突。

内置 Spark provider：

```text
csv
json
parquet
orc
text
jdbc
libsvm
```

特殊 source：

- `hive` 是 catalog 表语义：`load hive.\`db.table\` as t` 编译成 `CREATE OR REPLACE TEMPORARY VIEW t AS SELECT * FROM db.table`。

外部 provider：

- `excel` 编译成 `USING excel`，provider jar 需要通过运行环境提供。
- 本地 MVP 可用 `-Dsparkone.jars.packages=dev.mauch:spark-excel_2.12:3.5.6_0.31.2`。
- 未来接 Kyuubi 时，在 Kyuubi/Spark engine 配置 `spark.jars.packages` 或 engine classpath。

新增数据源时：

- 能用 Spark SQL provider 表达的，只在 `DataSourceResolver` 增加别名。
- 需要特殊 catalog 语义的，增加特殊 source 分支。
- 不把 connector 依赖默认加进主 `pom.xml`，除非它成为 SparkOne 自身运行所必需的核心依赖。
