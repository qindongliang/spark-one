# Dependencies

核心版本见根目录 `pom.xml`：

- Scala: `2.12.19`
- Spark SQL / Hive: `3.5.7`
- ANTLR: `4.9.3`
- Javalin: `4.6.7`
- CodeMirror WebJar: `5.65.19`
- Jackson Scala module: `2.15.2`
- HOCON parser: Lightbend Config `com.typesafe:config 1.4.3`
- Apache Curator: `2.13.0`，与 Spark 3.3.x/3.5.x 发行依赖保持兼容
- SLF4J API: `2.0.7`
- JUnit: `4.13.2`

环境：

- `.sdkmanrc` 指定 Java `17.0.14-tem`、Scala `2.13.8`。
- 项目实际编译 Scala 版本由 Maven 控制，为 `2.12.19`。
- 构建前建议运行：

```bash
sdk env
```

Java 17 与 Spark：

- Spark 在 Java 17 下需要 module open 参数。
- 参数放在 `.mvn/jvm.config`。
- 这些参数来自 Spark `JavaModuleOptions.defaultModuleOptions()`。

Shade jar：

- `maven-shade-plugin` 会生成包含 Spark local runtime 的 fat jar。
- 已过滤 `META-INF/*.SF`、`*.DSA`、`*.RSA`，避免签名文件导致 `java -jar` 启动失败。
- fat jar 当前会比较大，这是因为 MVP 内置 Spark local runtime。

数据源依赖：

- Spark core 内置的 `csv/json/parquet/orc/text/libsvm` 等 provider 可直接走 Spark SQL。
- Spark 底层 JDBC 由 `load/save mysql` 的 runtime adapter 使用；SparkOne DSL 不暴露 `load/save jdbc`。
- Doris 4.x 读写由 Spark Doris Catalog / Connector 提供，Spark 3.5 可通过运行环境引入 `org.apache.doris:spark-doris-connector-spark-3.5:25.2.0`；SparkOne 主包不默认内置该 connector。
- `excel` 不是 Spark core 内置，不默认打进 SparkOne 主包。
- provider 别名不要写死在前端或 runtime，统一放在 `DataSourceResolver`。
- 外部 provider 在 local 模式通过 `engines.local.jars.packages`、`engines.local.jars.jars` 管理；Kyuubi 模式放到 Kyuubi/Spark engine classpath。
- shade 保留 `ServicesResourceTransformer`，仅用于未来确有必要随主包合并 service 文件的场景。

Spark engine 扩展依赖：

- `sparkone-hdfs-overwrite-extension_2.12` 将 Spark SQL、Curator 和 SLF4J 标记为 `provided`，部署到 Kyuubi 时复用 Spark/Hadoop engine classpath，避免重复打包造成版本冲突。
- Local server 通过 Maven reactor 依赖该模块并直接注册 extension；Kyuubi engine 需要单独部署扩展 JAR 并配置 `spark.sql.extensions`。

配置文件依赖：

- 启动配置使用 HOCON。
- HOCON 解析使用 Lightbend Config `com.typesafe:config`。它是 JVM 通用配置库，不引入 ANTLR runtime，也不会和 Spark 固定的 ANTLR `4.9.3` 冲突。

日志依赖：

- 使用 Spark 3.5 自带的 Log4j2 体系。
- 显式固定 `slf4j-api` 到 `2.0.7`，匹配 Spark 的 `log4j-slf4j2-impl`。
- 不额外引入 Logback 或 `slf4j-simple`，避免日志后端冲突。
- 默认配置在 `sparkone-server/src/main/resources/log4j2.xml`，Console 输出到 `SYSTEM_OUT`，避免 IDEA 把普通 INFO 日志当作 stderr 渲染成红色。

Hive 依赖：

- 主包引入 `spark-hive_2.12`，仅用于 Spark 内置 Hive catalog / metastore client。
- 不混入本地 `/Users/qindongliang/bigdata/spark-3.3.4-jdk17-scala-2.13/jars`，避免 Spark 3.5/3.3 与 Scala 2.12/2.13 冲突。
- 当前 Spark 3.5.7 默认传递 Hadoop client 3.3.4。若测试集群是 Hadoop 2.8.5，不建议在 Spark 3.5 上强行替换 Hadoop 依赖；更稳妥的长期路线是通过 Kyuubi/Spark engine 使用集群匹配的 Hadoop/Hive classpath。
