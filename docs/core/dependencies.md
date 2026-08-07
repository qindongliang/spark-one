# Dependencies

核心版本见根目录 `pom.xml`：

- Scala: `2.12.15`
- Spark SQL / Hive: `3.3.4`
- ANTLR: `4.8`
- Javalin: `4.6.7`
- CodeMirror WebJar: `5.65.19`
- Jackson Scala module: `2.13.4`
- HOCON parser: Lightbend Config `com.typesafe:config 1.4.3`
- Apache Curator: `2.13.0`，与 Spark 3.3.x 发行依赖保持兼容
- SLF4J API: `1.7.32`
- JUnit: `4.13.2`

环境：

- `.sdkmanrc` 指定 Java `17.0.14-tem`、Scala `2.13.8`。
- 项目实际编译 Scala 版本由 Maven 控制，为 `2.12.15`。
- 构建前建议运行：

```bash
sdk env
```

Java 17 与 Spark：

- Spark 在 Java 17 下需要 module open 参数。
- Maven 进程参数放在 `.mvn/jvm.config`；Surefire fork 的测试 JVM 由根 POM 的 `spark.test.jvm.module.options` 使用同一组参数。
- 这些参数来自 Spark `JavaModuleOptions.defaultModuleOptions()`。

Shade jar：

- `maven-shade-plugin` 会生成包含 Spark local runtime 的 fat jar。
- 已过滤 `META-INF/*.SF`、`*.DSA`、`*.RSA`，避免签名文件导致 `java -jar` 启动失败。
- fat jar 当前会比较大，这是因为 MVP 内置 Spark local runtime。

数据源依赖：

- Spark core 内置的 `csv/json/parquet/orc/text/libsvm` 等 provider 可直接走 Spark SQL。
- Spark 底层 JDBC 统一由 Catalog 承载：ODEP 动态路径使用 `jdbc.alias.table`，静态路径使用 `catalog_static.database.table`。`load jdbc` 的 MySQL 分区读取复用 Engine 内 `queryone_mysql` provider；`save jdbc` 只允许静态三段式 Catalog 目标。
- Doris 4.x 读写由 Spark Doris Catalog / Connector 提供，Spark 3.3 可通过运行环境引入 `org.apache.doris:spark-doris-connector-spark-3.3:25.2.0`；QueryOne 主包不默认内置该 connector。
- `excel` 不是 Spark core 内置，不默认打进 QueryOne 主包。
- provider 别名不要写死在前端或 runtime，统一放在 `DataSourceResolver`。
- 外部 provider 在 local 模式通过 `engines.local.jars.packages`、`engines.local.jars.jars` 管理；Kyuubi 模式放到 Kyuubi/Spark engine classpath。
- shade 保留 `ServicesResourceTransformer`，仅用于未来确有必要随主包合并 service 文件的场景。

Spark engine 扩展依赖：

- `queryone-hdfs-workspace-extension_2.12` 承载受控 HDFS load/overwrite，将 Spark SQL、Curator 和 SLF4J 标记为 `provided`，部署到 Kyuubi 时复用 Spark/Hadoop engine classpath，避免重复打包造成版本冲突。
- `queryone-odep-authz-extension` 承载 Engine 资源提取、Kyuubi session user 签名校验、HDFS workspace ownership 和 ODEP 批量鉴权；它以 `provided` 方式复用 `queryone-hdfs-workspace-extension_2.12` 的路径解析与 managed load plan 标记，因此生产 Engine 必须同时部署两个 JAR。
- Local server 通过 Maven reactor 依赖 HDFS extension、ODEP Catalog 模块、MySQL provider 和 ODEP authz extension，并直接注册 Local 专用扩展与路由 Catalog；这些类已进入 server fat jar。Kyuubi Spark Engine 仍需单独部署相应 JAR 并配置 `spark.sql.extensions`，且继续使用 Kyuubi 签名扩展入口。

配置文件依赖：

- 启动配置使用 HOCON。
- HOCON 解析使用 Lightbend Config `com.typesafe:config`。它是 JVM 通用配置库，不引入 ANTLR runtime，也不会和 Spark 固定的 ANTLR `4.8` 冲突。

日志依赖：

- 使用 Spark 3.3 自带的 Log4j2 体系。
- 显式固定 `slf4j-api` 到 `1.7.32`，匹配 Spark 的 `log4j-slf4j-impl`。
- 不额外引入 Logback 或 `slf4j-simple`，避免日志后端冲突。
- 默认配置在 `queryone-server/src/main/resources/log4j2.xml`，Console 输出到 `SYSTEM_OUT`，避免 IDEA 把普通 INFO 日志当作 stderr 渲染成红色。

Hive 依赖：

- 主包引入 `spark-hive_2.12`，仅用于 Spark 内置 Hive catalog / metastore client。
- 不混入任何 Scala 2.13 或其他 Spark 小版本的本地 JAR，避免与 Spark 3.3.4 / Scala 2.12.15 冲突。
- Maven Central 的 Spark 3.3.4 默认传递 Hadoop client 3.3.2；Kyuubi Engine 使用公司构建的 Spark 3.3.4 / Hadoop 2.8.5 `SPARK_HOME`。QueryOne Engine 扩展将 Spark/Hadoop 标记为 `provided`，不会把 Maven Hadoop 客户端带入 Engine。
