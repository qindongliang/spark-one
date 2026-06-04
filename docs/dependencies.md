# Dependencies

核心版本见 `pom.xml`：

- Scala: `2.12.19`
- Spark SQL: `3.5.7`
- ANTLR: `4.9.3`
- Javalin: `4.6.7`
- CodeMirror WebJar: `5.65.19`
- Jackson Scala module: `2.15.2`
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

- Spark core 内置的 `csv/json/parquet/orc/text/jdbc/libsvm` 等 provider 可直接走 Spark SQL。
- `excel` 不是 Spark core 内置，不默认打进 SparkOne 主包。
- provider 别名不要写死在前端或 runtime，统一放在 `DataSourceResolver`。
- 外部 provider 通过 `spark.jars.packages`、`spark.jars` 或 Kyuubi engine classpath 管理。
- shade 保留 `ServicesResourceTransformer`，仅用于未来确有必要随主包合并 service 文件的场景。
