# Runtime

当前 runtime 是本地开发测试台：

```text
Javalin HTTP service
  -> SparkOneCompiler
  -> SparkSession local[*]
  -> result rows
```

入口：

- `ai.sparkone.server.SparkOneServer`

应用启动方法：

- 统一见 [startup.md](startup.md)。

启动：

```bash
sdk env
mvn exec:java -Dexec.mainClass=ai.sparkone.server.SparkOneServer
mvn exec:java -Dexec.mainClass=ai.sparkone.server.SparkOneServer -Dexec.args="--conf conf/sparkone.conf"
```

默认地址：

```text
http://127.0.0.1:7070
```

指定端口：

```bash
mvn exec:java -Dexec.mainClass=ai.sparkone.server.SparkOneServer -Dexec.args=7071
mvn exec:java -Dexec.mainClass=ai.sparkone.server.SparkOneServer -Dexec.args="--port 7071"
```

本地加载外部数据源 provider：

```bash
mvn exec:java \
  -Dexec.mainClass=ai.sparkone.server.SparkOneServer \
  -Dspark.jars.packages=dev.mauch:spark-excel_2.12:3.5.6_0.31.2
```

HOCON 里对应 Spark submit 语义：

```hocon
jars {
  # 等价于 spark-submit --packages / spark.jars.packages
  packages = "dev.mauch:spark-excel_2.12:3.5.6_0.31.2"

  # 等价于 spark-submit --jars / spark.jars，用于依赖 jar
  jars = "/path/to/provider.jar,/path/to/another-provider.jar"

  # 等价于 spark-submit --files / spark.files，用于普通文件分发，不进入 classpath
  files = "/path/to/app.conf,/path/to/dict.txt"

  # spark.jars.repositories
  repositories = "https://repo1.maven.org/maven2"
}
```

可选配置：

- `spark.master` -> Spark master，HOCON 中对应 `spark.master`
- `spark.driver.host` -> Spark driver 对 executor 广播的地址，HOCON 中对应 `spark.driverHost`
- `spark.driver.bindAddress` -> Spark driver 实际绑定地址，HOCON 中对应 `spark.driverBindAddress`
- `spark.jars.packages` -> Maven 坐标形式的外部 provider 包
- `spark.jars` -> 本地 jar 文件，HOCON 中对应 `jars.jars`
- `spark.files` -> 普通文件分发，HOCON 中对应 `jars.files`
- `spark.jars.repositories` -> 额外 Maven 仓库
- `sparkone.hadoop.conf.dir` / `HADOOP_CONF_DIR` -> 加载 `core-site.xml`、`hdfs-site.xml`、`yarn-site.xml`、`mapred-site.xml`
- `sparkone.hadoop.conf.files` / `SPARKONE_HADOOP_CONF_FILES` -> 加载额外 Hadoop XML，支持逗号或系统 path separator 分隔
- `sparkone.hadoop.group.static.mapping.overrides` / `SPARKONE_HADOOP_GROUP_STATIC_MAPPING_OVERRIDES` -> 本地调试时覆盖 Hadoop 用户组静态映射
- `sparkone.hive.enabled` / `SPARKONE_HIVE_ENABLED` -> 启用 Spark Hive support
- `sparkone.hive.conf.file` / `SPARKONE_HIVE_CONF_FILE` -> 加载指定 `hive-site.xml`
- `sparkone.hive.conf.dir` / `HIVE_CONF_DIR` -> 从目录加载 `hive-site.xml`

注意：

- `spark.files` / `--files` 只是把普通文件分发到 driver/executor 工作目录，不会自动加入 classpath，不能用于加载 Excel 这类 DataSource provider。
- `spark.jars` / `jars.jars` 才用于 jar 分发；SparkOne 在嵌入式本地启动时会额外把本地 jar 注入当前 driver classloader，保证 Spark SQL 能发现 provider 短名。
- `local[*]` 下 SparkOne 会默认把 `spark.driver.host` 和 `spark.driver.bindAddress` 设为 `127.0.0.1`。非 local master 不会自动设置；YARN/client 模式如需显式配置，`driverHost` 必须是 executor 可访问的地址。
- `spark.kerberos.principal` -> Spark 原生 keytab 登录 principal，HOCON 中对应 `spark.kerberos.principal`
- `spark.kerberos.keytab` -> Spark 原生 keytab 文件，HOCON 中对应 `spark.kerberos.keytab`
- `java.security.krb5.conf` -> JVM Kerberos realm 配置文件，HOCON 中对应 `kerberos.krb5Conf`

常用程序参数：

- `--conf`
- `--host`
- `--port`
- `--hive` / `--hive-enabled`
- `--hadoop-conf-dir`
- `--hadoop-conf-files`
- `--hive-conf`
- `--hive-conf-dir`
- `--principal`
- `--keytab`
- `--krb5-conf`
- `--hadoop-group-static-overrides`
- `--log-level`

配置模板：

- `conf/sparkone.conf.template` 是可提交模板。
- `conf/sparkone.conf` 是本地实际配置，已被 `.gitignore` 忽略。
- 命令行参数优先级高于配置文件，可用 `--conf conf/sparkone.conf --port 7071` 临时覆盖端口。

API：

- `POST /api/compile`
- `POST /api/run`

请求格式：

```json
{
  "script": "select 1 as id;",
  "limit": 200
}
```

当前限制：

- 只适合作为本地测试服务。
- 没有多租户、权限、任务队列、session 池。
- 结果最多限制到 1000 行以内，服务端会 clamp。

后续接 Kyuubi：

- 不要改 compiler。
- 新增 `KyuubiJdbcRuntime`，替换 `SparkOneRuntime` 的执行方式。
- 编译出的 Spark SQL 顺序提交给 Kyuubi。
- 外部 provider jar 放在 Kyuubi/Spark engine classpath，不放在 SparkOne 主包里。
