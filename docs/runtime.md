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
mvn exec:java -Dexec.mainClass=ai.sparkone.server.SparkOneServer -Dexec.args="--conf conf/sparkone.toml"
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
  -Dsparkone.jars.packages=dev.mauch:spark-excel_2.12:3.5.6_0.31.2
```

可选配置：

- `sparkone.jars.packages` / `SPARKONE_JARS_PACKAGES` -> `spark.jars.packages`
- `sparkone.jars` / `SPARKONE_JARS` -> `spark.jars`
- `sparkone.jars.repositories` / `SPARKONE_JARS_REPOSITORIES` -> `spark.jars.repositories`
- `sparkone.hadoop.conf.dir` / `HADOOP_CONF_DIR` -> 加载 `core-site.xml`、`hdfs-site.xml`、`yarn-site.xml`、`mapred-site.xml`
- `sparkone.hadoop.conf.files` / `SPARKONE_HADOOP_CONF_FILES` -> 加载额外 Hadoop XML，支持逗号或系统 path separator 分隔
- `sparkone.hive.enabled` / `SPARKONE_HIVE_ENABLED` -> 启用 Spark Hive support
- `sparkone.hive.conf.file` / `SPARKONE_HIVE_CONF_FILE` -> 加载指定 `hive-site.xml`
- `sparkone.hive.conf.dir` / `HIVE_CONF_DIR` -> 从目录加载 `hive-site.xml`
- `sparkone.kerberos.principal` / `SPARKONE_KERBEROS_PRINCIPAL` -> 可选 keytab 登录 principal
- `sparkone.kerberos.keytab` / `SPARKONE_KERBEROS_KEYTAB` -> 可选 keytab 文件

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
- `--log-level`

配置模板：

- `conf/sparkone.toml.template` 是可提交模板。
- `conf/sparkone.toml` 是本地实际配置，已被 `.gitignore` 忽略。
- 命令行参数优先级高于配置文件，可用 `--conf conf/sparkone.toml --port 7071` 临时覆盖端口。

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
