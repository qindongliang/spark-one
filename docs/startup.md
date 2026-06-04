# Startup

SparkOne 当前是本地开发测试服务，入口类：

```text
ai.sparkone.server.SparkOneServer
```

默认访问地址：

```text
http://127.0.0.1:7070
```

## Maven

基础启动：

```bash
sdk env
mvn exec:java -Dexec.mainClass=ai.sparkone.server.SparkOneServer
```

指定端口：

```bash
mvn exec:java \
  -Dexec.mainClass=ai.sparkone.server.SparkOneServer \
  -Dexec.args="--port 7071"
```

## TOML

推荐本地开发和 IDEA 都使用 TOML：

```bash
cp conf/sparkone.toml.template conf/sparkone.toml
```

启动：

```bash
mvn exec:java \
  -Dexec.mainClass=ai.sparkone.server.SparkOneServer \
  -Dexec.args="--conf conf/sparkone.toml"
```

临时覆盖端口：

```bash
mvn exec:java \
  -Dexec.mainClass=ai.sparkone.server.SparkOneServer \
  -Dexec.args="--conf conf/sparkone.toml --port 7071"
```

说明：

- `conf/sparkone.toml.template` 是可提交模板。
- `conf/sparkone.toml` 是本地实际配置，已被 `.gitignore` 忽略。
- 命令行参数优先级高于 TOML。

## IDEA

创建 `Application` 运行配置：

- Main class: `ai.sparkone.server.SparkOneServer`
- Working directory: `/Users/qindongliang/project/ai/spark-one`
- Use classpath of module: `spark-one`
- Program arguments: `--conf conf/sparkone.toml`

运行前先复制模板：

```bash
cp conf/sparkone.toml.template conf/sparkone.toml
```

Java 17 运行 Spark 需要 `.mvn/jvm.config` 中的 module open 参数。若 IDEA 没自动带上，把 `.mvn/jvm.config` 内容复制到 VM options。

日志配置：

- 默认使用 `src/main/resources/log4j2.xml`。
- Console appender 输出到 `SYSTEM_OUT`，IDEA 中普通 INFO/WARN 日志不应再因为 stderr 被整体渲染成红色。
- 日志级别可通过 `conf/sparkone.toml` 的 `[server] logLevel = "warn"` 或启动参数 `--log-level warn` 调整。

如果 IDEA 启动时报类似错误：

```text
java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$ cannot access class sun.nio.ch.DirectBuffer
```

说明 VM options 没带上 Spark 在 Java 17 下需要的 module open 参数，至少需要包含：

```text
-XX:+IgnoreUnrecognizedVMOptions
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
-Djdk.reflect.useDirectMethodHandle=false
```

## HDFS And Hive

推荐把测试环境参数写入 `conf/sparkone.toml`：

```toml
[server]
host = "127.0.0.1"
port = 7070

[spark]
master = "local[*]"

[hadoop]
confDir = "/Users/qindongliang/bigdata/hadoop/etc/hadoop"

[hive]
enabled = true
confFile = "/Users/qindongliang/bigdata/hive/conf/hive-site.xml"

[kerberos]
principal = "odep"
keytab = "/Users/qindongliang/bigdata/odep.keytab"
```

也可以不使用 TOML，直接传程序参数：

```bash
mvn exec:java \
  -Dexec.mainClass=ai.sparkone.server.SparkOneServer \
  -Dexec.args="--hive-enabled \
    --hadoop-conf-dir /Users/qindongliang/bigdata/hadoop/etc/hadoop \
    --hive-conf /Users/qindongliang/bigdata/hive/conf/hive-site.xml \
    --principal odep \
    --keytab /Users/qindongliang/bigdata/odep.keytab"
```

如果不用 keytab 自动登录，也可以先手动拿票据：

```bash
export KRB5CCNAME=/tmp/krb5cc_$(id -u)
kinit -kt /Users/qindongliang/bigdata/odep.keytab odep
```

更多 HDFS/Hive 说明见 [hadoop-hive.md](hadoop-hive.md)。

## Smoke Test

启动后打开页面：

```text
http://127.0.0.1:7070
```

页面里可以执行：

```sql
show databases;
show tables in default;
select 1 as id;
```

也可以用 API：

```bash
curl -s http://127.0.0.1:7070/api/run \
  -H 'Content-Type: application/json' \
  -d '{"script":"select 1 as id;","limit":20}'
```
