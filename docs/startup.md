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

## HOCON

推荐本地开发和 IDEA 都使用 HOCON：

```bash
cp conf/sparkone.conf.template conf/sparkone.conf
```

默认启动会自动读取 `conf/sparkone.conf`：

```bash
mvn exec:java -Dexec.mainClass=ai.sparkone.server.SparkOneServer
```

也可以显式指定配置文件：

```bash
mvn exec:java \
  -Dexec.mainClass=ai.sparkone.server.SparkOneServer \
  -Dexec.args="--conf conf/sparkone.conf"
```

临时覆盖端口：

```bash
mvn exec:java \
  -Dexec.mainClass=ai.sparkone.server.SparkOneServer \
  -Dexec.args="--conf conf/sparkone.conf --port 7071"
```

说明：

- `conf/sparkone.conf.template` 是可提交模板。
- `conf/sparkone.conf` 是本地实际配置，已被 `.gitignore` 忽略。
- 如果没有传 `--conf`，启动时会自动读取存在的 `conf/sparkone.conf`。
- 命令行参数优先级高于 HOCON。

## IDEA

创建 `Application` 运行配置：

- Main class: `ai.sparkone.server.SparkOneServer`
- Working directory: `/Users/qindongliang/project/ai/spark-one`
- Use classpath of module: `spark-one`
- Program arguments: 可留空；如果要显式指定配置文件，可填 `--conf conf/sparkone.conf`

运行前先复制模板：

```bash
cp conf/sparkone.conf.template conf/sparkone.conf
```

Java 17 运行 Spark 需要 `.mvn/jvm.config` 中的 module open 参数。若 IDEA 没自动带上，把 `.mvn/jvm.config` 内容复制到 VM options。

如果查询 Hive/HDFS 时报：

```text
SIMPLE authentication is not enabled. Available: [TOKEN, KERBEROS]
```

通常说明当前进程没有读到 Kerberos/Hadoop/Hive 配置，或 keytab 登录没有生效。先确认 IDEA 的 Working directory 是项目根目录，并且 `conf/sparkone.conf` 存在；或者在 Program arguments 显式填写 `--conf conf/sparkone.conf`。

如果 Hive 查询时出现 `id: odep: no such user` 这类本机用户组 WARN，说明 Hadoop 在 macOS 上尝试解析 Kerberos 用户对应的本地 Unix 组。`conf/sparkone.conf` 里可用 `hadoop.groupStaticOverrides = "odep=odep"` 处理；未显式配置时，SparkOne 会根据 Kerberos principal 自动补一条 short name 映射。

日志配置：

- 默认使用 `src/main/resources/log4j2.xml`。
- Console appender 输出到 `SYSTEM_OUT`，IDEA 中普通 INFO/WARN 日志不应再因为 stderr 被整体渲染成红色。
- 日志级别可通过 `conf/sparkone.conf` 的 `server.logLevel = "warn"` 或启动参数 `--log-level warn` 调整。

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

推荐把测试环境参数写入 `conf/sparkone.conf`：

```hocon
server {
  host = "127.0.0.1"
  port = 7070
  showCompiledSql = false
}

spark {
  master = "local[*]"
  # local[*] 会默认使用 127.0.0.1；YARN/client 模式不要把 driverHost 固定为 127.0.0.1。
  # driverHost = "127.0.0.1"
  # driverBindAddress = "127.0.0.1"

  kerberos {
    principal = "odep@HADOOP.COM"
    keytab = "/Users/qindongliang/bigdata/odep.keytab"
  }
}

hadoop {
  confDir = "/Users/qindongliang/bigdata/hadoop/etc/hadoop"
  groupStaticOverrides = "odep=odep"
}

hive {
  enabled = true
  confFile = "/Users/qindongliang/bigdata/hive/conf/hive-site.xml"
}

kerberos {
  krb5Conf = "/etc/krb5.conf"
}

save {
  overwritePolicy = "requireExplicit"
  overwriteBackup = "rename"
  overwriteBackupPath = "/tmp/sparkone_back"
  allowMysqlOverwrite = false
  allowDorisOverwrite = false
  allowNativeInsertOverwrite = false
  allowNativeDropTable = false
  # 生产环境可打开全局保护 overwrite 的高危边界目录，命中后不能被 SQL 或 SET 覆盖。
  # 规则：禁止覆盖这些路径本身以及它们的上级目录；允许覆盖其下更具体的业务目录。
  # 支持整段通配符 "*"：例如 "/*" 保护所有一级目录，"/*/*" 保护所有一级和二级目录。
  # overwriteProtectedPaths = [
  #   "/",
  #   "/user",
  #   "/tmp",
  # ]
}

datasources.mysql {
  analytics {
    url = "jdbc:mysql://127.0.0.1:3306/app?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "change-me"

    options {
      fetchsize = 1000
      batchsize = 1000
    }
  }
}

catalogs {
  doris {
    fenodes = "fe-1:8030,fe-2:8030"
    queryPort = 9030
    user = "root"
    password = "change-me"

    options {
      doris.request.retries = 3
    }
  }
}

jars {
  packages = "com.mysql:mysql-connector-j:8.4.0,org.apache.doris:spark-doris-connector-spark-3.5:25.2.0"
}
```

也可以不使用 HOCON，直接传程序参数：

```bash
mvn exec:java \
  -Dexec.mainClass=ai.sparkone.server.SparkOneServer \
  -Dexec.args="--hive-enabled \
    --hadoop-conf-dir /Users/qindongliang/bigdata/hadoop/etc/hadoop \
    --hive-conf /Users/qindongliang/bigdata/hive/conf/hive-site.xml \
    --principal odep@HADOOP.COM \
    --keytab /Users/qindongliang/bigdata/odep.keytab \
    --krb5-conf /etc/krb5.conf"
```

如果不用 keytab 自动登录，也可以先手动拿票据：

```bash
export KRB5CCNAME=/tmp/krb5cc_$(id -u)
kinit -kt /Users/qindongliang/bigdata/odep.keytab odep@HADOOP.COM
```

更多 HDFS/Hive 说明见 [hadoop-hive.md](hadoop-hive.md)。

## Preview

页面和 `/api/run` 默认只做小结果预览：

```hocon
preview {
  maxRows = 10
}
```

- `maxRows` 是每条 statement 的服务端预览行数上限；页面 `Rows` 和 API 请求里的 `limit` 只能调小，不能超过它。
- `load ... as t` 执行后默认只展示 schema；页面点该结果的 Preview tab，或调用 `/api/preview`，才会预览刚注册的临时视图 `t`。

## Save Overwrite Safety

文件类 `save overwrite` 默认要求语句显式确认：

```sql
save overwrite result as parquet.`/tmp/result`
options sparkoneOverwrite="allow";
```

全局策略在 HOCON 的 `save` 中配置：

- `overwritePolicy = "requireExplicit"`：默认值，每条 overwrite 都要写 `sparkoneOverwrite="allow"`。
- `overwritePolicy = "allow"`：全局允许覆盖。
- `overwritePolicy = "deny"`：全局拒绝覆盖；单条语句的 `sparkoneOverwrite="allow"` 不能绕过。
- `overwriteBackup = "rename"`：默认值，目标存在时先移动到 `overwriteBackupPath`。
- `overwriteBackup = "trash"`：目标存在时先移动到 Hadoop Trash。
- `overwriteBackup = "none"`：不做备份，直接覆盖。
- `overwriteBackupPath = "/tmp/sparkone_back"`：`rename` 备份根目录；不带 scheme 时按目标文件系统解析。
- `allowMysqlOverwrite = false`：默认禁止 `save overwrite ... as mysql`；确需覆盖时只从启动配置打开，并且单条语句仍要写 `sparkoneOverwrite="allow"`。
- `allowDorisOverwrite = false`：默认禁止 `save overwrite ... as doris`；确需覆盖时只从启动配置打开，并且单条语句仍要写 `sparkoneOverwrite="allow"`。
- `allowNativeInsertOverwrite = false`：默认禁止原生 Spark SQL `INSERT OVERWRITE`，避免绕过 SparkOne Safe Save。
- `allowNativeDropTable = false`：默认禁止原生 Spark SQL `DROP TABLE`，避免误删 Hive/catalog 表；该开关只从启动配置读取。
- `overwriteProtectedPaths = [...]`：全局保护 overwrite 边界路径，一行一个；支持 `/*`、`/*/*` 这类整段通配；命中后不允许被单条 SQL 或 `SET` 覆盖。

`save { ... }` 下的策略参数只从启动 HOCON 或启动属性读取，不允许被页面里的 `SET sparkone.save...` 或单条 SQL `options` 覆盖。单条 SQL 里的 `sparkoneOverwrite="allow"` 只作为 `requireExplicit` 模式下的确认信号，不会传给底层 Spark provider，也不能绕过全局 `deny` 或 Doris/MySQL 覆盖写开关。

配置 `/public/odep/user` 后，`/public/odep/user` 本身和它的上级目录会被拦截，`/public/odep/user/userA` 这类具体业务目录可以写。

完整测试案例见 [safe-save.md](safe-save.md)。

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
  -d '{"script":"select 1 as id;","limit":10}'
```
