# Startup

QueryOne 当前是本地开发测试服务，入口类：

```text
queryone.server.QueryOneServer
```

默认访问地址：

```text
http://127.0.0.1:7070
```

登录页面和 `/api/*` 开发接口缺省关闭。本地开发需要在 HOCON 中显式开启：

```hocon
server.developmentAccessEnabled = true
```

测试和生产环境必须设为 `false`。关闭后仅保留 `/healthz` 和 `/internal/v1/*`；Kubernetes HTTP 探针使用 `GET /healthz`，成功时返回 HTTP `200` 和文本 `OK`。

引擎细节不要继续堆在启动文档里：

- Local engine 配置见 [../engines/local.md](../engines/local.md)。
- Kyuubi engine 配置见 [../engines/kyuubi.md](../engines/kyuubi.md)。
- 通用 API、preview 和限制见 [../engines/overview.md](../engines/overview.md)。

## IDEA

用 IDEA 导入仓库根目录的 `pom.xml`。根 POM 是 Maven aggregator，会自动识别：

- `queryone-server`
- `queryone-mysql-provider`
- `queryone-hdfs-workspace-extension`

## Maven

基础启动：

```bash
sdk env
mvn -pl queryone-server exec:java -Dexec.mainClass=queryone.server.QueryOneServer
```

指定端口：

```bash
mvn -pl queryone-server exec:java \
  -Dexec.mainClass=queryone.server.QueryOneServer \
  -Dexec.args="--port 7071"
```

## HOCON

推荐本地开发和 IDEA 都使用 HOCON：

```bash
cp conf/queryone.conf.template conf/queryone.conf
```

默认启动会自动读取 `conf/queryone.conf`：

```bash
mvn -pl queryone-server exec:java -Dexec.mainClass=queryone.server.QueryOneServer
```

也可以显式指定配置文件：

```bash
mvn -pl queryone-server exec:java \
  -Dexec.mainClass=queryone.server.QueryOneServer \
  -Dexec.args="--conf conf/queryone.conf"
```

临时覆盖端口：

```bash
mvn -pl queryone-server exec:java \
  -Dexec.mainClass=queryone.server.QueryOneServer \
  -Dexec.args="--conf conf/queryone.conf --port 7071"
```

说明：

- `conf/queryone.conf.template` 是可提交模板。
- `conf/queryone.conf` 是本地实际配置，已被 `.gitignore` 忽略。
- 如果没有传 `--conf`，启动时会自动读取存在的 `conf/queryone.conf`。
- 命令行参数优先级高于 HOCON。

## IDEA

创建 `Application` 运行配置：

- Main class: `queryone.server.QueryOneServer`
- Working directory: `/Users/qindongliang/project/ai/query-one`
- Use classpath of module: `query-one`
- Program arguments: 可留空；如果要显式指定配置文件，可填 `--conf conf/queryone.conf`

运行前先复制模板：

```bash
cp conf/queryone.conf.template conf/queryone.conf
```

Java 17 运行 Spark 需要 `.mvn/jvm.config` 中的 module open 参数。若 IDEA 没自动带上，把 `.mvn/jvm.config` 内容复制到 VM options。

如果查询 Hive/HDFS 时报：

```text
SIMPLE authentication is not enabled. Available: [TOKEN, KERBEROS]
```

通常说明当前进程没有读到 Kerberos/Hadoop/Hive 配置，或 keytab 登录没有生效。先确认 IDEA 的 Working directory 是项目根目录，并且 `conf/queryone.conf` 存在；或者在 Program arguments 显式填写 `--conf conf/queryone.conf`。

如果 Hive 查询时出现 `id: odep: no such user` 这类本机用户组 WARN，说明 Hadoop 在 macOS 上尝试解析 Kerberos 用户对应的本地 Unix 组。`conf/queryone.conf` 里可用 `engines.local.hadoop.groupStaticOverrides = "odep=odep"` 处理；未显式配置时，QueryOne 会根据 Kerberos principal 自动补一条 short name 映射。

日志配置：

- 默认使用 `queryone-server/src/main/resources/log4j2.xml`。
- Console appender 输出到 `SYSTEM_OUT`，IDEA 中普通 INFO/WARN 日志不应再因为 stderr 被整体渲染成红色。
- 日志级别可通过 `conf/queryone.conf` 的 `server.logLevel = "warn"` 或启动参数 `--log-level warn` 调整。

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

推荐把测试环境参数写入 `conf/queryone.conf`：

```hocon
server {
  host = "127.0.0.1"
  port = 7070
  showCompiledSql = false
}

engines {
  default = "local"

  local {
    type = "local"
    enabled = true
    label = "Local"

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

    catalogs {
      mysql_static {
        url = "jdbc:mysql://127.0.0.1:3306/?databaseTerm=SCHEMA&useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
        driver = "com.mysql.cj.jdbc.Driver"
        user = "root"
        password = "change-me"

        options {
          fetchsize = 1000
        }
      }

      doris_static {
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
      packages = "com.mysql:mysql-connector-j:8.4.0,org.apache.doris:spark-doris-connector-spark-3.3:25.2.0"
    }
  }
}
```

MySQL 连接只配置在 `engines.local.catalogs.mysql_static`，不再维护 `datasources.mysql`：

```sql
show namespaces in mysql_static;
show tables in mysql_static.app;
select * from mysql_static.app.some_table limit 10;
load jdbc.`mysql_static.app.some_table` as some_table;
```

HOCON 会把该配置注册成 `mysql_static`，避免占用默认 ODEP 路由的 `jdbc`/`doris` 名称。静态 Catalog 可直接三段式查询，也可通过 `load/save jdbc` 使用；它不走 RMS，但 SQL 不能覆盖连接参数。未知 Catalog 仍 fail closed。

注意 MySQL catalog 的 JDBC URL 需要带 `databaseTerm=SCHEMA`。Spark JDBC Catalog 会把 `show tables in mysql_static.app` 里的 `app` 作为 JDBC `schemaPattern` 查询元数据；MySQL Connector/J 默认把 database 当作 JDBC catalog，导致 schemaPattern 过滤不到预期库。设置后，MySQL database 会按 schema 暴露，`show namespaces/tables` 的层级才和 Spark catalog 语法一致。

Local 默认注册 ODEP Catalog 和 RMS 鉴权扩展，不需要功能开关。要在 IDEA 中调试 ODEP 数据链路，可配置 `engines.local.odep.apiUrl/appId/signKey` 和可选超时；`ODEP_API_URL`、`ODEP_KYUUBI_APP_ID`、`ODEP_KYUUBI_SIGN_KEY` 及超时环境变量会覆盖 HOCON。普通启动和 `select 1` 不读取这些配置，第一次访问 ODEP Catalog 或需鉴权资源时才校验并调用接口。

也可以不使用 HOCON，直接传程序参数：

```bash
mvn -pl queryone-server exec:java \
  -Dexec.mainClass=queryone.server.QueryOneServer \
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

更多 HDFS/Hive 说明见 [../data/hadoop-hive.md](../data/hadoop-hive.md)。

## Preview

页面和 `/api/run` 默认只做小结果预览：

```hocon
preview {
  maxRows = 10
}
```

- `maxRows` 是每条 statement 的服务端预览行数上限；页面 `Rows` 和 API 请求里的 `limit` 只能调小，不能超过它。
- `load ... as t` 执行后默认只展示 schema；页面点该结果的 Preview tab，或调用 `/api/preview`，才会预览刚注册的临时视图 `t`。

## Write Safety

写入权限由代码中的固定能力矩阵决定，不再提供全局 overwrite policy、文件备份、protected paths 或 MySQL/Doris overwrite 开关。Hive、Doris、MySQL 和 external path overwrite 永久拒绝，配置不能放开。

受控 HDFS workspace 使用 `/public/odep/user/${username}`。文件 load/overwrite DSL 只接受相对路径：load 由 Spark driver extension 解析 workspace owner 和路径并注册临时视图；overwrite 只允许当前 subject 自己的 workspace，并额外通过 ZK 排他和同级 staging 发布。跨 owner load 和原生绝对 HDFS relation 读取走 ODEP/RMS `hdfs read` 鉴权；原生路径在 Analyzer 访问 HDFS 前只鉴权一次，允许后才解析 relation，analysis 使用当前计划的证明本地复核。Local 使用服务端 TenantContext subject，Kyuubi 使用签名 subject；原生文件路径写入、文件 append、本地文件、S3、OSS、glob 和百分号编码裸路径读写保持拒绝。Local 的 ZK/workspace 配置见 [../engines/local.md](../engines/local.md)，Kyuubi 的扩展部署见 [../engines/kyuubi.md](../engines/kyuubi.md)。

原生 SQL 只允许查询和只读检查命令。`CREATE/DROP/ALTER/INSERT` 等 DDL/DML 在 Compile 阶段永久拒绝，必须通过平台外流程治理表结构，并通过 QueryOne `save` 写入；不存在可以放开的 HOCON 配置。

完整能力矩阵和测试案例见 [../data/safe-save.md](../data/safe-save.md)。

## Smoke Test

启动后打开页面：

```text
http://127.0.0.1:7070
```

页面里可以执行：

```sql
show catalogs;
show namespaces in hive;
show tables in hive.default;
select 1 as id;
```

也可以用 API：

```bash
curl -s http://127.0.0.1:7070/api/run \
  -H 'Content-Type: application/json' \
  -d '{"script":"select 1 as id;","limit":10}'
```
