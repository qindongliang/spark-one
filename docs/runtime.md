# Runtime

当前 runtime 是本地开发测试台：

```text
Javalin HTTP service
  -> SparkOneCompiler
  -> selected engine
     -> local SparkSession
     -> Kyuubi JDBC gateway
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

```hocon
engines {
  local {
    type = "local"

    jars {
      packages = "dev.mauch:spark-excel_2.12:3.5.6_0.31.2"
    }
  }
}
```

HOCON 里 local 引擎对应 Spark submit 语义：

```hocon
engines {
  local {
    type = "local"

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
  }
}
```

可选配置：

- `spark.master` -> Spark master，HOCON 中对应 `engines.local.spark.master`
- `spark.driver.host` -> Spark driver 对 executor 广播的地址，HOCON 中对应 `engines.local.spark.driverHost`
- `spark.driver.bindAddress` -> Spark driver 实际绑定地址，HOCON 中对应 `engines.local.spark.driverBindAddress`
- `spark.jars.packages` -> Maven 坐标形式的外部 provider 包，HOCON 中对应 `engines.local.jars.packages`
- `spark.jars` -> 本地 jar 文件，HOCON 中对应 `engines.local.jars.jars`
- `spark.files` -> 普通文件分发，HOCON 中对应 `engines.local.jars.files`
- `spark.jars.repositories` -> 额外 Maven 仓库，HOCON 中对应 `engines.local.jars.repositories`
- `sparkone.hadoop.conf.dir` / `HADOOP_CONF_DIR` -> 加载 `core-site.xml`、`hdfs-site.xml`、`yarn-site.xml`、`mapred-site.xml`，HOCON 中对应 `engines.local.hadoop.confDir`
- `sparkone.hadoop.conf.files` / `SPARKONE_HADOOP_CONF_FILES` -> 加载额外 Hadoop XML，HOCON 中对应 `engines.local.hadoop.confFiles`
- `sparkone.hadoop.group.static.mapping.overrides` / `SPARKONE_HADOOP_GROUP_STATIC_MAPPING_OVERRIDES` -> 本地调试时覆盖 Hadoop 用户组静态映射，HOCON 中对应 `engines.local.hadoop.groupStaticOverrides`
- `sparkone.hive.enabled` / `SPARKONE_HIVE_ENABLED` -> 启用 Spark Hive support，HOCON 中对应 `engines.local.hive.enabled`
- `sparkone.hive.conf.file` / `SPARKONE_HIVE_CONF_FILE` -> 加载指定 `hive-site.xml`，HOCON 中对应 `engines.local.hive.confFile`
- `sparkone.hive.conf.dir` / `HIVE_CONF_DIR` -> 从目录加载 `hive-site.xml`，HOCON 中对应 `engines.local.hive.confDir`

注意：

- `spark.files` / `engines.local.jars.files` 只是把普通文件分发到 driver/executor 工作目录，不会自动加入 classpath，不能用于加载 Excel 这类 DataSource provider。
- `spark.jars` / `engines.local.jars.jars` 才用于 jar 分发；SparkOne 在嵌入式本地启动时会额外把本地 jar 注入当前 driver classloader，保证 Spark SQL 能发现 provider 短名。
- `local[*]` 下 SparkOne 会默认把 `spark.driver.host` 和 `spark.driver.bindAddress` 设为 `127.0.0.1`。非 local master 不会自动设置；YARN/client 模式如需显式配置，`driverHost` 必须是 executor 可访问的地址。
- `spark.kerberos.principal` -> Spark 原生 keytab 登录 principal，HOCON 中对应 `engines.local.spark.kerberos.principal`
- `spark.kerberos.keytab` -> Spark 原生 keytab 文件，HOCON 中对应 `engines.local.spark.kerberos.keytab`
- `java.security.krb5.conf` -> JVM Kerberos realm 配置文件，HOCON 中对应 `engines.local.kerberos.krb5Conf`

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
- `POST /api/preview`

请求格式：

```json
{
  "engine": "local",
  "script": "select 1 as id;",
  "limit": 10
}
```

`engine` 可省略，默认使用 HOCON `engines.default`。当前推荐只保留两类执行引擎：

- `local`：进程内 `SparkSession`，用于 IDEA / 本地开发调试。
- `kyuubi`：通过 Kyuubi JDBC 提交 Spark SQL，作为远程 SQL gateway；YARN、Kubernetes、Standalone 等终态由 Kyuubi engine 侧配置，SparkOne 不直接提交。

配置示例：

```hocon
engines {
  default = "local"

  local {
    type = "local"
    enabled = true
    label = "Local"

    spark {
      master = "local[*]"
    }
  }

  kyuubi {
    type = "kyuubi"
    enabled = true
    label = "Kyuubi"
    url = "jdbc:kyuubi://kyuubi-host:10009/default"

    # 默认不传 user。业务执行身份以 Kyuubi engine 侧配置为准。
    # 只有 Kyuubi Server 开启客户端认证时，才配置 user/password/options。
    # user = "sparkone"
    # password = "change-me"
    # options {
    #   kyuubiClientPrincipal = "sparkone@HADOOP.COM"
    #   kyuubiClientKeytab = "/path/to/sparkone.keytab"
    #   kyuubiServerPrincipal = "kyuubi/kyuubi-host@HADOOP.COM"
    # }
  }
}
```

SparkOne 连接 Kyuubi 时不负责选择 Spark/YARN/Hive 的执行用户。统一执行身份应放在 Kyuubi Server/engine 配置中，例如本地单用户测试可在 Kyuubi 侧设置 `kyuubi.engine.share.level=SERVER`、`kyuubi.engine.doAs.enabled=false`，并由 `spark.kerberos.principal`、`spark.kerberos.keytab` 决定 Spark engine 登录身份。

Kyuubi 交互说明：

- SparkOne 使用 Kyuubi 官方推荐的 JDBC driver，默认 URL 形如 `jdbc:kyuubi://host:10009/default`。
- Kyuubi JDBC 协议兼容 HiveServer2，但连接的是 Kyuubi Server，不是把请求转发给 HiveServer2。
- 预览数据来自 JDBC `ResultSet`，和 Kyuubi Spark engine 是 client/cluster、运行在 YARN/Kubernetes/Standalone 无直接绑定。
- Kyuubi 模式下临时视图存在于 JDBC session 对应的远端 Spark engine 中；SparkOne 会复用服务进程内的 Kyuubi connection，以支持同一会话内的 `load ... as t` 后续 preview。
- `load/save mysql` 当前依赖 SparkOne 本地 DataFrame adapter，Kyuubi 模式暂不支持；远程建议使用 Spark catalog SQL 或 Kyuubi/Spark 侧已配置好的 datasource/catalog。
- Kyuubi 模式无法执行 local 的文件目录备份流程；`save overwrite` 仍会遵守 SparkOne 的显式确认/deny 开关，实际提交和权限边界由 Kyuubi/Spark/Hadoop 侧负责。

结果预览：

- `preview.maxRows`：每条 statement 默认最多预览多少行，默认 `10`。服务端会把请求里的 `limit` clamp 到 `1..preview.maxRows`，页面输入不能放大这个上限。
- `load ... as t` 执行后默认返回临时视图 `t` 的 schema，不自动 collect 数据；需要预览数据时调用 `/api/preview`，请求体为 `{"table":"t","limit":10}`。

当前限制：

- 只适合作为本地测试服务。
- 没有多租户、权限、任务队列、session 池。
- 结果行数由 `preview.maxRows` 控制，默认 10 行；不要在共享环境里把它调得过大。

Kyuubi 依赖：

- SparkOne 主包内置 `org.apache.kyuubi:kyuubi-hive-jdbc`，版本由 Maven property `kyuubi.jdbc.version` 控制；不要使用 shaded JDBC 胖包，避免它内嵌的 SLF4J 1.x 类污染 Spark/Log4j2 日志绑定。
- 外部 Spark datasource provider jar 应放在 Kyuubi/Spark engine classpath，不放在 SparkOne 主包里。
