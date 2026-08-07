# Local Engine

Local engine 是 QueryOne 的本地开发测试台。它在 QueryOne 服务进程内启动 `SparkSession`，适合 IDEA 调试、编译器验证、前端冒烟和本机数据源实验，不是多租户生产运行时。

Local 默认具备与 Kyuubi Spark Engine 一致的 ODEP 数据链路：启动时注册 `jdbc`/`doris` 路由 Catalog、`queryone_mysql` 分区读取 provider 和 RMS 鉴权扩展，不提供额外功能开关。注册是惰性的，普通 `select 1`、编译器调试和服务启动不会访问 ODEP；第一次枚举或读取 ODEP Catalog、第一次分析需鉴权资源时才读取 `engines.local.odep` 或同名 `ODEP_*` 环境变量并调用 ODEP。缺少配置或接口异常时对应资源访问 fail closed。

## 配置示例

```hocon
engines {
  default = "local"

  local {
    type = "local"
    enabled = true
    label = "Local"

    odep {
      apiUrl = "http://127.0.0.1:8080"
      appId = "queryone"
      signKey = "change-me"
      connectTimeoutSeconds = 5
      requestTimeoutSeconds = 60
    }

    spark {
      master = "local[*]"
    }

    overwrite {
      zkConnect = "127.0.0.1:2181"
      zkRoot = "/queryone/overwrite"
      workspaceRoot = "/public/odep/user"
      zkSessionTimeoutMs = 60000
      zkConnectionTimeoutMs = 15000
    }
  }
}
```

## Spark Submit 语义

Local engine 的 `jars` 配置对齐 Spark submit：

```hocon
engines {
  local {
    type = "local"

    jars {
      # 等价于 spark-submit --packages / spark.jars.packages
      packages = "dev.mauch:spark-excel_2.12:3.3.4_0.31.2"

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

注意：

- `spark.files` / `engines.local.jars.files` 只是把普通文件分发到 driver/executor 工作目录，不会自动加入 classpath，不能用于加载 Excel 这类 DataSource provider。
- `spark.jars` / `engines.local.jars.jars` 才用于 jar 分发；QueryOne 在嵌入式本地启动时会额外把本地 jar 注入当前 driver classloader，保证 Spark SQL 能发现 provider 短名。
- `local[*]` 下 QueryOne 会默认把 `spark.driver.host` 和 `spark.driver.bindAddress` 设为 `127.0.0.1`。非 local master 不会自动设置；YARN/client 模式如需显式配置，`driverHost` 必须是 executor 可访问的地址。

## 常用配置映射

- `spark.master` -> `engines.local.spark.master`
- `spark.driver.host` -> `engines.local.spark.driverHost`
- `spark.driver.bindAddress` -> `engines.local.spark.driverBindAddress`
- `spark.jars.packages` -> `engines.local.jars.packages`
- `spark.jars` -> `engines.local.jars.jars`
- `spark.files` -> `engines.local.jars.files`
- `spark.jars.repositories` -> `engines.local.jars.repositories`
- `queryone.hadoop.conf.dir` / `HADOOP_CONF_DIR` -> `engines.local.hadoop.confDir`
- `queryone.hadoop.conf.files` / `QUERYONE_HADOOP_CONF_FILES` -> `engines.local.hadoop.confFiles`
- `queryone.hadoop.group.static.mapping.overrides` / `QUERYONE_HADOOP_GROUP_STATIC_MAPPING_OVERRIDES` -> `engines.local.hadoop.groupStaticOverrides`
- `queryone.hive.enabled` / `QUERYONE_HIVE_ENABLED` -> `engines.local.hive.enabled`
- `queryone.hive.conf.file` / `QUERYONE_HIVE_CONF_FILE` -> `engines.local.hive.confFile`
- `queryone.hive.conf.dir` / `HIVE_CONF_DIR` -> `engines.local.hive.confDir`
- `spark.kerberos.principal` -> `engines.local.spark.kerberos.principal`
- `spark.kerberos.keytab` -> `engines.local.spark.kerberos.keytab`
- `java.security.krb5.conf` -> `engines.local.kerberos.krb5Conf`
- `spark.queryone.overwrite.zk.connect` -> `engines.local.overwrite.zkConnect`
- `spark.queryone.overwrite.zk.root` -> `engines.local.overwrite.zkRoot`
- `spark.queryone.overwrite.workspaceRoot` -> `engines.local.overwrite.workspaceRoot`
- `spark.queryone.overwrite.zk.sessionTimeoutMs` -> `engines.local.overwrite.zkSessionTimeoutMs`
- `spark.queryone.overwrite.zk.connectionTimeoutMs` -> `engines.local.overwrite.zkConnectionTimeoutMs`

## ODEP 与 RMS 鉴权

Local 默认从 `engines.local.odep` 读取 ODEP API 配置。环境变量优先级更高，可用于临时覆盖 HOCON：

```bash
export ODEP_API_URL=http://127.0.0.1:8080
export ODEP_KYUUBI_APP_ID=queryone
export ODEP_KYUUBI_SIGN_KEY=change-me
export ODEP_CONNECT_TIMEOUT_SECONDS=3
export ODEP_REQUEST_TIMEOUT_SECONDS=10
```

Kyuubi Spark Engine 不读取 QueryOne Server HOCON，继续通过上述环境变量或部署平台的 Secret 注入配置。实际 `conf/queryone.conf` 已被 Git 忽略；提交的模板只能保留占位密钥。

- `jdbc.<alias>.<table>`、`doris.<alias>.<table>`、Hive 表、跨 owner HDFS load 和原生绝对 HDFS 读取使用同一套 RMS 资源判定；原生绝对路径在 Analyzer 访问 HDFS 前只请求一次 RMS，并在 analysis 后使用当前计划的证明本地复核。
- Local Run 和 Preview 都从服务端 `TenantContext.username` 设置当前 subject；SQL、DSL options 和 Spark `SET` 不能替换它。
- Kyuubi 扩展仍只接受 ECDSA session 签名，不会回退到 Local subject。
- 当前用户自己的 managed workspace load/overwrite继续按 ownership 判定；跨 owner overwrite 和原生文件写入继续拒绝。
- 以 `_static` 结尾的 V2 Catalog 和 `queryone_mysql` 静态分区 relation 不走 RMS；未知 V1 provider 和未知 V2 Catalog 仍 fail closed。Local 不再提供 `load/save mysql` adapter。

Local subject 来自开发登录 session，不等价于 RMS 登录认证，因此这条链路只用于在 IDEA 中断点调试 ODEP Catalog、授权请求和 Spark LogicalPlan，不能作为生产安全边界。

Local runtime 会直接注册 `QueryOneHdfsOverwriteExtensions` 和 `QueryOneLocalOdepAuthzExtension`。`overwrite.workspaceRoot` 同时用于受控 HDFS load 和 overwrite；load 不依赖 ZooKeeper。没有配置 `zkConnect` 时，受控 HDFS overwrite 会 fail closed，受控 load 和其他只读能力不受影响。

Local engine 是单进程测试台，仍使用一个 SparkSession 和全局执行锁，只接受 `sessionMode=tenant_shared`，不承诺多租户并发隔离。`run_isolated` 是生产 Kyuubi 路径的能力，Local 收到该模式会明确拒绝，避免给定时任务提供错误的隔离语义。

## 相关文档

- 启动方式：[../ops/startup.md](../ops/startup.md)
- HDFS/Hive/Kerberos：[../data/hadoop-hive.md](../data/hadoop-hive.md)
- 数据源配置：[../data/datasources.md](../data/datasources.md)
- 写入安全：[../data/safe-save.md](../data/safe-save.md)
