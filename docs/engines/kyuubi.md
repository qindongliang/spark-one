# Kyuubi Engine

Kyuubi engine 通过 Kyuubi JDBC 提交 Spark SQL，是 SparkOne 面向远程 SQL gateway 的主路径。SparkOne 不直接管理 YARN、Kubernetes、Standalone、Spark engine classpath、catalog 密钥或执行用户；这些都应放在 Kyuubi/Spark/Hadoop 环境里。

## 推荐配置拆分

三类配置分别解决“可信公共能力”“Spark 运行环境”和“SparkOne 路由选择”，不要把同一个参数复制到三套 profile：

| 配置层 | 放置内容 | 不应放置 |
| --- | --- | --- |
| `kyuubi-defaults.conf` | Kyuubi 认证与前端、固定 principal/keytab、Catalog 密钥、SparkOne extension、公共 JAR、engine 共享策略、profile advisor | `spark.master`、deploy mode、队列、资源、driver 地址 |
| `kyuubi-session-<profile>.conf` | subdomain、master、deploy mode、队列、driver 网络、资源、动态分配、event log、TTL | keytab、Catalog 密钥、extension、公共 JAR |
| `sparkone.conf` | engine id、展示名称、Kyuubi JDBC 地址、profile 名称 | `spark.*` 启动参数、Catalog 密钥、远端 keytab |

这个边界让认证和数据访问配置只维护一次，也让三种运行环境只能由服务端受信任文件定义。`kyuubi.session.conf.restrict.list` 会在 Kyuubi Server 拒绝 JDBC 客户端直接注入 `spark.*` 和 engine 路由参数；`FileSessionConfAdvisor` 在这次校验之后加载管理员维护的 profile。

Kyuubi Server 启动 Spark SQL engine 后，还会把合并后的 profile 作为内部 session 配置传给 engine。engine 默认继承同一份 restrict list，如果继续使用 `spark.*` 通配限制，会把可信 profile 误判为客户端注入并拒绝连接。因此每套受信任 profile 都要将传给 engine 的 `kyuubi.session.conf.restrict.list` 覆写为空。这个覆写发生在 Server 完成外部配置校验之后，不会取消 Kyuubi Server 自身的限制。

### 配置加载与优先级

`kyuubi-defaults.conf` 和 profile 不是两个同时修改 Kyuubi Server 全局状态的配置文件。Kyuubi 中至少有两个独立的配置作用域：

| 配置对象 | 何时创建 | 配置来源 | 主要影响 |
| --- | --- | --- | --- |
| Kyuubi Server frontend | Server 启动时 | `kyuubi-defaults.conf` | frontend SessionManager、operation、认证、端口、服务发现 |
| Spark engine/backend | 首次连接某个 engine space、启动 engine 时 | `kyuubi-defaults.conf` 公共基线 + JDBC OpenSession 配置 + 当前所选 profile | `spark-submit`、Spark engine 的 backend SessionManager、资源和 engine 生命周期 |

启用 `FileSessionConfAdvisor` 后，一条连接只会加载一个 `$KYUUBI_CONF_DIR/kyuubi-session-<profile>.conf`。`local`、`yarn-client` 和 `yarn-cluster` 是三个并列 profile，不会彼此继承；它们都以 `kyuubi-defaults.conf` 为公共基线：

| JDBC 中的 profile 值 | 实际加载文件 | engine subdomain | Spark 运行方式 |
| --- | --- | --- | --- |
| `local` | `kyuubi-session-local.conf` | `local` | `local[2]` |
| `yarn-client` | `kyuubi-session-yarn-client.conf` | `yarn-client` | `yarn` + `client` |
| `yarn-cluster` | `kyuubi-session-yarn-cluster.conf` | `yarn-cluster` | `yarn` + `cluster` |

三份文件都必须位于每个 Kyuubi Server 的同一个 `$KYUUBI_CONF_DIR` 下，文件名由 profile 值严格拼接得到。对一个尚未启动的新 engine，其有效配置优先级可以简化为：

```text
Kyuubi 内置默认值
  < kyuubi-defaults.conf
  < JDBC OpenSession 中允许客户端设置的配置
  < 当前选中的 kyuubi-session-<profile>.conf
```

profile 在最后合并，因此同名键由 profile 覆盖 JDBC 和公共基线。客户端配置会先经过 `kyuubi.session.conf.restrict.list` 校验；被限制的键不是“优先级较低”，而是直接拒绝，根本不会进入合并。

这个优先级只适用于所选 engine 的启动配置和 backend 配置，不适用于已经初始化的 Kyuubi Server frontend。profile 中的 `kyuubi.session.idle.timeout` 不会反向修改 Server frontend SessionManager；profile 中的 master、资源和生命周期参数也不会原地重配已经运行的 engine。修改公共 Server 参数必须重启每个 Kyuubi Server；修改 profile 后要让 engine 级全局参数确定生效，应等待 profile 缓存刷新并停止旧 engine，再由新连接重新拉起。

### ODEP 数据源进程快照

`sparkone-kyuubi-odep-plugin` 是 Kyuubi Server 侧的 `SessionConfAdvisor`。Kyuubi 1.9.4 在每个 Server 的首个 Session 创建时初始化 advisor，插件使用 OpenAPI 签名从 ODEP 拉取一次 `/api/datasource/snapshot`，把完整快照保存在当前 Server 进程内存，并把可映射的数据源转换为 Spark Catalog session overlay。本阶段不定时刷新；ODEP 注册信息变化后重启所有 Kyuubi Server，共享模式下再停止旧 engine。

构建插件：

```bash
scripts/build.sh sparkone-kyuubi-odep-plugin
```

把 `sparkone-kyuubi-odep-plugin/target/sparkone-kyuubi-odep-plugin-0.1.0-SNAPSHOT.jar` 放入每个 Kyuubi Server 的 `$KYUUBI_HOME/jars`。插件依赖的 Jackson、SLF4J 和 `kyuubi-server-plugin` 已由 Kyuubi 1.9.4 提供，不需要重复复制。

Kyuubi Server 进程必须配置：

```bash
export ODEP_API_URL=https://odep-api.example
export ODEP_KYUUBI_APP_ID=app_kyuubi
export ODEP_KYUUBI_SIGN_KEY='<sign-key>'
export ODEP_CONNECT_TIMEOUT_SECONDS=5
export ODEP_REQUEST_TIMEOUT_SECONDS=60
```

`ODEP_API_URL` 是 ODEP API 根地址，插件固定请求其 `/api/datasource/snapshot`。`ODEP_API_URL`、`ODEP_KYUUBI_APP_ID`、`ODEP_KYUUBI_SIGN_KEY` 中任意一个缺失、HTTP/业务响应失败、JSON 结构非法或仍包含 `${...}` 占位符，首个 Session 创建失败；Kyuubi Server 进程本身仍保持运行。日志只记录数据源数量、生成的 Catalog 数量以及不支持映射的 `type/alias`，不记录 options、URL、用户或密码。

`kyuubi-defaults.conf` 中把 ODEP advisor 放在文件 advisor 后面：

```properties
kyuubi.session.conf.advisor org.apache.kyuubi.session.FileSessionConfAdvisor,ai.sparkone.kyuubi.odep.OdepDatasourceSessionConfAdvisor
kyuubi.session.conf.restrict.list spark.*,kyuubi.engine.share.level,kyuubi.engine.share.level.subdomain
kyuubi.server.redaction.regex (?i)secret|password|passwd|token|access[._-]?key|spark[.]sql[.]catalog[.].*[.](url|user|doris[.]fenodes)
spark.redaction.regex (?i)secret|password|passwd|token|access[._-]?key|spark[.]sql[.]catalog[.].*[.](url|user|doris[.]fenodes)
```

Kyuubi 1.9.4 按 advisor 声明顺序合并 overlay，后面的 ODEP 配置覆盖 profile 中同名的静态 Catalog。客户端配置仍先经过 Server restrict list，不能覆盖受控 Catalog。两条 redaction 正则还会分别遮蔽 Kyuubi Engine 启动命令和 Spark 日志/UI 中的 Catalog URL、用户名、密码及 Doris FE 地址。当前映射规则为：

| ODEP type | Spark Catalog 名 | 实现 |
| --- | --- | --- |
| `jdbc` 且 URL/driver 为 MySQL | `mysql_<alias>` | Spark `JDBCTableCatalog`，URL 自动补 `databaseTerm=SCHEMA` |
| `doris` | `doris_<alias>` | Doris `DorisTableCatalog` |
| `es`、`solr` 和其他类型 | 暂不生成 | 保留在 Server 内存快照并记录告警 |

例如 ODEP 中 `jdbc/dworks` 和 `doris/recommend` 分别注册为 `mysql_dworks`、`doris_recommend`。插件只负责 Catalog 配置；MySQL driver、Doris connector 等运行依赖仍必须安装在 Spark engine classpath。

该方案使用 Kyuubi 1.9.4 原生的 advisor 懒加载行为，不修改 Kyuubi 源码，也不需要维护自定义 Kyuubi 构建。部署后可主动创建一次测试连接进行预热，使 ODEP 配置错误在业务流量进入前暴露。

### 公共 Kyuubi 配置

`$KYUUBI_CONF_DIR/kyuubi-defaults.conf` 的推荐结构如下。密码和 keytab 路径应使用实际值，并限制该文件仅 Kyuubi 运行账号可读；不要提交到 SparkOne 仓库。

```properties
# Hadoop/Kerberos
hadoop.security.authentication              kerberos
hadoop.security.authorization               true
hadoop.security.auth_to_local                RULE:[1:$1@$0](odep@HADOOP.COM)s/.*/odep/ DEFAULT
kyuubi.kinit.principal                       odep@HADOOP.COM
kyuubi.kinit.keytab                          /etc/security/keytabs/odep.keytab

# Kyuubi Server/engine 服务发现
kyuubi.ha.addresses                          192.168.200.69:2181
kyuubi.ha.namespace                          sparkone-kyuubi

# 所有 Spark engine 共用的身份、依赖和 SparkOne 扩展
spark.kerberos.principal                     odep@HADOOP.COM
spark.kerberos.keytab                        /etc/security/keytabs/odep.keytab
spark.jars                                   /opt/sparkone/sparkone-hdfs-overwrite-extension.jar,/opt/sparkone/sparkone-mysql-provider.jar,/opt/connectors/spark-doris-connector.jar,/opt/connectors/mysql-connector-j.jar
spark.driver.userClassPathFirst              true
spark.executor.userClassPathFirst            true
spark.sql.extensions                         ai.sparkone.extension.overwrite.SparkOneHdfsOverwriteExtensions
spark.sparkone.overwrite.zk.connect          192.168.200.69:2181
spark.sparkone.overwrite.zk.root             /sparkone/overwrite
spark.sparkone.overwrite.workspaceRoot       /public/odep/user
spark.sparkone.overwrite.zk.sessionTimeoutMs 60000
spark.sparkone.overwrite.zk.connectionTimeoutMs 15000

# 所有运行环境共用的 Catalog；敏感值只保留在 Kyuubi 主机
spark.sql.catalog.mysql                      org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
spark.sql.catalog.mysql.url                  jdbc:mysql://mysql-host:3306/?databaseTerm=SCHEMA
spark.sql.catalog.mysql.driver               com.mysql.cj.jdbc.Driver
spark.sql.catalog.mysql.user                 <mysql-user>
spark.sql.catalog.mysql.password             <mysql-password>
spark.sql.catalog.mysql.fetchsize            1000
spark.sql.catalog.doris                      org.apache.doris.spark.catalog.DorisTableCatalog
spark.sql.catalog.doris.doris.fenodes        doris-fe-1:8030,doris-fe-2:8030
spark.sql.catalog.doris.doris.user           <doris-user>
spark.sql.catalog.doris.doris.password       <doris-password>
spark.sql.catalog.doris.doris.query.port     9030
spark.sql.catalog.doris.doris.request.retries 3

# Kyuubi engine 启动日志，以及 Spark UI、YARN 和 event log 中的敏感配置脱敏
kyuubi.server.redaction.regex                 (?i)secret|password|passwd|token|access[._-]?key|spark[.]sql[.]catalog[.].*[.](url|user|doris[.]fenodes)
spark.redaction.regex                         (?i)secret|password|passwd|token|access[._-]?key|spark[.]sql[.]catalog[.].*[.](url|user|doris[.]fenodes)

# Kyuubi 服务和 engine 共享策略
kyuubi.authentication                        NONE
kyuubi.frontend.bind.host                    192.168.202.187
kyuubi.frontend.protocols                    THRIFT_BINARY,REST
kyuubi.frontend.thrift.binary.bind.port      10009
kyuubi.frontend.rest.bind.port               10099
kyuubi.engine.type                           SPARK_SQL
kyuubi.engine.share.level                    USER
kyuubi.engine.doAs.enabled                   false
kyuubi.engine.single.spark.session           false

# 只允许客户端选择管理员定义的 profile，不允许直接改 Spark 或 engine 分组
kyuubi.session.conf.advisor                  org.apache.kyuubi.session.FileSessionConfAdvisor
kyuubi.session.conf.file.reload.interval     PT1M
kyuubi.session.conf.restrict.list            spark.*,kyuubi.engine.share.level,kyuubi.engine.share.level.subdomain,kyuubi.session.conf.restrict.list,kyuubi.session.conf.ignore.list
```

这里故意不设置 `spark.master`、`spark.submit.deployMode`、`spark.driver.host` 和 YARN 资源参数。`spark.jars` 使用 Kyuubi gateway 本机可读的路径，`spark-submit` 会在 YARN 模式下负责上传和分发；JAR 必须与实际 `SPARK_HOME` 的 Spark/Scala 版本匹配。

`kyuubi.server.redaction.regex` 负责 Kyuubi Server 日志中的 engine 启动命令和配置，`spark.redaction.regex` 负责 Spark UI、YARN 与 event log。两者会遮蔽通用密钥和 ODEP Catalog 的连接地址、用户、密码，只影响后续输出，不会清理已有日志，也不会加密 `kyuubi-defaults.conf`；配置文件仍应限制为 Kyuubi 运行账号可读。redaction 也不会遮蔽操作系统进程列表中的 `spark-submit --conf` 参数，MVP 环境必须限制其他账号查看 Kyuubi/Spark 进程；生产化前应进一步改为不经过进程命令行传递 Catalog 密钥。

### 三套运行 profile

Local profile `$KYUUBI_CONF_DIR/kyuubi-session-local.conf`：

```properties
kyuubi.engine.share.level.subdomain          local
kyuubi.session.conf.restrict.list=
kyuubi.session.idle.timeout                  PT6H
kyuubi.session.engine.idle.timeout           PT30M

spark.app.name                               SparkOne-Kyuubi-Local
spark.master                                 local[2]
spark.driver.host                            192.168.202.187
spark.driver.bindAddress                     0.0.0.0
spark.eventLog.enabled                       false
```

YARN client profile `$KYUUBI_CONF_DIR/kyuubi-session-yarn-client.conf`：

```properties
kyuubi.engine.share.level.subdomain          yarn-client
kyuubi.session.conf.restrict.list=
kyuubi.session.idle.timeout                  PT30M
kyuubi.session.engine.idle.timeout           PT30M

spark.app.name                               SparkOne-Kyuubi-YarnClient
spark.master                                 yarn
spark.submit.deployMode                      client
spark.yarn.queue                             test
spark.driver.host                            192.168.202.187
spark.driver.bindAddress                     0.0.0.0
spark.driver.memory                          1g
spark.yarn.am.memory                         512m

spark.dynamicAllocation.enabled              true
spark.dynamicAllocation.shuffleTracking.enabled false
spark.dynamicAllocation.minExecutors         0
spark.dynamicAllocation.initialExecutors     1
spark.dynamicAllocation.maxExecutors         1
spark.dynamicAllocation.executorIdleTimeout  60s
spark.executor.cores                         1
spark.executor.memory                        1g
spark.shuffle.service.enabled                true

spark.eventLog.enabled                       true
spark.eventLog.dir                           hdfs://nameservice1/tmp/spark/applicationHistory
```

YARN cluster profile `$KYUUBI_CONF_DIR/kyuubi-session-yarn-cluster.conf`：

```properties
kyuubi.engine.share.level.subdomain          yarn-cluster
kyuubi.session.conf.restrict.list=
kyuubi.session.idle.timeout                  PT30M
kyuubi.session.engine.idle.timeout           PT30M

spark.app.name                               SparkOne-Kyuubi-YarnCluster
spark.master                                 yarn
spark.submit.deployMode                      cluster
spark.yarn.queue                             test
spark.driver.memory                          1g

spark.dynamicAllocation.enabled              true
spark.dynamicAllocation.shuffleTracking.enabled false
spark.dynamicAllocation.minExecutors         0
spark.dynamicAllocation.initialExecutors     1
spark.dynamicAllocation.maxExecutors         1
spark.dynamicAllocation.executorIdleTimeout  60s
spark.executor.cores                         1
spark.executor.memory                        1g
spark.shuffle.service.enabled                true

spark.eventLog.enabled                       true
spark.eventLog.dir                           hdfs://nameservice1/tmp/spark/applicationHistory
```

当前集群的 YARN NodeManager 已启用与 Spark 3.3.4 匹配的 `spark_shuffle` auxiliary service，因此 profile 使用 external shuffle service，并显式关闭 shuffle tracking。`spark.shuffle.service.enabled=true` 只提供 shuffle 文件托管能力，必须同时开启 dynamic allocation 才会增减 executor。`spark_shuffle` 运行在 NodeManager 进程内，不会出现独立的 `spark_shuffle` 进程。

cluster profile 不设置 `spark.driver.host`；driver 由 YARN ApplicationMaster 所在容器发布地址。client profile 的 driver 位于 Kyuubi gateway，YARN NodeManager 必须能访问配置的地址和端口范围。

三个 `subdomain` 不能相同。固定服务账号、`USER` share level 和不同 subdomain 共同形成三套独立的 engine 池；同一 profile 的多个 SparkOne session 会复用对应 engine。

### SparkOne 入口

```hocon
engines {
  default = "kyuubi_yarn_cluster"

  kyuubi_local {
    type = "kyuubi"
    enabled = true
    label = "Kyuubi Local"
    url = "jdbc:kyuubi://192.168.200.69:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=sparkone-kyuubi?kyuubi.session.conf.profile=local"
  }

  kyuubi_yarn_client {
    type = "kyuubi"
    enabled = true
    label = "YARN Client"
    url = "jdbc:kyuubi://192.168.200.69:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=sparkone-kyuubi?kyuubi.session.conf.profile=yarn-client"
  }

  kyuubi_yarn_cluster {
    type = "kyuubi"
    enabled = true
    label = "YARN Cluster"
    url = "jdbc:kyuubi://192.168.200.69:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=sparkone-kyuubi?kyuubi.session.conf.profile=yarn-cluster"
  }
}
```

### JDBC URL 如何生效

以 YARN cluster 入口为例：

```text
jdbc:kyuubi://192.168.200.69:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=sparkone-kyuubi?kyuubi.session.conf.profile=yarn-cluster
```

这条 URL 不是把所有参数原样交给 Kyuubi Server，而是由 JDBC 客户端和 Kyuubi Server 分段处理：

| URL 片段 | 处理方 | 实际作用 |
| --- | --- | --- |
| `jdbc:kyuubi://` | JDBC 客户端 | 使用 Kyuubi JDBC/HiveServer2 兼容协议 |
| `192.168.200.69:2181` | JDBC 客户端 | 在 service discovery 模式下表示 ZooKeeper 地址，不是 Kyuubi Thrift 端口 |
| `/default` | Kyuubi Server/engine session | 打开 Session 后使用 `default` database |
| `serviceDiscoveryMode=zooKeeper` | JDBC 客户端 | 连接前先从 ZooKeeper 解析 Kyuubi Server |
| `zooKeeperNamespace=sparkone-kyuubi` | JDBC 客户端 | 从 `/sparkone-kyuubi` 读取 Server 注册节点，必须匹配 Server 的 `kyuubi.ha.namespace` |
| `kyuubi.session.conf.profile=yarn-cluster` | Kyuubi Server | 随 `OpenSession` 请求进入 Server，触发加载 `kyuubi-session-yarn-cluster.conf` |

完整链路如下：

1. 每个 Kyuubi Server 根据 `kyuubi.ha.addresses=192.168.200.69:2181` 和 `kyuubi.ha.namespace=sparkone-kyuubi`，把自身可连接的 Thrift 地址注册到 `/sparkone-kyuubi`。
2. SparkOne 新建 JDBC connection 时，Kyuubi JDBC driver 连接 ZooKeeper，从存活节点中随机选择一个 Server，再连接该 Server 的实际 Thrift 地址。
3. JDBC driver 发送 `OpenSession`，其中包含初始 database 和 `kyuubi.session.conf.profile=yarn-cluster`。service discovery 两个参数已经在客户端完成使命，不负责选择 Spark master 或 deploy mode。
4. Kyuubi Server 先校验客户端配置，再由 `FileSessionConfAdvisor` 读取 `$KYUUBI_CONF_DIR/kyuubi-session-yarn-cluster.conf`，并把 profile 覆盖到当前 Session 的 engine 配置上。
5. `kyuubi.engine.share.level=USER`、固定执行账号和 `kyuubi.engine.share.level.subdomain=yarn-cluster` 共同确定 engine space。已存在时复用对应 engine，不存在时按合并后的 `spark.master=yarn`、`spark.submit.deployMode=cluster` 等参数启动新 YARN Application。

所以 ZooKeeper 只负责“选中哪个 Kyuubi Server”，profile 负责“选中哪套 Spark 运行配置”。一个 connection 建立后会固定在选中的 Server 和 backend engine session 上，不会逐条 SQL 重新轮询；Server 故障后需要建立新 connection 才会重新发现。两个 Kyuubi Server 必须使用相同的 `kyuubi.ha.namespace`、`kyuubi-defaults.conf` 和 profile 文件，否则同一条 SparkOne URL 会因随机选中的 Server 不同而产生不一致行为。

SparkOne 连接 Kyuubi 时不负责选择 Spark/YARN/Hive 的执行用户；统一执行身份由 Kyuubi 的 `spark.kerberos.principal` 和 `spark.kerberos.keytab` 决定。当前使用固定服务账号时，三个 SparkOne engine 都不需要配置 JDBC `user/password`。

profile 文件在缓存过期后只影响新连接的 engine 启动配置和 backend session 配置；它不会重配 Kyuubi Server 已初始化的 frontend SessionManager，已经启动的 engine 也不会原地切换 master、deploy mode 或资源。修改 profile 后应停止对应旧 engine，再由 SparkOne 新连接按新配置拉起。

### 生命周期同名参数示例

假设 `kyuubi-defaults.conf` 中启用以下测试配置，并重启两个 Kyuubi Server：

```properties
kyuubi.session.idle.timeout                 PT1M
kyuubi.operation.idle.timeout               PT1M
kyuubi.session.check.interval                PT10S
kyuubi.session.engine.check.interval         PT10S
```

同时 `kyuubi-session-yarn-cluster.conf` 中配置：

```properties
kyuubi.session.idle.timeout                 PT1M
kyuubi.session.engine.idle.timeout          PT1M
```

最终有效值不是把两份文件简单合成一套，而是按 frontend 和 backend 分开：

| 参数 | Kyuubi Server frontend | YARN cluster engine/backend | 关系 |
| --- | --- | --- | --- |
| `kyuubi.operation.idle.timeout` | `PT1M` | `PT1M` | profile 未配置，engine 继承公共值 |
| `kyuubi.session.idle.timeout` | `PT1M` | `PT1M` | frontend 使用 defaults；backend 使用 profile 覆盖值，本例恰好相同 |
| `kyuubi.session.check.interval` | `PT10S` | `PT10S` | profile 未配置，engine 继承公共值；两层各自运行 checker |
| `kyuubi.session.engine.idle.timeout` | 不控制 Server 退出 | `PT1M` | profile 覆盖 Kyuubi 内置默认值 `PT30M` |
| `kyuubi.session.engine.check.interval` | 不会让 Server 自我退出 | `PT10S` | engine 继承公共值，用它检查 engine idle timeout |

如果把 cluster profile 的 `kyuubi.session.idle.timeout` 改为 `PT5M`，结果是 Server frontend 仍按 defaults 的 `PT1M`，新 cluster engine 的 backend session 按 profile 的 `PT5M`。这不是 defaults 与 profile 谁“全局优先”的问题，而是两层 SessionManager 读取了不同配置对象。

`run_isolated` 正常结束会主动 `CloseSession`，不等待 operation/session idle；最后一个 backend session 关闭后，cluster engine 按 `PT1M` idle timeout 加最多约 `PT10S` checker 间隔退出。`tenant_shared` 不主动关闭，frontend 与 backend 分别依赖各自的 operation/session checker；详细时序和保守估算见 [资源缩容与停止语义](resource-lifecycle.md)。

## 交互边界

- SparkOne 使用 Kyuubi 官方推荐的 JDBC driver，当前通过 `192.168.200.69:2181` 和 `sparkone-kyuubi` namespace 动态发现 Kyuubi Server。
- Kyuubi JDBC 协议兼容 HiveServer2，但连接的是 Kyuubi Server，不是把请求转发给 HiveServer2。
- 预览数据来自 JDBC `ResultSet`，和 Kyuubi Spark engine 是 client/cluster、运行在 YARN/Kubernetes/Standalone 无直接绑定。
- Kyuubi 模式下临时视图存在于 JDBC session 对应的远端 Spark engine 中；SparkOne 按逻辑租户复用独立 connection，以支持同一租户 `load ... as t` 后续 preview，同时避免不同租户共享临时视图。
- 逻辑租户不会覆盖 Kyuubi JDBC 的 `user/password/options`；连接仍使用启动配置中的固定服务账号，租户身份只进入 SparkOne 的权限决策上下文。
- `save` 在提交 Kyuubi 前同样生成携带逻辑租户的 `WritePlan` 并执行固定能力矩阵；Hive、Doris、MySQL 和 external path overwrite 永久拒绝。
- Hive、Doris、MySQL Catalog append 会在同一租户 JDBC session 内依次执行目标 `LIMIT 0`、源 `LIMIT 0`，再按目标列顺序生成显式 column list `INSERT` 并执行 `EXPLAIN`。目标不存在、列名集合不一致或类型不兼容时不会提交写语句。
- Catalog append 的最终 SQL 使用 Spark 3.3 已支持的 column list 语法，同一路径兼容 Spark 3.3.x–3.5.x，不会先尝试 3.5 的 `BY NAME` 再回退。
- Kyuubi 查询和只读预检遇到失效连接可以重连一次；携带 `WritePlan` 的 `save` 写语句永不自动重试。写入连接中断时返回“状态未知”，由用户核查目标后决定是否重新提交。
- 文件 `load` 只接受 workspace 相对路径，向 Kyuubi 提交 SparkOne 内部命令，由远端 extension 根据逻辑租户解析最终 HDFS 路径并注册临时视图；原生文件 provider relation 会在提交前拒绝。
- 受控 HDFS overwrite 会向 Kyuubi 提交 SparkOne 内部命令，由 Spark engine extension 在远端 driver 内完成 ZK 排他、staging 写入和 HDFS rename 发布。该 statement 属于写操作，连接异常时同样不会自动重试。

## Session 模式

`POST /api/run` 支持请求级 `sessionMode`，不需要创建或管理显式 session id：

| 值 | Kyuubi JDBC session | 关闭方式 | 跨 Run 临时视图 | 同租户并发 | 用途 |
| --- | --- | --- | --- | --- | --- |
| `tenant_shared` | 按逻辑租户复用一个 connection | Kyuubi timeout 被动回收 | 支持 | 支持 | SQL 编辑器 |
| `run_isolated` | 每次 Run 新建 connection | Run 结束时 SparkOne 主动关闭 | 不支持 | 支持 | 定时任务 |

省略 `sessionMode` 时默认使用 `tenant_shared`。编辑器固定发送该值；定时任务提交方必须显式发送 `run_isolated`，并保证 `view -> select/save` 位于同一次 Run 内。`/api/preview` 只读取租户共享 session，不用于隔离任务。

这里的“长驻 engine”是按需启动、可跨 Run 复用的 YARN Application，不是永久不退出，也不是每个任务启动一个 Application：

- `run_isolated` 在 Run 的 `finally` 中主动执行 `Connection.close()`。Kyuubi 会立即关闭 frontend session 中的 operation 和对应 backend session，正常路径不等待 `kyuubi.operation.idle.timeout` 或 `kyuubi.session.idle.timeout`。对应 engine 没有其他 session 时，才开始等待 `kyuubi.session.engine.idle.timeout=PT30M`；实际退出时间还会受 engine check interval 影响。
- `tenant_shared` 会保留 JDBC connection 以维持临时视图，依赖 Kyuubi checker 被动回收。Server frontend session 中的 `LaunchEngine` 或其他终态 operation 未清理时，“无 operation”时间为 `0`；checker 必须先等待 `kyuubi.operation.idle.timeout` 清理 operation，然后才开始等待 `kyuubi.session.idle.timeout`。
- `kyuubi.operation.idle.timeout` 只清理长期未访问且已经进入终态的 operation handle，不终止运行中的查询。限制运行中 SQL 应使用 `kyuubi.operation.query.timeout`。
- frontend `LaunchEngine` operation 使用 Server 启动配置中的 `kyuubi.operation.idle.timeout`。只在 profile 中设置同名参数仅影响新 engine 的 backend operation；要测试 frontend 被动回收，必须在 `kyuubi-defaults.conf` 同时配置 operation idle、session idle 和 session check interval，并重启 Kyuubi Server。
- YARN engine 内的 backend session 被回收后，engine 才进入 30 分钟 idle 倒计时。Kyuubi 日志中的 `current count` 表示 frontend/backend SessionManager 中的 session 数，不表示仍有多少个 YARN Application。
- Kyuubi Server frontend session 与 engine backend session 是两层。profile 中的 `kyuubi.session.idle.timeout` 只进入新 engine；当前 `kyuubi-defaults.conf` 没有覆写 Server 全局值，因此 frontend session 仍是默认 6 小时。backend session/engine 被回收后，SparkOne 会在下一次只读操作发现失效连接并重连一次，但 session 状态已经丢失，写操作不会自动重试。
- `tenant_shared` 的 YARN backend session idle 和 engine idle 都是 30 分钟，因此 Application 回收通常包含这两段超时；遗留终态 backend operation 时，还可能先叠加 engine 中的 operation idle timeout。`run_isolated` 主动关闭 backend session，不等待第一段 session idle。Local profile 把 backend session idle 设为 6 小时，优先保留编辑器状态。
- engine 被回收后，下一次连接会重新创建同一 profile 的 YARN Application，产生一次冷启动；动态分配只增减该 Application 内的 executor，不会为每个 Run 创建新的 Application。

executor 缩容、engine/Application 退出、Kyuubi Server 停止，以及内嵌/外置 ZooKeeper 故障时的完整时间线见 [资源缩容与停止语义](resource-lifecycle.md)。

共享模式只保证单个 Run 内语句顺序，不提供多个并发 Run 之间的脚本级事务。同租户并发修改同名临时视图、临时 UDF、当前 database 或 Spark SQL 配置时，最后生效的操作取决于实际执行顺序。不同目标写入可以并发；相同受控 HDFS overwrite 目标仍由 Spark engine extension 的 ZK 锁保证只有一个任务进入写入流程。

Kyuubi engine 必须保持：

```properties
kyuubi.engine.share.level=USER
kyuubi.engine.single.spark.session=false
```

`USER` 允许不同 JDBC session 复用 Spark engine/SparkContext；关闭 `single.spark.session` 则保证每个 connection 仍有独立 SparkSession。SparkOne 使用固定 Kyuubi 服务账号时不能开启 `kyuubi.engine.single.spark.session=true`，否则不同逻辑租户及隔离任务可能共享临时视图、SQL 配置和 UDF。

## 受控 HDFS workspace 扩展

构建扩展 JAR：

```bash
./scripts/build.sh sparkone-hdfs-overwrite-extension
```

将 `sparkone-hdfs-overwrite-extension_2.12-0.1.0-SNAPSHOT.jar` 放入 Kyuubi Spark engine driver classpath，并在 engine 侧设置：

```properties
spark.sql.extensions=ai.sparkone.extension.overwrite.SparkOneHdfsOverwriteExtensions
spark.sparkone.overwrite.zk.connect=192.168.200.69:2181
spark.sparkone.overwrite.zk.root=/sparkone/overwrite
spark.sparkone.overwrite.workspaceRoot=/public/sparkone/user
spark.sparkone.overwrite.zk.sessionTimeoutMs=60000
spark.sparkone.overwrite.zk.connectionTimeoutMs=15000
```

如果 engine 已配置 Ranger、Iceberg 等其他 extension，`spark.sql.extensions` 使用逗号拼接，不能覆盖已有值。扩展依赖 Spark/Hadoop 发行包已有的 Curator/ZooKeeper 类，不额外打入一套版本，避免 Kyuubi engine classpath 冲突。

这些参数属于 engine 可信配置，不能放入 SparkOne HOCON 的 Kyuubi JDBC options，也不能由 DSL `LOAD/SAVE OPTIONS` 传入。Spark driver 仍使用 Kyuubi 配置的固定 keytab 账号访问 HDFS；内部命令携带的逻辑租户只用于将相对路径约束到 `/public/sparkone/user/${username}`。`workspaceRoot` 同时用于受控 load 和 overwrite；ZooKeeper 参数只在 overwrite 时使用，纯 load 不加锁。

锁节点格式为 `/sparkone/overwrite/<tenant>/<readable-relative-path>--<qualified-target-sha256>`，末级为 ephemeral node，value 只保存 `operationId` 和完整 qualified target。旧版 extension 使用 `/sparkone/overwrite/<sha256>`；切换节点格式时必须先确认没有 overwrite 正在运行，再统一升级并重启所有 Local 和 Kyuubi Spark Engine，不能让新旧 extension 并行执行。

内部命令只允许由 SparkOne `LOAD/SAVE` 编译链路生成，作为原生 SQL 提交会被 SparkOne 拒绝。MVP 不对内部 payload 增加签名，因此 Kyuubi JDBC 必须保持平台内部服务边界，不能把使用固定 keytab 身份的直连入口开放给终端租户。

## 数据源归属

- 外部 Spark datasource provider jar 应放在 Kyuubi/Spark engine classpath，不放在 SparkOne 主包里。
- `sparkone-hdfs-overwrite-extension` jar 同样属于 Spark engine classpath，并通过 `spark.sql.extensions` 注册；模块名保持兼容，但 extension 同时承载受控 HDFS load 和 overwrite。
- 远端 catalog 使用 `<catalog>.<database>.<table>` 三段式；每个连接实例注册独立 catalog，并使用 `mysql_<instance>`、`doris_<instance>` 命名。`hive` 由 SparkOne 编译为内置 `spark_catalog`，不在 Kyuubi 配置伪造同名 catalog。
- `load mysql` 在 Kyuubi 模式下优先使用 `mysql.\`catalog.db.table\`` 语义，连接信息来自 Kyuubi/Spark engine 的 `spark.sql.catalog.<catalog>.*`。
- 无分片参数时，Kyuubi `load mysql.\`catalog.db.table\`` 编译成远端 catalog SQL。
- 带 `partitionColumn` 或其他受控大表读取参数时，编译成 `USING sparkone_mysql`，由 provider 在 Spark engine 内复用 catalog 连接配置；只写 `partitionColumn` 时会在远端自动查询 `lowerBound/upperBound`，`numPartitions` 默认 `10`，`fetchsize` 默认 `10000`。
- `save append ... as mysql.\`catalog.db.table\`` 复用远端 JDBC Catalog 并走受控 Catalog `WritePlan`；路径必须是三段式，不接受 SQL `OPTIONS`，overwrite 永久拒绝。
- MySQL save 的 URL、用户名、密码和 driver 只存在于 Kyuubi/Spark engine 的 `spark.sql.catalog.<catalog>.*` 配置，不进入 SparkOne SQL。

更完整的数据源语义见 [../data/datasources.md](../data/datasources.md)。

## Kyuubi/Spark UI

- Kyuubi Server Web UI 和 Spark engine UI 是两件事。源码或本地二进制包没有用 `./build/dist --web-ui ...` 打包时，Kyuubi Web UI 可能显示 `The Web UI is currently unavailable`；这不影响 Kyuubi JDBC、REST，也不影响 Spark engine 的 Spark UI。
- Spark UI 属于 Kyuubi 拉起的 Spark engine。local/client 模式常见地址是 `http://127.0.0.1:4040`；如果 engine 跑在 YARN/Kubernetes/cluster 模式，应从 Kyuubi engine 日志、YARN application、Kubernetes driver service 或 Spark tracking URL 进入。
- 验证 `sparkone_mysql` 大表读取参数时，看 Spark UI 的 Jobs/Stages 和 SQL/DataFrame 页更直接。只写 `partitionColumn` 时默认 `numPartitions=10`；`select count(*) from orders_big` 对应的 JDBC scan stage 如果有 `10/10` tasks，并且 `EXPLAIN FORMATTED SELECT count(*) FROM orders_big` 里出现 `Scan JDBCRelation(...) [numPartitions=10]`，即可证明分区参数已进入 Spark JDBC reader。

## 依赖

- SparkOne 主包内置 `org.apache.kyuubi:kyuubi-hive-jdbc`，版本由 Maven property `kyuubi.jdbc.version` 控制；不要使用 shaded JDBC 胖包，避免它内嵌的 SLF4J 1.x 类污染 Spark/Log4j2 日志绑定。
- 外部 Spark datasource provider jar 应放在 Kyuubi/Spark engine classpath，不放在 SparkOne 主包里。

## 相关文档

- 引擎能力差异：[capability-diff.md](capability-diff.md)
- 数据源配置：[../data/datasources.md](../data/datasources.md)
- SQL 编辑器 Kyuubi 测试：[../ui/editor-testing.md](../ui/editor-testing.md#测试-kyuubi-sparkone_mysql-provider)
