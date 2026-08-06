# Kyuubi Engine

Kyuubi engine 通过 Kyuubi JDBC 提交 Spark SQL，是 SparkOne 面向远程 SQL gateway 的主路径。SparkOne 不直接管理 YARN、Kubernetes、Standalone、Spark engine classpath、catalog 密钥或执行用户；这些都应放在 Kyuubi/Spark/Hadoop 环境里。

## 推荐配置拆分

三类配置分别解决“可信公共能力”“Spark 运行环境”和“SparkOne 路由选择”，不要把同一个参数复制到三套 profile：

| 配置层 | 放置内容 | 不应放置 |
| --- | --- | --- |
| `kyuubi-defaults.conf` | Kyuubi 认证与前端、固定 principal/keytab、静态 Catalog、ODEP 路由类、SparkOne extension、公共 JAR、engine 共享策略、profile advisor | `spark.master`、deploy mode、队列、资源、driver 地址 |
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

### ODEP 数据源按需加载

`sparkone-kyuubi-odep-plugin` 是 Spark Engine 侧的路由 Catalog，不是 Kyuubi Server `SessionConfAdvisor`。Engine 启动时只接收固定的 `jdbc`、`doris` Catalog 类名，不携带任何数据源 URL、用户名或密码。Catalog 实例初始化也不访问 ODEP：首次 `SHOW NAMESPACES IN jdbc|doris` 时调用 `POST /api/datasource/index`，以 form 参数传 `type` 获取非敏感 alias 索引；首次访问某个 alias 的表时，再调用 SparkOne/Kyuubi 专用的 `POST /api/datasource/resolve`，以 form 参数传 `type + alias` 获取该数据源连接配置。`/resolve` 在 ODEP 内复用当前环境的 `common-url.rms.api` 和 `pk.name` 解析 PK 占位符；MLSQL 旧客户端使用的 `/detail` 保持不变。

索引按 type、解析配置按 `type + alias` 缓存在当前 Spark Engine JVM，不设置 TTL，也不 reload。ODEP 注册信息变化后只需停止旧 Engine，让后续连接创建新 Engine；不需要为了刷新数据源重启 Kyuubi Server。connection 级 Engine 会随新连接自然生效，共享 Engine 需要人工停止。

构建插件：

```bash
scripts/build.sh sparkone-kyuubi-odep-plugin
```

ODEP API 部署后，可以列出指定类型的数据源，或者在不打印解析配置值的情况下验证指定 alias：

```bash
scripts/tests/odep-datasource-api.sh jdbc
scripts/tests/odep-datasource-api.sh jdbc search_prod
```

把 `sparkone-kyuubi-odep-plugin/target/sparkone-kyuubi-odep-plugin-0.1.0-SNAPSHOT.jar` 加入 `spark.jars`，供 Spark Engine 加载路由 Catalog。Kyuubi Server classpath 不需要这个 JAR。插件依赖的 Spark、Jackson 和 SLF4J 由 Engine 提供，不需要打入插件 JAR。

Spark Engine driver 进程必须能读取：

```bash
export ODEP_API_URL=https://odep-api.example
export ODEP_KYUUBI_APP_ID=app_kyuubi
export ODEP_KYUUBI_SIGN_KEY='<sign-key>'
export ODEP_CONNECT_TIMEOUT_SECONDS=5
export ODEP_REQUEST_TIMEOUT_SECONDS=60
```

`ODEP_API_URL` 是 ODEP API 根地址。`ODEP_API_URL`、`ODEP_KYUUBI_APP_ID`、`ODEP_KYUUBI_SIGN_KEY` 中任意一个缺失时，首次实例化 `jdbc` 或 `doris` Catalog 失败；索引、解析请求失败或响应非法时，当前 SQL 失败，但 Kyuubi Server 和 Engine 进程仍保持运行。插件不记录连接 options、URL、用户或密码。

local 和 YARN client 模式的 Engine driver 通常继承 Kyuubi Server 启动环境，因此可以在 Server 启动环境配置这些变量。YARN cluster 等远端 driver 必须由集群 secret 机制或受控启动环境注入；不要把 sign key 放进 `spark.*` 配置，否则会重新出现在 `spark-submit --conf`、Spark UI 或 event log 中。

`kyuubi-defaults.conf` 只保留文件 advisor，并注册两个固定路由 Catalog：

```properties
kyuubi.session.conf.advisor org.apache.kyuubi.session.FileSessionConfAdvisor
kyuubi.session.conf.restrict.list spark.*,kyuubi.engine.share.level,kyuubi.engine.share.level.subdomain
spark.sql.catalog.jdbc ai.sparkone.kyuubi.odep.catalog.OdepRoutingCatalog
spark.sql.catalog.doris ai.sparkone.kyuubi.odep.catalog.OdepRoutingCatalog
kyuubi.server.redaction.regex (?i)secret|password|passwd|token|access[._-]?key|spark[.]sql[.]catalog[.].*[.](url|user|doris[.]fenodes)
spark.redaction.regex (?i)secret|password|passwd|token|access[._-]?key|spark[.]sql[.]catalog[.].*[.](url|user|doris[.]fenodes)
```

客户端配置仍先经过 Server restrict list，不能覆盖受控 Catalog。ODEP 连接配置由 Engine 内 HTTP 请求取得，不进入 session overlay 或 Engine 启动命令。两条 redaction 正则继续保护静态 Catalog 和误入配置的敏感值。当前映射规则为：

| ODEP type | Spark Catalog 名 | 实现 |
| --- | --- | --- |
| `jdbc` | `jdbc` | alias 路由到独立 `JDBCTableCatalog`；MySQL URL 自动补 `databaseTerm=SCHEMA` |
| `doris` | `doris` | alias 路由到独立 Doris `DorisTableCatalog` |
| `es`、`solr` 和其他类型 | 暂不注册 | Engine 不请求这些类型 |

`jdbc` 和 `doris` 数据源必须有 `physicalNamespace` 才会由 `/index` 发布。alias 是用户 SQL 中的逻辑库名，必须符合 `[A-Za-z_][A-Za-z0-9_]*`；`physicalNamespace` 是底层真实库。例如：

```sql
show namespaces in jdbc;
show tables in jdbc.search_prod;
select * from jdbc.search_prod.orders limit 10;
load jdbc.`search_prod.orders` as orders;

show namespaces in doris;
show tables in doris.recommend_prod;
select * from doris.recommend_prod.r_qa_log limit 10;
load doris.`recommend_prod.r_qa_log` as qa_log;
```

路由 Catalog 将 `jdbc.search_prod.orders` 映射到对应 JDBC 连接的 `<physicalNamespace>.orders`，Doris 同理。`load jdbc` 无 OPTIONS 时走 Catalog；ODEP MySQL alias 带 `partitionColumn/lowerBound/upperBound/numPartitions/fetchsize` 时走 Engine 内 `sparkone_mysql` provider，并复用相同 alias 的连接和真实库。连接和密钥仍只来自 ODEP，SQL 不能覆盖；动态 alias 的 `save jdbc` 不开放。

当前 `kyuubi-defaults.conf` 中的静态数据源使用独立 Catalog 名：

```sql
show namespaces in mysql_static;
show tables in mysql_static.Dworks;
select * from mysql_static.Dworks.cloud_host_info limit 10;
load jdbc.`mysql_static.Dworks.cloud_host_info` as static_mysql_hosts;

show namespaces in doris_static;
show tables in doris_static.dataagent;
select * from doris_static.dataagent.r_qa_log limit 10;
load doris.`doris_static.dataagent.r_qa_log` as static_qa_log;
```

ODEP 模式下，`spark.sql.catalog.jdbc.*` 和 `spark.sql.catalog.doris.*` 两个完整前缀由 ODEP 路由 Catalog 独占，不能再配置同名静态连接参数。路由 Catalog 初始化时发现同名前缀残留的 `url`、`doris.fenodes` 等静态参数会直接报冲突。静态 `mysql_static`、`doris_static` 与 ODEP 的 `jdbc`、`doris` 不重名，可以同时存在。

插件只负责 Catalog 配置和 alias 路由；JDBC driver、Doris connector 等运行依赖仍必须安装在 Spark engine classpath。

该方案只使用 Spark Catalog 扩展点，不修改 Kyuubi 源码，也不需要维护自定义 Kyuubi 构建。部署后可执行一次 `SHOW NAMESPACES` 和测试表查询预热索引与目标 alias，使配置错误在业务流量进入前暴露。

### ODEP Engine 资源鉴权

`sparkone-kyuubi-odep-authz-extension` 对原生绝对 HDFS relation 使用两阶段安全校验。Spark parser 完成纯语法解析后、Analyzer 开始前提取路径并调用一次 `POST /api/sparkone/authz/check`，只有允许后才会进入文件枚举和 schema inference；允许结果以 subject、read 动作和规范化路径绑定到当前 LogicalPlan，Analyzer 完成后只在本地核对最终 relation，路径或 subject 不一致时 fail closed，不再调用 ODEP。其他 Catalog 资源仍在 Analyzer 完成后调用 ODEP。Engine 不直接访问 RMS；Kyuubi 使用 session 签名用户名查询 RMS 资源。Local server 默认装配同一套资源提取和 API 客户端，但使用服务端 `TenantContext.username` 作为 Local subject，不接受 Kyuubi 签名也不构成生产安全边界。当前映射为：

| Spark 资源 | ODEP 请求 |
| --- | --- |
| `jdbc.<alias>.<table>` | `jdbc + alias + table` |
| `doris.<alias>.<table>` | `doris + alias + table` |
| `spark_catalog.<database>.<table>` | `hive + database + table` |
| 跨 owner 的受控 HDFS load、无 Catalog 的绝对 HDFS 文件关系 | `hdfs + 绝对 path + read` |

查询资源使用 `read`，Catalog 写入目标使用 `write`。当前用户自己的 managed HDFS load/overwrite 依据签名 subject 与 workspace owner 直接判定，不调用 ODEP；跨 owner managed load 和原生绝对 HDFS relation 调用 ODEP `read`；跨 owner overwrite 及所有原生文件路径写入直接拒绝。原生路径禁止 glob、百分号编码、authority、路径穿越和重复分隔符。临时视图展开后检查底层资源；同一阶段内的重复资源合并为一次批量请求。允许的原生绝对 HDFS relation 只产生一次 `phase=pre-analysis` 请求；CSV schema inference 展开的子文件以及最终 relation 通过当前计划的授权证明本地校验。证明不跨 SQL、不设 TTL，也不替代 Catalog 的 analysis 鉴权。ODEP 拒绝、超时、响应不完整、会话用户签名无效以及无法识别的外部数据源都会在 Engine 内 fail closed。

构建并部署扩展：

```bash
scripts/build.sh sparkone-kyuubi-odep-authz-extension
```

将 `sparkone-kyuubi-odep-authz-extension-0.1.0-SNAPSHOT.jar` 加入 `spark.jars`，并将扩展类追加到 `spark.sql.extensions`。该扩展与 ODEP Catalog 插件共用前文的五个 `ODEP_*` 环境变量，不做跨 SQL 或 TTL 权限缓存。

可信 subject 使用 Kyuubi 原生 session user 签名。Server 和 Spark Engine 两个开关必须同时打开：

```properties
kyuubi.session.user.sign.enabled            true
spark.kyuubi.session.user.sign.enabled      true
```

Kyuubi Server 为每个 session user 生成 ECDSA 签名，Spark operation 通过 local properties 把用户名、公钥和签名传入当前语句线程。扩展验证签名后才把用户名作为 ODEP `subject`，不接受 SQL、Spark `SET` 或客户端 options 提供的替代用户名。

ODEP API 部署后可先独立验证 RMS 配置：

```bash
scripts/tests/odep-authz-api.sh alice allow \
  '[{"resourceType":"doris","database":"analytics","table":"events","action":"read"}]'
scripts/tests/odep-authz-api.sh alice allow \
  '[{"resourceType":"hdfs","path":"/public/odep/user/alice/data","action":"read"}]'
```

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
spark.jars                                   \
  /opt/sparkone/sparkone-kyuubi-odep-plugin.jar,\
  /opt/sparkone/sparkone-kyuubi-odep-authz-extension.jar,\
  /opt/sparkone/sparkone-hdfs-overwrite-extension.jar,\
  /opt/sparkone/sparkone-mysql-provider.jar,\
  /opt/connectors/spark-doris-connector.jar,\
  /opt/connectors/mysql-connector-j.jar
spark.driver.userClassPathFirst              true
spark.executor.userClassPathFirst            true
spark.sql.extensions                         ai.sparkone.extension.overwrite.SparkOneHdfsOverwriteExtensions,ai.sparkone.kyuubi.odep.authz.SparkOneOdepAuthzExtension
spark.kyuubi.session.user.sign.enabled       true
spark.sparkone.overwrite.zk.connect          192.168.200.69:2181
spark.sparkone.overwrite.zk.root             /sparkone/overwrite
spark.sparkone.overwrite.workspaceRoot       /public/odep/user
spark.sparkone.overwrite.zk.sessionTimeoutMs 60000
spark.sparkone.overwrite.zk.connectionTimeoutMs 15000

# ODEP 路由类是固定启动配置，连接详情由 Engine 首次访问 alias 时获取。
spark.sql.catalog.jdbc                       ai.sparkone.kyuubi.odep.catalog.OdepRoutingCatalog
spark.sql.catalog.doris                      ai.sparkone.kyuubi.odep.catalog.OdepRoutingCatalog
# 不要在 defaults 或 session profile 中给这两个前缀增加静态连接参数。

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
kyuubi.session.user.sign.enabled             true

# 只允许客户端选择管理员定义的 profile，不允许直接改 Spark 或 engine 分组
kyuubi.session.conf.advisor                  org.apache.kyuubi.session.FileSessionConfAdvisor
kyuubi.session.conf.file.reload.interval     PT1M
kyuubi.session.conf.restrict.list            spark.*,kyuubi.engine.share.level,kyuubi.engine.share.level.subdomain,kyuubi.session.conf.restrict.list,kyuubi.session.conf.ignore.list
```

这里故意不设置 `spark.master`、`spark.submit.deployMode`、`spark.driver.host` 和 YARN 资源参数。`spark.jars` 使用 Kyuubi gateway 本机可读的路径，`spark-submit` 会在 YARN 模式下负责上传和分发；JAR 必须与实际 `SPARK_HOME` 的 Spark/Scala 版本匹配。

`kyuubi.server.redaction.regex` 负责 Kyuubi Server 日志中的 engine 启动命令和配置，`spark.redaction.regex` 负责 Spark UI、YARN 与 event log。两者会遮蔽通用密钥和静态 Catalog 的连接地址、用户、密码，只影响后续输出，不会清理已有日志，也不会加密 `kyuubi-defaults.conf`。ODEP 数据源连接配置不再经过 `spark-submit --conf`；ODEP API 凭据由 Engine 环境提供，仍应使用运行账号权限和 secret 机制保护。

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

三个 `subdomain` 不能相同。统一 Kerberos 执行账号、`USER` share level 和不同 subdomain 共同形成三套独立的 engine 池；同一 profile、同一 RMS 用户的多个 SparkOne session 会复用对应 engine，不同用户仍进入不同 USER engine。

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

SparkOne 连接 Kyuubi 时使用页面登录用户名作为 JDBC session user，用于 USER engine 分组和 ODEP 鉴权；Spark/YARN/Hive 的统一物理执行身份仍由 Kyuubi 的 `spark.kerberos.principal` 和 `spark.kerberos.keytab` 决定。三个 SparkOne engine 不配置固定 JDBC `user`，只有 Kyuubi Server 开启客户端认证时才配置统一 `password` 和认证 options。

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
- 逻辑租户会覆盖 Kyuubi JDBC 配置中的 `user`，使用 `TenantContext.username` 建立 session；统一 `password/options` 保持启动配置，Spark/HDFS 物理执行仍使用 Kyuubi 的 Kerberos principal/keytab。
- `save` 在提交 Kyuubi 前同样生成携带逻辑租户的 `WritePlan` 并执行固定能力矩阵；Hive、Doris、MySQL 和 external path overwrite 永久拒绝。
- Hive、Doris、MySQL Catalog append 会在同一租户 JDBC session 内依次执行目标 `LIMIT 0`、源 `LIMIT 0`，再按目标列顺序生成显式 column list `INSERT` 并执行 `EXPLAIN`。目标不存在、列名集合不一致或类型不兼容时不会提交写语句。
- Catalog append 的最终 SQL 使用 Spark 3.3.4 已支持的 column list 语法，不会尝试更高版本的 `BY NAME` 再回退。
- Kyuubi 查询和只读预检遇到失效连接可以重连一次；携带 `WritePlan` 的 `save` 写语句永不自动重试。写入连接中断时返回“状态未知”，由用户核查目标后决定是否重新提交。
- 文件 `load` 只接受 workspace 相对路径，向 Kyuubi 提交 SparkOne 内部命令，由远端 extension 根据 workspace owner 解析最终 HDFS 路径并注册临时视图；跨 owner load 走 ODEP/RMS read 鉴权。原生文件 provider relation 只开放无 authority 的绝对 HDFS/viewfs 路径读取，并走同一鉴权。
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

`USER` 允许同一 RMS 用户的不同 JDBC session 复用 Spark engine/SparkContext，不同用户名仍进入不同 USER engine；关闭 `single.spark.session` 则保证每个 connection 仍有独立 SparkSession。SparkOne 使用页面登录用户名建立 Kyuubi JDBC session，Spark/HDFS 物理访问仍由统一 Kerberos principal 承担。不能开启 `kyuubi.engine.single.spark.session=true`，否则同一用户的并发 connection 及隔离任务可能共享临时视图、SQL 配置和 UDF。

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
spark.sparkone.overwrite.workspaceRoot=/public/odep/user
spark.sparkone.overwrite.zk.sessionTimeoutMs=60000
spark.sparkone.overwrite.zk.connectionTimeoutMs=15000
```

如果 engine 已配置 Ranger、Iceberg 等其他 extension，`spark.sql.extensions` 使用逗号拼接，不能覆盖已有值。扩展依赖 Spark/Hadoop 发行包已有的 Curator/ZooKeeper 类，不额外打入一套版本，避免 Kyuubi engine classpath 冲突。

这些参数属于 engine 可信配置，不能放入 SparkOne HOCON 的 Kyuubi JDBC options，也不能由 DSL `LOAD/SAVE OPTIONS` 传入。Spark driver 仍使用 Kyuubi 配置的固定 keytab 账号访问 HDFS；内部命令携带的 workspace owner 只用于将相对路径约束到 `/public/odep/user/${owner}`。`workspaceRoot` 同时用于受控 load 和 overwrite；ZooKeeper 参数只在 overwrite 时使用，纯 load 不加锁。

锁节点格式为 `/sparkone/overwrite/<tenant>/<readable-relative-path>--<qualified-target-sha256>`，末级为 ephemeral node，value 只保存 `operationId` 和完整 qualified target。旧版 extension 使用 `/sparkone/overwrite/<sha256>`；切换节点格式时必须先确认没有 overwrite 正在运行，再统一升级并重启所有 Local 和 Kyuubi Spark Engine，不能让新旧 extension 并行执行。

内部命令只允许由 SparkOne `LOAD/SAVE` 编译链路生成，作为原生 SQL 提交会被 SparkOne 拒绝。MVP 不对内部 payload 增加签名，因此 Kyuubi JDBC 必须保持平台内部服务边界，不能把使用固定 keytab 身份的直连入口开放给终端租户。

## 数据源归属

- 外部 Spark datasource provider jar 应放在 Kyuubi/Spark engine classpath，不放在 SparkOne 主包里。
- `sparkone-hdfs-overwrite-extension` jar 同样属于 Spark engine classpath，并通过 `spark.sql.extensions` 注册；模块名保持兼容，但 extension 同时承载受控 HDFS load 和 overwrite。
- `sparkone-kyuubi-odep-authz-extension` jar 属于 Spark engine classpath，通过 `spark.sql.extensions` 注册；它在 Engine 内校验 workspace ownership、拒绝原生文件写入，并调用 ODEP 权限接口，不进入 Kyuubi Server classpath，也不直接访问 RMS。
- 远端 Catalog 统一使用三段式：Hive 为 `hive.<database>.<table>`；ODEP 为 `jdbc.<alias>.<table>`、`doris.<alias>.<table>`；当前静态数据源为 `mysql_static.<database>.<table>`、`doris_static.<database>.<table>`。`hive` 由 SparkOne 编译为内置 `spark_catalog`，不在 Kyuubi 配置伪造同名 Catalog。
- `load jdbc.\`catalog_static.db.table\`` 复用 Kyuubi/Spark engine 的 `spark.sql.catalog.<catalog>.*`；无分片参数时编译成远端 catalog SQL。
- 带 `partitionColumn` 或其他受控大表读取参数时，编译成 `USING sparkone_mysql`，由 provider 在 Spark engine 内复用 catalog 连接配置；只写 `partitionColumn` 时会在远端自动查询 `lowerBound/upperBound`，`numPartitions` 默认 `10`，`fetchsize` 默认 `10000`。
- `save append ... as jdbc.\`catalog_static.db.table\`` 复用远端 JDBC Catalog 并走受控 Catalog `WritePlan`；路径必须是三段式，不接受 SQL `OPTIONS`，overwrite 永久拒绝。
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
- SQL 编辑器 Catalog/远程 Engine 测试：[../ui/editor-testing/kyuubi.md](../ui/editor-testing/kyuubi.md#测试-sparkone_mysql-providerlocalkyuubi)
