# Kyuubi Engine

Kyuubi engine 通过 Kyuubi JDBC 提交 Spark SQL，是 SparkOne 面向远程 SQL gateway 的主路径。SparkOne 不直接管理 YARN、Kubernetes、Standalone、Spark engine classpath、catalog 密钥或执行用户；这些都应放在 Kyuubi/Spark/Hadoop 环境里。

## 配置示例

```hocon
engines {
  default = "kyuubi"

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

## 交互边界

- SparkOne 使用 Kyuubi 官方推荐的 JDBC driver，默认 URL 形如 `jdbc:kyuubi://host:10009/default`。
- Kyuubi JDBC 协议兼容 HiveServer2，但连接的是 Kyuubi Server，不是把请求转发给 HiveServer2。
- 预览数据来自 JDBC `ResultSet`，和 Kyuubi Spark engine 是 client/cluster、运行在 YARN/Kubernetes/Standalone 无直接绑定。
- Kyuubi 模式下临时视图存在于 JDBC session 对应的远端 Spark engine 中；SparkOne 按逻辑租户复用独立 connection，以支持同一租户 `load ... as t` 后续 preview，同时避免不同租户共享临时视图。
- 逻辑租户不会覆盖 Kyuubi JDBC 的 `user/password/options`；连接仍使用启动配置中的固定服务账号，租户身份只进入 SparkOne 的权限决策上下文。
- `save` 在提交 Kyuubi 前同样生成携带逻辑租户的 `WritePlan` 并执行固定能力矩阵；Hive、Doris、MySQL 和 external path overwrite 永久拒绝。
- Hive、Doris、MySQL Catalog append 会在同一租户 JDBC session 内依次执行目标 `LIMIT 0`、源 `LIMIT 0`，再按目标列顺序生成显式 column list `INSERT` 并执行 `EXPLAIN`。目标不存在、列名集合不一致或类型不兼容时不会提交写语句。
- Catalog append 的最终 SQL 使用 Spark 3.3 已支持的 column list 语法，同一路径兼容 Spark 3.3.x–3.5.x，不会先尝试 3.5 的 `BY NAME` 再回退。
- Kyuubi 查询和只读预检遇到失效连接可以重连一次；携带 `WritePlan` 的 `save` 写语句永不自动重试。写入连接中断时返回“状态未知”，由用户核查目标后决定是否重新提交。
- 受控 HDFS overwrite 会向 Kyuubi 提交 SparkOne 内部命令，由 Spark engine extension 在远端 driver 内完成 ZK 排他、staging 写入和 HDFS rename 发布。该 statement 属于写操作，连接异常时同样不会自动重试。

## Session 模式

`POST /api/run` 支持请求级 `sessionMode`，不需要创建或管理显式 session id：

| 值 | Kyuubi JDBC session | 跨 Run 临时视图 | 同租户并发 | 用途 |
| --- | --- | --- | --- | --- |
| `tenant_shared` | 按逻辑租户复用一个 connection | 支持 | 支持 | SQL 编辑器 |
| `run_isolated` | 每次 Run 新建并在结束后关闭 connection | 不支持 | 支持 | 定时任务 |

省略 `sessionMode` 时默认使用 `tenant_shared`。编辑器固定发送该值；定时任务提交方必须显式发送 `run_isolated`，并保证 `view -> select/save` 位于同一次 Run 内。`/api/preview` 只读取租户共享 session，不用于隔离任务。

共享模式只保证单个 Run 内语句顺序，不提供多个并发 Run 之间的脚本级事务。同租户并发修改同名临时视图、临时 UDF、当前 database 或 Spark SQL 配置时，最后生效的操作取决于实际执行顺序。不同目标写入可以并发；相同受控 HDFS overwrite 目标仍由 Spark engine extension 的 ZK 锁保证只有一个任务进入写入流程。

Kyuubi engine 必须保持：

```properties
kyuubi.engine.share.level=USER
kyuubi.engine.single.spark.session=false
```

`USER` 允许不同 JDBC session 复用 Spark engine/SparkContext；关闭 `single.spark.session` 则保证每个 connection 仍有独立 SparkSession。SparkOne 使用固定 Kyuubi 服务账号时不能开启 `kyuubi.engine.single.spark.session=true`，否则不同逻辑租户及隔离任务可能共享临时视图、SQL 配置和 UDF。

## 受控 HDFS overwrite 扩展

构建扩展 JAR：

```bash
./scripts/build.sh sparkone-hdfs-overwrite-extension
```

将 `sparkone-hdfs-overwrite-extension_2.12-0.1.0-SNAPSHOT.jar` 放入 Kyuubi Spark engine driver classpath，并在 engine 侧设置：

```properties
spark.sql.extensions=ai.sparkone.extension.overwrite.SparkOneHdfsOverwriteExtensions
spark.sparkone.overwrite.zk.connect=zk-1:2181,zk-2:2181
spark.sparkone.overwrite.zk.root=/sparkone/overwrite
spark.sparkone.overwrite.workspaceRoot=/public/sparkone/user
spark.sparkone.overwrite.zk.sessionTimeoutMs=60000
spark.sparkone.overwrite.zk.connectionTimeoutMs=15000
```

如果 engine 已配置 Ranger、Iceberg 等其他 extension，`spark.sql.extensions` 使用逗号拼接，不能覆盖已有值。扩展依赖 Spark/Hadoop 发行包已有的 Curator/ZooKeeper 类，不额外打入一套版本，避免 Kyuubi engine classpath 冲突。

这些参数属于 engine 可信配置，不能放入 SparkOne HOCON 的 Kyuubi JDBC options，也不能由 DSL `SAVE OPTIONS` 传入。Spark driver 仍使用 Kyuubi 配置的固定 keytab 账号访问 HDFS；内部命令携带的逻辑租户只用于将相对路径约束到 `/public/sparkone/user/${username}`。

锁节点格式为 `/sparkone/overwrite/<tenant>/<readable-relative-path>--<qualified-target-sha256>`，末级为 ephemeral node，value 只保存 `operationId` 和完整 qualified target。旧版 extension 使用 `/sparkone/overwrite/<sha256>`；切换节点格式时必须先确认没有 overwrite 正在运行，再统一升级并重启所有 Local 和 Kyuubi Spark Engine，不能让新旧 extension 并行执行。

内部命令只允许由 SparkOne `SAVE` 编译链路生成，作为原生 SQL 提交会被 SparkOne 拒绝。MVP 不对内部 payload 增加签名，因此 Kyuubi JDBC 必须保持平台内部服务边界，不能把使用固定 keytab 身份的直连入口开放给终端租户。

## 数据源归属

- 外部 Spark datasource provider jar 应放在 Kyuubi/Spark engine classpath，不放在 SparkOne 主包里。
- `sparkone-hdfs-overwrite-extension` jar 同样属于 Spark engine classpath，并通过 `spark.sql.extensions` 注册。
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
