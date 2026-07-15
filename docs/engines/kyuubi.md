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
- Hive、Doris Catalog append 会在同一租户 JDBC session 内依次执行目标 `LIMIT 0`、源 `LIMIT 0`，再按目标列顺序生成显式 column list `INSERT` 并执行 `EXPLAIN`。目标不存在、列名集合不一致或类型不兼容时不会提交写语句。
- Catalog append 的最终 SQL 使用 Spark 3.3 已支持的 column list 语法，同一路径兼容 Spark 3.3.x–3.5.x，不会先尝试 3.5 的 `BY NAME` 再回退。
- Kyuubi 查询和只读预检遇到失效连接可以重连一次；携带 `WritePlan` 的 `save` 写语句永不自动重试。写入连接中断时返回“状态未知”，由用户核查目标后决定是否重新提交。
- 受控 HDFS overwrite 的 staging executor 尚未开放，因此当前不会向 Kyuubi 提交文件 overwrite SQL。后续 executor 应在 SparkOne 核心执行层实现 workspace 解析和 staging 流程，不依赖 Kyuubi 切换 keytab 执行身份。

## 数据源归属

- 外部 Spark datasource provider jar 应放在 Kyuubi/Spark engine classpath，不放在 SparkOne 主包里。
- `load mysql` 在 Kyuubi 模式下优先使用 `mysql.\`catalog.db.table\`` 语义，连接信息来自 Kyuubi/Spark engine 的 `spark.sql.catalog.<catalog>.*`。
- 无分片参数时，Kyuubi `load mysql.\`catalog.db.table\`` 编译成远端 catalog SQL。
- 带 `partitionColumn` 或其他受控大表读取参数时，编译成 `USING sparkone_mysql`，由 provider 在 Spark engine 内复用 catalog 连接配置；只写 `partitionColumn` 时会在远端自动查询 `lowerBound/upperBound`，`numPartitions` 默认 `10`，`fetchsize` 默认 `10000`。
- `save mysql` 仍不支持 Kyuubi adapter。原生 catalog 写入也会被 SparkOne 的原生写保护拦截，当前 Kyuubi 路径不提供 MySQL 写入旁路。

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
