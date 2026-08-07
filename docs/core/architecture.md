# Architecture

QueryOne SQL 当前是 MLSQL-inspired 的最小可用版本，核心目标是：

```text
QueryOne DSL + native Spark SQL script
  -> compile
  -> Spark SQL statements / versioned internal overwrite command
  -> local SparkSession runtime or Kyuubi SQL gateway
```

边界：

- 编译层只负责把少量 DSL 编译成 Spark SQL。
- 普通 SQL 除 `hive` 到内置 `spark_catalog` 的逻辑别名外原样透传，由 Spark parser 和 Spark runtime 处理。
- Web 服务默认仍是本地测试台，方便快速 compile/run。
- 当前用户名登录只创建开发态逻辑租户上下文，不是生产认证；生产身份后续由 RMS 提供。
- Kyuubi 作为远程 SQL gateway 接入；YARN、Kubernetes、Standalone 等终态由 Kyuubi engine 侧承接，不放进 QueryOne。
- 受控 HDFS load/overwrite 是 Spark SQL 难以安全表达的少数 runtime adapter：薄 compiler 生成内部命令，`queryone-hdfs-overwrite-extension` 在 Spark driver 内解析 workspace owner 和相对路径；overwrite 的任务状态和文件发布职责不会放进 QueryOne Web 进程。
- ODEP JDBC/Doris Catalog 与 RMS-backed 鉴权在 Local、Kyuubi 两条 Spark 执行路径共用：Local 默认装配路由 Catalog、MySQL provider 和 Local subject 扩展，Kyuubi 由 Spark Engine 配置装配并使用签名 session subject。ODEP 连接只在首次资源访问时懒解析。

主要代码：

- `queryone-server/src/main/scala/ai/queryone/sql/QueryOneCompiler.scala`
- `queryone-server/src/main/scala/ai/queryone/sql/SparkSqlValidator.scala`
- `queryone-server/src/main/scala/ai/queryone/runtime/QueryOneRuntime.scala`
- `queryone-server/src/main/scala/ai/queryone/runtime/QueryOneEngine.scala`
- `queryone-server/src/main/scala/ai/queryone/server/QueryOneServer.scala`
- `queryone-hdfs-overwrite-extension/src/main/scala/ai/queryone/extension/overwrite`

推荐继续演进的方向：

- 保持 compiler 薄。
- 保持 `local` 和 `kyuubi` 两种执行引擎：local 用于开发调试，Kyuubi 用于远程 gateway。
- 不恢复 MLSQL 那种重 session/job/auth/runtime 混合结构。
