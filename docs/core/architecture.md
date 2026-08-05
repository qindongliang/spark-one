# Architecture

SparkOne SQL 当前是 MLSQL-inspired 的最小可用版本，核心目标是：

```text
SparkOne DSL + native Spark SQL script
  -> compile
  -> Spark SQL statements / versioned internal overwrite command
  -> local SparkSession runtime or Kyuubi SQL gateway
```

边界：

- 编译层只负责把少量 DSL 编译成 Spark SQL。
- 普通 SQL 除 `hive` 到内置 `spark_catalog` 的逻辑别名外原样透传，由 Spark parser 和 Spark runtime 处理。
- Web 服务默认仍是本地测试台，方便快速 compile/run。
- 当前用户名登录只创建开发态逻辑租户上下文，不是生产认证；生产身份后续由 RMS 提供。
- Kyuubi 作为远程 SQL gateway 接入；YARN、Kubernetes、Standalone 等终态由 Kyuubi engine 侧承接，不放进 SparkOne。
- 受控 HDFS load/overwrite 是 Spark SQL 难以安全表达的少数 runtime adapter：薄 compiler 生成内部命令，`sparkone-hdfs-overwrite-extension` 在 Spark driver 内解析 workspace owner 和相对路径；overwrite 的任务状态和文件发布职责不会放进 SparkOne Web 进程。

主要代码：

- `sparkone-server/src/main/scala/ai/sparkone/sql/SparkOneCompiler.scala`
- `sparkone-server/src/main/scala/ai/sparkone/sql/SparkSqlValidator.scala`
- `sparkone-server/src/main/scala/ai/sparkone/runtime/SparkOneRuntime.scala`
- `sparkone-server/src/main/scala/ai/sparkone/runtime/SparkOneEngine.scala`
- `sparkone-server/src/main/scala/ai/sparkone/server/SparkOneServer.scala`
- `sparkone-hdfs-overwrite-extension/src/main/scala/ai/sparkone/extension/overwrite`

推荐继续演进的方向：

- 保持 compiler 薄。
- 保持 `local` 和 `kyuubi` 两种执行引擎：local 用于开发调试，Kyuubi 用于远程 gateway。
- 不恢复 MLSQL 那种重 session/job/auth/runtime 混合结构。
