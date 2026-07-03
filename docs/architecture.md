# Architecture

SparkOne SQL 当前是 MLSQL-inspired 的最小可用版本，核心目标是：

```text
SparkOne DSL + native Spark SQL script
  -> compile
  -> Spark SQL statements
  -> local SparkSession runtime or Kyuubi SQL gateway
```

边界：

- 编译层只负责把少量 DSL 编译成 Spark SQL。
- 普通 SQL 原样透传，由 Spark parser 和 Spark runtime 处理。
- Web 服务默认仍是本地测试台，方便快速 compile/run。
- Kyuubi 作为远程 SQL gateway 接入；YARN、Kubernetes、Standalone 等终态由 Kyuubi engine 侧承接，不放进 SparkOne。

主要代码：

- `src/main/scala/ai/sparkone/sql/SparkOneCompiler.scala`
- `src/main/scala/ai/sparkone/sql/SparkSqlValidator.scala`
- `src/main/scala/ai/sparkone/runtime/SparkOneRuntime.scala`
- `src/main/scala/ai/sparkone/runtime/SparkOneEngine.scala`
- `src/main/scala/ai/sparkone/server/SparkOneServer.scala`

推荐继续演进的方向：

- 保持 compiler 薄。
- 保持 `local` 和 `kyuubi` 两种执行引擎：local 用于开发调试，Kyuubi 用于远程 gateway。
- 不恢复 MLSQL 那种重 session/job/auth/runtime 混合结构。
