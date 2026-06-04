# Architecture

SparkOne SQL 当前是 MLSQL-inspired 的最小可用版本，核心目标是：

```text
SparkOne DSL + native Spark SQL script
  -> compile
  -> Spark SQL statements
  -> local SparkSession runtime for MVP
```

边界：

- 编译层只负责把少量 DSL 编译成 Spark SQL。
- 普通 SQL 原样透传，由 Spark parser 和 Spark runtime 处理。
- Web 服务只是本地测试台，方便快速 compile/run。
- 后续接 Kyuubi 时，应替换 runtime 层，不要重写 compiler。

主要代码：

- `src/main/scala/ai/sparkone/sql/SparkOneCompiler.scala`
- `src/main/scala/ai/sparkone/sql/SparkSqlValidator.scala`
- `src/main/scala/ai/sparkone/runtime/SparkOneRuntime.scala`
- `src/main/scala/ai/sparkone/server/SparkOneServer.scala`

推荐继续演进的方向：

- 保持 compiler 薄。
- 将 runtime 抽象成 local Spark runtime 和 Kyuubi JDBC runtime 两种实现。
- 不恢复 MLSQL 那种重 session/job/auth/runtime 混合结构。
