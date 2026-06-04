# AGENTS.md

默认用中文解释。

这是一个 SQL-first 的 SparkOne MVP。进入项目后先读：

- 架构与边界：[docs/architecture.md](docs/architecture.md)
- 编译器与 ANTLR：[docs/compiler.md](docs/compiler.md)
- 依赖与环境：[docs/dependencies.md](docs/dependencies.md)
- 数据源扩展：[docs/datasources.md](docs/datasources.md)
- 运行与部署：[docs/runtime.md](docs/runtime.md)
- 前端页面：[docs/frontend.md](docs/frontend.md)
- 提交与 PR 规范：[docs/commits.md](docs/commits.md)
- 本地参考仓库：[docs/references.md](docs/references.md)

关键约束：

- 不实现 Spark SQL parser，Spark SQL 语法交给 Spark `SparkSqlParser`。
- ANTLR 仅解析 SparkOne 的薄 DSL，当前只覆盖 `load/save`。
- 数据源别名和特殊 source 统一放在 `DataSourceResolver`。
- ANTLR 版本必须跟 Spark 3.5.x 对齐为 `4.9.3`。
- 当前 runtime 是本地 `SparkSession local[*]` 测试台，不是多租户生产运行时。
- 前端使用 `src/main/resources/public` 静态资源，不把页面写进 Scala 字符串。

最佳路线：

- 默认把 SparkOne 薄 DSL 编译成 Spark 原生 SQL，再交给 `SparkSession.sql(...)`，未来可平滑切到 Kyuubi。
- Spark SQL 是开放执行协议，优先承载加载、计算、保存等 SQL-first 数据分析链路。
- DataFrame API 只作为少数 Spark SQL 难表达能力的 runtime adapter，不作为 MVP 主路径。
- 不复刻 MLSQL 的重运行时；吸收其 SQL 体验，但保持 compiler/runtime 边界轻。
