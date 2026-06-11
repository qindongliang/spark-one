# AGENTS.md

默认用中文解释。

这是一个 SQL-first 的 SparkOne MVP。进入项目后先读：

- 架构与边界：[docs/architecture.md](docs/architecture.md)
- 编译器与 ANTLR：[docs/compiler.md](docs/compiler.md)
- 依赖与环境：[docs/dependencies.md](docs/dependencies.md)
- 数据源扩展：[docs/datasources.md](docs/datasources.md)
- HDFS 与 Hive 对接：[docs/hadoop-hive.md](docs/hadoop-hive.md)
- Safe Save 保护：[docs/safe-save.md](docs/safe-save.md)
- 应用启动方法：[docs/startup.md](docs/startup.md)
- SQL 编辑器测试：[docs/editor-testing.md](docs/editor-testing.md)
- 运行与部署：[docs/runtime.md](docs/runtime.md)
- 前端页面：[docs/frontend.md](docs/frontend.md)
- 提交与 PR 规范：[docs/commits.md](docs/commits.md)
- 本地参考仓库：[docs/references.md](docs/references.md)

关键约束：

- 不实现 Spark SQL parser，Spark SQL 语法交给 Spark `SparkSqlParser`。
- ANTLR 仅解析 SparkOne 的薄 DSL，当前覆盖 `load/save/view`。
- 数据源别名和特殊 source 统一放在 `DataSourceResolver`。
- ANTLR 版本必须跟 Spark 3.5.x 对齐为 `4.9.3`。
- 当前 runtime 是本地 `SparkSession local[*]` 测试台，不是多租户生产运行时。
- 后端执行链路捕获异常时，必须先在服务端日志记录失败语句、关键上下文和异常堆栈；前端只展示用户可读错误，不能成为唯一排障入口。
- 前端使用 `src/main/resources/public` 静态资源，不把页面写进 Scala 字符串。

最佳路线：

- 默认把 SparkOne 薄 DSL 编译成 Spark 原生 SQL，再交给 `SparkSession.sql(...)`，未来可平滑切到 Kyuubi。
- Spark SQL 是开放执行协议，优先承载加载、计算、保存等 SQL-first 数据分析链路。
- `view name as select ...` 是注册临时视图的主语法糖；不支持尾部 `select ... as table`，避免跟 Spark 原生别名冲突。
- `save ... as hive` 走 Spark catalog 表写入，优先编译成 `INSERT INTO/OVERWRITE TABLE`，不使用文件目录备份语义。
- MySQL 统一使用 `load/save mysql`，连接信息来自 TOML；不支持 `load/save jdbc`。
- 原生 `DROP TABLE` 默认被危险 DDL 策略拦截，只能通过启动 TOML 显式打开。
- DataFrame API 只作为少数 Spark SQL 难表达能力的 runtime adapter，不作为 MVP 主路径。
- 不复刻 MLSQL 的重运行时；吸收其 SQL 体验，但保持 compiler/runtime 边界轻。

提交与 PR：

- 提交信息默认使用中文类型前缀：`功能：`、`修复：`、`重构：`、`更新：`、`清理：`。
- 每次提交聚焦一个逻辑变更，例如 DSL/compiler、runtime、前端、配置、文档或测试。
- commit 正文按主题分段说明改动原因和影响，不写验证命令；验证信息放最终回复或 PR 描述。
- 详细规范见 [docs/commits.md](docs/commits.md)。
