# AGENTS.md

默认用中文解释。

这是一个 SQL-first 的 SparkOne MVP。项目目标是以 SparkOne + Kyuubi 打造新一代数据计算平台：SparkOne 保持轻量 SQL/DSL 编译与提交入口，Kyuubi 承接远程 Spark SQL gateway 和多运行环境接入，底层语义尽量贴近 Spark 原生 SQL。

演进原则：

- 借鉴旧平台的 SQL 体验、数据平台入口、作业提交链路和工程经验，但不照搬重运行时。
- 弃用旧实现里笨重、强耦合、运行时职责混杂、与 Spark SQL 原生语义不够适配的部分。
- 新能力优先落在 Spark 原生 SQL、Spark catalog、Kyuubi engine 配置和薄 DSL 编译层，不把 SparkOne 做成新的大而全计算引擎。

本地参考项目：

- `references/kyuubi`：Kyuubi 原生源码，是后续远程 SQL gateway、会话、认证、引擎接入的首要参考。
- `references/spark`：Spark 原生源码，是 Spark SQL 语法、parser、catalog、执行语义的首要参考。
- `references/rms`：旧平台登录与跳转入口；核心链路是从 RMS 进入 ODEP Web 数据平台。
- `references/odep-web`：旧计算平台前端，可参考交互、页面组织、用户工作流和 SQL 编辑体验。
- `references/odep-system`：旧计算平台后端服务，可参考平台服务边界、任务提交链路，以及通过 MLSQL 提交 Spark 计算任务的对接方式。
- `references/mlsql`：旧平台包装 Spark 服务的 SQL 引擎；只吸收有价值的 SQL 使用体验，不复刻其重 session/job/auth/runtime 混合结构。

旧平台链路可理解为：`rms -> odep-web -> odep-system -> mlsql -> spark`，也就是从 RMS 登录跳转到 ODEP Web，再由 ODEP System 访问 MLSQL 提交 Spark 计算任务。SparkOne 的目标不是复制这条链路，而是把其中有价值的数据平台体验收敛到更轻、更贴近 Spark SQL 原生协议的 `sparkone -> kyuubi -> spark` 路线。

进入项目后先读：

- 文档索引：[docs/README.md](docs/README.md)
- 架构与边界：[docs/core/architecture.md](docs/core/architecture.md)
- 编译器与 ANTLR：[docs/core/compiler.md](docs/core/compiler.md)
- 执行引擎概览：[docs/engines/overview.md](docs/engines/overview.md)
- Local 引擎：[docs/engines/local.md](docs/engines/local.md)
- Kyuubi 引擎：[docs/engines/kyuubi.md](docs/engines/kyuubi.md)
- 数据源扩展：[docs/data/datasources.md](docs/data/datasources.md)
- 应用启动方法：[docs/ops/startup.md](docs/ops/startup.md)
- SQL 编辑器测试：[docs/ui/editor-testing.md](docs/ui/editor-testing.md)

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
- MySQL 统一使用 `load/save mysql`，连接信息来自 HOCON；不支持 `load/save jdbc`。
- 原生 `DROP TABLE` 默认被危险 DDL 策略拦截，只能通过启动 HOCON 显式打开。
- DataFrame API 只作为少数 Spark SQL 难表达能力的 runtime adapter，不作为 MVP 主路径。
- 不复刻 MLSQL 的重运行时；吸收其 SQL 体验，但保持 compiler/runtime 边界轻。

提交与 PR：

- 提交信息默认使用中文类型前缀：`功能：`、`修复：`、`重构：`、`更新：`、`清理：`。
- 每次提交聚焦一个逻辑变更，例如 DSL/compiler、runtime、前端、配置、文档或测试。
- commit 正文按主题分段说明改动原因和影响，不写验证命令；验证信息放最终回复或 PR 描述。
- 详细规范见 [docs/ops/commits.md](docs/ops/commits.md)。
