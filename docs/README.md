# SparkOne 文档索引

文档按职责分层，而不是按历史新增顺序平铺。阅读时优先从核心边界和引擎选择开始，再进入具体数据源、前端或运维手册。

## Core

- [架构与边界](core/architecture.md)：SparkOne、Kyuubi、Spark 的职责边界。
- [身份与租户上下文](core/identity.md)：开发登录、TenantContext 和 Kyuubi JDBC session 隔离。
- [编译器与 ANTLR](core/compiler.md)：薄 DSL 编译策略和 Spark SQL parser 边界。
- [依赖与环境](core/dependencies.md)：Maven、Spark、ANTLR、provider 依赖原则。

## Engines

- [执行引擎概览](engines/overview.md)：HTTP API、engine 选择、preview 和当前限制。
- [Local engine](engines/local.md)：本地 `SparkSession` 调试台和 Spark submit 风格配置。
- [Kyuubi engine](engines/kyuubi.md)：远程 SQL gateway、session、UI、依赖和配置归属。
- [资源缩容与停止语义](engines/resource-lifecycle.md)：Spark executor、Spark engine、YARN Application、Kyuubi Server 和 ZooKeeper 的生命周期与 Kyuubi 参数总表。
- [Local 与 Kyuubi 能力差异](engines/capability-diff.md)：哪些能力对等、哪些只属于 local 或 Kyuubi。

## Data

- [数据源扩展](data/datasources.md)：`load/save`、Hive、MySQL、Doris、外部 provider。
- [数据质量 Assert](data/assertions.md)：结果表或内联 SELECT 的逐行谓词、失败短路、能力边界和支持图。
- [HDFS 与 Hive 对接](data/hadoop-hive.md)：本地 Hadoop/Hive/Kerberos 配置与排障。
- [写入安全](data/safe-save.md)：固定能力矩阵、`WritePlan`、受控 HDFS workspace 和原生写入旁路保护。

## UI

- [前端页面](ui/frontend.md)：静态前端资源、API 调用和后续页面演进。
- [SQL 编辑器测试](ui/editor-testing.md)：编辑器、DSL、数据源、Kyuubi provider、HDFS/Hive 测试手册。
- [Assert 测试用例](ui/assertion-testing.md)：Local/Kyuubi 的通过、失败、短路与常用数据质量场景。

## Ops

- [应用启动方法](ops/startup.md)：IDEA、Maven、HOCON、Smoke Test。
- [本地参考仓库](ops/references.md)：`references/*` 的定位和阅读入口。
- [提交与 PR 规范](ops/commits.md)：中文提交前缀、正文和 PR 描述规范。
