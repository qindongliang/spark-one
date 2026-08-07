# Engines Overview

QueryOne 的执行层保持轻量：服务端接收 SQL/DSL 脚本，先经过 `QueryOneCompiler`，再把编译后的 Spark SQL 交给当前选择的 engine。

```text
Javalin HTTP service
  -> QueryOneCompiler
  -> selected engine
     -> local SparkSession
     -> Kyuubi JDBC gateway
  -> result rows
```

入口：

- `queryone.server.QueryOneServer`
- 应用启动方法见 [../ops/startup.md](../ops/startup.md)。

## Engine 分类

当前只推荐保留两类执行引擎：

- [Local](local.md)：进程内 `SparkSession`，用于 IDEA、本地调试、MVP 冒烟验证。
- [Kyuubi](kyuubi.md)：通过 Kyuubi JDBC 提交 Spark SQL，作为远程 SQL gateway；YARN、Kubernetes、Standalone 等终态由 Kyuubi engine 侧承接。

Local 和 Kyuubi 的 SQL、ODEP Catalog、RMS 鉴权和数据执行主路径保持对等；仅会话签名/恢复、ZooKeeper 服务发现、`run_isolated` 和生产多租户隔离保留为 Kyuubi 能力。具体边界见 [capability-diff.md](capability-diff.md)。

## API

- `POST /api/compile`
- `POST /api/run`
- `POST /api/preview`

请求格式：

```json
{
  "engine": "local",
  "script": "select 1 as id;",
  "limit": 10
}
```

`engine` 可省略，默认使用 HOCON `engines.default`。

## Preview

- `preview.maxRows`：每条 statement 默认最多预览多少行，默认 `10`。服务端会把请求里的 `limit` clamp 到 `1..preview.maxRows`，页面输入不能放大这个上限。
- `load ... as t` 执行后会注册临时视图 `t`。模板配置默认 `preview.defaultTab = "schema"`，此时 Run 只展示 schema；如果本地配置改成 `preview`，或者用户点击结果区 Preview tab，前端会调用 `/api/preview`，请求体为 `{"table":"t","limit":10}`。
- Kyuubi operation log 中的 `SELECT * FROM \`t\` LIMIT 101` 通常来自 QueryOne 预览请求，不是 `load` 语句本身在全量读取。`101` 是预览行数加 1，用于判断是否截断。

## 当前限制

- QueryOne 仍是 SQL-first MVP；当前只提供开发态逻辑租户和 Kyuubi JDBC session 隔离，不提供生产认证、完整权限系统或任务队列。
- Local 只适合作为开发测试台。
- Kyuubi 侧的执行身份、engine 生命周期、connector classpath、catalog 密钥和集群资源调度都由 Kyuubi/Spark/Hadoop 环境负责。
