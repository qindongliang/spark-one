# Frontend

当前前端是 Javalin 托管的静态资源，不使用 React，也不使用服务端模板。

文件：

- `sparkone-server/src/main/resources/public/index.html`
- `sparkone-server/src/main/resources/public/style.css`
- `sparkone-server/src/main/resources/public/app.js`

服务端挂载：

- `SparkOneServer` 使用 `config.addStaticFiles("/public", Location.CLASSPATH)`。
- `/` 通过 `addSinglePageRoot` 返回 `index.html`。

当前页面能力：

- SQL 编辑框。
- 执行引擎选择：`Local` 用于本地开发调试，`Kyuubi` 用于远程 SQL gateway。
- 页面采用上方编辑器、下方结果区布局。
- SQL 语法高亮和行号，基于 CodeMirror 5 WebJar。
- 编辑器使用 `text/x-sparkone-sql`，基于 CodeMirror Spark SQL mode 追加 `view/load/save/assert/message/options/partitionBy/mysql/doris` 关键词。
- `Compile` 调 `/api/compile`，只有 `server.showCompiledSql = true` 时显示。
- `Run` 调 `/api/run`，请求中带上当前选择的 `engine`。
- 有选区时 `Compile` / `Run` 只提交选中的 SQL；没有选区时提交整篇脚本。
- `Compile` 展示编译后的 SQL。
- `Run` 默认不展示每条 statement 的编译后 SQL；需要展示时配置 `server.showCompiledSql = true`。
- 结果区的 Preview tab 用 `/api/preview` 显式拉取 `load` 后临时视图的预览数据；普通 `Run` 对 `load` 默认 schema-first。
- Kyuubi 模式的 Preview 通过同一个 Kyuubi JDBC session 读取远端临时视图；如果 Kyuubi session 失效，需要重新执行创建临时视图的脚本。
- `Rows` 默认按 `preview.maxRows` 展示 10 行，页面输入会被服务端配置上限 clamp。
- 展示执行结果 schema 和 preview tabs，宽表在结果区横向滚动。
- `assert` 结果展示独立的 `passed/failed/error` 状态；失败时继续用结果表展示受限的违规样本。

为什么暂不上 React：

- 当前交互很小，原生 JS 足够。
- 静态资源结构更适合 IDEA 调试。
- CodeMirror 已通过 Maven/WebJar 引入，不依赖外部 CDN。

什么时候考虑 React：

- 多 Tab。
- 执行历史。
- 保存脚本。
- 任务轮询。
- 表结构浏览。
- Explain/lineage 可视化。
- Kyuubi session 管理。
