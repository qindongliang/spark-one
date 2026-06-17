# Frontend

当前前端是 Javalin 托管的静态资源，不使用 React，也不使用服务端模板。

文件：

- `src/main/resources/public/index.html`
- `src/main/resources/public/style.css`
- `src/main/resources/public/app.js`

服务端挂载：

- `SparkOneServer` 使用 `config.addStaticFiles("/public", Location.CLASSPATH)`。
- `/` 通过 `addSinglePageRoot` 返回 `index.html`。

当前页面能力：

- SQL 编辑框。
- 页面采用上方编辑器、下方结果区布局。
- SQL 语法高亮和行号，基于 CodeMirror 5 WebJar。
- 编辑器使用 `text/x-sparkone-sql`，基于 CodeMirror Spark SQL mode 追加 `view/load/save/options/partitionBy/mysql/doris` 关键词。
- `Compile` 调 `/api/compile`。
- `Run` 调 `/api/run`。
- 有选区时 `Compile` / `Run` 只提交选中的 SQL；没有选区时提交整篇脚本。
- `Compile` 展示编译后的 SQL。
- `Run` 默认不展示每条 statement 的编译后 SQL；需要展示时配置 `server.showCompiledSql = true`。
- 展示执行结果 schema 和 rows，宽表在结果区横向滚动。

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
