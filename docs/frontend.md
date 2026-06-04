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
- `Compile` 调 `/api/compile`。
- `Run` 调 `/api/run`。
- 展示编译后的 SQL。
- 展示执行结果 schema 和 rows。

为什么暂不上 React：

- 当前交互很小，原生 JS 足够。
- 静态资源结构更适合 IDEA 调试。
- 后续引入 CodeMirror 或 React 都方便。

什么时候考虑 React：

- 多 Tab。
- 执行历史。
- 保存脚本。
- 任务轮询。
- 表结构浏览。
- Explain/lineage 可视化。
- Kyuubi session 管理。
