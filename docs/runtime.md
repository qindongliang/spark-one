# Runtime

当前 runtime 是本地开发测试台：

```text
Javalin HTTP service
  -> SparkOneCompiler
  -> SparkSession local[*]
  -> result rows
```

入口：

- `ai.sparkone.server.SparkOneServer`

启动：

```bash
sdk env
mvn exec:java -Dexec.mainClass=ai.sparkone.server.SparkOneServer
```

默认地址：

```text
http://127.0.0.1:7070
```

指定端口：

```bash
mvn exec:java -Dexec.mainClass=ai.sparkone.server.SparkOneServer -Dexec.args=7071
```

API：

- `POST /api/compile`
- `POST /api/run`

请求格式：

```json
{
  "script": "select 1 as id;",
  "limit": 200
}
```

当前限制：

- 只适合作为本地测试服务。
- 没有多租户、权限、任务队列、session 池。
- 结果最多限制到 1000 行以内，服务端会 clamp。

后续接 Kyuubi：

- 不要改 compiler。
- 新增 `KyuubiJdbcRuntime`，替换 `SparkOneRuntime` 的执行方式。
- 编译出的 Spark SQL 顺序提交给 Kyuubi。
