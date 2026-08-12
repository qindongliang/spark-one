# 身份与租户上下文

当前登录页只用于本地开发环境选择逻辑租户，不是生产认证。用户输入用户名后，QueryOne 在服务端创建随机 session，并通过 HttpOnly Cookie 关联后续请求。

开发登录和页面 API 由 `server.developmentAccessEnabled` 控制，缺省为 `false`。仅本地开发配置应设为 `true`；测试和生产环境必须设为 `false`。关闭后，页面静态资源以及 `/api/config`、`/api/session`、`/api/login`、`/api/logout`、`/api/compile`、`/api/run`、`/api/preview` 均不注册并返回 `404`，不影响 ODEP 使用的 `/internal/v1/*` 签名接口。

约束：

- 用户名只允许字母、数字、`.`、`_`、`-`，必须以字母或数字开头，最长 64 个字符。
- `/api/compile`、`/api/run`、`/api/preview` 必须从服务端 session 取得 `TenantContext`。
- SQL 请求体和 DSL options 不接受 username，不能覆盖当前鉴权 subject。`load options owner="..."`
  只选择要读取的 workspace，跨 owner 仍以当前 subject 走 ODEP/RMS 鉴权；`save options owner` 拒绝。
- Local engine 仍是单 SparkSession 开发测试台，不提供生产多租户隔离。Run 和 Preview 会把服务端 `TenantContext.username` 放入语句线程的 Local subject 作用域，供 ODEP 鉴权扩展使用，请求结束后立即恢复。
- Kyuubi engine 为每个逻辑租户维护独立 JDBC session，JDBC user 使用 `TenantContext.username`；Spark/HDFS 物理访问仍使用 Kyuubi 配置的统一 Kerberos principal/keytab。
- Kyuubi Engine 权限扩展只信任 operation 传入并通过 ECDSA 校验的 session user；Local 专用扩展只读取 QueryOne runtime 设置的 Local subject。两个入口不互相回退，也不接受请求体、SQL 或 Spark 配置覆盖鉴权 subject。

生产环境接入 RMS 时，应由 RMS 登录结果创建同一种 `TenantContext`，替换开发登录入口，不改变编译和执行链路。未经 RMS 认证前，当前用户名登录不能作为安全边界。

## API

- `GET /api/session`：返回当前开发 session。
- `POST /api/login`：请求体为 `{"username":"alice"}`。
- `POST /api/logout`：删除当前 session。

未登录调用编译、执行或预览接口时返回 HTTP `401`。
