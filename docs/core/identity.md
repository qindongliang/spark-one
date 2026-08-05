# 身份与租户上下文

当前登录页只用于开发测试环境选择逻辑租户，不是生产认证。用户输入用户名后，SparkOne 在服务端创建随机 session，并通过 HttpOnly Cookie 关联后续请求。

约束：

- 用户名只允许字母、数字、`.`、`_`、`-`，必须以字母或数字开头，最长 64 个字符。
- `/api/compile`、`/api/run`、`/api/preview` 必须从服务端 session 取得 `TenantContext`。
- SQL 请求体和 DSL options 不接受 username，不能覆盖当前鉴权 subject。`load options owner="..."`
  只选择要读取的 workspace，跨 owner 仍以当前 subject 走 ODEP/RMS 鉴权；`save options owner` 拒绝。
- Local engine 仍是单 SparkSession 开发测试台，不提供生产多租户隔离。
- Kyuubi engine 为每个逻辑租户维护独立 JDBC session，JDBC user 使用 `TenantContext.username`；Spark/HDFS 物理访问仍使用 Kyuubi 配置的统一 Kerberos principal/keytab。
- Engine 权限扩展只信任 Kyuubi operation 传入并通过 ECDSA 校验的 session user，不接受请求体、SQL 或 Spark 配置覆盖鉴权 subject。

生产环境接入 RMS 时，应由 RMS 登录结果创建同一种 `TenantContext`，替换开发登录入口，不改变编译和执行链路。未经 RMS 认证前，当前用户名登录不能作为安全边界。

## API

- `GET /api/session`：返回当前开发 session。
- `POST /api/login`：请求体为 `{"username":"alice"}`。
- `POST /api/logout`：删除当前 session。

未登录调用编译、执行或预览接口时返回 HTTP `401`。
