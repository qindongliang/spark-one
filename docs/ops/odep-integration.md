# ODEP 第一阶段接入

第一阶段把 ODEP Web 的 SQL V2 文件接入 QueryOne，旧 SQL 文件仍走原来的 MLSQL 链路：

```text
旧链路：RMS -> ODEP Web -> ODEP System -> MLSQL -> Spark
新链路：RMS -> ODEP Web -> ODEP System -> QueryOne -> Kyuubi -> Spark
```

ODEP Web 只负责文件类型、engine 选择和结果轮询；QueryOne 负责编译、会话和执行；ODEP System 负责任务记录、结果持久化和旧页面结果格式适配。

## 配置

### ODEP System

在 ODEP Web 的 Spring 配置中只需增加 QueryOne 服务根地址。URL 为空时 QueryOne 链路不可用，不影响旧链路：

```properties
queryone.backend.url=http://queryone.example.internal:7070
```

ODEP System 到 QueryOne 的签名账号默认复用 ODEP System 已有的：

```properties
rms.oauth.client.api.app-id=...
rms.oauth.client.api.sign=...
```

如果要拆分专用账号，可以覆盖：

```properties
queryone.backend.app-id=odep-system
queryone.backend.sign-key=...
```

`queryone.backend.app-id/sign-key` 只用于 ODEP System -> QueryOne，不要填写 QueryOne/Kyuubi 调 ODEP API 使用的 `app_kyuubi` 密钥。`queryone.backend.url` 应指向 QueryOne HTTP 服务根地址，不要带 `/internal/v1/*`。

QueryOne 请求在独立的 `queryOneExecutor` 中等待结果，不占用旧链路的 `skoneExecutor`。第一阶段使用固定线程池默认值，不暴露调优配置。

### QueryOne

在 `conf/queryone.conf` 中配置与 ODEP System 对应的账号：

```hocon
internalApi {
  auth {
    appId = "odep-system"
    signKey = "..."
    clockSkewSeconds = 300
    nonceTtlSeconds = 600
  }
}
```

`appId` 和 `signKey` 必须分别与 ODEP System 的最终 `queryone.backend.app-id` 和 `queryone.backend.sign-key`（或其默认回退值）一致。两项未配置时，QueryOne 的 `/internal/v1/*` 返回 404；这不会影响 QueryOne 原有的 UI API。

QueryOne 可以注册多个 engine，例如：

```hocon
engines {
  default = "kyuubi_yarn_cluster"
  kyuubi_yarn_cluster {
    type = "kyuubi"
    enabled = true
    url = "jdbc:kyuubi://zk-1:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=queryone-kyuubi?kyuubi.session.conf.profile=yarn-cluster"
    user = "app_kyuubi"
    password = "..."
  }
  kyuubi_yarn_client {
    type = "kyuubi"
    enabled = true
    label = "YARN Client"
    url = "jdbc:kyuubi://zk-1:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=queryone-kyuubi?kyuubi.session.conf.profile=yarn-client"
  }
}
```

ODEP System 通过签名接口动态获取已启用的 Kyuubi engine，不重复配置 engine 列表。ODEP Web 的 SQL V2“引擎”下拉框展示 `label`，提交时以 engine `id` 作为 `runServerName`。`app_kyuubi` 仍然只负责 QueryOne/Kyuubi 调 ODEP API 的身份，不作为 ODEP System 调 QueryOne 的账号。

结果上限只由 QueryOne 的 `preview.maxRows` 控制。若第一阶段需要返回最多 1000 行，应配置：

```hocon
preview {
  maxRows = 1000
}
```

## 接口契约

### ODEP Web

- `GET /webapi/queryone/engines`：代理 QueryOne 返回当前启用的 Kyuubi engine 和默认 engine。
- `POST /webapi/query`：SQL V2 文件必须传 `scriptType=script_v2`，`runServerName` 是选择的 QueryOne engine id。
- 普通 SQL 继续传 `scriptType=script`，仍由 MLSQL 处理。
- SQL V2 对用户只提供异步提交和结果轮询，超时选项为 1、5、10、20 分钟。
- SQL V2 第一阶段不支持表单参数、一次性调度、MLSQL 服务状态和旧 MLSQL 代码补全。

### QueryOne 内部接口

ODEP System 调用：

```text
POST /internal/v1/engines
POST /internal/v1/run
```

`/internal/v1/engines` 返回已启用的 Kyuubi engine 的 `id/label/engineType` 和默认 engine；Local engine 不发布给 ODEP。

请求 body 至少包含：

```json
{
  "requestId": "...",
  "subject": "alice",
  "script": "select 1",
  "engine": "kyuubi_yarn_cluster"
}
```

ODEP 不传 `limit` 和 `sessionMode`；QueryOne 分别使用 `preview.maxRows` 和 `tenant_shared` 默认值。

请求必须带以下签名头：

```text
X-QueryOne-App-Id
X-QueryOne-Timestamp
X-QueryOne-Nonce
X-QueryOne-Body-SHA256
X-QueryOne-Signature
```

签名采用 ODEP 现有 `OpenApiUtil.createSHA1Sign` 的排序和 `appSignKey` 约定，并额外绑定请求路径、requestId、subject、body SHA-256、时间戳和 nonce。QueryOne 会校验时间窗口、body hash 和 nonce 重放；服务进程内鉴权器为单实例。

响应必须返回与请求相同的 `requestId`。ODEP System 会把 `statements[*].schema/rows` 适配为 ODEP 旧结果结构，并从失败 statement 中提取可读错误。

## 鉴权路径迁移

QueryOne 的资源鉴权统一调用：

```text
POST /api/queryone/authz/check
```

旧的 `/api/sparkone/authz/check` 不再作为 QueryOne 链路的目标路径。部署时需要同时升级 ODEP API 和 QueryOne 授权扩展，避免一端仍请求旧路径。

## 联调顺序

1. 先在 QueryOne 开启 `internalApi.auth`，使用 curl 或 ODEP client 验证 `/internal/v1/engines` 和 `/internal/v1/run` 的签名、body hash、时间戳和重复 nonce 校验。
2. 在 ODEP System 配置 `queryone.backend.url`，确认 `/webapi/queryone/engines` 返回已启用的 Kyuubi engine。
3. 在 ODEP Web 新建 `SQL V2 文件`，选择一个 QueryOne engine，执行 `select 1`，观察 `/webapi/polling/run/result` 的进度和结果。
4. 验证 QueryOne 返回失败 statement 时，ODEP 页面显示具体错误；再执行一次旧 SQL 文件，确认仍访问 MLSQL profile。
5. 验证 Redis/数据库中的执行历史、最新结果和审计日志均带有 `executionBackend=QUERYONE`，并确认 QueryOne 任务不会调用 MLSQL progress 或 kill 接口。

## 第一阶段边界

- QueryOne 任务使用 ODEP 现有 requestId、执行历史和结果存储，不新增数据库表。
- QueryOne 任务暂不支持 kill；ODEP 会明确返回 409，避免误调用 MLSQL kill API。
- QueryOne URL 未配置时 engine 列表为空；SQL V2 不能降级到 MLSQL。
- ODEP 用户侧只提供异步任务；第一阶段由 ODEP 保存任务状态，QueryOne `/internal/v1/run` 仍同步返回执行结果，不新增第二套任务中心。
- 第一阶段只支持执行和结果获取，调度、参数化表单、QueryOne 专用补全和更细粒度权限策略留到后续阶段。
