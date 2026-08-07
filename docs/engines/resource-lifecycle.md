# QueryOne / Kyuubi 资源缩容与停止语义

本文按当前 QueryOne 测试环境说明各组件的生命周期：Spark 3.3.4 运行在 YARN，NodeManager 已启用匹配版本的 `spark_shuffle`，Kyuubi profile 使用 `USER` share level。这里的“缩容”包含两类完全不同的动作：

- Spark dynamic allocation 增减同一个 Application 内的 executor。
- Kyuubi engine 空闲回收会结束整个 Spark engine 和 YARN Application。

Kyuubi Server 和 ZooKeeper 不参与 executor 数量计算；ZooKeeper 负责服务发现或写入锁，不是 YARN Application 的进程托管器。

## 生命周期总览

| 组件 | 回收单位 | 主要触发条件 | 回收结果 |
| --- | --- | --- | --- |
| Spark executor | 单个 executor container | Dynamic allocation 判断 executor 空闲 | executor 减少，driver 和 YARN Application 继续运行 |
| Spark SQL engine | 整个 Spark engine | 没有活动 session，且超过 engine idle timeout | SparkContext 停止，YARN Application 结束 |
| Kyuubi Server | 整个 gateway 进程 | 人工/进程管理器停止，或服务发现 ZK 长时间 `LOST` | 不再提供 JDBC/REST 服务 |
| Kyuubi 服务发现 ZK | 注册与协调服务 | 内嵌 ZK 随 Server 退出；外置 ZK 由独立集群管理 | 影响 Server/engine 注册、发现和故障处理 |
| QueryOne overwrite ZK | 单目标写入锁 | 外置 ZK 不可连接或 session 丢失 | 影响受控 HDFS overwrite，不直接回收 executor 或 engine |

## Kyuubi 生命周期参数总表

以下默认值以当前参考源码 Kyuubi 1.9.4 为准，“当前值”指本文档定义的 `kyuubi-defaults.conf` 和 QueryOne 三套 profile；没有显式覆写的参数继承 Kyuubi 默认值。`kyuubi-defaults.conf` 控制 Kyuubi Server 自身并提供 engine 公共启动配置，profile overlay 会并入所选连接的 engine 启动配置和 backend session 配置；它不会重配 Kyuubi Server 已初始化的 SessionManager，也不会改变已经运行的 engine。

### Session 与 Operation

| 参数 | Kyuubi 默认值 | QueryOne 当前值 | 生命周期作用 |
| --- | --- | --- | --- |
| `kyuubi.session.idle.timeout` | `PT6H` | Server frontend `PT6H`；Local backend `PT6H`；两个 YARN backend `PT30M` | Session 同时满足“超过最后访问时间”和“没有 operation”的空闲时间后，才具备关闭条件 |
| `kyuubi.session.check.interval` | `PT5M` | 继承默认值 | SessionManager 每 5 分钟检查 session timeout，并在同一轮检查中清理过期 operation |
| `kyuubi.session.close.on.disconnect` | `true` | 继承默认值 | 客户端连接断开时立即关闭其 Kyuubi session；设为 `false` 时 session 可以在连接断开后继续存活到 idle timeout |
| `kyuubi.batch.session.idle.timeout` | 回退到 `kyuubi.session.idle.timeout`，即默认 `PT6H` | 继承默认值，当前 QueryOne 不使用 batch API | 只控制 Kyuubi batch session，不控制 JDBC session |
| `kyuubi.operation.idle.timeout` | `PT3H` | 继承默认值 | 已进入终态且长时间未被访问的 operation handle 才会被清理；运行中的 operation 不受该参数关闭 |
| `kyuubi.operation.query.timeout` | 未设置 | 未设置 | 可限制单条查询运行时间；查询超时不等于 session 或 engine 立即退出 |
| `kyuubi.operation.interrupt.on.cancel` | `true` | 继承默认值 | query timeout 或客户端 cancel 后是否中断 Spark task；只影响任务结束速度 |

Kyuubi JDBC 链路包含两层 session：

| 层级 | 所在进程 | 配置来源 | 当前空闲超时 |
| --- | --- | --- | --- |
| Frontend session | Kyuubi Server | Server 启动时读取的 `kyuubi-defaults.conf` | `PT6H` |
| Backend session | Spark SQL engine | engine 启动配置和选中的 profile | Local `PT6H`；YARN `PT30M` |

profile 中的 `kyuubi.session.idle.timeout=PT30M` 控制 YARN engine 内的 backend session，不会把 Kyuubi Server frontend session 改成 30 分钟。backend session 超时后，engine 可以继续进入 30 分钟 idle 回收，而 QueryOne 缓存的 frontend JDBC connection 可能仍存在但已经失效。下一次只读操作会按当前实现重连一次并创建新 session；写操作不会自动重试，临时视图和其他 session 状态也不会恢复。

任一层的 session timeout 都不是从 SQL 执行结束后精确计时。检查条件同时使用 session 的最后访问时间和“无 operation”时间，因此正常关闭 `Statement`/`ResultSet` 很重要。未关闭的已完成 operation 最长可能先等待 `kyuubi.operation.idle.timeout` 被清理，然后才重新开始 session 的无 operation 空闲计时。

### 主动关闭与被动回收

QueryOne 的两种 Session 模式不是只在“是否复用 connection”上不同，关闭路径也不同：

| Session 模式 | 关闭方式 | `kyuubi.operation.idle.timeout` | `kyuubi.session.idle.timeout` | `kyuubi.session.engine.idle.timeout` |
| --- | --- | --- | --- | --- |
| `run_isolated` | Run 的 `finally` 主动执行 `Connection.close()` | 正常关闭路径不等待 | 正常关闭路径不等待 | 最后一个 backend session 关闭后开始等待 |
| `tenant_shared` | QueryOne 保留 connection，由 Kyuubi checker 被动回收 | frontend/backend 中有遗留终态 operation 时可能先等待 | operation 全部清理后再等待 | engine 中最后一个 backend session 关闭后开始等待 |

`Connection.close()` 会向 Kyuubi Server 发送 `CloseSession`。Kyuubi 随即关闭该 frontend session 中的 operation 和对应的 engine backend session，所以 `run_isolated` 正常结束时，`kyuubi.operation.idle.timeout` 和 `kyuubi.session.idle.timeout` 不参与这次 Session 关闭。只有主动关闭 RPC 失败，或者异常断连没有被及时识别时，Session 才可能退回依赖断连检测或 timeout 的被动回收路径。

`tenant_shared` 为了保留临时视图而不主动关闭 connection。Kyuubi 创建 engine 时，Server frontend session 内会留下一个已经完成的 `LaunchEngine` operation；它和普通查询 operation 一样属于 operation handle。只要 operation handle 还在，Session 的“无 operation”时间就是 `0`，`kyuubi.session.idle.timeout` 就算已经超过也不能关闭该 Session。checker 会先按 `kyuubi.operation.idle.timeout` 清理终态 operation，Session 变成无 operation 后才开始计算 session idle。因此，被动回收的保守估算是：

```text
Frontend Session 实际关闭时间
  ≈ 最后一次访问
  + kyuubi.operation.idle.timeout
  + 最多一个 kyuubi.session.check.interval
  + kyuubi.session.idle.timeout
  + 最多一个 kyuubi.session.check.interval
```

这是上界估算，不表示每次都要完整等待两段时间：客户端已经关闭 operation handle 时，可以直接从 session idle 开始计算。`kyuubi.operation.idle.timeout` 只清理已经进入 `FINISHED`、`CANCELED`、`ERROR` 等终态且长时间未访问的 operation，不会终止仍在运行的 SQL；限制运行中查询应使用 `kyuubi.operation.query.timeout`，并保持 `kyuubi.operation.interrupt.on.cancel=true`。

参数作用域与 Session 层级一致：Server frontend `LaunchEngine` operation 读取 Kyuubi Server 启动时的 `kyuubi-defaults.conf`；profile 中的同名配置只进入新 engine，影响 engine backend operation。测试 frontend 被动回收时，应把 `kyuubi.operation.idle.timeout`、`kyuubi.session.idle.timeout` 和 `kyuubi.session.check.interval` 一起写入 `kyuubi-defaults.conf` 并重启 Server，不能只修改 YARN profile。

Kyuubi 日志中的 `Checking sessions timeout, current count: 1` 统计的是该 SessionManager 中仍存在的 Session，不是 YARN Application 数。YARN Application 已经退出，只能说明 engine/backend 生命周期结束；Server frontend Session 仍可能因为 `LaunchEngine` operation 或自己的 idle timeout 尚未清理而继续显示。

在没有遗留 operation 的正常路径上，可以用以下上界估算：

```text
Session 实际关闭时间
  ≈ 最后访问/最后一个 operation 关闭时间
  + kyuubi.session.idle.timeout
  + 最多一个 kyuubi.session.check.interval
```

因此 YARN engine 的 backend session 空闲 30 分钟后，不是精确在第 30 分钟关闭，而是通常在 30 至 35 分钟之间被检查到。Server frontend session 使用当前全局 `PT6H`，通常在 6 小时至 6 小时 5 分钟之间被检查到。调小检查周期可以提高回收时间精度，但会增加 Server 和各 engine 的定时扫描频率。

### Engine 范围与回收

| 参数 | Kyuubi 默认值 | QueryOne 当前值 | 生命周期作用 |
| --- | --- | --- | --- |
| `kyuubi.engine.share.level` | `USER` | `USER` | 决定哪些 session 共享一个 engine；共享范围越大，最后一个 session 释放的时间通常越晚 |
| `kyuubi.engine.share.level.subdomain` | 未设置 | `local`、`yarn-client`、`yarn-cluster` | 在 `USER` 范围内继续拆分 engine 池，使三套 profile 各自拥有独立 engine 生命周期 |
| `kyuubi.session.engine.idle.timeout` | `PT30M` | 三套 profile 均为 `PT30M` | engine 没有活动 session 且空闲超过该值后自我终止；`PT0S` 或负值表示禁用空闲退出 |
| `kyuubi.session.engine.check.interval` | `PT1M` | 继承默认值 | 每 1 分钟检查 engine idle timeout，也用于检查 Spark engine max lifetime |
| `kyuubi.session.engine.spark.max.lifetime` | `PT0S` | 继承默认值，即禁用 | 为 engine 设置与是否空闲无关的最大寿命；到期先从服务发现中摘除，待 session 释放后退出 |
| `kyuubi.session.engine.spark.max.lifetime.gracefulPeriod` | `PT0S` | 继承默认值，即无限等待 | max lifetime 到期后的排空宽限期；正值到期后只会强制关闭没有任何 operation handle 的 session，不会强杀仍带 operation 的 session |
| `kyuubi.session.engine.spark.max.initial.wait` | `PT1M` | 继承默认值，但当前不适用 | 仅 `CONNECTION` share level 生效；engine 启动后一直没有首个连接时自我终止，当前 `USER` share level 不使用该逻辑 |

没有设置 max lifetime 时，正常 engine 回收的估算为：

```text
Engine / YARN Application 实际退出时间
  ≈ 最后一个 session 释放时间
  + kyuubi.session.engine.idle.timeout
  + 最多一个 kyuubi.session.engine.check.interval
```

`max.lifetime` 是“防止 engine 永久长驻”的硬年龄门槛，但不是强制作业超时。到达最大寿命后，Kyuubi 先禁止新 session 发现该 engine；只要仍有带 operation 的 session，engine 仍可能超过 `max.lifetime + gracefulPeriod` 运行。因此单条任务的强制运行上限应使用 `kyuubi.operation.query.timeout` 或 Spark 作业侧超时，不能只依赖 engine max lifetime。

### Engine 启动、连接与探活

| 参数 | Kyuubi 默认值 | QueryOne 当前值 | 生命周期作用 |
| --- | --- | --- | --- |
| `kyuubi.session.engine.initialize.timeout` | `PT3M` | 继承默认值 | 等待 engine 启动并注册的整体上限；超时后 Server 会清理启动进程，并尝试终止已提交的 YARN Application |
| `kyuubi.server.limit.engine.startup` | 未设置 | 未设置 | 限制单个 Kyuubi Server 并发启动 engine 的数量；等待启动许可同样受 `initialize.timeout` 限制 |
| `kyuubi.session.engine.launch.async` | `true` | 继承默认值 | 是否异步启动 engine；改变客户端何时拿到 session 响应，不改变 engine idle TTL |
| `kyuubi.session.engine.login.timeout` | `PT15S` | 继承默认值 | Server 建立到已发现 engine 的 Thrift 连接超时 |
| `kyuubi.session.engine.open.max.attempts` | `9` | 继承默认值 | 打开 engine 遇到特定失败时的最大重试次数 |
| `kyuubi.session.engine.open.retry.wait` | `PT10S` | 继承默认值 | 两次打开 engine 尝试之间的等待时间 |
| `kyuubi.session.engine.open.onFailure` | `RETRY` | 继承默认值 | 打开失败时仅重试，或立即/重试后摘除旧 engine 注册 |
| `kyuubi.engine.yarn.submit.timeout` | `PT30S` | 继承默认值 | `spark-submit` 返回后 YARN Application 暂时不可见的容忍窗口，不是 engine 总启动超时 |
| `kyuubi.session.engine.startup.waitCompletion` | `true` | 继承默认值 | 是否保留并等待 engine 启动进程；只有 driver 不在本机的 yarn-cluster 等模式才适合设为 `false` |
| `kyuubi.session.engine.startup.destroy.timeout` | `PT5S` | 继承默认值 | `waitCompletion=false` 时，优雅销毁启动进程的等待时间，超时后强制销毁该进程 |
| `kyuubi.session.engine.alive.probe.enabled` | `false` | 继承默认值 | 开启后由 Server 建立伴随连接，周期性探测 engine |
| `kyuubi.session.engine.alive.probe.interval` | `PT10S` | 继承默认值 | alive probe 发送周期；关闭探活时不生效 |
| `kyuubi.session.engine.alive.timeout` | `PT2M` | 继承默认值 | 最近一个窗口内没有成功 probe 时，将 engine 标记为不可用并关闭对应 Server session，间接促使 engine 进入 idle |
| `kyuubi.session.engine.alive.max.failures` | `3` | 继承默认值 | Kyuubi 1.9.4 中只定义了该配置但探活实现没有读取它，当前版本不要把它当作生效的失败阈值 |

`initialize.timeout` 是冷启动失败保护，`yarn.submit.timeout` 是 YARN 可见性等待，`alive.timeout` 是运行期探活窗口，三者作用阶段不同。当前探活默认关闭，不能依赖它自动识别“进程仍在但已无法响应”的 engine；是否开启应结合网络抖动和误判风险单独压测。

以下配置不产生自动 TTL，但决定人工停止和启动失败清理是否能真正结束 YARN Application：

| 参数 | Kyuubi 默认值 | QueryOne 当前值 | 生命周期作用 |
| --- | --- | --- | --- |
| `kyuubi.engine.ui.stop.enabled` | `true` | 继承默认值 | 是否允许从 Spark engine Web UI 停止整个 engine |
| `kyuubi.yarn.user.strategy` | `NONE` | 继承默认值 | Kyuubi 构造 YARN client 查询/终止 Application 时使用 Server 用户、管理员用户或 Application owner |
| `kyuubi.yarn.user.admin` | `yarn` | 继承默认值，`ADMIN` 策略时才生效 | `kyuubi.yarn.user.strategy=ADMIN` 时使用的 YARN 管理账号 |

若 `kyuubi.yarn.user.strategy` 对应账号没有目标 Application 的管理权限，`initialize.timeout` 到期后“尝试 kill”可能失败并留下孤立 Application，应同时检查 Kyuubi 日志和 YARN 状态。

### 异常摘除

| 参数 | Kyuubi 默认值 | QueryOne 当前值 | 生命周期作用 |
| --- | --- | --- | --- |
| `kyuubi.engine.deregister.exception.classes` | 空集合 | 继承默认值 | 指定需要计数并触发 engine 摘除的异常类；为空时不按异常类摘除 |
| `kyuubi.engine.deregister.exception.messages` | 空集合 | 继承默认值 | 指定需要计数的异常消息或堆栈模式；为空时不按消息摘除 |
| `kyuubi.engine.deregister.job.max.failures` | `4` | 继承默认值 | TTL 窗口内匹配异常达到该次数后摘除 engine |
| `kyuubi.engine.deregister.exception.ttl` | `PT30M` | 继承默认值 | 匹配异常的累计时间窗口，超出窗口未达阈值时视为已经从临时故障恢复 |

这组参数只有先配置 classes 或 messages 匹配规则才会生效。“摘除”表示停止 engine discovery service、阻止新连接复用，不等于立即杀死当前 YARN Application；已有 session 释放后，engine 再按 idle/max lifetime 等策略退出。

### ZooKeeper 与 Graceful Stop

| 参数 | Kyuubi 默认值 | QueryOne 当前值 | 生命周期作用 |
| --- | --- | --- | --- |
| `kyuubi.ha.addresses` | 空 | `192.168.200.69:2181` | 空值使 Kyuubi Server 启动内嵌 ZK；当前使用外置 ZK |
| `kyuubi.ha.namespace` | `kyuubi` | `queryone-kyuubi` | 隔离 ServerSpace 和 EngineSpace；Server、engine 和 JDBC service discovery 必须使用同一 namespace |
| `kyuubi.ha.zookeeper.session.timeout` | `60000ms` | 继承默认值 | ZK session 在持续断连后失效的基础窗口；进入 `LOST` 后 ephemeral 注册失效 |
| `kyuubi.ha.zookeeper.connection.timeout` | `15000ms` | 继承默认值 | 新建到 ZK ensemble 连接的超时，不是已连接 session 的失效时间 |
| `kyuubi.ha.zookeeper.connection.retry.policy` | `EXPONENTIAL_BACKOFF` | 继承默认值 | `LOST` 后额外重连宽限期所使用的重试策略 |
| `kyuubi.ha.zookeeper.connection.max.retries` | `3` | 继承默认值 | 默认重试策略的最大重试次数 |
| `kyuubi.ha.zookeeper.connection.base.retry.wait` | `1000ms` | 继承默认值 | 指数退避的初始等待时间 |
| `kyuubi.ha.zookeeper.connection.max.retry.wait` | `30000ms` | 继承默认值 | 有界指数退避的单次最大等待，或 `UNTIL_ELAPSED` 策略的总时长；默认无界指数退避策略不使用这个上限 |
| `kyuubi.ha.zookeeper.node.creation.timeout` | `PT2M` | 继承默认值 | 启动或重新注册服务发现节点的等待上限 |
| `kyuubi.zookeeper.embedded.tick.time` | `3000ms` | 继承默认值 | 内嵌 ZK 的基础 tick；只对内嵌模式生效 |
| `kyuubi.zookeeper.embedded.min.session.timeout` | `6000ms` | 继承默认值 | 内嵌 ZK 允许客户端协商的最小 session timeout |
| `kyuubi.zookeeper.embedded.max.session.timeout` | `60000ms` | 继承默认值 | 内嵌 ZK 允许客户端协商的最大 session timeout |

ZK 故障的退出时间不是单独一个 timeout：先经过 ZK session timeout 进入 `LOST`，Kyuubi 再按 retry policy 给出额外重连宽限；仍未 `RECONNECTED` 才调用 graceful stop。

Kyuubi 1.9.4 的 Server graceful stop 没有“总排空超时”配置。它从服务发现中摘除后，每 10 秒检查一次活动 session；只要 session 一直不释放，Server 就会一直等待。以下两个参数只在 Server/engine 已经进入 stop 后限制 operation 执行线程池的关闭等待，并不能限制前面的 session 排空：

| 参数 | Kyuubi 默认值 | QueryOne 当前值 | 生命周期作用 |
| --- | --- | --- | --- |
| `kyuubi.backend.server.exec.pool.shutdown.timeout` | `PT10S` | 继承默认值 | Server operation 线程池进入 shutdown 后的等待时间 |
| `kyuubi.backend.engine.exec.pool.shutdown.timeout` | 回退到 Server 值，即 `PT10S` | 继承默认值 | Engine operation 线程池进入 shutdown 后的等待时间 |

如果运维上要求 Kyuubi Server 必须在固定时间内退出，需要由 systemd、Kubernetes 或停止脚本设置外层强制终止期限，并明确接受超过期限后中断在途 session 的风险。

### Kerberos 与凭据续期

以下参数不负责正常空闲回收，但在启用 Hadoop Kerberos 后会影响 Server 和长驻 engine 能否持续工作：

| 参数 | Kyuubi 默认值 | 生命周期作用 |
| --- | --- | --- |
| `kyuubi.kinit.interval` | `PT1H` | Kyuubi Server 使用 keytab 刷新本地 Kerberos credential cache 的周期 |
| `kyuubi.kinit.max.attempts` | `10` | 连续 `kinit` 失败达到阈值后 Kyuubi Server 调用 `System.exit(-1)`，这是会直接结束 Server 的生命周期参数 |
| `kyuubi.credentials.renewal.interval` | `PT1H` | 按用户续期 Hadoop delegation token 的周期 |
| `kyuubi.credentials.renewal.retry.wait` | `PT1M` | 获取新 delegation token 失败后的重试等待 |
| `kyuubi.credentials.update.wait.timeout` | `PT1M` | 等待用户凭据准备完成的上限 |
| `kyuubi.credentials.check.interval` | `PT5M` | 检查缓存用户凭据是否过期的周期 |
| `kyuubi.credentials.idle.timeout` | `PT6H` | 不活跃用户的缓存凭据回收时间；只清理凭据缓存，不结束 engine |

这组配置与 `kyuubi.session.idle.timeout`、`kyuubi.session.engine.idle.timeout` 相互独立。凭据续期失败通常先表现为后续 HDFS/YARN 访问失败，而不是 engine 自动按 TTL 退出；应对 `kinit` 和 delegation token 续期错误单独告警。

### 仅清理附属资源

| 参数 | Kyuubi 默认值 | QueryOne 当前值 | 生命周期作用 |
| --- | --- | --- | --- |
| `kyuubi.session.conf.file.reload.interval` | `PT10M` | `PT1M` | profile 文件缓存刷新周期，只影响之后创建的 session/engine |
| `kyuubi.session.engine.log.timeout` | `PT24H` | 继承默认值 | 清理 Server 侧保存的 engine `spark-submit` 启动日志，不结束 engine |
| `kyuubi.engine.user.isolated.spark.session.idle.timeout` | `PT6H` | 继承默认值，当前 `USER` share level 不适用 | 在 GROUP/SERVER 隔离场景回收 engine 内部的用户 SparkSession，不结束 engine |
| `kyuubi.engine.user.isolated.spark.session.idle.interval` | `PT1M` | 继承默认值，当前 `USER` share level 不适用 | 上述内部 SparkSession 的检查周期 |

## Spark Executor 缩容

当前集群使用 YARN NodeManager 内的 Spark external shuffle service：

```properties
spark.dynamicAllocation.enabled                  true
spark.dynamicAllocation.shuffleTracking.enabled false
spark.dynamicAllocation.minExecutors             0
spark.dynamicAllocation.initialExecutors         1
spark.dynamicAllocation.maxExecutors             1
spark.dynamicAllocation.executorIdleTimeout      60s
spark.shuffle.service.enabled                    true
```

执行时间线：

```text
任务结束
  -> executor 没有运行 task
  -> 空闲达到 executorIdleTimeout
  -> dynamic allocation 删除 executor
  -> 最低缩到 minExecutors=0
  -> driver / ApplicationMaster / YARN Application 仍然存活
```

`spark_shuffle` 保存已完成 executor 的 shuffle 文件，使 executor 删除后下游 stage 仍能读取 shuffle。它是 NodeManager 的 auxiliary service，不是独立进程；应从 NodeManager 配置、启动日志和 Spark executor 注册日志确认是否加载成功。

以下边界需要注意：

- `spark.shuffle.service.enabled=true` 本身不会缩容，必须同时启用 `spark.dynamicAllocation.enabled=true`。
- `minExecutors=0` 只允许 executor 缩到 0，不包含 driver 或 YARN ApplicationMaster。
- `maxExecutors=1` 表示当前 profile 最多只有一个 executor；需要并行扩容时按队列容量调大。
- `spark.dynamicAllocation.cachedExecutorIdleTimeout` 默认是无限。如果 executor 保存了缓存块，即使普通 idle timeout 已到，也可能继续保留；需要回收缓存 executor 时应显式设置有限值，并接受缓存重新计算。
- Properties 文件只支持 `#` 或 `!` 注释，不能使用 `//`。写成 `//spark.dynamicAllocation.enabled=true` 不会启用动态分配。
- `spark_shuffle` JAR 必须与应用使用的 Spark 版本兼容，并在所有可能承载 executor 的 NodeManager 上启用。

executor 缩到 0 后，新任务产生 backlog 时，dynamic allocation 会重新向 YARN 申请 executor，因此第一条新查询会承担 container 冷启动时间。

## Spark Engine 与 YARN Application 退出

Spark executor 缩到 0 不等于 engine 退出。Kyuubi Spark SQL engine 持有 SparkContext 和 JDBC session，仍会占用 driver 或 ApplicationMaster 资源。整个 Application 的退出由 Kyuubi session/engine timeout 控制：

```properties
kyuubi.session.idle.timeout            PT30M
kyuubi.session.check.interval          PT5M
kyuubi.session.engine.idle.timeout     PT30M
kyuubi.session.engine.check.interval   PT1M
```

两个 timeout 是当前 YARN profile 的显式值，两个 check interval 继承 Kyuubi 默认值，所以实际退出时间不是精确的 30 或 60 分钟。其他启动、探活、异常摘除和 ZK 故障参数见上面的生命周期参数总表。

### `run_isolated`

```text
Run 完成
  -> QueryOne 在 finally 主动执行 Connection.close()
  -> Kyuubi 立即关闭 frontend session 及其 operation
  -> Kyuubi 立即关闭对应的 engine backend session
  -> 若这是 engine 中最后一个 session
  -> engine idle 计时 30 分钟
  -> 下一次 engine timeout 检查时停止 Spark engine
  -> YARN Application 结束
```

没有其他共享 session 时，通常是 Run 完成后约 30 至 31 分钟退出。正常路径不等待 operation idle timeout 或 session idle timeout；这也是调整 `kyuubi.operation.idle.timeout` 对 `run_isolated` 看起来没有影响的原因。

### `tenant_shared`

YARN engine/backend 链路：

```text
最后一次 SQL 完成
  -> QueryOne 保留 JDBC connection
  -> 若 backend 有未关闭的终态查询 operation
  -> engine checker 等待 operation idle timeout 后清理 operation
  -> backend session 开始累计无 operation 空闲时间
  -> YARN engine backend session 空闲约 30 分钟
  -> 下一次 session timeout 检查关闭 backend session
  -> engine idle 再计时 30 分钟
  -> 下一次 engine timeout 检查停止 engine
  -> YARN Application 结束
```

Kyuubi Server frontend 链路：

```text
最后一次 frontend 访问
  -> QueryOne 保留 JDBC connection
  -> 若有 LaunchEngine/其他终态 frontend operation
  -> Server checker 等待 operation idle timeout 后清理 operation
  -> frontend session 开始累计无 operation 空闲时间
  -> 下一次 session timeout 检查关闭 frontend session
```

两条链路独立回收，不要求时间同步，frontend session 可能比 YARN Application 更早或更晚关闭。客户端已正常关闭所有 backend operation handle 时，按默认检查周期，YARN Application 完全空闲后通常约 60 至 66 分钟退出；存在未关闭的终态 backend operation 时，还会先受 engine 中 `kyuubi.operation.idle.timeout` 影响。同一 `USER + subdomain` engine 池内只要还有一个 backend session，engine 就不会进入最终回收。backend session 或 engine 被回收后，临时视图、临时 UDF、session SQL 配置和缓存都不再存在；Server frontend session 即使仍在，也不能保留这些 engine 内状态。

YARN deploy mode 只改变 driver 所在位置：

- `client`：driver/engine 进程位于 Kyuubi gateway 主机；停止 engine 后会关闭 SparkContext，YARN Application 随之结束。
- `cluster`：driver/engine 位于 YARN ApplicationMaster container；停止 engine 会直接结束整个 YARN Application。

下一次 QueryOne 连接同一 profile 时，Kyuubi 会重新创建 engine 和 YARN Application。这是 Application 冷启动，不是恢复已销毁的 engine。

## QueryOne 与 Kyuubi Server 停止

QueryOne 进程停止时，它持有的 JDBC connection 会断开。默认 `kyuubi.session.close.on.disconnect=true`，对应 Kyuubi session 关闭；最后一个 session 释放后，engine 进入 30 分钟 idle 倒计时。Kyuubi Server、ZooKeeper 和其他租户的 engine 不会因为 QueryOne 单进程停止而自动退出。

Kyuubi Server 是长驻 gateway，没有“没有 SQL 就自动缩成 0”的机制。`kyuubi.session.engine.idle.timeout` 只回收 query engine，不回收 Kyuubi Server。Server 进程应由 systemd、容器编排或其他服务管理器负责启动、停止和重启。

停止 Kyuubi Server 时：

- QueryOne 到该 Server 的 JDBC connection 会断开；写语句若正在执行，客户端应按“结果状态未知”处理。
- graceful stop 会先从服务发现中摘除 Server，停止接收新连接，并等待现有 session 结束。
- graceful stop 等待的是 Server frontend session。当前全局 idle timeout 仍为默认 `PT6H`，检查周期为 `PT5M`；如果客户端保持连接或 session 一直有 operation，Server 没有内置总超时可以强制结束排空。
- Spark engine 是独立进程或 YARN Application，不保证随 Server 进程立即退出；它还会受到 session timeout、engine idle timeout 和 ZooKeeper 连接状态控制。

## 两类 ZooKeeper

当前配置使用同一个外置 ZooKeeper，但包含两个职责和路径相互独立的客户端：

| 用途 | 配置 | 当前形态 |
| --- | --- | --- |
| Kyuubi Server/engine 服务发现 | `kyuubi.ha.addresses` | `192.168.200.69:2181`，namespace 为 `queryone-kyuubi` |
| QueryOne 受控 HDFS overwrite 锁 | `spark.queryone.overwrite.zk.connect` | `192.168.200.69:2181`，root 为 `/queryone/overwrite` |

两个用途虽然复用同一个外置 ensemble，仍然使用不同 namespace/root 和不同客户端；单个 ZooKeeper 故障会同时影响 Kyuubi 服务发现和 overwrite 排他锁。

## Kyuubi 内嵌 ZooKeeper 停止

未配置 `kyuubi.ha.addresses` 时，Kyuubi Server 会在本进程中启动内嵌 ZooKeeper，并把地址注入 Server 和 Spark engine。该模式用于单机或测试，不提供 Kyuubi Server 高可用。

正常停止 Kyuubi Server JVM 时，内嵌 ZooKeeper 也会停止。当前 Kyuubi 实现正常关闭内嵌 ZK 时会删除其数据目录，因此不能依赖重启后保留旧注册信息。

典型影响：

```text
Kyuubi Server JVM 停止
  -> 内嵌 ZK 停止
  -> QueryOne JDBC connection 断开
  -> engine 的 ZK client 进入 SUSPENDED / LOST
  -> 在重试宽限期内仍未 RECONNECTED
  -> engine 触发 graceful stop
  -> 等待活动 session 清零后退出
```

短暂断连后若在宽限期内进入 `RECONNECTED`，Kyuubi 使用的 persistent ephemeral node 可以重新注册，已运行 engine 不一定退出。持续断连进入 `LOST` 后，Kyuubi 会等待按重试策略计算的宽限时间；仍未恢复时调用 `stopGracefully(true)`。graceful stop 不会强杀正在执行的 session，因此长任务可能继续占用 engine，直到 session 结束或被其他超时策略关闭。

如果 Server 被 `kill -9`，shutdown hook 没有机会正常执行，但内嵌 ZK 端口仍会随进程消失，ephemeral registration 会失效。旧 engine 最终通过 ZK `LOST` 或自身 idle timeout 退出；Server 重启后应按冷启动处理，不把旧 engine 可发现性作为保证。

## Kyuubi 使用外置 ZooKeeper

当前测试环境配置为：

```properties
kyuubi.ha.addresses=192.168.200.69:2181
kyuubi.ha.namespace=queryone-kyuubi
```

该地址目前是单节点外置 ZooKeeper，使 Server 和 engine 注册信息独立于某个 Kyuubi Server，但不提供 ZooKeeper quorum 高可用。生产环境应改为至少三个 ZooKeeper 节点的 ensemble。是否具备 gateway 高可用，还取决于 Kyuubi Server 实例数量和 QueryOne 的 JDBC 入口：

| 维度 | 单 Kyuubi Server | 多 Kyuubi Server |
| --- | --- | --- |
| 停止一个 Server | gateway 完全不可用 | 其余 Server 可以继续接收新连接 |
| 已有 JDBC session | 连接中断，不能迁移或恢复 | 停止节点上的 session 不能迁移；其他节点上的 session 不受影响 |
| 新 JDBC connection | 必须等待唯一 Server 恢复 | 使用 ZK service discovery 或负载均衡入口时可路由到存活 Server |
| 已运行 Spark engine | 注册仍在 ZK，但没有 Server 可代理请求 | 其他 Server 可以发现并复用 |
| 临时视图和 session 配置 | 原 session 断开后丢失 | 不会随客户端切换到另一个 Server |
| 外置 ZK 丢失 quorum | Server 和 engine 都可能进入 graceful stop | 所有 Server 和 engine 都受影响，多 Server 不能屏蔽共享 ZK 故障 |

### 外置 ZK + 单 Kyuubi Server

只有一个 Kyuubi Server 时，外置 ZK 解决的是注册信息与 Server 进程解耦，不解决 gateway 单点故障。QueryOne 到唯一 Server 的连接仍是单点：

```text
唯一 Kyuubi Server 停止
  -> QueryOne JDBC connection 全部断开
  -> 外置 ZK 继续运行
  -> Spark engine 的注册节点继续存在
  -> 没有 Kyuubi Server 可以代理新请求
  -> engine session 关闭或超时
  -> 最后一个 session 释放后进入 engine idle
  -> engine / YARN Application 最终退出
```

Server 停止不会让 engine 的 ZK client 进入 `LOST`，因为外置 ZK 仍然正常。engine 不会仅因为唯一 Server 消失而立即退出，而是继续遵循 session idle、engine idle 和 max lifetime 等自身策略。

如果 Kyuubi Server 在 engine 退出前恢复，并使用相同的 `kyuubi.ha.addresses`、namespace、share level、用户和 subdomain，新 Server 可以从 ZK 重新发现仍存活的 engine。它只能创建新的 Kyuubi/JDBC session，不能恢复旧 frontend session；旧 session 中的临时视图、临时 UDF 和 SQL 配置不能依赖这种方式恢复。

外置 ZK 正常但唯一 Kyuubi Server 停止时：

- 直接执行中的 SQL 可能仍在 engine 内运行，但 QueryOne 已无法取得可靠结果；写语句按“状态未知”处理。
- graceful stop 会等待已有 session 结束；直接杀进程会立即中断客户端连接。
- Server 恢复前不能提交新任务，即使 YARN 上的 engine 仍是 `RUNNING`。
- Server 恢复较慢时，engine 可能先达到 idle timeout 并退出，后续连接将冷启动新的 YARN Application。

### 外置 ZK + 多 Kyuubi Server

多个 Kyuubi Server 使用相同 `kyuubi.ha.addresses` 和 `kyuubi.ha.namespace` 时，会注册到同一个 ServerSpace。Spark engine 独立注册到 EngineSpace，不归创建它的某一个 Server 私有；其他 Server 可以按相同 engine share key 发现并连接该 engine。

优雅停止其中一个 Server 的时间线：

```text
从 ZK 删除目标 Server 注册
  -> 目标 Server 不再接收新 JDBC connection
  -> 已有 session 继续由目标 Server 服务
  -> 新 connection 路由到其他 Server
  -> 已有 session 全部关闭
  -> 目标 Server 退出
  -> Spark engine 继续服务其他 Server 的 session
```

这里没有 JDBC session 迁移。连接一旦建立，就固定在接收它的 Kyuubi Server；该 Server 突然故障时，客户端需要重新连接其他 Server，并创建新的 engine session。`tenant_shared` 的临时视图和 session 状态不会自动复制到新 session，写语句连接中断仍按“状态未知”处理。

多 Server 真正生效还要求 QueryOne 不绑定单一地址：

- 使用 Kyuubi JDBC 的 ZooKeeper service discovery，让新 connection 从 ServerSpace 选择存活 Server；或
- 在多个 Kyuubi Server 前提供健康检查和负载均衡入口。

当前 QueryOne 使用 `jdbc:kyuubi://192.168.200.69:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=queryone-kyuubi?...`。新 connection 会从 ServerSpace 选择存活的 Kyuubi Server；已有 JDBC session 仍固定在最初选中的 Server，不会因节点停止而迁移。

同一个 engine 是否退出取决于所有 Kyuubi Server 打开的 engine session 总数：

- 任意 Server 上仍有活动 session，engine 都不会进入最终 idle 回收。
- 停止一个 Server 只会释放该 Server 持有的 session，不会直接杀死共享 engine。
- 所有 Kyuubi Server 都停止但外置 ZK 仍正常时，engine 注册可能暂时保留；session 释放后按 engine idle timeout 退出。
- 只剩最后一个 Server 时，系统退化成“外置 ZK + 单 Server”，不再具备 gateway 冗余。

### 外置 ZK 自身停止

- 只停止一个 ZK 节点且 ensemble 仍有 quorum：客户端可以切换到其他节点，通常不会触发 Server 或 engine 退出。
- 整个 ensemble 丢失 quorum 但尚未超过 session timeout：已有 Server 到 engine 的直连请求可能继续，新连接、engine 发现和创建可能失败或等待。
- ZK client 进入 `LOST` 且在重试宽限期内没有 `RECONNECTED`：Kyuubi Server 和 Spark engine 都会触发 graceful stop，停止接收新连接并等待活动 session 清零后退出。
- ZK 恢复发生在 graceful stop 触发前：客户端进入 `RECONNECTED`，注册节点由 Curator 恢复，组件继续运行。

默认 ZK session timeout 是 60 秒，之后还会叠加连接重试策略对应的宽限时间。最终时间应以日志中的 `Zookeeper client connection state changed to: LOST` 和 `Give up retry and stop gracefully` 为准。

外置 ZK 整体停机属于控制面故障，不是资源缩容手段。不要通过停止 ZooKeeper 来回收 engine；需要回收 engine 时使用 idle timeout、Kyuubi Engine UI/`kyuubi-ctl` 的 graceful delete，或 YARN 运维手段。

## QueryOne Overwrite 外置 ZooKeeper 停止

`spark.queryone.overwrite.zk.connect` 只服务受控 HDFS overwrite：

- 新 overwrite 在取得锁之前连接不上 ZK，会在写 staging 前失败，避免无锁启动写入。
- 普通查询、Catalog 读写、受控 HDFS load、executor dynamic allocation 和 engine idle timeout 不依赖这个锁 ZK。
- 已经取得 ephemeral lock 的 overwrite 如果在执行中丢失 ZK session，锁节点会消失。当前 extension 不具备 fencing token，正在运行的 writer 不会因为锁丢失自动停止。

因此 overwrite ZK 丢失 quorum 时，应暂停新的受控 HDFS overwrite，等待 ZK 恢复，并检查目标、staging 和 backup 状态后再恢复写入。不要仅凭客户端报错自动重试写语句。

如果同一个外置 ensemble 同时配置给 `kyuubi.ha.addresses` 和 `spark.queryone.overwrite.zk.connect`，ensemble 故障会同时触发 Kyuubi 服务发现故障和 overwrite 锁故障，两组影响都要处理。

## 场景速查

| 场景 | Executor | Engine / YARN Application | Kyuubi Server |
| --- | --- | --- | --- |
| 无 SQL，所有服务健康 | 约 60 秒后可缩到 0 | session 和 engine timeout 后退出 | 继续长驻 |
| `run_isolated` 完成 | 按 dynamic allocation 缩容 | 最后 session 关闭后约 30 至 31 分钟退出 | 继续长驻 |
| `tenant_shared` 完全空闲 | 按 dynamic allocation 缩容 | 通常约 60 至 66 分钟退出 | 继续长驻 |
| QueryOne 停止 | 对应 Application 继续按策略缩容 | session 释放后进入 engine idle | 不受影响 |
| Kyuubi Server + 内嵌 ZK 停止 | 短期可能继续存在 | ZK `LOST`/session idle 后退出 | 已停止 |
| 唯一 Kyuubi Server 停止，外置 ZK 正常 | 继续按策略缩容 | 暂时无 Server 代理；session 释放后空闲退出 | gateway 完全不可用 |
| 多 Server 中一个停止，外置 ZK 正常 | 继续按策略缩容 | 可由其他 Server 发现；按全部 session 判断是否空闲 | 其他 Server 继续服务 |
| 所有 Kyuubi Server 停止，外置 ZK 正常 | 短期可能继续存在 | 注册暂时保留；session 释放后空闲退出 | gateway 完全不可用 |
| 外置 ZK 丢失 quorum 并持续 | 已有 executor 可能暂时运行 | ZK `LOST` 后排空 session 并退出 | ZK `LOST` 后排空 session 并退出 |
| 仅 overwrite ZK 停止 | 不受影响 | 不受 engine TTL 之外的额外影响 | 不受影响 |

## 运维判断顺序

遇到“为什么资源还没有释放”时按以下顺序排查：

1. 在 Spark UI Executors 页面确认 dynamic allocation 是否启用、是否存在运行 task 或缓存块。
2. 在 YARN UI 区分 executor container 已释放但 Application 仍是 `RUNNING`，还是整个 Application 已结束。
3. 在 Kyuubi engine 日志确认活动 session 数量，以及 `Closing session ... idle`、`Idled for more than ... terminating`。
4. 在 Kyuubi Server/engine 日志确认 ZooKeeper 状态是 `SUSPENDED`、`LOST` 还是 `RECONNECTED`。
5. 对受控 HDFS overwrite，单独确认锁 ZooKeeper quorum、目标目录、staging 和 backup，不把查询成功等同于写入锁健康。
