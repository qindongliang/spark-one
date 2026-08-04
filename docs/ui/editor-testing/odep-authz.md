# ODEP 库表鉴权测试

本文用于验证 SparkOne 页面经 Kyuubi 提交 SQL 时，Spark Engine 是否使用当前 RMS 真实用户，
在物理执行前完成 JDBC、Doris、Hive 库表权限检查。测试分为 ODEP API 独立验证和 SparkOne
页面端到端验证；接口通过不代表 Engine 链路通过，两部分都必须执行。

## 测试目标

通过标准：

- `subject` 是 SparkOne 页面登录时填写的 RMS 真实用户名。
- `database` 是 SQL 中的逻辑 alias，例如 `ask00`，不是数据源的物理库名。
- 仅配置库级白名单时，该库下未单独配置的表也能访问。
- 表级白名单只允许指定表；没有匹配权限时默认拒绝。
- 黑名单优先于白名单，包括“库白名单 + 表黑名单”的组合。
- `jdbc`、`doris`、`hive` 资源类型互不替代，`read`、`write` 动作互不替代。
- JOIN 等多资源 SQL 使用一次批量鉴权，任一资源拒绝则整条 SQL 不执行。
- 临时视图最终检查底层真实表，不能绕过权限。
- 不合并 `app_username` 或伴生账号权限。
- ODEP 异常、超时、响应不完整、Kyuubi 用户签名无效和未知 Catalog 均默认拒绝。

本轮只验证 Kyuubi Engine 鉴权。Local engine 不加载该扩展，不能用来判断本功能是否生效。

## 鉴权链路

```text
SparkOne 页面登录的 RMS 用户名
  -> TenantContext.username
  -> Kyuubi JDBC session user
  -> Kyuubi ECDSA session user 签名
  -> Spark Engine 验签并提取 LogicalPlan 资源
  -> POST /api/sparkone/authz/check
  -> ODEP 查询该 subject 的 RMS 资源
  -> Spark Engine 在物理执行前允许或拒绝 SQL
```

Engine 发送给 ODEP 的资源映射：

| SQL 中的资源 | ODEP `resourceType` | `database` | `table` |
| --- | --- | --- | --- |
| `jdbc.ask00.users` | `jdbc` | `ask00` | `users` |
| `doris.analytics.events` | `doris` | `analytics` | `events` |
| `hive.default.users` | `hive` | `default` | `users` |

查询源表使用 `read`，写入目标表使用 `write`。同一 SQL 的重复资源会去重，多表资源合并为一次
请求。ODEP 对批量请求使用 AND 语义：所有 decision 都允许时，总体 `allowed` 才是 `true`。

## 权限格式与判定规则

RMS 中 JDBC、Doris、Hive 的资源内容格式：

```text
white:<database>:<action>
white:<database>:<table>:<action>
black:<database>:<action>
black:<database>:<table>:<action>
```

示例：

```text
资源类型 资源内容
jdbc    white:ask00:read
jdbc    white:ask00:authz_write_target:write
jdbc    black:ask00:secret_users:read
doris   white:analytics:events:read
hive    white:default:users:read
```

判定结果：

| 条件 | `allowed` | `reason` |
| --- | --- | --- |
| 命中任意黑名单 | `false` | `BLACKLISTED` |
| 未命中黑名单且命中白名单 | `true` | `MATCHED` |
| 没有匹配资源 | `false` | `NO_MATCHING_RESOURCE` |

库级规则能匹配该库下全部表，表级规则只匹配指定表。只要命中黑名单就拒绝，不受 RMS 资源
返回顺序影响。`resourceType` 和 `action` 会转成小写；`database` 和 `table` 只去除首尾空格，
当前按大小写精确匹配，RMS 配置应与 SQL 解析得到的 alias 和表名保持一致。

HDFS 不是本轮库表测试的主项，但同一接口也支持目录前缀权限：

```text
hdfs white:/public/odep/user/alice:read
hdfs white:/public/odep/user/alice:write
hdfs black:/public/odep/user/alice/secret:read
```

## 测试数据准备

准备三个 RMS 中真实存在的测试用户。下文使用以下占位名称，执行时替换为真实用户名：

| 测试角色 | 文档示例用户名 | 用途 |
| --- | --- | --- |
| A | `authz_db_user` | 库级授权、黑名单和写权限 |
| B | `authz_table_user` | 仅表级授权 |
| C | `authz_empty_user` | 无匹配权限和用户隔离 |

准备以下真实可访问的数据和隔离写入目标；表名不一致时统一替换后续示例：

| 类型 | 逻辑 alias/database | 表 | 要求 |
| --- | --- | --- | --- |
| JDBC | `ask00` | `users` | 至少一行可读数据 |
| JDBC | `ask00` | `orders` | 至少一行可读数据 |
| JDBC | `ask00` | `secret_users` | 至少一行可读数据 |
| JDBC | `ask00` | `authz_write_target` | 隔离测试表，字段与写入 SQL一致 |
| Doris | `analytics` | `events` | 至少一行可读数据 |
| Hive | `default` | `users` | 至少一行可读数据 |

`authz_write_target` 建议使用仅供本测试的表，例如：

```text
id BIGINT, name STRING/VARCHAR
```

不要在生产业务表上执行写权限用例。写入前记录目标表中 `id = 900001` 的行数，测试完成后按
测试环境的数据清理流程删除该行。

### RMS 授权基线

给用户 A 配置：

```text
jdbc  white:ask00:read
jdbc  black:ask00:secret_users:read
jdbc  white:ask00:authz_write_target:write
doris white:analytics:events:read
hive  white:default:users:read
```

关键点：`users` 和 `orders` 不配置表级 JDBC 白名单，只依赖 `white:ask00:read`。这就是“仅库
授权，table 没设置”的验证条件。

给用户 B 只配置：

```text
jdbc white:ask00:users:read
```

用户 B 不配置 `white:ask00:read`，不配置 `orders`。用户 C 不配置上述四类资源。若要验证
`app_username` 不合并，可以只给用户 C 的伴生账号配置 `white:ask00:users:read`，但不要给
用户 C 本人配置。

RMS 资源缓存若不是实时刷新，授权变更后应等待缓存刷新或按现有运维方式清理缓存，再开始测试。

## 环境与部署检查

### ODEP 与扩展配置

确认 ODEP 已部署：

```text
POST /api/sparkone/authz/check
```

确认 Spark Engine 使用的 JAR 中包含：

```text
sparkone-kyuubi-odep-authz-extension-0.1.0-SNAPSHOT.jar
```

`$KYUUBI_CONF_DIR/kyuubi-defaults.conf` 至少包含：

```properties
spark.jars                                  /opt/sparkone/sparkone-kyuubi-odep-plugin.jar,/opt/sparkone/sparkone-kyuubi-odep-authz-extension.jar
spark.sql.extensions                        ai.sparkone.kyuubi.odep.authz.SparkOneOdepAuthzExtension
kyuubi.session.user.sign.enabled            true
spark.kyuubi.session.user.sign.enabled      true
```

如果还启用了其他 Spark SQL extension，使用英文逗号追加，不能覆盖已有类。例如：

```properties
spark.sql.extensions ai.sparkone.extension.overwrite.SparkOneHdfsOverwriteExtensions,ai.sparkone.kyuubi.odep.authz.SparkOneOdepAuthzExtension
```

Engine 进程必须能读取：

```text
ODEP_API_URL
ODEP_KYUUBI_APP_ID
ODEP_KYUUBI_SIGN_KEY
ODEP_CONNECT_TIMEOUT_SECONDS
ODEP_REQUEST_TIMEOUT_SECONDS
```

后两个变量可省略，默认分别为 5 秒和 60 秒。前三个变量缺失时，首次需要鉴权的 SQL 应失败。

修改 `kyuubi-defaults.conf`、环境变量或扩展 JAR 后，应重启 Kyuubi Server，并停止旧 Spark
Engine，确保下一次连接拉起新 Engine。只刷新 RMS 授权不需要重启 Engine。

### SparkOne 页面检查

1. 启动 SparkOne，访问 `http://127.0.0.1:7070`。
2. 输入用户 A 的 RMS 真实用户名登录，不要输入物理 Kerberos 统一账号。
3. Engine 选择 `Kyuubi`，不要选择 `Local`。
4. Session 先选择 `Tenant shared`；会话隔离用例再切换到 `Run isolated`。
5. 执行 `select 1 as id;`，确认 SparkOne、Kyuubi 和 Engine 基础链路正常。

`select 1` 没有外部资源，不调用 ODEP 权限接口，只能作为连通性检查，不能证明鉴权生效。
`SHOW NAMESPACES` 和 `SHOW TABLES` 当前通常也不会提取库表资源，不能用它们的成功或失败判断
用户是否有数据访问权限。

## 自动化测试

先按仓库 `.sdkmanrc` 切换环境，并确认 Maven 实际使用 JDK 17：

```bash
sdk env
java -version
mvn -version
```

`mvn -version` 的 Java version 必须是 17，不能只检查当前 shell 的 `java -version`。

先验证 SparkOne Engine 扩展的资源提取、签名校验、允许和拒绝逻辑：

```bash
mvn -pl sparkone-kyuubi-odep-authz-extension -am test
```

验证 ODEP System 的权限解析和接口参数：

```bash
cd references/odep-system

mvn -pl odep-core \
  -Dtest=SparkOneAuthorizationServiceTest test

mvn -pl odep-api -am \
  -Dtest=SparkOneAuthzControllerTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

ODEP 仓库如果使用环境专用 Maven profile，追加该环境已有的 profile 参数，不要跳过测试。

## ODEP API 独立测试

在 SparkOne 仓库根目录设置接口凭据。密钥只放当前 shell 环境，不写入文档或 Git：

```bash
export ODEP_API_URL=http://127.0.0.1:8093
export ODEP_KYUUBI_APP_ID=app_kyuubi
export ODEP_KYUUBI_SIGN_KEY='实际签名密钥'
```

脚本会生成 `nonce`、`timestamp` 和签名，校验 HTTP/业务响应、逐项 decision 数量、资源回显、
总体 AND 语义及 reason：

```bash
scripts/tests/odep-authz-api.sh --help
```

以下命令中的用户名替换为实际用户。

### A01 仅库授权允许任意普通表

用户 A 的 RMS 只有 `jdbc white:ask00:read`，没有 `users` 和 `orders` 表级白名单：

```bash
scripts/tests/odep-authz-api.sh authz_db_user allow \
  '[{"resourceType":"jdbc","database":"ask00","table":"users","action":"read"}]'

scripts/tests/odep-authz-api.sh authz_db_user allow \
  '[{"resourceType":"jdbc","database":"ask00","table":"orders","action":"read"}]'
```

两个请求都应输出 `allowed=True reason=MATCHED`。

### A02 表级授权不扩散到其他表

```bash
scripts/tests/odep-authz-api.sh authz_table_user allow \
  '[{"resourceType":"jdbc","database":"ask00","table":"users","action":"read"}]'

scripts/tests/odep-authz-api.sh authz_table_user deny \
  '[{"resourceType":"jdbc","database":"ask00","table":"orders","action":"read"}]'
```

第二个请求应为 `NO_MATCHING_RESOURCE`。

### A03 黑名单覆盖库白名单

```bash
scripts/tests/odep-authz-api.sh authz_db_user deny \
  '[{"resourceType":"jdbc","database":"ask00","table":"secret_users","action":"read"}]'
```

应为 `BLACKLISTED`，即使同一用户同时具有 `white:ask00:read`。

### A04 批量请求任一拒绝则总体拒绝

```bash
requests='[{"resourceType":"jdbc","database":"ask00","table":"users","action":"read"},'
requests+='{"resourceType":"jdbc","database":"ask00","table":"secret_users","action":"read"}]'

scripts/tests/odep-authz-api.sh authz_db_user deny "$requests"
```

第一项应为 `MATCHED`，第二项应为 `BLACKLISTED`，总体 `allowed=False`。

### A05 类型和动作隔离

用户 B 只有 JDBC `users:read`：

```bash
scripts/tests/odep-authz-api.sh authz_table_user deny \
  '[{"resourceType":"doris","database":"ask00","table":"users","action":"read"}]'

scripts/tests/odep-authz-api.sh authz_table_user deny \
  '[{"resourceType":"jdbc","database":"ask00","table":"users","action":"write"}]'
```

两个请求都应为 `NO_MATCHING_RESOURCE`。JDBC read 不能替代 Doris read 或 JDBC write。

### A06 Doris 和 Hive 允许

```bash
scripts/tests/odep-authz-api.sh authz_db_user allow \
  '[{"resourceType":"doris","database":"analytics","table":"events","action":"read"}]'

scripts/tests/odep-authz-api.sh authz_db_user allow \
  '[{"resourceType":"hive","database":"default","table":"users","action":"read"}]'
```

### A07 用户本人无权限时拒绝

```bash
scripts/tests/odep-authz-api.sh authz_empty_user deny \
  '[{"resourceType":"jdbc","database":"ask00","table":"users","action":"read"}]'
```

应为 `NO_MATCHING_RESOURCE`。即使用户 C 的 `app_username` 或伴生账号具有该资源，结果仍应拒绝。

## SparkOne 页面端到端测试

页面用例必须点击 `Run`。`Compile` 只验证 DSL 编译，不会连接远端表，不能证明 Engine 已鉴权。
每次切换用户后先 `Log out`，再用对应 RMS 真实用户名登录；不要只改 SQL 或浏览器显示文本。

### E01 库级白名单允许读取

以用户 A 登录，选择 Kyuubi：

```sql
select *
from jdbc.ask00.users
limit 10;

select *
from jdbc.ask00.orders
limit 10;
```

两条 SQL 都应成功。Engine 日志应分别出现：

```text
ODEP authorization allowed, subject=authz_db_user, resourceCount=1
```

这条用例确认只配置 `white:ask00:read` 时，无需再给 `users`、`orders` 设置 table 权限。

### E02 表级白名单只允许指定表

退出用户 A，以用户 B 登录：

```sql
select * from jdbc.ask00.users limit 10;
```

应成功。再单独 Run：

```sql
select * from jdbc.ask00.orders limit 10;
```

应在执行前失败，错误包含：

```text
Resource access denied: jdbc:ask00.orders:read
```

目标 JDBC 服务不应收到第二条 SQL 的数据查询。

### E03 黑名单覆盖库白名单

以用户 A 登录：

```sql
select * from jdbc.ask00.secret_users limit 10;
```

应在执行前失败，错误包含：

```text
Resource access denied: jdbc:ask00.secret_users:read
```

### E04 多表 JOIN 批量鉴权

以用户 A 登录：

```sql
select u.*, s.*
from jdbc.ask00.users u
join jdbc.ask00.secret_users s on u.id = s.id
limit 10;
```

整条 SQL 应失败。ODEP 应收到一个包含两个资源的批量请求；Engine 拒绝日志应指出
`secret_users:read:BLACKLISTED`。允许的 `users` 不能使 JOIN 部分执行。

再用两个允许表验证批量通过：

```sql
select u.*, o.*
from jdbc.ask00.users u
join jdbc.ask00.orders o on u.id = o.user_id
limit 10;
```

应成功，Engine 日志中的 `resourceCount=2`。

### E05 临时视图不能绕过权限

以用户 A 登录，单独 Run：

```sql
load jdbc.`ask00.users` as authz_users;
select * from authz_users limit 10;
```

应成功。再执行：

```sql
load jdbc.`ask00.secret_users` as authz_secret_users;
select * from authz_secret_users limit 10;
```

应拒绝 `jdbc:ask00.secret_users:read`。如果 `load` 只注册了临时视图而页面没有
主动取数，点击 Preview 或执行后续 `select` 时仍必须检查展开后的底层表。

### E06 Doris 与 Hive 资源类型

以用户 A 登录分别执行：

```sql
select * from doris.analytics.events limit 10;
select * from hive.default.users limit 10;
```

两条都应成功，Engine 分别发送 `doris + analytics + events` 和
`hive + default + users`。RMS 中同名 JDBC 权限不能替代这两种类型。

### E07 write 动作独立鉴权

先以只有 read 权限的用户 B 执行：

```sql
insert into jdbc.ask00.authz_write_target
select cast(900001 as bigint) as id, 'authz-test' as name;
```

应在写入前失败，错误包含：

```text
jdbc:ask00.authz_write_target:write
```

目标表不能新增数据。再以具有 `white:ask00:authz_write_target:write` 的用户 A 执行同一 SQL，
应写入成功。测试后检查目标表并清理 `id = 900001` 的测试数据。

如果底层 Catalog 本身不支持 `INSERT INTO`，先换成已验证可写的隔离表；不能把 Catalog 写入
能力错误当作鉴权拒绝。拒绝用例仍应在实际 connector 写入前由鉴权扩展拦截。

### E08 RMS 用户隔离与 app_username 不合并

用户 A 查询 `jdbc.ask00.users` 应成功。退出后以用户 C 登录，执行相同 SQL应返回：

```text
jdbc:ask00.users:read
```

Engine 日志中的 `subject` 必须从用户 A 变为用户 C。用户 C 的伴生账号即使有权限也不能使
请求通过，这证明 ODEP 使用用户本人资源，不合并 `app_username`。

### E09 两种 Session 模式保持相同权限

以用户 A 登录，分别在 `Tenant shared` 和 `Run isolated` 下执行：

```sql
select * from jdbc.ask00.users limit 10;
select * from jdbc.ask00.secret_users limit 10;
```

两种模式都应是第一条允许、第二条拒绝。Session 是否复用只影响临时视图生命周期，不应改变
subject 或权限结果。`Run isolated` 中需要把 `load` 和使用临时视图的语句放在同一次 Run。

### E10 无资源 SQL 和元数据命令边界

```sql
select 1 as id;
show namespaces in jdbc;
show tables in jdbc.ask00;
```

这些语句成功不代表用户拥有表数据权限。随后必须执行实际 `SELECT` 验证。当前扩展对无外部
资源的 SQL 不调用 ODEP，元数据枚举也不是本轮表访问鉴权的覆盖范围。

### E11 未知 Catalog 默认拒绝

如果环境配置了 `mysql_static`，执行：

```sql
select * from mysql_static.some_database.some_table limit 10;
```

当前扩展应在执行前失败并包含：

```text
Unsupported catalog for authorization: mysql_static
```

这是 fail-closed 行为。当前允许的表 Catalog 只有 `jdbc`、`doris`、`spark_catalog` 和
`session_catalog`；SparkOne 的 `hive` 会改写为内置 Spark catalog。

## Fail-closed 故障测试

以下用例会修改服务状态或 Engine 配置，只在隔离测试环境执行，每次只引入一个故障：

| 故障 | 操作 | 预期 |
| --- | --- | --- |
| ODEP 不可达 | 临时停止 ODEP 或将隔离 Engine 指向不可达地址 | SQL 失败，包含 `Resource authorization service failed` |
| ODEP 超时 | 将隔离环境超时调小并让接口响应超过阈值 | SQL 失败，底层表不执行 |
| appId/sign key 错误 | 给隔离 Engine 配置错误凭据 | ODEP 业务请求失败，SQL 拒绝 |
| Engine 缺少必需变量 | 移除一个 `ODEP_API_URL/APP_ID/SIGN_KEY` 后重建 Engine | 首条外部资源 SQL 失败 |
| Server 签名开关关闭 | 关闭 `kyuubi.session.user.sign.enabled` 后重启并重建 Engine | 外部资源 SQL 因缺少可信 session 属性失败 |
| Engine 验签开关关闭 | 关闭 `spark.kyuubi.session.user.sign.enabled` 后重建 Engine | 外部资源 SQL 提示必须启用该开关 |

每个故障用例完成后恢复配置、重启 Kyuubi Server、停止故障 Engine，再重新执行 E01 和 E03，
确认允许与拒绝链路均恢复。不要只用 `select 1` 做恢复验收，因为它不触发鉴权。

## 日志检查

允许场景的 Engine 日志：

```text
ODEP authorization allowed, subject=<RMS用户名>, resourceCount=<资源数>
```

拒绝场景的 Engine 日志：

```text
ODEP authorization denied, subject=<RMS用户名>, resources=<type:database.table:action:reason>
```

接口异常应有完整异常堆栈：

```text
ODEP authorization failed, subject=<RMS用户名>, resourceCount=<资源数>
```

同时检查：

- ODEP 接口日志中的 subject、请求次数和时间与页面 Run 对应。
- 多表 SQL 是一次批量请求，不是每张表单独一次请求。
- 拒绝发生在目标 JDBC/Doris/Hive 执行之前。
- 日志不输出 `ODEP_KYUUBI_SIGN_KEY`、数据源密码或完整连接参数。
- 页面只显示可读错误；服务端仍保留失败语句、subject、资源和异常堆栈。

`Cannot modify the value of a Spark config: spark.driver.host`、`spark.eventLog.enabled` 或
`spark.driver.bindAddress` 是 Kyuubi session 尝试二次设置 Spark 静态配置的 WARN，与权限决策
无关。只要 Engine 已正常启动，不应将该 WARN 记为鉴权失败。

## 测试结果记录

| 编号 | 场景 | 用户 | 预期 | 实际 | Engine/ODEP 日志证据 | 结果 |
| --- | --- | --- | --- | --- | --- | --- |
| A01 | 仅库授权读取两张表 | A | 允许 |  |  |  |
| A02 | 仅表授权及其他表拒绝 | B | 一允许一拒绝 |  |  |  |
| A03 | 黑名单覆盖库白名单 | A | 拒绝/BLACKLISTED |  |  |  |
| A04 | 批量 AND 语义 | A | 总体拒绝 |  |  |  |
| A05 | 类型与动作隔离 | B | 拒绝 |  |  |  |
| A06 | Doris/Hive API | A | 允许 |  |  |  |
| A07 | app_username 不合并 | C | 拒绝 |  |  |  |
| E01 | 页面库级授权 | A | 允许 |  |  |  |
| E02 | 页面表级授权 | B | 一允许一拒绝 |  |  |  |
| E03 | 页面黑名单 | A | 拒绝/BLACKLISTED |  |  |  |
| E04 | 页面 JOIN 批量鉴权 | A | 一拒绝一允许 |  |  |  |
| E05 | 临时视图底表鉴权 | A | 一允许一拒绝 |  |  |  |
| E06 | Doris/Hive 类型 | A | 允许 |  |  |  |
| E07 | read/write 隔离 | A/B | 一允许一拒绝 |  |  |  |
| E08 | RMS 用户隔离 | A/C | 一允许一拒绝 |  |  |  |
| E09 | 两种 Session 模式 | A | 结果一致 |  |  |  |
| E10 | 无资源 SQL 边界 | C | SQL 成功但不代表有权限 |  |  |  |
| E11 | 未知 Catalog | 任意 | 默认拒绝 |  |  |  |

发布准入要求：A01-A07、E01-E10 全部通过；E11 与计划启用的 Catalog 行为一致；至少完成一次
ODEP 不可达的 fail-closed 验证。任何拒绝用例只在数据源执行后才报错，都视为不通过。

## 已知边界

- 本功能是 SQL 物理执行前的 Engine 资源鉴权，不负责 RMS 登录认证和页面密码校验。
- `database` 使用 SQL 可见的逻辑 alias。ODEP 路由到 `physicalNamespace` 后不能改用物理库名鉴权。
- 当前只识别单层 namespace；多层 namespace 默认拒绝。
- 当前不对 `SHOW NAMESPACES`、`SHOW TABLES` 等元数据枚举做库表鉴权。
- `SELECT 1` 等无外部资源查询不会解析 Kyuubi 用户签名，也不会调用 ODEP。
- 静态 `mysql_static`、`doris_static` Catalog 当前不在允许列表，会默认拒绝。
- ODEP 不缓存权限判定；RMS 侧是否即时生效取决于 RMS SDK/资源缓存刷新机制。
- 权限接口只读取 `getPlatformUserResourceFormat(subject)`，不合并 `app_username`。
