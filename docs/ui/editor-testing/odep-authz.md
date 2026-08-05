# ODEP 库表与 HDFS 鉴权测试

本文用于验证 SparkOne 页面经 Kyuubi 提交 SQL 时，Spark Engine 是否使用当前 RMS 真实用户，
在物理执行前完成 JDBC、Doris、Hive 库表及 HDFS 路径权限检查。测试分为 ODEP API 独立验证和 SparkOne
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
- 当前用户自己的 managed HDFS load/overwrite 不调用 ODEP，路径固定在 `/public/odep/user/${subject}`。
- `load ... options owner="..."` 只扩展读取体验：跨 owner 时按解析后的绝对路径调用 RMS `hdfs read`，`owner` 不替代 subject。
- 原生绝对 HDFS/viewfs relation 只允许读取并走 RMS `hdfs read`；原生文件路径写入直接拒绝。
- `save options owner=...` 和跨 owner overwrite 拒绝，RMS HDFS write 权限不能扩大写入范围。
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

HDFS 请求只使用 `path` 和 `action`：

| SQL | ODEP 请求 | 说明 |
| --- | --- | --- |
| `load parquet.\`reports/daily\`` | 不调用 | subject 自己的 workspace |
| `load csv.\`t.csv\` options owner="firefly"` | `hdfs + /public/odep/user/firefly/t.csv + read` | subject 仍是 `qindongliang` |
| `select * from csv.\`/public/odep/user/firefly/t.csv\`` | `hdfs + /public/odep/user/firefly/t.csv + read` | 原生绝对路径只读 |
| `save overwrite ... parquet.\`reports/daily\`` | 不调用 | 只允许 subject 自己的 workspace |
| 任意原生文件路径写入 | 直接拒绝 | 不查询 RMS write 权限 |

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

HDFS 使用目录前缀权限：

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

HDFS 端到端测试还需准备：

- 当前用户使用 RMS 真实用户 `qindongliang`，自有测试目录是 `/public/odep/user/qindongliang/sparkone-authz-test/self-roundtrip`，脚本会创建或覆盖它，不需要 RMS 资源。
- 跨用户 workspace owner 使用 `firefly`，授权读取现有 CSV 目录 `/public/odep/user/firefly/t.csv`。
- 给 `qindongliang` 配置 `hdfs white:/public/odep/user/firefly/t.csv:read`，不要把权限配置给 `firefly` 后误认为当前用户会继承。
- 使用已存在的 `/public/odep/user/firefly/t1.csv` 作为拒绝用例，不给 `qindongliang` 配置该路径或能覆盖它的上级目录白名单。

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
sparkone-hdfs-overwrite-extension_2.12-0.1.0-SNAPSHOT.jar
```

`$KYUUBI_CONF_DIR/kyuubi-defaults.conf` 至少包含：

```properties
spark.jars                                  /opt/sparkone/sparkone-kyuubi-odep-plugin.jar,/opt/sparkone/sparkone-hdfs-overwrite-extension.jar,/opt/sparkone/sparkone-kyuubi-odep-authz-extension.jar
spark.sql.extensions                        ai.sparkone.extension.overwrite.SparkOneHdfsOverwriteExtensions,ai.sparkone.kyuubi.odep.authz.SparkOneOdepAuthzExtension
spark.sparkone.overwrite.workspaceRoot      /public/odep/user
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
mvn -pl sparkone-server,sparkone-kyuubi-odep-authz-extension -am test
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

## HDFS 完整手工测试示例

下面的案例使用当前测试集群中已经存在的两个真实 workspace。页面登录用户固定为 `qindongliang`，
跨用户 owner 固定为 `firefly`。手工案例和自动脚本共用同一组路径。

| 项目 | 固定示例值 |
| --- | --- |
| 页面登录 subject | `qindongliang` |
| 跨用户 workspace owner | `firefly` |
| 当前用户自有读写目录 | `/public/odep/user/qindongliang/sparkone-authz-test/self-roundtrip` |
| 已存在且待授权的 CSV 目录 | `/public/odep/user/firefly/t.csv` |
| 已存在但保持未授权的 CSV 目录 | `/public/odep/user/firefly/t1.csv` |

`qindongliang` 是 SparkOne 页面登录时使用的 RMS 真实用户名，也是 Engine 鉴权的 subject。
`firefly` 只表示目标路径所属的 workspace，不替代 subject。Kyuubi Spark Engine 仍由统一
Kerberos 账号运行。

### 确认现有 HDFS 数据

本地已经通过下面的只读命令确认两个 workspace 均存在。正式测试前再执行一次，防止历史目录被
其他任务清理或改写。

```bash
hadoop fs -ls /public/odep/user/qindongliang
hadoop fs -ls /public/odep/user/firefly
hadoop fs -ls /public/odep/user/firefly/t.csv
hadoop fs -ls /public/odep/user/firefly/t1.csv
```

`firefly/t.csv` 当前包含 `_SUCCESS` 和一个 CSV part 文件，文件内容如下。

```text
id
1
```

`firefly/t1.csv` 也包含 `_SUCCESS` 和一个 CSV part 文件，因此它是真实存在的拒绝夹具。两者当前
目录权限为 `drwxr-xr-x`，part 文件权限为 `-rw-r--r--`，Kyuubi 的统一账号具备物理读取条件。
RMS 仍会在 Spark 读取文件前决定是否允许访问。

不要向 `firefly/t.csv` 或 `firefly/t1.csv` 写入任何数据。当前用户写入测试只使用尚未存在的
`/public/odep/user/qindongliang/sparkone-authz-test`。如果拒绝目录消失，Spark 可能先返回
`PATH_NOT_FOUND`，此时不能证明 RMS 权限检查生效。

### 配置 RMS 资源

只给 `qindongliang` 增加下面一条资源。

| 资源类型 | 资源内容 |
| --- | --- |
| `hdfs` | `white:/public/odep/user/firefly/t.csv:read` |

不要给 `qindongliang` 配置 `/public/odep/user/firefly/t1.csv`，也不要配置
`white:/public/odep/user/firefly:read` 这类能覆盖两个目录的上级白名单。开始测试前应检查该用户
现有 HDFS 资源，确保 `t1.csv` 会稳定命中 `NO_MATCHING_RESOURCE`。

ODEP 的 HDFS 前缀匹配按路径段处理。`t.csv` 白名单只匹配该目录本身及其子路径，不会匹配同级的
`t1.csv`，因此这两个现有目录可以稳定组成允许和拒绝用例。

`qindongliang` 自己的 workspace 不需要 RMS HDFS 资源。Engine 通过可信 session subject 判断 workspace
ownership，并直接允许 managed load 和 managed overwrite。也不要添加 HDFS write 白名单，写权限
不会扩大 SparkOne 的写入范围。

授权完成后等待 RMS 资源缓存刷新。先调用 ODEP 接口验证授权路径和未授权路径。

```bash
scripts/tests/odep-authz-api.sh qindongliang allow \
  '[{"resourceType":"hdfs","path":"/public/odep/user/firefly/t.csv","action":"read"}]'

scripts/tests/odep-authz-api.sh qindongliang deny \
  '[{"resourceType":"hdfs","path":"/public/odep/user/firefly/t1.csv","action":"read"}]'
```

第一条应输出 `allowed=True reason=MATCHED`，第二条应输出
`allowed=False reason=NO_MATCHING_RESOURCE`。不要用该 API 直接检查 `qindongliang` 自己的 workspace
来判断 ownership，因为 ownership 放行发生在 Engine 内，Engine 不会为自有 managed 路径调用此接口。

### 登录页面并选择 Engine

打开 SparkOne 页面，以 `qindongliang` 登录，Engine 选择 `Kyuubi Local`。Session 建议选择
`Run isolated`，每个依赖临时视图的案例都在同一次 Run 中提交。先执行 `select 1 as id;` 确认
基础链路可用，这条 SQL 本身不触发权限检查。

### 自有 workspace 读写

一次 Run 执行下面整段 SQL。

```sql
view sparkone_hdfs_authz_seed as
select * from values
  (1L, 'qindongliang'),
  (2L, 'qindongliang')
as sparkone_hdfs_authz_seed(id, owner_name);

save overwrite sparkone_hdfs_authz_seed
as parquet.`sparkone-authz-test/self-roundtrip`;

load parquet.`sparkone-authz-test/self-roundtrip`
as sparkone_hdfs_authz_self;

select count(*) as row_count
from sparkone_hdfs_authz_self;
```

查询结果应为 `row_count=2`。最终目录应位于
`/public/odep/user/qindongliang/sparkone-authz-test/self-roundtrip`。Engine 日志应包含以下
ownership 放行记录，ODEP 不应收到这次 Run 的 HDFS 权限请求。

```text
Managed HDFS authorization allowed by workspace ownership, subject=qindongliang, action=write
Managed HDFS authorization allowed by workspace ownership, subject=qindongliang, action=read
```

### 使用 owner 读取其他用户 workspace

一次 Run 执行下面整段 SQL。

```sql
load csv.`t.csv`
options owner="firefly" and header="true" and inferSchema="true"
as sparkone_hdfs_authz_firefly_csv;

select id
from sparkone_hdfs_authz_firefly_csv;

select count(*) as row_count
from sparkone_hdfs_authz_firefly_csv;
```

结果应有一行 `id=1`，且 `row_count=1`。这里显式设置 `header="true"`，所以 CSV 第一行作为
列名，不计入数据行；`inferSchema="true"` 只影响字段类型推断，不影响行数。ODEP 收到的 subject
仍是 `qindongliang`，资源应为下面这一项。

```text
hdfs:/public/odep/user/firefly/t.csv:read
```

Engine 日志应包含以下记录。

```text
ODEP authorization allowed, subject=qindongliang, resourceCount=1
```

这条用例同时证明 `owner` 只改变 workspace 路径归属，不替换当前用户，也不会作为 CSV reader
option 下传。

### 使用原生 relation 读取任意已授权绝对路径

执行下面的原生 Spark SQL。

```sql
select count(*) as row_count
from csv.`/public/odep/user/firefly/t.csv`;
```

原生 CSV relation 没有设置 `header=true`，因此表头和值都会作为数据读取，结果应为 `row_count=2`。
ODEP 请求使用当前登录用户和规范化后的绝对路径。

```text
subject=qindongliang
hdfs:/public/odep/user/firefly/t.csv:read
```

再单独验证无 authority 的 HDFS URI。

```sql
select count(*) as row_count
from csv.`hdfs:///public/odep/user/firefly/t.csv`;
```

结果应为 `row_count=2`，发送给 ODEP 的 path 仍为
`/public/odep/user/firefly/t.csv`，不带 `hdfs://`。使用 ViewFS 的环境还可以把
URI 改为 `viewfs:///public/odep/user/firefly/t.csv` 验证同一行为；没有配置
ViewFS 的环境不执行该变体。

### 文件格式参数与 `count(*)` 口径

下面的差异属于 Spark reader 语义，与 ODEP/RMS 是否授权无关。测试时应固定参数，避免把解析差异
误判为鉴权或数据丢失问题。

| 格式 | 表头和 Schema 默认行为 | 对 `count(*)` 的影响 |
| --- | --- | --- |
| CSV | `header=false`，`inferSchema=false` | 未设置 `header=true` 时，表头作为普通数据行计数；目录包含多个带表头的 CSV 文件时，各文件的表头都可能被计入 |
| Parquet | 没有文本表头，Schema 来自文件元数据 | 只统计实际数据行；`header`、`inferSchema` 不适用 |
| Excel | 当前 `spark-excel 0.31.2` 默认 `header=true`、`inferSchema=false` | 默认不统计读取范围的首行；显式设置 `header=false` 时，首行作为数据计数 |

CSV 和 Excel 测试建议显式设置 `header`，不要依赖 provider 默认值。`inferSchema` 只决定字段类型，
不会增加或减少数据行。Excel 还应显式设置实际读取区域，例如：

```sql
load excel.`users.xlsx`
options header="true"
  and inferSchema="true"
  and dataAddress="'Sheet1'!A1"
as users_excel;

select count(*) as row_count
from users_excel;
```

`dataAddress` 的起始行必须是真正的表头行。如果 Sheet 顶部有标题或说明文字，应把起始位置调整到
实际表头；否则错误的首行会被当作列名，真正的表头可能进入数据并影响 `count(*)`。

### 拒绝未授权但真实存在的绝对路径

执行下面的原生 Spark SQL。

```sql
select *
from csv.`/public/odep/user/firefly/t1.csv`;
```

页面应在读取文件前返回下面的错误，不应附带 `NO_MATCHING_RESOURCE`。

```text
Resource access denied: hdfs:/public/odep/user/firefly/t1.csv:read
```

Engine 日志仍保留 RMS reason，便于排障。

```text
ODEP authorization denied, subject=qindongliang, resources=hdfs:/public/odep/user/firefly/t1.csv:read:NO_MATCHING_RESOURCE
```

如果页面得到两行数据，说明权限没有在物理读取前拦截。如果页面得到 `PATH_NOT_FOUND`，说明夹具
没有准备好。如果页面得到 HDFS `Permission denied`，说明先被 NameNode ACL 拦截，这三种结果都
不能记为本用例通过。

### 验证路径输入边界

下面每条语句分别点击 Compile，均应失败。

```sql
load csv.`/public/odep/user/firefly/t.csv`
as invalid_absolute_load;

load csv.`../firefly/t.csv`
as invalid_traversal_load;

select * from csv.`t.csv`;

select * from csv.`hdfs://nameservice1/public/odep/user/firefly/t.csv`;

select * from csv.`file:///tmp/t.csv`;

select * from csv.`s3a://bucket/t.csv`;
```

前两条应包含 `LOAD managed HDFS requires a relative tenant workspace path`。后四条应包含
`only supported file providers with an absolute HDFS path are allowed`。这组用例确认以下边界。

- `load` 只接受受控 workspace 相对路径，跨用户读取必须显式使用 `options owner`。
- 原生文件 relation 只接受 `/absolute/path`、`hdfs:///absolute/path` 或
  `viewfs:///absolute/path`。
- 相对 relation、带 authority 的 URI、本地文件和对象存储路径不会进入 RMS 鉴权，直接编译拒绝。

### 验证所有文件写入边界

先验证 `save owner` 不能把数据写入其他用户 workspace。

```sql
view sparkone_hdfs_authz_save_owner as select 1L as id;

save overwrite sparkone_hdfs_authz_save_owner
as parquet.`sparkone-authz-test/save-owner-denied`
options owner="firefly";
```

Compile 应失败并包含以下错误。

```text
SAVE managed HDFS option is not allowed: owner
```

再验证 SparkOne 页面不接受原生文件路径写入。

```sql
insert overwrite directory '/public/odep/user/qindongliang/sparkone-authz-test/native-write-denied'
using parquet
select 1L as id;
```

Compile 应失败并包含 `only allows native read-only SQL`，ODEP 不应收到 HDFS write 请求。

最后在隔离测试环境直连 Kyuubi，验证绕过 SparkOne 编译器时 Engine 仍会拦截原生写入。

```bash
$KYUUBI_HOME/bin/beeline \
  -u 'jdbc:kyuubi://kyuubi-host:10009/default' \
  -n qindongliang \
  -e "insert overwrite directory '/public/odep/user/qindongliang/sparkone-authz-test/direct-write-denied' using parquet select 1L as id"
```

预期在写文件前返回 `Native HDFS path writes are disabled`。即使临时给该路径增加 RMS HDFS
write 白名单，结果也必须保持拒绝，因为原生文件写入不会调用 ODEP。

## HDFS 端到端自动测试

先确认跨 owner 的共享目录已存在、当前 RMS 用户拥有该目录的 `hdfs read` 权限，并选择一个明确未授权的绝对路径。然后在仓库根目录执行：

```bash
SPARKONE_USERNAME=qindongliang \
SPARKONE_ENGINE=kyuubi_local \
SPARKONE_SHARED_OWNER=firefly \
SPARKONE_SHARED_RELATIVE_PATH=t.csv \
SPARKONE_SHARED_ABSOLUTE_PATH=/public/odep/user/firefly/t.csv \
SPARKONE_DENIED_ABSOLUTE_PATH=/public/odep/user/firefly/t1.csv \
SPARKONE_SHARED_FORMAT=csv \
scripts/tests/kyuubi-hdfs-authz.sh
```

共享数据不是 Parquet 时，通过 `SPARKONE_SHARED_FORMAT=csv` 等参数指定 provider；原生绝对路径默认由 `/public/odep/user/${owner}/${relativePath}` 拼出，也可以用 `SPARKONE_SHARED_ABSOLUTE_PATH` 指向 workspace 外的共享目录。完整参数见：

```bash
scripts/tests/kyuubi-hdfs-authz.sh --help
```

脚本通过与页面相同的登录和 `/api/run` 接口依次验证：

| 编号 | 场景 | 预期 |
| --- | --- | --- |
| H01 | 当前用户 managed overwrite 后 managed load | 成功，ODEP 无 HDFS 请求 |
| H02 | `load ... options owner="firefly"` | 成功，ODEP 收到 firefly 目录的 read 请求 |
| H03 | 原生绝对 HDFS relation | 成功，ODEP 收到绝对路径 read 请求 |
| H04 | 原生未授权绝对路径 | 执行前拒绝，错误不附带 RMS reason |
| H05 | `save ... options owner="firefly"` | 编译拒绝 |
| H06 | SparkOne 原生路径写入 | 编译拒绝 |

H06 验证 SparkOne 入口的第一层限制。Engine 层对直连 Kyuubi 的原生文件写入还有独立拒绝规则，
完整命令见上一节。单元测试 `LogicalPlanResourceExtractorTest.rejectsNativeHdfsPathWrites` 也会直接
构造 Spark 写计划验证该规则。

## HDFS 测试数据清理

自动和手工用例通过后，先在 RMS 删除本节新增的
`white:/public/odep/user/firefly/t.csv:read`。确认下面的 `qindongliang` 目录只用于本次测试，再执行
精确路径清理。`firefly/t.csv` 和 `firefly/t1.csv` 是已有历史数据，不能删除或覆盖。

```bash
hadoop fs -rm -r /public/odep/user/qindongliang/sparkone-authz-test
```

HDFS 开启 Trash 时可以按集群策略恢复；关闭 Trash 时目录删除不可恢复，所以清理前必须再次核对
完整路径。

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
| H01 | 自有 workspace load/overwrite | qindongliang | 允许且不调用 ODEP |  |  |  |
| H02 | 跨 owner managed load | qindongliang | RMS read 允许 |  |  |  |
| H03 | 原生绝对 HDFS relation | qindongliang | RMS read 允许 |  |  |  |
| H04 | 未授权绝对路径 | qindongliang | 执行前拒绝 |  |  |  |
| H05 | save owner | qindongliang | 编译拒绝 |  |  |  |
| H06 | 原生路径写入 | qindongliang | SparkOne/Engine 均拒绝 |  |  |  |

发布准入要求：A01-A07、E01-E10、H01-H06 全部通过；E11 与计划启用的 Catalog 行为一致；至少完成一次
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
- HDFS RMS 资源只扩大读取范围，不扩大 SparkOne 的文件写入范围；用户不能用 write 白名单绕过 workspace ownership。
- `load owner` 只改变 workspace 路径归属，Kyuubi 签名 subject 仍是当前 RMS 用户。
