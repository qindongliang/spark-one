# SQL 编辑器基础测试

这个页面是 SparkOne MVP 的本地测试台，用来快速验证 Spark SQL、SparkOne 薄 DSL 转译、HDFS/Hive 配置和数据源读写。

数据质量 `assert` 的完整用例单独见 [Assert 测试用例](../assertion-testing.md)。

访问地址通常是：

```text
http://127.0.0.1:7070
```

首次打开页面需要输入用户名。这个页面只在开发测试环境选择逻辑租户，不校验密码，也不代表生产身份认证。登录后刷新页面应保持当前 session；点击 `Log out` 后，编译、执行和预览接口都应返回未登录状态。

页面选择 Kyuubi 引擎时会显示 `Session` 下拉框，可以直接切换 `Tenant shared` 和 `Run isolated` 测试两种会话模式。Local 引擎不显示该控件。

## 页面区域

- 左侧编辑器：输入一段 SQL 脚本，可以包含多条语句，用分号 `;` 分隔。
- `Session`：只在 Kyuubi 引擎下显示。`Tenant shared` 表示同一租户共享会话、支持跨 Run 临时视图；`Run isolated` 表示每次 Run 使用独立会话，临时视图不能跨 Run，适合模拟定时任务。
- `Compile`：只编译，不执行。只有 `server.showCompiledSql = true` 时才显示，适合检查 `load/save` 这类 SparkOne DSL 被转成了什么 Spark SQL。
- `Run`：编译后按顺序执行每条 SQL，后面的语句可以使用前面创建的临时视图。
- `Preview`：在结果区的 Preview tab 里显示；对 `load ... as t`，先 `Run` 注册临时视图并展示 schema，再点该结果里的 `Preview` tab 显式拉取 `t` 的预览数据。
- 选中执行：如果编辑器里有选中的 SQL，`Compile` 和 `Run` 只处理选中部分；没有选区时处理整篇脚本。
- `Run` 默认隐藏每条 statement 的编译后 SQL；如果需要调试转译结果，在 `conf/sparkone.conf` 里配置 `server.showCompiledSql = true`。
- `Rows`：控制每条 statement 最多预览多少行；默认上限是 `preview.maxRows = 10`，页面输入只能小于或等于该 HOCON 上限。
- 默认结果 tab 由 `preview.defaultTab` 控制，可选 `schema` 或 `preview`；默认是 `schema`。
- 右侧结果区：展示每条语句的编译后 SQL、耗时、schema 和预览数据；schema 和预览数据通过 tab 切换，失败语句会显示错误信息。

## 基础冒烟测试

最小 SQL：

```sql
select 1 as id;
```

查看当前已实例化的 Catalog 和 Hive namespace：

```sql
show catalogs;
show namespaces in hive;
show tables in hive.default;
```

`hive` 是 SparkOne 对内置 `spark_catalog` 的逻辑别名。注意 Spark SQL 使用复数
`SHOW DATABASES`，不支持 `show database in hive`；测试手册统一使用等价且更清晰的
`SHOW NAMESPACES IN hive`。`SHOW CATALOGS` 不枚举全部配置，只列出当前 Session 已实例化
的 Catalog；Kyuubi 下的完整验证顺序见 [Kyuubi 数据源测试](kyuubi.md#catalog-配置与-show-catalogs-的差异)。

## 默认结果 Tab

在 `conf/sparkone.conf` 中可以控制运行结果默认展示 schema 还是 preview：

```hocon
preview {
  maxRows = 10
  defaultTab = "preview"
}
```

可选值：

- `schema`：默认展示字段结构，适合检查表结构和类型。
- `preview`：默认展示结果行；对 `load ... as t` 会自动加载一次 `t` 的预览数据。

改完配置后需要重启服务。可以用下面的 SQL 快速验证：

```sql
select 1 as id, 'beijing' as city;
```

## 多语句上下文

同一次 `Run` 内，多条 SQL 会在同一个 `SparkSession` 中顺序执行。因此可以先创建临时视图，再查询它：

```sql
view users as
select * from values
  ('beijing', 1),
  ('shanghai', 2),
  ('beijing', 3)
as users(city, id);

view city_stats as
select city, count(*) as cnt
from users
group by city;

select * from city_stats order by city;
```

页面服务不重启时，临时视图会留在当前本地 Spark 会话里；服务重启后临时视图会消失。

## Kyuubi Session 隔离

页面选择 Kyuubi 引擎后，可以用 `Session` 下拉框验证两种运行方式。

选择 `Tenant shared`，先单独 Run：

```sql
view editor_session_probe as
select 1 as id, 'shared' as mode;
```

再单独 Run：

```sql
select * from editor_session_probe;
```

第二次应查询成功，因为同一登录租户复用了 Kyuubi session。该模式仍允许同租户并发 Run，共享的是 connection/session，不是串行执行锁。

切换为 `Run isolated` 后，仍分两次执行上面的 SQL。第二次应提示找不到 `editor_session_probe`，因为每次 Run 都创建并关闭独立 Kyuubi session。把创建和查询放进同一次 Run 则应成功：

```sql
view isolated_session_probe as
select 1 as id, 'isolated' as mode;

select * from isolated_session_probe;
```

切回 Local 引擎后，`Session` 控件应隐藏，并按默认的 `tenant_shared` 请求值提交；该值不会改变 Local 引擎已有的 SparkSession 行为。

## 脚本变量 Set

SparkOne 支持脚本内变量，变量只在同一次 `Run` 内按顺序生效，后续语句用 `${name}` 引用。

普通字面量变量：

```sql
set biz_date = "2026-03-14";

select '${biz_date}' as dt;
```

SQL 变量使用 `set name as select ...`，执行时取查询结果第一行第一列作为变量值：

```sql
set start_date as select date_sub(date '2026-03-15', 1) as dt;
set end_date as select date '2026-03-15' as dt;

view source_events as
select * from values
  (1, timestamp '2026-03-14 10:00:00'),
  (2, timestamp '2026-03-15 00:00:00')
as source_events(id, create_time);

select *
from source_events
where create_time >= timestamp '${start_date}'
  and create_time < timestamp '${end_date}';
```

MySQL 增量加载可按同样方式拼接 `load mysql ... where` 的过滤条件：

```sql
set start_date as select date_sub(current_date(), 1) as dt;
set end_date as select current_date() as dt;

load mysql.`analytics.orders`
where "createTime >= '${start_date}' and createTime < '${end_date}'"
as orders_delta;
```

注意：

- `Compile` 只展示占位动作，不执行 SQL 变量查询；要看到 `${name}` 的运行时替换效果，请用 `Run`。
- `Run` 时 `set` 语句只更新变量，不展示内部 schema 和 preview；重点看后续业务查询语句的结果。
- SparkOne 不支持 MLSQL 的 `set name = \`select ...\` where type = "sql"` 写法；请使用 `set name as select ...`。

## Spark SQL 原生能力

普通 Spark SQL 会原样交给 Spark 执行。常用测试：

```sql
select current_date() as dt, current_timestamp() as ts;
```

```sql
select city, count(*) as cnt
from users
group by city
having count(*) > 1;
```

```sql
with city_stats as (
  select city, count(*) as cnt
  from users
  group by city
)
select * from city_stats order by cnt desc;
```

`WITH` 是查询内的临时 CTE，只在当前这一条 SQL 内有效；`CREATE OR REPLACE TEMPORARY VIEW` 会把结果注册到当前 Spark 会话，后续语句可以继续引用。

## View As 语法糖

SparkOne 支持 `view name as select ...` 语法糖，用于把查询结果注册成当前 Spark 会话里的临时视图，避免反复书写 `CREATE OR REPLACE TEMPORARY VIEW`：

```sql
view city_stats as
select city, count(*) as cnt
from users
group by city;

select * from city_stats order by cnt desc;
```

它等价于：

```sql
CREATE OR REPLACE TEMPORARY VIEW city_stats AS
select city, count(*) as cnt
from users
group by city;
```

注意：

- SparkOne 不再支持尾部 `select ... as table` 语法糖，避免跟 Spark 原生列别名、表别名产生歧义。
- `view myview as select current_date() as dt, current_timestamp() as ts` 会注册成 `myview`，其中 `as dt`、`as ts` 都是字段别名。
- `select * from users as u` 这种 Spark 原生表别名会保持原样；`u` 只是本条查询内的别名，不会注册成临时视图。
- 生成的目标统一使用 `CREATE OR REPLACE TEMPORARY VIEW`，表示只在当前 Spark 会话内有效。

原生别名和 `view` 语法糖可以混用：

```sql
view joined_orders as
select u.id, o.order_id
from users as u
join orders as o on u.id = o.user_id;
```

这里 `as u`、`as o` 是 Spark SQL 的表别名，`joined_orders` 才是 SparkOne 临时视图名。
