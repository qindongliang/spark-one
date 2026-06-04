# SQL 编辑器测试手册

这个页面是 SparkOne MVP 的本地测试台，用来快速验证 Spark SQL、SparkOne 薄 DSL 转译、HDFS/Hive 配置和数据源读写。

访问地址通常是：

```text
http://127.0.0.1:7070
```

## 页面区域

- 左侧编辑器：输入一段 SQL 脚本，可以包含多条语句，用分号 `;` 分隔。
- `Compile`：只编译，不执行。适合检查 `load/save` 这类 SparkOne DSL 被转成了什么 Spark SQL。
- `Run`：编译后按顺序执行每条 SQL，后面的语句可以使用前面创建的临时视图。
- `Rows`：控制每条查询最多展示多少行，服务端会限制在 `1` 到 `1000`。
- 右侧结果区：展示每条语句的编译后 SQL、耗时、schema 和结果数据；失败语句会显示错误信息。

## 基础冒烟测试

最小 SQL：

```sql
select 1 as id;
```

查看当前 catalog：

```sql
show databases;
show tables;
```

如果已经启用 Hive，可以指定库：

```sql
show tables in default;
```

## 多语句上下文

同一次 `Run` 内，多条 SQL 会在同一个 `SparkSession` 中顺序执行。因此可以先创建临时视图，再查询它：

```sql
create or replace temporary view users as
select * from values
  ('beijing', 1),
  ('shanghai', 2),
  ('beijing', 3)
as users(city, id);

create or replace temporary view city_stats as
select city, count(*) as cnt
from users
group by city;

select * from city_stats order by city;
```

页面服务不重启时，临时视图会留在当前本地 Spark 会话里；服务重启后临时视图会消失。

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

## 使用 SparkOne Load DSL

`load` 是 SparkOne 提供的薄 DSL，目的是让加载数据更接近 MLSQL 写法。推荐先点 `Compile` 看转译结果。

CSV：

```sql
load csv.`/tmp/users.csv`
where header="true" and inferSchema="true"
as users;

select * from users limit 20;
```

它会编译成类似：

```sql
CREATE OR REPLACE TEMPORARY VIEW users
USING csv
OPTIONS (path '/tmp/users.csv', header 'true', inferSchema 'true');
```

Parquet：

```sql
load parquet.`/tmp/users_parquet` as users_parquet;

select * from users_parquet limit 20;
```

JSON：

```sql
load json.`/tmp/events.json` as events;

select * from events limit 20;
```

Hive 表：

```sql
load hive.`default.some_table` as some_table;

select * from some_table limit 20;
```

`hive` 是特殊 catalog 语义，会编译成：

```sql
CREATE OR REPLACE TEMPORARY VIEW some_table AS
SELECT * FROM default.some_table;
```

## 使用 SparkOne Save DSL

当前 MVP 只支持 `save overwrite`。它会转成 Spark SQL 的 `INSERT OVERWRITE DIRECTORY`。

保存成 Parquet：

```sql
create or replace temporary view city_stats as
select city, count(*) as cnt
from users
group by city;

save overwrite city_stats as parquet.`/tmp/city_stats_parquet`;
```

保存成 CSV：

```sql
save overwrite city_stats as csv.`/tmp/city_stats_csv`
where header="true";
```

当前不支持：

```sql
save append city_stats as parquet.`/tmp/city_stats_parquet`;
save ignore city_stats as parquet.`/tmp/city_stats_parquet`;
save errorifexists city_stats as parquet.`/tmp/city_stats_parquet`;
```

这些模式会在编译阶段报错，后续需要时再扩展 compiler。

## HDFS 和 Hive 测试

如果使用 `conf/sparkone.toml` 配置了 Hadoop/Hive/Kerberos，页面里可以直接写 HDFS 路径或 Hive 表。

HDFS CSV：

```sql
load csv.`hdfs:///tmp/users.csv`
where header="true" and inferSchema="true"
as users;

select * from users limit 20;
```

Hive：

```sql
show databases;
show tables in default;

load hive.`default.some_table` as t;
select * from t limit 20;
```

如果遇到认证、权限、NameNode 或 Hive metastore 错误，优先检查启动配置，而不是 SQL 编辑器本身。相关配置见 [hadoop-hive.md](hadoop-hive.md) 和 [startup.md](startup.md)。

## Excel 测试

`excel` 当前只是 provider 别名，主包不内置 Excel connector。要测试 Excel，启动时必须提供对应 provider jar 或 Maven package，例如在 `conf/sparkone.toml` 中配置：

```toml
[jars]
packages = "dev.mauch:spark-excel_2.12:3.5.6_0.31.2"
```

然后页面里可以写：

```sql
load excel.`/tmp/users.xlsx`
where header="true" and inferSchema="true"
as users_excel;

select * from users_excel limit 20;
```

如果 provider 没加载，`Compile` 可能成功，但 `Run` 会失败，因为真正解析 provider 的是 Spark runtime。

## Compile 和 Run 的使用建议

- 写普通 Spark SQL 时，通常直接点 `Run`。
- 写 `load/save` 时，先点 `Compile` 确认转译出来的 Spark SQL 符合预期，再点 `Run`。
- 多条语句调试时，先把前置建表/建视图语句和最后查询语句放在同一次脚本里。
- 查询大表时先加 `limit`，并把页面右上角 `Rows` 控制在较小值。
- 保存数据前先用 `select count(*)` 或抽样查询确认临时视图内容。

## 常见问题

`Run` 成功但没有表格结果：

- DDL、`CREATE VIEW`、`INSERT OVERWRITE DIRECTORY` 等语句本身可能没有业务行数据返回，这是正常现象。

`Compile` 成功但 `Run` 失败：

- 常见原因是文件路径不存在、HDFS/Hive 权限不足、provider jar 未加载，或 Spark runtime 不支持对应数据源。

`load hive...` 带 options 报错：

- 当前 `hive` 是 catalog 表语义，不支持 `where/options` 参数。

临时视图查不到：

- 确认创建视图和查询视图在同一个服务进程内执行。
- 如果刚重启过服务，需要重新执行创建视图语句。

SQL 里有注释：

```sql
-- 单行注释
select 1;

/* 块注释 */
select 2;
```

注释会被 SparkOne DSL parser 忽略，不会作为独立语句执行。
