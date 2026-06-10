# Compiler

SparkOne compiler 的原则是：只解析 SparkOne 自己的薄 DSL，不解析 Spark SQL。

当前 DSL：

```sql
load parquet.`/tmp/users` as users;
load hive.`default.users` as hive_users;
load excel.`/tmp/users.xlsx` options header="true" as users_excel;
view city_stats as select city, count(*) as cnt from users group by city;
save overwrite users as parquet.`/tmp/users_out`;
```

编译结果示例：

```sql
CREATE OR REPLACE TEMPORARY VIEW users USING parquet OPTIONS (path '/tmp/users');
CREATE OR REPLACE TEMPORARY VIEW hive_users AS SELECT * FROM default.users;
CREATE OR REPLACE TEMPORARY VIEW users_excel USING excel OPTIONS (path '/tmp/users.xlsx', header 'true');
CREATE OR REPLACE TEMPORARY VIEW city_stats AS select city, count(*) as cnt from users group by city;
INSERT OVERWRITE DIRECTORY '/tmp/users_out' USING parquet SELECT * FROM users;
```

普通 Spark SQL 原样透传：

```sql
create or replace temporary view city_stats as
select city, count(*) as cnt
from users
group by city;
```

重要决策：

- 不支持尾部 `select ... as table` 这种自定义糖，避免跟 Spark 原生列别名、表别名冲突。
- 推荐使用 `view name as select ...` 语法糖，编译成 Spark 原生 `CREATE OR REPLACE TEMPORARY VIEW name AS SELECT ...`。
- `SparkSqlValidator` 使用 `org.apache.spark.sql.execution.SparkSqlParser` 校验生成 SQL。
- 不要使用 `CatalystSqlParser` 作为最终校验器；它会拒绝部分 Spark SQL execution 层语法。
- 数据源映射集中在 `DataSourceResolver`，不要把 provider 别名和特殊 source 判断散落在 compiler 主流程。
- `hive` 是 catalog 表语义，编译成 `CREATE ... AS SELECT * FROM db.table`。
- `excel` 是外部 Spark DataSource provider，编译成 `USING excel`，由依赖注册 provider 短名。

ANTLR 文件：

- `src/main/antlr4/ai/sparkone/sql/parser/SparkOneDsl.g4`

ANTLR 注意事项：

- Spark 3.5.x 的 SQL parser 使用 ANTLR `4.9.3` 生成。
- 项目必须保持 ANTLR runtime/plugin 为 `4.9.3`。
- 不要单独升级 ANTLR，否则可能导致 Spark `SqlBaseLexer` 反序列化 ATN 失败。
