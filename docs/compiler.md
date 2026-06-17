# Compiler

SparkOne compiler 的原则是：只解析 SparkOne 自己的薄 DSL，不解析 Spark SQL。

当前 DSL：

```sql
load parquet.`/tmp/users` as users;
load hive.`default.users` as hive_users;
load excel.`/tmp/users.xlsx` options header="true" as users_excel;
load mysql.`analytics.users` as users_mysql;
load doris.`app.users` as users_doris;
load doris.`app.orders` where "biz_date = '2026-06-17'" as doris_orders;
view city_stats as select city, count(*) as cnt from users group by city;
save overwrite users as parquet.`/tmp/users_out`;
save append city_stats as hive.`default.city_stats` partitionBy dt;
save append city_stats as mysql.`analytics.city_stats`;
```

编译结果示例：

```sql
CREATE OR REPLACE TEMPORARY VIEW users USING parquet OPTIONS (path '/tmp/users');
CREATE OR REPLACE TEMPORARY VIEW hive_users AS SELECT * FROM default.users;
CREATE OR REPLACE TEMPORARY VIEW users_excel USING excel OPTIONS (path '/tmp/users.xlsx', header 'true');
SELECT 'LOAD MYSQL' AS sparkone_action, 'users AS users_mysql' AS sparkone_target;
CREATE OR REPLACE TEMPORARY VIEW users_doris AS SELECT * FROM doris.app.users;
CREATE OR REPLACE TEMPORARY VIEW doris_orders AS SELECT * FROM doris.app.orders WHERE biz_date = '2026-06-17';
CREATE OR REPLACE TEMPORARY VIEW city_stats AS select city, count(*) as cnt from users group by city;
INSERT OVERWRITE DIRECTORY '/tmp/users_out' USING parquet SELECT * FROM users;
INSERT INTO TABLE default.city_stats PARTITION (dt) SELECT * FROM city_stats;
SELECT 'SAVE MYSQL' AS sparkone_action, 'city_stats TO city_stats' AS sparkone_target;
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
- `load hive` 是 catalog 表读取语义，编译成 `CREATE ... AS SELECT * FROM db.table`。
- `save ... as hive` 是 catalog 表写入语义，编译成 `INSERT INTO/OVERWRITE TABLE db.table SELECT * FROM source`。
- `save ... partitionBy col1, col2` 只用于 catalog 表写入，编译成 Spark SQL 动态分区 `PARTITION (col1, col2)`。
- `load/save mysql` 是薄 runtime adapter：连接信息从 HOCON 读取，编译展示安全占位 SQL，执行时使用 Spark JDBC reader/writer。
- `load doris` 是 Spark Doris Catalog 语法糖：`load doris.\`db.table\` as t` 编译成 `CREATE ... AS SELECT * FROM doris.db.table`；追加 `where "..."` 时编译成 `SELECT * FROM doris.db.table WHERE ...`。
- Doris 推荐直接使用标准 Spark SQL：`show namespaces in doris`、`select * from doris.db.table`；裸写 `show databases` 和 `db.table` 仍表示默认 Hive catalog。
- 不支持 `load/save jdbc`，避免账号密码和连接串散落在 SQL 里。
- `excel` 是外部 Spark DataSource provider，编译成 `USING excel`，由依赖注册 provider 短名。

ANTLR 文件：

- `src/main/antlr4/ai/sparkone/sql/parser/SparkOneDsl.g4`

ANTLR 注意事项：

- Spark 3.5.x 的 SQL parser 使用 ANTLR `4.9.3` 生成。
- 项目必须保持 ANTLR runtime/plugin 为 `4.9.3`。
- 不要单独升级 ANTLR，否则可能导致 Spark `SqlBaseLexer` 反序列化 ATN 失败。
