# Compiler

SparkOne compiler 的原则是：只解析 SparkOne 自己的薄 DSL，不解析 Spark SQL。

当前 DSL：

```sql
load parquet.`/tmp/users` as users;
load hive.`default.users` as hive_users;
load hive.`default.users` where "dt = date '2026-06-17'" as hive_users_0617;
load excel.`/tmp/users.xlsx` options header="true" as users_excel;
load mysql.`analytics.users` as users_mysql;
load doris.`app.users` as users_doris;
load doris.`app.orders` where "biz_date = '2026-06-17'" as doris_orders;
set biz_date = "2026-06-17";
set start_date as select date_sub(current_date(), 1) as dt;
view city_stats as select city, count(*) as cnt from users group by city;
save overwrite users as parquet.`reports/users_out`;
save append city_stats as hive.`default.city_stats` partitionBy dt;
save append city_stats as mysql.`analytics.city_stats`;
save append city_stats as doris.`app.city_stats`;
```

编译结果示例：

```sql
CREATE OR REPLACE TEMPORARY VIEW users USING parquet OPTIONS (path '/tmp/users');
CREATE OR REPLACE TEMPORARY VIEW hive_users AS SELECT * FROM default.users;
CREATE OR REPLACE TEMPORARY VIEW hive_users_0617 AS SELECT * FROM default.users WHERE dt = date '2026-06-17';
CREATE OR REPLACE TEMPORARY VIEW users_excel USING excel OPTIONS (path '/tmp/users.xlsx', header 'true');
SELECT 'LOAD MYSQL' AS sparkone_action, 'users AS users_mysql' AS sparkone_target;
CREATE OR REPLACE TEMPORARY VIEW users_doris AS SELECT * FROM doris.app.users;
CREATE OR REPLACE TEMPORARY VIEW doris_orders AS SELECT * FROM doris.app.orders WHERE biz_date = '2026-06-17';
SELECT 'SET' AS sparkone_action, 'biz_date' AS sparkone_target;
SELECT 'SET' AS sparkone_action, 'start_date' AS sparkone_target;
CREATE OR REPLACE TEMPORARY VIEW city_stats AS select city, count(*) as cnt from users group by city;
SELECT 'SAVE CATALOG' AS sparkone_action, 'city_stats TO default.city_stats' AS sparkone_target;
SELECT 'SAVE MYSQL' AS sparkone_action, 'city_stats TO city_stats' AS sparkone_target;
SELECT 'SAVE CATALOG' AS sparkone_action, 'city_stats TO doris.app.city_stats' AS sparkone_target;
```

受控 HDFS overwrite 会先生成 `WritePlan` 并通过固定能力矩阵，但当前 staging executor 尚未开放，因此上面的 `reports/users_out` 会在 SQL 渲染阶段 fail closed，不会生成可执行 SQL。

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
- `set name = "literal"` 是 SparkOne 脚本变量，后续语句可用 `${name}` 引用；变量只在单次脚本运行内有效。
- `set name as select ...` 是 SQL 变量语法，会在 runtime 执行查询，取第一行第一列转成字符串后写入变量；纯 compile 接口不会执行 Spark 查询。
- SparkOne 只支持普通字面量变量和 `set name as select ...` SQL 变量；不复刻 MLSQL 的 `where type="sql"`、`type="shell"`、`type="conf"`、`defaultParam`、`scope`、`mode` 等运行时能力。
- 每条编译结果携带 `StatementIntent`。原生 SQL 只允许查询和 `SHOW/DESCRIBE/EXPLAIN/USE` 等只读命令；原生 DDL、DML、`SET/RESET` 和未识别 command 默认拒绝。
- SparkOne `load/view` 内部生成的 `CREATE TEMPORARY VIEW` 依靠受控 intent 执行；用户直接提交原生 `CREATE VIEW` 不会被放行。
- `SparkSqlValidator` 使用 `org.apache.spark.sql.execution.SparkSqlParser` 校验生成 SQL。
- 不要使用 `CatalystSqlParser` 作为最终校验器；它会拒绝部分 Spark SQL execution 层语法。
- 数据源映射集中在 `DataSourceResolver`，不要把 provider 别名和特殊 source 判断散落在 compiler 主流程。
- `load hive` 是 catalog 表读取语义，编译成 `CREATE ... AS SELECT * FROM db.table`；追加 `where "..."` 时编译成 `SELECT * FROM db.table WHERE ...`。
- `save ... as hive` 是 catalog 表写入语义；当前只允许 append。compiler 生成 `WritePlan` 和安全占位 SQL，runtime 取得两端 schema 后生成带显式目标列清单和源列投影的 `INSERT INTO TABLE`。
- `save ... partitionBy col1, col2` 只用于 catalog 表写入，runtime 最终渲染为 Spark SQL 动态分区 `PARTITION (col1, col2)`。
- local 引擎的 `load/save mysql.\`connection.table\`` 是薄 runtime adapter：连接信息从 HOCON 读取，编译展示安全占位 SQL，执行时使用 Spark JDBC reader/writer。
- Kyuubi 引擎的 `save mysql.\`catalog.database.table\`` 生成 MySQL Catalog `WritePlan`，复用远端 Spark JDBC Catalog；不接受 SQL `OPTIONS`，SQL 中不携带 URL、用户名或密码。
- `load mysql ... options partitionColumn="id"` 会在执行侧自动查询 `lowerBound/upperBound`；`where` 存在时按过滤后数据取边界，不存在时按原表取边界。`numPartitions` 默认 `10`，`fetchsize` 默认 `10000`。
- `load doris` 是 Spark Doris Catalog 语法糖：`load doris.\`db.table\` as t` 编译成 `CREATE ... AS SELECT * FROM doris.db.table`；追加 `where "..."` 时编译成 `SELECT * FROM doris.db.table WHERE ...`。
- `save ... as doris` 是 Spark Doris Catalog 表写入语义；当前只允许 append，和 Hive 共用延迟渲染的显式列写入逻辑。
- Hive、MySQL、Doris overwrite 由固定能力矩阵永久拒绝，不存在可以放开的 SQL option 或 HOCON 开关；`partitionBy` 不用于 Doris Catalog 写入。
- `save append` 写 Hive、MySQL、Doris 时要求目标表已存在；SparkOne 不自动建表，目标表和结构变更必须由平台外的 Hive/Doris/MySQL 管理入口完成。
- Hive、Doris、MySQL append 在执行前要求源和目标列名集合完全一致，并在写入前校验类型兼容；写入统一按目标列顺序投影，不按列位置映射，也不为缺失 nullable 列自动补 `NULL`。
- compiler 对每条 `save` 先生成携带逻辑租户、目标分类和执行类型的 `WritePlan`。Catalog 最终 SQL 延迟到 runtime 取得 schema 后渲染，Compile 接口只展示无副作用的安全占位 SQL。
- 已识别的文件 provider 只有相对路径可进入受控 HDFS 分类；所有文件 append 永久拒绝。绝对路径和 URI 均属于 external path，本地文件、S3、OSS 等 external path 的 append/overwrite 都永久拒绝。受控 HDFS overwrite 在 staging executor 就绪前继续 fail closed。
- `StatementPolicy` 在 compiler 统一出口使用 Spark `SparkSqlParser` 校验原生只读边界，因此 Local/Kyuubi 的 Compile 和 Run 行为一致。
- Doris 推荐直接使用标准 Spark SQL：`show namespaces in doris`、`select * from doris.db.table`；裸写 `show databases` 和 `db.table` 仍表示默认 Hive catalog。
- 不支持 `load/save jdbc`，避免账号密码和连接串散落在 SQL 里。
- `excel` 是外部 Spark DataSource provider，编译成 `USING excel`，由依赖注册 provider 短名。

ANTLR 文件：

- `sparkone-server/src/main/antlr4/ai/sparkone/sql/parser/SparkOneDsl.g4`

ANTLR 注意事项：

- Spark 3.5.x 的 SQL parser 使用 ANTLR `4.9.3` 生成。
- 项目必须保持 ANTLR runtime/plugin 为 `4.9.3`。
- 不要单独升级 ANTLR，否则可能导致 Spark `SqlBaseLexer` 反序列化 ATN 失败。
