# Compiler

SparkOne compiler 的原则是：只解析 SparkOne 自己的薄 DSL，不解析 Spark SQL。

当前 DSL：

```sql
load parquet.`datasets/users` as users;
load hive.`default.users` as hive_users;
load hive.`default.users` where "dt = date '2026-06-17'" as hive_users_0617;
load excel.`imports/users.xlsx` options header="true" as users_excel;
load jdbc.`search_prod.users` as users_jdbc;
load mysql.`analytics.users` as users_mysql;
load doris.`app.users` as users_doris;
load doris.`app.orders` where "biz_date = '2026-06-17'" as doris_orders;
set biz_date = "2026-06-17";
set start_date as select date_sub(current_date(), 1) as dt;
view city_stats as select city, count(*) as cnt from users group by city;
assert city_stats where "cnt > 0" message "城市统计存在空结果" on failure fail;
assert (
  select city, count(*) as cnt from users group by city
) where "cnt > 0" message "城市统计存在空结果" on failure stop;
save overwrite users as parquet.`reports/users_out`;
save append city_stats as hive.`default.city_stats` partitionBy dt;
save append city_stats as mysql.`analytics.city_stats`;
save append city_stats as doris.`app.city_stats`;
```

编译结果示例：

```sql
MANAGED HDFS LOAD
  tenant: <当前登录用户>
  view: users
  format: parquet
  source: datasets/users
  options: {}
CREATE OR REPLACE TEMPORARY VIEW hive_users AS SELECT * FROM spark_catalog.default.users;
CREATE OR REPLACE TEMPORARY VIEW hive_users_0617 AS SELECT * FROM spark_catalog.default.users WHERE dt = date '2026-06-17';
MANAGED HDFS LOAD
  tenant: <当前登录用户>
  view: users_excel
  format: excel
  source: imports/users.xlsx
  options: {header='true'}
CREATE OR REPLACE TEMPORARY VIEW users_jdbc AS SELECT * FROM jdbc.search_prod.users;
SELECT 'LOAD MYSQL' AS sparkone_action, 'users AS users_mysql' AS sparkone_target;
CREATE OR REPLACE TEMPORARY VIEW users_doris AS SELECT * FROM doris.app.users;
CREATE OR REPLACE TEMPORARY VIEW doris_orders AS SELECT * FROM doris.app.orders WHERE biz_date = '2026-06-17';
SELECT 'SET' AS sparkone_action, 'biz_date' AS sparkone_target;
SELECT 'SET' AS sparkone_action, 'start_date' AS sparkone_target;
CREATE OR REPLACE TEMPORARY VIEW city_stats AS select city, count(*) as cnt from users group by city;
SELECT * FROM city_stats WHERE NOT COALESCE((cnt > 0), FALSE);
SELECT * FROM (
  select city, count(*) as cnt from users group by city
) sparkone_assert_input WHERE NOT COALESCE((cnt > 0), FALSE);
SELECT 'SAVE CATALOG' AS sparkone_action, 'city_stats TO spark_catalog.default.city_stats' AS sparkone_target;
SELECT 'SAVE MYSQL' AS sparkone_action, 'city_stats TO city_stats' AS sparkone_target;
SELECT 'SAVE CATALOG' AS sparkone_action, 'city_stats TO doris.app.city_stats' AS sparkone_target;
```

受控 HDFS load 和 overwrite 都会编译为版本化内部命令。Local 或 Kyuubi 的 Spark extension 在 driver 内根据逻辑租户和相对路径解析最终 workspace 路径：load 注册临时视图；overwrite 先生成 `WritePlan` 并通过固定能力矩阵，再执行 ZK 排他、staging 写入和 HDFS rename 发布。内部命令不是新的用户 SQL 语法，Compile/Run API 会把 Base64 payload 转成可读的 `MANAGED HDFS LOAD/OVERWRITE` 摘要。

普通只读 Spark SQL 除 Hive 逻辑别名外原样透传：

```sql
with city_stats as (
  select city, count(*) as cnt
  from users
  group by city
)
select * from city_stats;
```

重要决策：

- 不支持尾部 `select ... as table` 这种自定义糖，避免跟 Spark 原生列别名、表别名冲突。
- 推荐使用 `view name as select ...` 语法糖，编译成 Spark 原生 `CREATE OR REPLACE TEMPORARY VIEW name AS SELECT ...`。
- `assert table where "<predicate>" message "<message>"` 检查已有结果表；`assert (<select>) where ...` 是一次性检查的内联语法糖。二者都编译成只读违规行查询：零条违规行通过，有违规行返回有限样本并停止脚本。
- `on failure fail|stop` 只改变违规行命中后的顶层任务结果，默认是 `fail`。`fail` 停止脚本并使 Run 失败；`stop` 停止脚本但使 Run 成功。违规查询自身的 SQL、连接或权限异常始终使 Run 失败。
- ANTLR 对内联 `assert` 只识别外层和嵌套括号边界，不解析 SELECT 语义；内层查询和谓词都由 Spark SQL parser 校验。完整语义见[数据质量 Assert](../data/assertions.md)。
- `set name = "literal"` 是 SparkOne 脚本变量，后续语句可用 `${name}` 引用；变量只在单次脚本运行内有效。
- `set name as select ...` 是 SQL 变量语法，会在 runtime 执行查询，取第一行第一列转成字符串后写入变量；纯 compile 接口不会执行 Spark 查询。
- SparkOne 只支持普通字面量变量和 `set name as select ...` SQL 变量；不复刻 MLSQL 的 `where type="sql"`、`type="shell"`、`type="conf"`、`defaultParam`、`scope`、`mode` 等运行时能力。
- 每条编译结果携带 `StatementIntent`。原生 SQL 只允许查询和 `SHOW/DESCRIBE/EXPLAIN/USE` 等只读命令；原生 DDL、DML、`SET/RESET` 和未识别 command 默认拒绝。
- SparkOne `load/view` 内部生成的临时视图依靠受控 intent 执行；用户直接提交原生 `CREATE VIEW` 不会被放行。
- 已识别文件 provider 的 `load` 只接受租户 workspace 相对路径；原生 SQL 或 `view` 中直接使用文件 provider relation 会被拒绝，必须先通过受控 `load ... as view` 注册临时视图。
- `SparkSqlValidator` 使用 `org.apache.spark.sql.execution.SparkSqlParser` 校验生成 SQL。
- 不要使用 `CatalystSqlParser` 作为最终校验器；它会拒绝部分 Spark SQL execution 层语法。
- 数据源映射集中在 `DataSourceResolver`，不要把 provider 别名和特殊 source 判断散落在 compiler 主流程。
- ODEP Catalog 表使用 `<provider>.<alias>.<table>` 三段式，`jdbc`、`doris` 顶层 Catalog 在 Spark Engine 内把 alias 路由到连接和 `physicalNamespace`；不增加四段式 SQL。
- `hive.db.table` 是 `spark_catalog.db.table` 的逻辑别名；compiler 只改写 Spark AST 确认的表引用和两段 namespace，不改写字符串、注释、列限定符或 `hive.table` 两段表名。
- `load hive` 是 catalog 表读取语义，编译成 `CREATE ... AS SELECT * FROM spark_catalog.db.table`；追加 `where "..."` 时编译成 `SELECT * FROM spark_catalog.db.table WHERE ...`。
- `save ... as hive` 是 catalog 表写入语义；当前只允许 append。compiler 生成 `WritePlan` 和安全占位 SQL，runtime 取得两端 schema 后生成带显式目标列清单和源列投影的 `INSERT INTO TABLE`。
- `save ... partitionBy col1, col2` 只用于 catalog 表写入，runtime 最终渲染为 Spark SQL 动态分区 `PARTITION (col1, col2)`。
- local 引擎的 `load/save mysql.\`connection.table\`` 是薄 runtime adapter：连接信息从 HOCON 读取，编译展示安全占位 SQL，执行时使用 Spark JDBC reader/writer。
- `load jdbc.\`alias.table\`` 是 ODEP JDBC 路由的只读语法糖；无 OPTIONS 时编译为 `SELECT * FROM jdbc.alias.table`。ODEP MySQL alias 带 `partitionColumn/lowerBound/upperBound/numPartitions/fetchsize` 时编译为 Engine 内的 `sparkone_mysql` provider 读取；不允许在 SQL 中传 `url/user/password/driver/dbtable/query`。`save jdbc` 当前不支持。
- Kyuubi 引擎的 `save mysql.\`catalog.database.table\`` 生成 MySQL Catalog `WritePlan`，复用远端 Spark JDBC Catalog；不接受 SQL `OPTIONS`，SQL 中不携带 URL、用户名或密码。
- `load mysql ... options partitionColumn="id"` 会在执行侧自动查询 `lowerBound/upperBound`；`where` 存在时按过滤后数据取边界，不存在时按原表取边界。`numPartitions` 默认 `10`，`fetchsize` 默认 `10000`。
- `load doris` 是 Spark Doris Catalog 语法糖：ODEP 模式下 `load doris.\`alias.table\` as t` 编译成 `CREATE ... AS SELECT * FROM doris.alias.table`；当前 Kyuubi 静态 Catalog 使用 `load doris.\`doris_static.db.table\``。
- `save ... as doris` 是 Spark Doris Catalog 表写入语义；当前只允许 append，和 Hive 共用延迟渲染的显式列写入逻辑。
- Hive、MySQL、Doris overwrite 由固定能力矩阵永久拒绝，不存在可以放开的 SQL option 或 HOCON 开关；`partitionBy` 不用于 Doris Catalog 写入。
- `save append` 写 Hive、MySQL、Doris 时要求目标表已存在；SparkOne 不自动建表，目标表和结构变更必须由平台外的 Hive/Doris/MySQL 管理入口完成。
- Hive、Doris、MySQL append 在执行前要求源和目标列名集合完全一致，并在写入前校验类型兼容；写入统一按目标列顺序投影，不按列位置映射，也不为缺失 nullable 列自动补 `NULL`。
- compiler 对每条 `save` 先生成携带逻辑租户、目标分类和执行类型的 `WritePlan`。Catalog 最终 SQL 延迟到 runtime 取得 schema 后渲染，Compile 接口只展示无副作用的安全占位 SQL。
- 已识别的文件 provider 只有相对路径可进入受控 HDFS load/overwrite；所有文件 append 永久拒绝。文件 load 的绝对路径和 URI 在编译期拒绝；本地文件、S3、OSS 等 external path 的 append/overwrite 也永久拒绝。受控 overwrite 缺少 ZK 或 Spark extension 配置时 fail closed。
- `StatementPolicy` 在 compiler 统一出口使用 Spark `SparkSqlParser` 校验原生只读边界，因此 Local/Kyuubi 的 Compile 和 Run 行为一致。
- ODEP 数据源推荐直接使用标准 Spark SQL：`show namespaces in jdbc`、`show tables in jdbc.alias`、`select * from jdbc.alias.table`；Doris 同理。当前静态数据源使用 `mysql_static.db.table`、`doris_static.db.table`，Hive 用户侧使用 `hive.db.table`。裸写 `show databases` 和 `db.table` 仍表示当前 Catalog。
- `excel` 是外部 Spark DataSource provider，编译成 `USING excel`，由依赖注册 provider 短名。

ANTLR 文件：

- `sparkone-server/src/main/antlr4/ai/sparkone/sql/parser/SparkOneDsl.g4`

ANTLR 注意事项：

- Spark 3.5.x 的 SQL parser 使用 ANTLR `4.9.3` 生成。
- 项目必须保持 ANTLR runtime/plugin 为 `4.9.3`。
- 不要单独升级 ANTLR，否则可能导致 Spark `SqlBaseLexer` 反序列化 ATN 失败。
