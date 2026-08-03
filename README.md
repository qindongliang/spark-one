# SparkOne SQL

SparkOne SQL is a SQL-first MVP inspired by MLSQL. It keeps the custom DSL
small and delegates Spark SQL syntax to Spark itself.

## Modules

Import the repository root `pom.xml` in IDEA. The root project is a Maven
aggregator and exposes four modules:

- `sparkone-server`: SparkOne Web/API/compiler/runtime.
- `sparkone-mysql-provider`: Spark datasource provider jar for Kyuubi/Spark engine.
- `sparkone-hdfs-overwrite-extension`: Spark driver extension for managed HDFS workspace reads and overwrite.
- `sparkone-kyuubi-odep-plugin`: Spark Engine routing catalog with lazy ODEP datasource resolution.

## Compiler Strategy

SparkOne only parses its own thin commands with ANTLR:

- `load <format>.\`<path>\` as <view>`
- `save overwrite <view> as <format>.\`<path>\``

Everything else is treated as native Spark SQL and validated with Spark's
`SparkSqlParser`; only queries and read-only inspection commands are allowed.

ANTLR is pinned to `4.9.3` because Spark 3.5.x generates its SQL parser with
that runtime. Do not upgrade ANTLR independently from Spark.

## Runtime

The MVP includes a local test service backed by `SparkSession` in `local[*]`
mode. It is intentionally not a multi-tenant runtime. The future Kyuubi adapter
should replace this runtime layer without changing the compiler.

## Example

```sql
view users as
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

## Run

Use the SDKMAN environment first. Spark on Java 17 also needs the JVM module
options in `.mvn/jvm.config`. More startup methods are documented in
[`docs/ops/startup.md`](docs/ops/startup.md).

```bash
sdk env
mvn test
mvn -pl sparkone-server exec:java -Dexec.mainClass=ai.sparkone.server.SparkOneServer
mvn -pl sparkone-server exec:java -Dexec.mainClass=ai.sparkone.server.SparkOneServer -Dexec.args="--conf conf/sparkone.conf"
```

Open:

```text
http://127.0.0.1:7070
```

SQL 编辑器测试方法见 [`docs/ui/editor-testing.md`](docs/ui/editor-testing.md)。

Use another port:

```bash
mvn -pl sparkone-server exec:java -Dexec.mainClass=ai.sparkone.server.SparkOneServer -Dexec.args=7071
mvn -pl sparkone-server exec:java -Dexec.mainClass=ai.sparkone.server.SparkOneServer -Dexec.args="--conf conf/sparkone.conf --port 7071"
```

Compile only:

```bash
curl -s http://127.0.0.1:7070/api/compile \
  -H 'Content-Type: application/json' \
  -d '{"script":"select 1 as id;","limit":10}'
```

Run SQL:

```bash
curl -s http://127.0.0.1:7070/api/run \
  -H 'Content-Type: application/json' \
  -d '{"script":"select 1 as id;","limit":10}'
```

## Local References

Reference repositories are available under `references/`:

- `references/mlsql`
- `references/kyuubi`
- `references/spark`
