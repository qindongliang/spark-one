# SparkOne SQL

SparkOne SQL is a SQL-first MVP inspired by MLSQL. It keeps the custom DSL
small and delegates Spark SQL syntax to Spark itself.

## Compiler Strategy

SparkOne only parses its own thin commands with ANTLR:

- `load <format>.\`<path>\` as <view>`
- `save overwrite <view> as <format>.\`<path>\``

Everything else is treated as native Spark SQL, passed through unchanged, and
validated with Spark's `SparkSqlParser`.

ANTLR is pinned to `4.9.3` because Spark 3.5.x generates its SQL parser with
that runtime. Do not upgrade ANTLR independently from Spark.

## Runtime

The MVP includes a local test service backed by `SparkSession` in `local[*]`
mode. It is intentionally not a multi-tenant runtime. The future Kyuubi adapter
should replace this runtime layer without changing the compiler.

## Example

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

## Run

Use the SDKMAN environment first. Spark on Java 17 also needs the JVM module
options in `.mvn/jvm.config`. More startup methods are documented in
[`docs/startup.md`](docs/startup.md).

```bash
sdk env
mvn test
mvn exec:java -Dexec.mainClass=ai.sparkone.server.SparkOneServer
mvn exec:java -Dexec.mainClass=ai.sparkone.server.SparkOneServer -Dexec.args="--conf conf/sparkone.conf"
```

Open:

```text
http://127.0.0.1:7070
```

SQL 编辑器测试方法见 [`docs/editor-testing.md`](docs/editor-testing.md)。

Use another port:

```bash
mvn exec:java -Dexec.mainClass=ai.sparkone.server.SparkOneServer -Dexec.args=7071
mvn exec:java -Dexec.mainClass=ai.sparkone.server.SparkOneServer -Dexec.args="--conf conf/sparkone.conf --port 7071"
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
