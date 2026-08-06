# MySQL 测试

本页验证统一后的 JDBC Catalog 语义。Local 和 Kyuubi 使用相同 SQL，差别只在 Catalog 配置归属：Local 从 SparkOne HOCON 注入，Kyuubi 从远端 Spark Engine 配置读取。

## 前置配置

Local 配置：

```hocon
engines.local {
  type = "local"

  catalogs.mysql_static {
    url = "jdbc:mysql://127.0.0.1:3306/?databaseTerm=SCHEMA"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "change-me"

    options {
      fetchsize = 1000
    }
  }
}
```

Kyuubi/Spark Engine 配置同名 Spark Catalog：

```properties
spark.sql.catalog.mysql_static=org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
spark.sql.catalog.mysql_static.url=jdbc:mysql://mysql.example:3306/?databaseTerm=SCHEMA
spark.sql.catalog.mysql_static.driver=com.mysql.cj.jdbc.Driver
spark.sql.catalog.mysql_static.user=reader
spark.sql.catalog.mysql_static.password=change-me
```

静态 Catalog 名必须以 `_static` 结尾。连接参数只能存在于可信配置，不能通过 SQL `OPTIONS` 下发。

## 直接查询

直接 `select` 使用 Spark 原生三段式：

```sql
show namespaces in mysql_static;
show tables in mysql_static.Dworks;

select *
from mysql_static.Dworks.cloud_host_info
limit 10;
```

这条路径不经过 RMS。`mysql_static` 是组件/Catalog，`Dworks` 是真实数据库，最后一段是表名。

## LOAD

无分区参数时，DSL 只生成同一个三段式 Catalog 查询：

```sql
load jdbc.`mysql_static.Dworks.cloud_host_info`
where "id > 0"
as mysql_hosts;

select * from mysql_hosts limit 10;
```

Compile 结果应类似：

```sql
CREATE OR REPLACE TEMPORARY VIEW mysql_hosts AS
SELECT * FROM mysql_static.Dworks.cloud_host_info WHERE id > 0
```

大表分区读取使用内部 `sparkone_mysql` provider：

```sql
load jdbc.`mysql_static.Dworks.cloud_host_info`
where "id > 0"
options partitionColumn="id"
and numPartitions="10"
and fetchsize="10000"
as mysql_hosts_big;

select count(*) from mysql_hosts_big;
```

只写 `partitionColumn` 时，provider 会先查询过滤结果的 `MIN/MAX` 作为边界。显式提供边界时必须同时提供 `lowerBound` 和 `upperBound`：

```sql
load jdbc.`mysql_static.Dworks.cloud_host_info`
options partitionColumn="id"
and lowerBound="1"
and upperBound="100000"
and numPartitions="16"
as mysql_hosts_bounded;
```

`lowerBound/upperBound` 只决定分区步长，不是业务过滤条件。过滤条件仍写在 `where` 中。

## SAVE

目标表必须由平台外 DDL 流程预先创建。SparkOne 只开放 append：

```sql
view mysql_save_source as
select 1 as id, 'alice' as name;

save append mysql_save_source
as jdbc.`mysql_static.Dworks.sparkone_user_result`;

select *
from mysql_static.Dworks.sparkone_user_result
where id = 1;
```

执行前会读取源和目标 schema，要求列名集合一致，并按目标列顺序生成 `INSERT INTO TABLE ... SELECT ...`。以下情况必须失败且不能写入：

- 目标表不存在。
- 源列缺失、重复或多出。
- 类型无法安全写入。
- 使用 `save overwrite`。
- 在 `save jdbc` 中携带 SQL `OPTIONS`。

## ODEP 动态 JDBC

动态路由继续使用 ODEP alias，第二段不是数据库名：

```sql
select * from jdbc.search_prod.orders limit 10;

load jdbc.`search_prod.orders`
where "status = 'ACTIVE'"
as active_orders;
```

ODEP 动态读取走 RMS 鉴权。MySQL alias 同样支持受控分区参数，但连接和真实数据库由 ODEP resolver 提供。

动态目标不开放写入：

```sql
save append active_orders as jdbc.`search_prod.target_orders`;
```

该语句应在 Compile 阶段提示 ODEP alias target 不支持；需要开发态写入时配置独立的 `_static` Catalog。

## 安全边界

以下选项在 `load jdbc` 中必须拒绝：

```sql
load jdbc.`mysql_static.Dworks.cloud_host_info`
options url="jdbc:mysql://other-host:3306/db"
as rejected_load;
```

禁止项包括 `url/user/password/driver/dbtable/query`。静态 Catalog 和内部静态 relation 不走 RMS；未知 V1 provider、非 `_static` 三段式 Catalog 和未知 V2 Catalog 仍 fail closed。
