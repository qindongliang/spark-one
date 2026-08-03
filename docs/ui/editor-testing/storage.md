# HDFS、Hive 和 Excel 测试

## HDFS 和 Hive 测试

如果使用 `conf/sparkone.conf` 配置了 Hadoop/Hive/Kerberos，Hive 表仍按 catalog 标识读取；HDFS 文件必须先放到当前租户的 `/public/sparkone/user/${username}` workspace，再在页面使用相对路径。

HDFS CSV：

```sql
load csv.`imports/users.csv`
options header="true" and inferSchema="true"
as users;

select * from users limit 20;
```

Hive：

```sql
show namespaces in hive;
show tables in hive.default;
select * from hive.default.some_table limit 10;

load hive.`default.some_table` as t;
select * from t limit 20;
```

如果遇到认证、权限、NameNode 或 Hive metastore 错误，优先检查启动配置，而不是 SQL 编辑器本身。相关配置见 [../../data/hadoop-hive.md](../../data/hadoop-hive.md) 和 [../../ops/startup.md](../../ops/startup.md)。

## Excel 测试

`excel` 当前只是 provider 别名，主包不内置 Excel connector。要测试 Excel，启动时必须提供对应 provider jar 或 Maven package，例如在 `conf/sparkone.conf` 中配置：

```hocon
engines {
  local {
    type = "local"

    jars {
      packages = "dev.mauch:spark-excel_2.12:3.5.6_0.31.2"
      # 或者直接指定本地 jar：
      # jars = "/Users/qindongliang/.m2/repository/dev/mauch/spark-excel_2.12/3.5.6_0.31.2/spark-excel_2.12-3.5.6_0.31.2.jar"
      # 如果只是分发普通配置文件，用 files，不要用来放 provider jar：
      # files = "/path/to/app.conf"
    }
  }
}
```

`packages` 使用 Maven 坐标，由 Spark/Ivy 解析依赖；`jars` 对应 Spark 原生 `spark.jars`，可以直接写本地 jar 的绝对路径。`files` 对应 Spark 原生 `spark.files`，只分发普通文件，不会加入 classpath，不能用来加载 Excel provider。

然后页面里可以写：

```sql
load excel.`imports/jupyter_tasks.xlsx`
options header="true" and inferSchema="true"
as users_excel;

select * from users_excel limit 20;
```

如果 provider 没加载，`Compile` 可能成功，但 `Run` 会失败，因为真正解析 provider 的是 Spark runtime。

如果启动 SparkContext 时出现 `Failed to connect to /192.168...` 且日志里有 `Added JAR ... at spark://.../jars/...`，通常是本地调试时 Spark driver 广播地址和实际绑定地址不一致。`conf/sparkone.conf` 的 `engines.local.spark` 建议保留：

```hocon
engines {
  local {
    type = "local"

    spark {
      driverHost = "127.0.0.1"
      driverBindAddress = "127.0.0.1"
    }
  }
}
```

这个配置只适合本地 `local[*]` 调试；如果改成 `master = "yarn"`，不要把 `driverHost` 固定为 `127.0.0.1`，应使用 executor 能访问到的 driver 地址，或交给 Spark/YARN 环境决定。
