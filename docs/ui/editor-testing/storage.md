# HDFS、Hive 和 Excel 测试

## HDFS 和 Hive 测试

如果使用 `conf/queryone.conf` 配置了 Hadoop/Hive/Kerberos，Hive 表仍按 catalog 标识读取；个人 HDFS 文件默认放到 `/public/odep/user/${username}` workspace，并在页面使用相对路径。读取其他用户 workspace 可以使用 `load ... options owner="..."`，读取任意已授权 HDFS 目录可以使用原生绝对路径 relation。Local 和 Kyuubi 共用 ODEP/RMS 资源规则：Local 使用开发态 `TenantContext` subject，Kyuubi 使用签名 session user；Local 配置来自 QueryOne HOCON，Kyuubi 配置来自远端 Spark Engine。

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

`excel` 当前只是 provider 别名，主包不内置 Excel connector。要测试 Excel，启动时必须提供对应 provider jar 或 Maven package，例如在 `conf/queryone.conf` 中配置：

```hocon
engines {
  local {
    type = "local"

    jars {
      packages = "dev.mauch:spark-excel_2.12:3.3.4_0.31.2"
      # 或者直接指定本地 jar：
      # jars = "/Users/qindongliang/.m2/repository/dev/mauch/spark-excel_2.12/3.3.4_0.31.2/spark-excel_2.12-3.3.4_0.31.2.jar"
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
options header="true"
  and inferSchema="true"
  and dataAddress="'Sheet1'!A1"
as users_excel;

select * from users_excel limit 20;

select count(*) as row_count from users_excel;
```

当前推荐的 `spark-excel 0.31.2` 默认使用 `header=true`、`inferSchema=false`。上例仍显式写出
`header`，保证读取范围第一行作为列名且不计入 `count(*)`；改为 `header=false` 后，第一行会作为
数据计数。`inferSchema` 只控制字段类型推断，不改变行数。

`dataAddress` 决定 Excel 的实际读取区域，起始行必须是真正的表头行。如果 Sheet 顶部还有标题、
说明或空行，应调整为实际表头的位置，例如 `"'Sheet1'!A3"`，否则选错的首行会被当作列名，
真正的表头可能进入数据并改变 `count(*)`。Excel 的默认行为与 CSV 不同：CSV 默认
`header=false`，未显式设置时会把表头作为普通数据计数；Parquet 没有文本表头，只统计实际数据行，
也不使用 `header` 或 `inferSchema` 参数。

如果 provider 没加载，`Compile` 可能成功，但 `Run` 会失败，因为真正解析 provider 的是 Spark runtime。

如果启动 SparkContext 时出现 `Failed to connect to /192.168...` 且日志里有 `Added JAR ... at spark://.../jars/...`，通常是本地调试时 Spark driver 广播地址和实际绑定地址不一致。`conf/queryone.conf` 的 `engines.local.spark` 建议保留：

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
