# Local Engine

Local engine 是 SparkOne 的本地开发测试台。它在 SparkOne 服务进程内启动 `SparkSession`，适合 IDEA 调试、编译器验证、前端冒烟和本机数据源实验，不是多租户生产运行时。

## 配置示例

```hocon
engines {
  default = "local"

  local {
    type = "local"
    enabled = true
    label = "Local"

    spark {
      master = "local[*]"
    }
  }
}
```

## Spark Submit 语义

Local engine 的 `jars` 配置对齐 Spark submit：

```hocon
engines {
  local {
    type = "local"

    jars {
      # 等价于 spark-submit --packages / spark.jars.packages
      packages = "dev.mauch:spark-excel_2.12:3.5.6_0.31.2"

      # 等价于 spark-submit --jars / spark.jars，用于依赖 jar
      jars = "/path/to/provider.jar,/path/to/another-provider.jar"

      # 等价于 spark-submit --files / spark.files，用于普通文件分发，不进入 classpath
      files = "/path/to/app.conf,/path/to/dict.txt"

      # spark.jars.repositories
      repositories = "https://repo1.maven.org/maven2"
    }
  }
}
```

注意：

- `spark.files` / `engines.local.jars.files` 只是把普通文件分发到 driver/executor 工作目录，不会自动加入 classpath，不能用于加载 Excel 这类 DataSource provider。
- `spark.jars` / `engines.local.jars.jars` 才用于 jar 分发；SparkOne 在嵌入式本地启动时会额外把本地 jar 注入当前 driver classloader，保证 Spark SQL 能发现 provider 短名。
- `local[*]` 下 SparkOne 会默认把 `spark.driver.host` 和 `spark.driver.bindAddress` 设为 `127.0.0.1`。非 local master 不会自动设置；YARN/client 模式如需显式配置，`driverHost` 必须是 executor 可访问的地址。

## 常用配置映射

- `spark.master` -> `engines.local.spark.master`
- `spark.driver.host` -> `engines.local.spark.driverHost`
- `spark.driver.bindAddress` -> `engines.local.spark.driverBindAddress`
- `spark.jars.packages` -> `engines.local.jars.packages`
- `spark.jars` -> `engines.local.jars.jars`
- `spark.files` -> `engines.local.jars.files`
- `spark.jars.repositories` -> `engines.local.jars.repositories`
- `sparkone.hadoop.conf.dir` / `HADOOP_CONF_DIR` -> `engines.local.hadoop.confDir`
- `sparkone.hadoop.conf.files` / `SPARKONE_HADOOP_CONF_FILES` -> `engines.local.hadoop.confFiles`
- `sparkone.hadoop.group.static.mapping.overrides` / `SPARKONE_HADOOP_GROUP_STATIC_MAPPING_OVERRIDES` -> `engines.local.hadoop.groupStaticOverrides`
- `sparkone.hive.enabled` / `SPARKONE_HIVE_ENABLED` -> `engines.local.hive.enabled`
- `sparkone.hive.conf.file` / `SPARKONE_HIVE_CONF_FILE` -> `engines.local.hive.confFile`
- `sparkone.hive.conf.dir` / `HIVE_CONF_DIR` -> `engines.local.hive.confDir`
- `spark.kerberos.principal` -> `engines.local.spark.kerberos.principal`
- `spark.kerberos.keytab` -> `engines.local.spark.kerberos.keytab`
- `java.security.krb5.conf` -> `engines.local.kerberos.krb5Conf`

## 相关文档

- 启动方式：[../ops/startup.md](../ops/startup.md)
- HDFS/Hive/Kerberos：[../data/hadoop-hive.md](../data/hadoop-hive.md)
- 数据源配置：[../data/datasources.md](../data/datasources.md)
- 写入安全：[../data/safe-save.md](../data/safe-save.md)
