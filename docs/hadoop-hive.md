# HDFS And Hive

SparkOne 对接 HDFS/Hive 的原则：

- 使用当前 Maven 依赖的 Spark `3.5.7` / Scala `2.12` runtime。
- 只读取测试环境的 Hadoop/Hive XML 配置，不把本地 Spark `3.3.4` / Scala `2.13` jar 目录塞进进程 classpath。
- HDFS 文件访问走 Spark/Hadoop 配置。
- Hive 表访问走 Spark 内置 Hive catalog 与 metastore client。

本机测试环境：

```bash
export KRB5CCNAME=/tmp/krb5cc_$(id -u)
kinit -kt /Users/qindongliang/bigdata/odep.keytab odep

HADOOP_CONF_DIR=/Users/qindongliang/bigdata/hadoop/etc/hadoop \
mvn exec:java \
  -Dexec.mainClass=ai.sparkone.server.SparkOneServer \
  -Dsparkone.hive.enabled=true \
  -Dsparkone.hive.conf.file=/Users/qindongliang/bigdata/hive/conf/hive-site.xml
```

也可以让 SparkOne 直接用 keytab 登录：

```bash
HADOOP_CONF_DIR=/Users/qindongliang/bigdata/hadoop/etc/hadoop \
mvn exec:java \
  -Dexec.mainClass=ai.sparkone.server.SparkOneServer \
  -Dsparkone.hive.enabled=true \
  -Dsparkone.hive.conf.file=/Users/qindongliang/bigdata/hive/conf/hive-site.xml \
  -Dsparkone.kerberos.principal=odep \
  -Dsparkone.kerberos.keytab=/Users/qindongliang/bigdata/odep.keytab
```

或者使用类似 Spark 的程序参数：

```bash
mvn exec:java \
  -Dexec.mainClass=ai.sparkone.server.SparkOneServer \
  -Dexec.args="--hive-enabled \
    --hadoop-conf-dir /Users/qindongliang/bigdata/hadoop/etc/hadoop \
    --hive-conf /Users/qindongliang/bigdata/hive/conf/hive-site.xml \
    --principal odep \
    --keytab /Users/qindongliang/bigdata/odep.keytab"
```

推荐 IDEA 使用本地 TOML 配置：

```bash
cp conf/sparkone.toml.template conf/sparkone.toml
```

IDEA 的 Program arguments 只需要：

```bash
--conf conf/sparkone.toml
```

验证 HDFS：

```sql
create or replace temporary view users
using csv
options (path 'hdfs:///tmp/users.csv', header 'true');

select * from users limit 10;
```

验证 Hive metastore：

```sql
show databases;
show tables in default;

load hive.`default.some_table` as t;
select * from t limit 10;
```

配置入口：

- `sparkone.hadoop.conf.dir` / `HADOOP_CONF_DIR`
- `sparkone.hadoop.conf.files` / `SPARKONE_HADOOP_CONF_FILES`
- `sparkone.hive.enabled` / `SPARKONE_HIVE_ENABLED`
- `sparkone.hive.conf.file` / `SPARKONE_HIVE_CONF_FILE`
- `sparkone.hive.conf.dir` / `HIVE_CONF_DIR`
- `sparkone.kerberos.principal` / `SPARKONE_KERBEROS_PRINCIPAL`
- `sparkone.kerberos.keytab` / `SPARKONE_KERBEROS_KEYTAB`

注意：

- macOS 上 `kinit` 默认可能写入 KCM ticket cache，Java/Hadoop 不一定能识别；推荐显式设置 `KRB5CCNAME=/tmp/krb5cc_$(id -u)`。
- 如果 Hive metastore 版本或协议不兼容，优先调整 Spark 3.5 的 Hive metastore client 配置，不引入 Spark 3.3.4 的 jar。
- 程序会把 XML 配置转换成 `spark.hadoop.*` 注入 Spark，并用加载到的 HadoopConf 初始化 `UserGroupInformation`。
