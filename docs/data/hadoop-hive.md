# HDFS And Hive

QueryOne 对接 HDFS/Hive 的原则：

- 使用当前 Maven 依赖的 Spark `3.3.4` / Scala `2.12.15` runtime。
- 只读取测试环境的 Hadoop/Hive XML 配置，不把本地 Spark `3.3.4` / Scala `2.13` jar 目录塞进进程 classpath。
- HDFS 文件访问走 Spark/Hadoop 配置。
- Hive 表访问走 Spark 内置 Hive catalog 与 metastore client。

本机测试环境：

`conf/queryone.conf` 中至少配置 local 引擎的 Hadoop/Hive XML：

```hocon
engines {
  local {
    type = "local"

    hadoop {
      confDir = "/Users/qindongliang/bigdata/hadoop/etc/hadoop"
      groupStaticOverrides = "odep=odep"
    }

    hive {
      enabled = true
      confFile = "/Users/qindongliang/bigdata/hive/conf/hive-site.xml"
    }
  }
}
```

如果启动前手动拿票据：

```bash
export KRB5CCNAME=/tmp/krb5cc_$(id -u)
kinit -kt /Users/qindongliang/bigdata/odep.keytab odep@HADOOP.COM

mvn -pl queryone-server exec:java \
  -Dexec.mainClass=ai.queryone.server.QueryOneServer \
  -Dexec.args="--conf conf/queryone.conf"
```

也可以让 QueryOne 直接用 keytab 登录：

```hocon
engines {
  local {
    type = "local"

    hadoop {
      confDir = "/Users/qindongliang/bigdata/hadoop/etc/hadoop"
      groupStaticOverrides = "odep=odep"
    }

    hive {
      enabled = true
      confFile = "/Users/qindongliang/bigdata/hive/conf/hive-site.xml"
    }

    spark {
      kerberos {
        principal = "odep@HADOOP.COM"
        keytab = "/Users/qindongliang/bigdata/odep.keytab"
      }
    }

    kerberos {
      krb5Conf = "/etc/krb5.conf"
    }
  }
}
```

```bash
mvn -pl queryone-server exec:java \
  -Dexec.mainClass=ai.queryone.server.QueryOneServer \
  -Dexec.args="--conf conf/queryone.conf"
```

或者使用类似 Spark 的程序参数：

```bash
mvn -pl queryone-server exec:java \
  -Dexec.mainClass=ai.queryone.server.QueryOneServer \
  -Dexec.args="--hive-enabled \
    --hadoop-conf-dir /Users/qindongliang/bigdata/hadoop/etc/hadoop \
    --hive-conf /Users/qindongliang/bigdata/hive/conf/hive-site.xml \
    --principal odep@HADOOP.COM \
    --keytab /Users/qindongliang/bigdata/odep.keytab \
    --krb5-conf /etc/krb5.conf"
```

推荐 IDEA 使用本地 HOCON 配置：

```bash
cp conf/queryone.conf.template conf/queryone.conf
```

应用启动时会默认读取存在的 `conf/queryone.conf`。IDEA 的 Program arguments 可以留空；如果要显式指定配置文件，可以填写：

```bash
--conf conf/queryone.conf
```

验证 HDFS：

```sql
load csv.`imports/users.csv`
options header="true"
as users;

select * from users limit 10;
```

测试文件应预先放到 `/public/odep/user/${username}/imports/users.csv`。`load` 接受 workspace 相对路径；原生文件 relation 可以读取经 RMS 授权的绝对 HDFS 路径。

验证 Hive metastore：

```sql
show namespaces in hive;
show tables in hive.default;
select * from hive.default.some_table limit 10;

load hive.`default.some_table` as t;
select * from t limit 10;
```

验证 Hive 表写入。请先通过平台外 Hive/catalog 管理入口创建 `default.queryone_hive_test(id int, name string)`；下面只是在 QueryOne 中执行的 SQL：

```sql
view hive_write_test as
select * from values
  (1, 'alice'),
  (2, 'bob')
as hive_write_test(id, name);

save append hive_write_test as hive.`default.queryone_hive_test`;

select * from hive.default.queryone_hive_test limit 10;
```

Hive Catalog overwrite 被固定能力矩阵永久拒绝：

```sql
save overwrite hive_write_test as hive.`default.queryone_hive_test`;
```

该语句应在编译阶段失败。Hive 写入当前只允许 `save append`；runtime 在校验源和目标列名集合后生成带显式目标列清单和源列投影的 `INSERT INTO TABLE`，当前基线为 Spark 3.3.4。建表、表格式和分区定义必须由平台外治理入口完成。

常见认证错误：

```text
SIMPLE authentication is not enabled. Available: [TOKEN, KERBEROS]
```

这说明当前进程按 SIMPLE 身份访问了启用 Kerberos 的 HDFS/Hive。优先检查：

- `conf/queryone.conf` 是否存在，或启动参数是否显式包含 `--conf conf/queryone.conf`。
- IDEA 的 Working directory 是否是 `/Users/qindongliang/project/ai/query-one`，否则默认配置文件路径会找不到。
- `engines.local.hadoop.confDir` 是否能读到 `core-site.xml`，其中应包含 `hadoop.security.authentication=kerberos`。
- `engines.local.hive.enabled=true` 且 `engines.local.hive.confFile` 指向正确的 `hive-site.xml`。
- `engines.local.spark.kerberos.principal/keytab` 和 `engines.local.kerberos.krb5Conf` 是否正确，principal 建议使用 keytab 里的完整主体名，例如 `odep@HADOOP.COM`；或者启动前是否已经手动 `kinit`。
- IDEA 控制台是否能看到 `Refreshed Hadoop UserGroupInformation from SparkContext HadoopConf`，且 `UGI security enabled after SparkContext start: true`。

配置入口：

- HOCON：`engines.local.hadoop.confDir`、`engines.local.hadoop.confFiles`、`engines.local.hadoop.groupStaticOverrides`
- HOCON：`engines.local.hive.enabled`、`engines.local.hive.confFile`、`engines.local.hive.confDir`
- HOCON：`engines.local.spark.kerberos.principal`、`engines.local.spark.kerberos.keytab`、`engines.local.kerberos.krb5Conf`
- 启动参数：`--hadoop-conf-dir`、`--hadoop-conf-files`、`--hive-conf`、`--hive-conf-dir`、`--principal`、`--keytab`、`--krb5-conf`
- 环境变量仍可作为 local Spark runtime 的兜底：`HADOOP_CONF_DIR`、`QUERYONE_HADOOP_CONF_FILES`、`QUERYONE_HADOOP_GROUP_STATIC_MAPPING_OVERRIDES`、`QUERYONE_HIVE_ENABLED`、`QUERYONE_HIVE_CONF_FILE`、`HIVE_CONF_DIR`

注意：

- macOS 上 `kinit` 默认可能写入 KCM ticket cache，Java/Hadoop 不一定能识别；推荐显式设置 `KRB5CCNAME=/tmp/krb5cc_$(id -u)`。
- 本地 macOS 没有 Kerberos 用户对应的系统账号时，Hadoop 默认组解析会执行类似 `id odep` 并打印 WARN。`conf/queryone.conf` 可配置 `engines.local.hadoop.groupStaticOverrides = "odep=odep"` 绕开本机 Unix 组查询；如果没有显式配置，QueryOne 会根据 Kerberos principal 自动补一条 short name 映射。
- 如果 Hive metastore 版本或协议不兼容，优先调整 Spark 3.3.4 的 Hive metastore client 配置，不引入其他 Spark 版本的 JAR。
- 程序会把 XML 配置转换成 `spark.hadoop.*` 注入 Spark，并用加载到的 HadoopConf 初始化 `UserGroupInformation`。
- Maven Central 的 Spark 3.3.4 默认传递 Hadoop client `3.3.2`；测试和生产 Kyuubi Engine 使用公司构建的 Spark 3.3.4 / Hadoop 2.8.5 `SPARK_HOME`。不要把两套 Hadoop 客户端 JAR 混在同一个 Engine classpath；集群 SQL 通过 Kyuubi Engine 验证，Local runtime 只用于开发测试。

如果 `SparkContext.hadoopConfiguration` 里是 `kerberos`，但 `UserGroupInformation.isSecurityEnabled` 是 `false`，HDFS RPC 仍会按 SIMPLE 发起。QueryOne 会在 `SparkSession.getOrCreate()` 后用 SparkContext 的 HadoopConf 重新刷新 UGI 并重新 keytab 登录，避免 Spark/Hive 初始化过程把 UGI 全局配置重置回 SIMPLE。
