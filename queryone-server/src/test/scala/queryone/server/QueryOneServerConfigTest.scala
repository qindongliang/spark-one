package queryone.server

import queryone.extension.hdfs.{ManagedHdfsLoadProtocol, ManagedHdfsLoadRequest, ManagedHdfsOverwriteProtocol, ManagedHdfsOverwriteRequest}
import org.junit.Assert._
import org.junit.Test

import java.nio.file.{Files, Paths}

final class QueryOneServerConfigTest {
  private val LocalPrefix = "queryone.engine.local.local.property"

  @Test
  def rendersManagedHdfsOverwriteCommandForUi(): Unit = {
    val command = ManagedHdfsOverwriteProtocol.render(ManagedHdfsOverwriteRequest(
      tenant = "alice",
      sourceTable = "city_stats",
      format = "csv",
      relativePath = "reports/daily",
      options = Map("header" -> "true", "delimiter" -> "|")))

    val display = QueryOneServer.displaySql(command)

    assertEquals(
      """MANAGED HDFS OVERWRITE
        |  tenant: alice
        |  source: city_stats
        |  format: csv
        |  target: reports/daily
        |  options: {delimiter='|', header='true'}""".stripMargin,
      display)
    assertFalse(display.contains(command.split(" ").last))
    assertEquals("select 1", QueryOneServer.displaySql("select 1"))
  }

  @Test
  def rendersManagedHdfsLoadCommandForUi(): Unit = {
    val command = ManagedHdfsLoadProtocol.render(ManagedHdfsLoadRequest(
      workspaceOwner = "alice",
      targetTable = "daily_result",
      format = "parquet",
      relativePath = "reports/daily",
      options = Map("mergeSchema" -> "false")))

    val display = QueryOneServer.displaySql(command)

    assertEquals(
      """MANAGED HDFS LOAD
        |  workspace owner: alice
        |  view: daily_result
        |  format: parquet
        |  source: reports/daily
        |  options: {mergeSchema='false'}""".stripMargin,
      display)
    assertFalse(display.contains(command.split(" ").last))
  }

  @Test
  def ignoresRemovedMysqlDatasourceConfig(): Unit = {
    val file = Files.createTempFile("queryone-mysql-datasource-", ".conf")
    Files.write(file,
      """engines.local {
        |  type = "local"
        |
        |  datasources.mysql.analytics {
        |    url = "jdbc:mysql://host:3306/app"
        |    driver = "com.mysql.cj.jdbc.Driver"
        |    user = "reader"
        |    password = "secret"
        |
        |    options {
        |      fetchsize = 1000
        |      zeroDateTimeBehavior = "convertToNull"
        |    }
        |  }
        |}
        |""".stripMargin.getBytes("UTF-8"))

    try {
      val properties = ServerConfigFile.load(file.toString)

      assertFalse(properties.keys.exists(_.contains("queryone.datasource.mysql")))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsLocalOdepConfigFromHocon(): Unit = {
    val file = Files.createTempFile("queryone-local-odep-", ".conf")
    Files.write(file,
      """engines.local {
        |  type = "local"
        |
        |  odep {
        |    apiUrl = "http://odep-api.example"
        |    appId = "queryone"
        |    signKey = "local-development-key"
        |    connectTimeoutSeconds = 3
        |    requestTimeoutSeconds = 10
        |  }
        |}
        |""".stripMargin.getBytes("UTF-8"))

    try {
      val properties = ServerConfigFile.load(file.toString)

      assertEquals("http://odep-api.example",
        properties(s"$LocalPrefix.queryone.odep.api.url"))
      assertEquals("queryone",
        properties(s"$LocalPrefix.queryone.odep.app.id"))
      assertEquals("local-development-key",
        properties(s"$LocalPrefix.queryone.odep.sign.key"))
      assertEquals("3",
        properties(s"$LocalPrefix.queryone.odep.connect.timeout.seconds"))
      assertEquals("10",
        properties(s"$LocalPrefix.queryone.odep.request.timeout.seconds"))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsManagedHdfsOverwriteConfigFromHocon(): Unit = {
    val file = Files.createTempFile("queryone-overwrite-", ".conf")
    Files.write(file,
      """engines.local {
        |  type = "local"
        |
        |  overwrite {
        |    zkConnect = "zk-1:2181,zk-2:2181"
        |    zkRoot = "/queryone/test-overwrite"
        |    workspaceRoot = "/public/queryone/user"
        |    zkSessionTimeoutMs = 60000
        |    zkConnectionTimeoutMs = 15000
        |  }
        |}
        |""".stripMargin.getBytes("UTF-8"))

    try {
      val properties = ServerConfigFile.load(file.toString)

      assertEquals("zk-1:2181,zk-2:2181",
        properties(s"$LocalPrefix.spark.queryone.overwrite.zk.connect"))
      assertEquals("/queryone/test-overwrite",
        properties(s"$LocalPrefix.spark.queryone.overwrite.zk.root"))
      assertEquals("/public/queryone/user",
        properties(s"$LocalPrefix.spark.queryone.overwrite.workspaceRoot"))
      assertEquals("60000",
        properties(s"$LocalPrefix.spark.queryone.overwrite.zk.sessionTimeoutMs"))
      assertEquals("15000",
        properties(s"$LocalPrefix.spark.queryone.overwrite.zk.connectionTimeoutMs"))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def ignoresRemovedMysqlDatasourceConfigWithHoconObjectReuse(): Unit = {
    val file = Files.createTempFile("queryone-mysql-datasource-reuse-", ".conf")
    Files.write(file,
      """engines.local {
        |  type = "local"
        |
        |  datasources.mysql.analytics {
        |    url = "jdbc:mysql://host:3306/app"
        |    user = "reader"
        |    password = ${?QUERYONE_TEST_MYSQL_PASSWORD}
        |
        |    options {
        |      fetchsize = 1000
        |      batchsize = 1000
        |    }
        |  }
        |
        |  datasources.mysql.reporting = ${engines.local.datasources.mysql.analytics}
        |  datasources.mysql.reporting.url = "jdbc:mysql://host:3306/reporting"
        |  datasources.mysql.reporting.options.fetchsize = 2000
        |}
        |""".stripMargin.getBytes("UTF-8"))

    try {
      val properties = ServerConfigFile.load(file.toString)

      assertFalse(properties.keys.exists(_.contains("queryone.datasource.mysql")))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsMysqlCatalogFromHocon(): Unit = {
    val file = Files.createTempFile("queryone-mysql-catalog-", ".conf")
    Files.write(file,
      """engines.local {
        |  type = "local"
        |
        |  catalogs.mysql_static {
        |    url = "jdbc:mysql://host:3306/?databaseTerm=SCHEMA"
        |    driver = "com.mysql.cj.jdbc.Driver"
        |    user = "reader"
        |    password = "secret"
        |
        |    options {
        |      dbtable = "unsafe_table"
        |      query = "select * from unsafe_table"
        |      fetchsize = 1000
        |    }
        |  }
        |}
        |""".stripMargin.getBytes("UTF-8"))

    try {
      val properties = ServerConfigFile.load(file.toString)

      assertEquals("org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog",
        properties(s"$LocalPrefix.spark.sql.catalog.mysql_static"))
      assertEquals("jdbc:mysql://host:3306/?databaseTerm=SCHEMA", properties(s"$LocalPrefix.spark.sql.catalog.mysql_static.url"))
      assertEquals("com.mysql.cj.jdbc.Driver", properties(s"$LocalPrefix.spark.sql.catalog.mysql_static.driver"))
      assertEquals("reader", properties(s"$LocalPrefix.spark.sql.catalog.mysql_static.user"))
      assertEquals("secret", properties(s"$LocalPrefix.spark.sql.catalog.mysql_static.password"))
      assertEquals("1000", properties(s"$LocalPrefix.spark.sql.catalog.mysql_static.fetchsize"))
      assertFalse(properties.contains(s"$LocalPrefix.spark.sql.catalog.mysql_static.dbtable"))
      assertFalse(properties.contains(s"$LocalPrefix.spark.sql.catalog.mysql_static.query"))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsDorisCatalogFromHocon(): Unit = {
    val file = Files.createTempFile("queryone-doris-catalog-", ".conf")
    Files.write(file,
      """engines.local {
        |  type = "local"
        |
        |  catalogs.doris_static {
        |    fenodes = "fe-1:8030,fe-2:8030"
        |    queryPort = 9030
        |    user = "reader"
        |    password = "secret"
        |
        |    options {
        |      doris.request.retries = 5
        |      doris.read.mode = "arrow"
        |    }
        |  }
        |}
        |""".stripMargin.getBytes("UTF-8"))

    try {
      val properties = ServerConfigFile.load(file.toString)

      assertEquals("org.apache.doris.spark.catalog.DorisTableCatalog", properties(s"$LocalPrefix.spark.sql.catalog.doris_static"))
      assertEquals("fe-1:8030,fe-2:8030", properties(s"$LocalPrefix.spark.sql.catalog.doris_static.doris.fenodes"))
      assertEquals("9030", properties(s"$LocalPrefix.spark.sql.catalog.doris_static.doris.query.port"))
      assertEquals("reader", properties(s"$LocalPrefix.spark.sql.catalog.doris_static.doris.user"))
      assertEquals("secret", properties(s"$LocalPrefix.spark.sql.catalog.doris_static.doris.password"))
      assertEquals("5", properties(s"$LocalPrefix.spark.sql.catalog.doris_static.doris.request.retries"))
      assertEquals("arrow", properties(s"$LocalPrefix.spark.sql.catalog.doris_static.doris.read.mode"))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsPreviewConfigFromHocon(): Unit = {
    val file = Files.createTempFile("queryone-preview-", ".conf")
    Files.write(file,
      """preview {
        |  maxRows = 25
        |  defaultTab = "preview"
        |}
        |""".stripMargin.getBytes("UTF-8"))

    try {
      val properties = ServerConfigFile.load(file.toString)

      assertEquals("25", properties("queryone.preview.maxRows"))
      assertEquals("preview", properties("queryone.preview.defaultTab"))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsExecutionEnginesFromHocon(): Unit = {
    val file = Files.createTempFile("queryone-engines-", ".conf")
    Files.write(file,
      """engines {
        |  default = "kyuubi"
        |
        |  local {
        |    type = "local"
        |    enabled = true
        |    label = "Local Dev"
        |    spark.master = "local[2]"
        |  }
        |
        |  kyuubi {
        |    type = "kyuubi"
        |    enabled = true
        |    label = "Kyuubi Gateway"
        |    url = "jdbc:kyuubi://host:10009/default"
        |    user = "reader"
        |    password = "secret"
        |
        |    options {
        |      kyuubiServerPrincipal = "kyuubi/host@EXAMPLE.COM"
        |    }
        |  }
        |}
        |""".stripMargin.getBytes("UTF-8"))

    try {
      val properties = ServerConfigFile.load(file.toString)

      assertEquals("kyuubi", properties("queryone.engine.default"))
      assertEquals("local", properties("queryone.engine.local.type"))
      assertEquals("true", properties("queryone.engine.local.enabled"))
      assertEquals("Local Dev", properties("queryone.engine.local.label"))
      assertEquals("local[2]", properties(s"$LocalPrefix.spark.master"))
      assertEquals("kyuubi", properties("queryone.engine.kyuubi.type"))
      assertEquals("true", properties("queryone.engine.kyuubi.enabled"))
      assertEquals("Kyuubi Gateway", properties("queryone.engine.kyuubi.label"))
      assertEquals("jdbc:kyuubi://host:10009/default", properties("queryone.engine.kyuubi.kyuubi.url"))
      assertEquals("reader", properties("queryone.engine.kyuubi.kyuubi.user"))
      assertEquals("secret", properties("queryone.engine.kyuubi.kyuubi.password"))
      assertEquals("kyuubi/host@EXAMPLE.COM", properties("queryone.engine.kyuubi.kyuubi.option.kyuubiServerPrincipal"))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def ignoresRemovedKyuubiMysqlLoadProfiles(): Unit = {
    val file = Files.createTempFile("queryone-kyuubi-mysql-load-profile-", ".conf")
    Files.write(file,
      """engines.kyuubi {
        |  type = "kyuubi"
        |  enabled = true
        |  url = "jdbc:kyuubi://host:10009/default"
        |
        |  mysqlLoadProfiles.sales {
        |    strategy = "provider"
        |    catalog = "mysql_A"
        |    namespace = "Dworks"
        |    provider = "queryone_mysql"
        |    remoteProfile = "mysql_A"
        |    allowedTables = ["orders", "users"]
        |    maxNumPartitions = 32
        |    defaultFetchSize = 10000
        |  }
        |}
        |""".stripMargin.getBytes("UTF-8"))

    try {
      val properties = ServerConfigFile.load(file.toString)
      assertFalse(properties.keys.exists(_.contains("mysqlLoadProfile")))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsInternalApiAuthConfig(): Unit = {
    val file = Files.createTempFile("queryone-internal-api-", ".conf")
    Files.write(file,
      """internalApi.auth {
        |  appId = "odep-system"
        |  signKey = "test-key"
        |  clockSkewSeconds = 120
        |  nonceTtlSeconds = 300
        |}
        |""".stripMargin.getBytes("UTF-8"))

    try {
      val properties = ServerConfigFile.load(file.toString)
      assertEquals("odep-system", properties("queryone.internal.auth.app.id"))
      assertEquals("test-key", properties("queryone.internal.auth.sign.key"))
      assertEquals("120", properties("queryone.internal.auth.clock.skew.seconds"))
      assertEquals("300", properties("queryone.internal.auth.nonce.ttl.seconds"))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsCommittedHoconTemplate(): Unit = {
    val template = Seq(
      Paths.get("conf/queryone.conf.template"),
      Paths.get("../conf/queryone.conf.template"))
      .find(Files.isRegularFile(_))
      .getOrElse(throw new IllegalStateException("Cannot find conf/queryone.conf.template"))
    val properties = ServerConfigFile.load(template.toString)

    assertEquals("127.0.0.1", properties("queryone.host"))
    assertEquals("local[*]", properties(s"$LocalPrefix.spark.master"))
    assertEquals("10", properties("queryone.preview.maxRows"))
    assertEquals("schema", properties("queryone.preview.defaultTab"))
    assertEquals("300", properties("queryone.internal.auth.clock.skew.seconds"))
    assertEquals("600", properties("queryone.internal.auth.nonce.ttl.seconds"))
    assertEquals("local", properties("queryone.engine.default"))
    assertEquals("local", properties("queryone.engine.local.type"))
    assertEquals("true", properties("queryone.engine.local.enabled"))
    assertEquals("192.168.200.69:2181",
      properties(s"$LocalPrefix.spark.queryone.overwrite.zk.connect"))
    assertEquals("kyuubi", properties("queryone.engine.kyuubi_local.type"))
    assertEquals("false", properties("queryone.engine.kyuubi_local.enabled"))
    assertEquals(
      "jdbc:kyuubi://192.168.200.69:2181/default;serviceDiscoveryMode=zooKeeper;" +
        "zooKeeperNamespace=queryone-kyuubi?kyuubi.session.conf.profile=local",
      properties("queryone.engine.kyuubi_local.kyuubi.url"))
    assertEquals(
      "jdbc:kyuubi://192.168.200.69:2181/default;serviceDiscoveryMode=zooKeeper;" +
        "zooKeeperNamespace=queryone-kyuubi?kyuubi.session.conf.profile=yarn-client",
      properties("queryone.engine.kyuubi_yarn_client.kyuubi.url"))
    assertEquals(
      "jdbc:kyuubi://192.168.200.69:2181/default;serviceDiscoveryMode=zooKeeper;" +
        "zooKeeperNamespace=queryone-kyuubi?kyuubi.session.conf.profile=yarn-cluster",
      properties("queryone.engine.kyuubi_yarn_cluster.kyuubi.url"))
    assertFalse(properties.keys.exists(_.contains("queryone.datasource.mysql")))
    assertEquals("org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog",
      properties(s"$LocalPrefix.spark.sql.catalog.mysql_static"))
    assertEquals("jdbc:mysql://127.0.0.1:3306/?databaseTerm=SCHEMA&useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false",
      properties(s"$LocalPrefix.spark.sql.catalog.mysql_static.url"))
  }
}
