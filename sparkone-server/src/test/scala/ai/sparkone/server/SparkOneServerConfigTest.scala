package ai.sparkone.server

import org.junit.Assert._
import org.junit.Test

import java.nio.file.{Files, Paths}

final class SparkOneServerConfigTest {
  private val LocalPrefix = "sparkone.engine.local.local.property"

  @Test
  def loadsMysqlDatasourceFromHocon(): Unit = {
    val file = Files.createTempFile("sparkone-mysql-datasource-", ".conf")
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

      assertEquals("jdbc:mysql://host:3306/app", properties(s"$LocalPrefix.sparkone.datasource.mysql.analytics.url"))
      assertEquals("com.mysql.cj.jdbc.Driver", properties(s"$LocalPrefix.sparkone.datasource.mysql.analytics.driver"))
      assertEquals("reader", properties(s"$LocalPrefix.sparkone.datasource.mysql.analytics.user"))
      assertEquals("secret", properties(s"$LocalPrefix.sparkone.datasource.mysql.analytics.password"))
      assertEquals("1000", properties(s"$LocalPrefix.sparkone.datasource.mysql.analytics.option.fetchsize"))
      assertEquals("convertToNull", properties(s"$LocalPrefix.sparkone.datasource.mysql.analytics.option.zeroDateTimeBehavior"))
      assertFalse(properties.contains(s"$LocalPrefix.spark.sql.catalog.mysql"))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsMysqlDatasourceWithHoconObjectReuse(): Unit = {
    val file = Files.createTempFile("sparkone-mysql-datasource-reuse-", ".conf")
    Files.write(file,
      """engines.local {
        |  type = "local"
        |
        |  datasources.mysql.analytics {
        |    url = "jdbc:mysql://host:3306/app"
        |    user = "reader"
        |    password = ${?SPARKONE_TEST_MYSQL_PASSWORD}
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

      assertEquals("jdbc:mysql://host:3306/reporting", properties(s"$LocalPrefix.sparkone.datasource.mysql.reporting.url"))
      assertEquals("reader", properties(s"$LocalPrefix.sparkone.datasource.mysql.reporting.user"))
      assertEquals("2000", properties(s"$LocalPrefix.sparkone.datasource.mysql.reporting.option.fetchsize"))
      assertEquals("1000", properties(s"$LocalPrefix.sparkone.datasource.mysql.reporting.option.batchsize"))
      assertFalse(properties.contains(s"$LocalPrefix.spark.sql.catalog.mysql"))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsMysqlCatalogFromHocon(): Unit = {
    val file = Files.createTempFile("sparkone-mysql-catalog-", ".conf")
    Files.write(file,
      """engines.local {
        |  type = "local"
        |
        |  catalogs.mysql {
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
        properties(s"$LocalPrefix.spark.sql.catalog.mysql"))
      assertEquals("jdbc:mysql://host:3306/?databaseTerm=SCHEMA", properties(s"$LocalPrefix.spark.sql.catalog.mysql.url"))
      assertEquals("com.mysql.cj.jdbc.Driver", properties(s"$LocalPrefix.spark.sql.catalog.mysql.driver"))
      assertEquals("reader", properties(s"$LocalPrefix.spark.sql.catalog.mysql.user"))
      assertEquals("secret", properties(s"$LocalPrefix.spark.sql.catalog.mysql.password"))
      assertEquals("1000", properties(s"$LocalPrefix.spark.sql.catalog.mysql.fetchsize"))
      assertFalse(properties.contains(s"$LocalPrefix.spark.sql.catalog.mysql.dbtable"))
      assertFalse(properties.contains(s"$LocalPrefix.spark.sql.catalog.mysql.query"))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsDorisCatalogFromHocon(): Unit = {
    val file = Files.createTempFile("sparkone-doris-catalog-", ".conf")
    Files.write(file,
      """engines.local {
        |  type = "local"
        |
        |  catalogs.doris {
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

      assertEquals("org.apache.doris.spark.catalog.DorisTableCatalog", properties(s"$LocalPrefix.spark.sql.catalog.doris"))
      assertEquals("fe-1:8030,fe-2:8030", properties(s"$LocalPrefix.spark.sql.catalog.doris.doris.fenodes"))
      assertEquals("9030", properties(s"$LocalPrefix.spark.sql.catalog.doris.doris.query.port"))
      assertEquals("reader", properties(s"$LocalPrefix.spark.sql.catalog.doris.doris.user"))
      assertEquals("secret", properties(s"$LocalPrefix.spark.sql.catalog.doris.doris.password"))
      assertEquals("5", properties(s"$LocalPrefix.spark.sql.catalog.doris.doris.request.retries"))
      assertEquals("arrow", properties(s"$LocalPrefix.spark.sql.catalog.doris.doris.read.mode"))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsPreviewConfigFromHocon(): Unit = {
    val file = Files.createTempFile("sparkone-preview-", ".conf")
    Files.write(file,
      """preview {
        |  maxRows = 25
        |  defaultTab = "preview"
        |}
        |""".stripMargin.getBytes("UTF-8"))

    try {
      val properties = ServerConfigFile.load(file.toString)

      assertEquals("25", properties("sparkone.preview.maxRows"))
      assertEquals("preview", properties("sparkone.preview.defaultTab"))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsExecutionEnginesFromHocon(): Unit = {
    val file = Files.createTempFile("sparkone-engines-", ".conf")
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

      assertEquals("kyuubi", properties("sparkone.engine.default"))
      assertEquals("local", properties("sparkone.engine.local.type"))
      assertEquals("true", properties("sparkone.engine.local.enabled"))
      assertEquals("Local Dev", properties("sparkone.engine.local.label"))
      assertEquals("local[2]", properties(s"$LocalPrefix.spark.master"))
      assertEquals("kyuubi", properties("sparkone.engine.kyuubi.type"))
      assertEquals("true", properties("sparkone.engine.kyuubi.enabled"))
      assertEquals("Kyuubi Gateway", properties("sparkone.engine.kyuubi.label"))
      assertEquals("jdbc:kyuubi://host:10009/default", properties("sparkone.engine.kyuubi.kyuubi.url"))
      assertEquals("reader", properties("sparkone.engine.kyuubi.kyuubi.user"))
      assertEquals("secret", properties("sparkone.engine.kyuubi.kyuubi.password"))
      assertEquals("kyuubi/host@EXAMPLE.COM", properties("sparkone.engine.kyuubi.kyuubi.option.kyuubiServerPrincipal"))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsKyuubiMysqlLoadProfilesFromHocon(): Unit = {
    val file = Files.createTempFile("sparkone-kyuubi-mysql-load-profile-", ".conf")
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
        |    provider = "sparkone_mysql"
        |    remoteProfile = "mysql_A"
        |    allowedTables = ["orders", "users"]
        |    maxNumPartitions = 32
        |    defaultFetchSize = 10000
        |  }
        |}
        |""".stripMargin.getBytes("UTF-8"))

    try {
      val properties = ServerConfigFile.load(file.toString)
      val prefix = "sparkone.engine.kyuubi.kyuubi.mysqlLoadProfile.sales"

      assertEquals("provider", properties(s"$prefix.strategy"))
      assertEquals("mysql_A", properties(s"$prefix.catalog"))
      assertEquals("Dworks", properties(s"$prefix.namespace"))
      assertEquals("sparkone_mysql", properties(s"$prefix.provider"))
      assertEquals("mysql_A", properties(s"$prefix.remoteProfile"))
      assertEquals("orders\nusers", properties(s"$prefix.allowedTables"))
      assertEquals("32", properties(s"$prefix.maxNumPartitions"))
      assertEquals("10000", properties(s"$prefix.defaultFetchSize"))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsCommittedHoconTemplate(): Unit = {
    val template = Seq(
      Paths.get("conf/sparkone.conf.template"),
      Paths.get("../conf/sparkone.conf.template"))
      .find(Files.isRegularFile(_))
      .getOrElse(throw new IllegalStateException("Cannot find conf/sparkone.conf.template"))
    val properties = ServerConfigFile.load(template.toString)

    assertEquals("127.0.0.1", properties("sparkone.host"))
    assertEquals("local[*]", properties(s"$LocalPrefix.spark.master"))
    assertEquals("10", properties("sparkone.preview.maxRows"))
    assertEquals("schema", properties("sparkone.preview.defaultTab"))
    assertEquals("local", properties("sparkone.engine.default"))
    assertEquals("local", properties("sparkone.engine.local.type"))
    assertEquals("true", properties("sparkone.engine.local.enabled"))
    assertEquals("kyuubi", properties("sparkone.engine.kyuubi.type"))
    assertEquals("false", properties("sparkone.engine.kyuubi.enabled"))
    assertEquals("jdbc:kyuubi://192.168.202.187:10009/default", properties("sparkone.engine.kyuubi.kyuubi.url"))
    assertEquals("jdbc:mysql://127.0.0.1:3306/app?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false",
      properties(s"$LocalPrefix.sparkone.datasource.mysql.analytics.url"))
    assertEquals("1000", properties(s"$LocalPrefix.sparkone.datasource.mysql.analytics.option.fetchsize"))
    assertEquals("org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog",
      properties(s"$LocalPrefix.spark.sql.catalog.mysql"))
    assertEquals("jdbc:mysql://127.0.0.1:3306/?databaseTerm=SCHEMA&useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false",
      properties(s"$LocalPrefix.spark.sql.catalog.mysql.url"))
  }
}
