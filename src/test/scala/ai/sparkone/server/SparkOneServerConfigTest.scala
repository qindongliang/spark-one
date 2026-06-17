package ai.sparkone.server

import org.junit.Assert._
import org.junit.Test

import java.nio.file.Files

final class SparkOneServerConfigTest {
  @Test
  def loadsMysqlDatasourceFromHocon(): Unit = {
    val file = Files.createTempFile("sparkone-mysql-datasource-", ".conf")
    Files.write(file,
      """datasources.mysql.analytics {
        |  url = "jdbc:mysql://host:3306/app"
        |  driver = "com.mysql.cj.jdbc.Driver"
        |  user = "reader"
        |  password = "secret"
        |
        |  options {
        |    fetchsize = 1000
        |    zeroDateTimeBehavior = "convertToNull"
        |  }
        |}
        |""".stripMargin.getBytes("UTF-8"))

    try {
      val properties = ServerConfigFile.load(file.toString)

      assertEquals("jdbc:mysql://host:3306/app", properties("sparkone.datasource.mysql.analytics.url"))
      assertEquals("com.mysql.cj.jdbc.Driver", properties("sparkone.datasource.mysql.analytics.driver"))
      assertEquals("reader", properties("sparkone.datasource.mysql.analytics.user"))
      assertEquals("secret", properties("sparkone.datasource.mysql.analytics.password"))
      assertEquals("1000", properties("sparkone.datasource.mysql.analytics.option.fetchsize"))
      assertEquals("convertToNull", properties("sparkone.datasource.mysql.analytics.option.zeroDateTimeBehavior"))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsMysqlDatasourceWithHoconObjectReuse(): Unit = {
    val file = Files.createTempFile("sparkone-mysql-datasource-reuse-", ".conf")
    Files.write(file,
      """datasources.mysql.analytics {
        |  url = "jdbc:mysql://host:3306/app"
        |  user = "reader"
        |  password = ${?SPARKONE_TEST_MYSQL_PASSWORD}
        |
        |  options {
        |    fetchsize = 1000
        |    batchsize = 1000
        |  }
        |}
        |
        |datasources.mysql.reporting = ${datasources.mysql.analytics}
        |datasources.mysql.reporting.url = "jdbc:mysql://host:3306/reporting"
        |datasources.mysql.reporting.options.fetchsize = 2000
        |""".stripMargin.getBytes("UTF-8"))

    try {
      val properties = ServerConfigFile.load(file.toString)

      assertEquals("jdbc:mysql://host:3306/reporting", properties("sparkone.datasource.mysql.reporting.url"))
      assertEquals("reader", properties("sparkone.datasource.mysql.reporting.user"))
      assertEquals("2000", properties("sparkone.datasource.mysql.reporting.option.fetchsize"))
      assertEquals("1000", properties("sparkone.datasource.mysql.reporting.option.batchsize"))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsDorisCatalogFromHocon(): Unit = {
    val file = Files.createTempFile("sparkone-doris-catalog-", ".conf")
    Files.write(file,
      """catalogs.doris {
        |  fenodes = "fe-1:8030,fe-2:8030"
        |  queryPort = 9030
        |  user = "reader"
        |  password = "secret"
        |
        |  options {
        |    doris.request.retries = 5
        |    doris.read.mode = "arrow"
        |  }
        |}
        |""".stripMargin.getBytes("UTF-8"))

    try {
      val properties = ServerConfigFile.load(file.toString)

      assertEquals("org.apache.doris.spark.catalog.DorisTableCatalog", properties("spark.sql.catalog.doris"))
      assertEquals("fe-1:8030,fe-2:8030", properties("spark.sql.catalog.doris.doris.fenodes"))
      assertEquals("9030", properties("spark.sql.catalog.doris.doris.query.port"))
      assertEquals("reader", properties("spark.sql.catalog.doris.doris.user"))
      assertEquals("secret", properties("spark.sql.catalog.doris.doris.password"))
      assertEquals("5", properties("spark.sql.catalog.doris.doris.request.retries"))
      assertEquals("arrow", properties("spark.sql.catalog.doris.doris.read.mode"))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsMysqlOverwriteSafetySwitchFromHocon(): Unit = {
    val file = Files.createTempFile("sparkone-save-mysql-overwrite-", ".conf")
    Files.write(file,
      """save {
        |  allowMysqlOverwrite = true
        |}
        |""".stripMargin.getBytes("UTF-8"))

    try {
      val properties = ServerConfigFile.load(file.toString)

      assertEquals("true", properties("sparkone.save.mysql.overwrite.enabled"))
    } finally {
      Files.deleteIfExists(file)
    }
  }

  @Test
  def loadsCommittedHoconTemplate(): Unit = {
    val properties = ServerConfigFile.load("conf/sparkone.conf.template")

    assertEquals("127.0.0.1", properties("sparkone.host"))
    assertEquals("local[*]", properties("spark.master"))
    assertEquals("false", properties("sparkone.save.mysql.overwrite.enabled"))
    assertEquals("jdbc:mysql://127.0.0.1:3306/app?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false",
      properties("sparkone.datasource.mysql.analytics.url"))
    assertEquals("1000", properties("sparkone.datasource.mysql.analytics.option.fetchsize"))
  }
}
