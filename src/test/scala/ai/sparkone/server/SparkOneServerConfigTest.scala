package ai.sparkone.server

import org.junit.Assert._
import org.junit.Test

import java.nio.file.Files

final class SparkOneServerConfigTest {
  @Test
  def loadsMysqlDatasourceFromToml(): Unit = {
    val file = Files.createTempFile("sparkone-mysql-datasource-", ".toml")
    Files.write(file,
      """[datasources.mysql.analytics]
        |url = "jdbc:mysql://host:3306/app"
        |driver = "com.mysql.cj.jdbc.Driver"
        |user = "reader"
        |password = "secret"
        |
        |[datasources.mysql.analytics.options]
        |fetchsize = "1000"
        |zeroDateTimeBehavior = "convertToNull"
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
  def loadsMysqlOverwriteSafetySwitchFromToml(): Unit = {
    val file = Files.createTempFile("sparkone-save-mysql-overwrite-", ".toml")
    Files.write(file,
      """[save]
        |allowMysqlOverwrite = true
        |""".stripMargin.getBytes("UTF-8"))

    try {
      val properties = ServerConfigFile.load(file.toString)

      assertEquals("true", properties("sparkone.save.mysql.overwrite.enabled"))
    } finally {
      Files.deleteIfExists(file)
    }
  }
}
