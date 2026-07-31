package ai.sparkone.provider.mysql

import org.apache.spark.sql.SparkSession
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import java.sql.{Connection, Driver, DriverPropertyInfo}
import java.util.Properties
import java.util.logging.Logger

final class SparkOneMysqlDataSourceTest {

  @Test
  def readsOdepMysqlAliasWithDynamicBoundsAndParallelPartitions(): Unit = {
    val h2Url = "jdbc:h2:mem:odep_mysql_provider;MODE=MYSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"
    val mysqlUrl = "jdbc:mysql:h2:mem:odep_mysql_provider;MODE=MYSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"
    val h2 = new org.h2.Driver
    val connection = h2.connect(h2Url, new Properties())
    try {
      val statement = connection.createStatement()
      try {
        statement.execute("CREATE SCHEMA `physical_search`")
        statement.execute(
          "CREATE TABLE `physical_search`.`drug_ai_drug_decision` " +
            "(`id` BIGINT PRIMARY KEY, `menu_id` VARCHAR(16), `name` VARCHAR(32))")
        statement.execute(
          "INSERT INTO `physical_search`.`drug_ai_drug_decision` VALUES " +
            "(1, '1_0', 'alpha'), (2, 'other', 'beta'), (3, '1_0', 'gamma')")
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("sparkone-odep-mysql-provider-test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config(
        "spark.sql.catalog.jdbc",
        "ai.sparkone.kyuubi.odep.catalog.OdepRoutingCatalog")
      .config("spark.sql.catalog.jdbc.odep.datasource.count", "1")
      .config("spark.sql.catalog.jdbc.odep.datasource.0.alias", "sync_search")
      .config(
        "spark.sql.catalog.jdbc.odep.datasource.0.physicalNamespace",
        "physical_search")
      .config("spark.sql.catalog.jdbc.odep.datasource.0.option.url", mysqlUrl)
      .config(
        "spark.sql.catalog.jdbc.odep.datasource.0.option.driver",
        classOf[MysqlLikeH2Driver].getName)
      .getOrCreate()

    try {
      spark.sparkContext.setLogLevel("ERROR")
      spark.sql(
        """CREATE OR REPLACE TEMPORARY VIEW filtered_drugs
          |USING sparkone_mysql
          |OPTIONS (
          |  catalog 'jdbc',
          |  alias 'sync_search',
          |  dbtable 'drug_ai_drug_decision',
          |  whereClauseBase64 'bWVudV9pZCA9ICcxXzAn',
          |  partitionColumn 'id',
          |  numPartitions '2',
          |  fetchsize '100'
          |)""".stripMargin)

      val data = spark.table("filtered_drugs")
      assertEquals(2, data.rdd.getNumPartitions)
      assertEquals(Seq("alpha", "gamma"), data.orderBy("id").collect().map(_.getString(2)).toSeq)
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }
}

final class MysqlLikeH2Driver extends Driver {
  private val delegate = new org.h2.Driver

  override def connect(url: String, info: Properties): Connection = {
    if (!acceptsURL(url)) null
    else delegate.connect("jdbc:h2:" + url.stripPrefix("jdbc:mysql:h2:"), info)
  }

  override def acceptsURL(url: String): Boolean = url.startsWith("jdbc:mysql:h2:")

  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] =
    Array.empty

  override def getMajorVersion: Int = 1

  override def getMinorVersion: Int = 0

  override def jdbcCompliant(): Boolean = false

  override def getParentLogger: Logger = Logger.getGlobal
}
