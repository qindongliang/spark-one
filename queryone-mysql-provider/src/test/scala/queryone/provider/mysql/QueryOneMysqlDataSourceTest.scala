package queryone.provider.mysql

import queryone.kyuubi.odep.OdepDatasourceResolver
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.Test

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.sql.{Connection, Driver, DriverPropertyInfo}
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger

final class QueryOneMysqlDataSourceTest {

  @Test
  def doesNotRequireOdepEnvironmentForStaticMysqlUsage(): Unit = {
    assertEquals("queryone_mysql", new QueryOneMysqlDataSource().shortName())
  }

  @Test
  def readsLazilyResolvedOdepAliasWithParallelPartitions(): Unit = {
    val h2Url = "jdbc:h2:mem:odep_mysql_provider;MODE=MYSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"
    val mysqlUrl = "jdbc:mysql:h2:mem:odep_mysql_provider;MODE=MYSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"
    val h2 = new org.h2.Driver
    val initialProperties = new Properties()
    initialProperties.setProperty("user", "sa")
    initialProperties.setProperty("password", "")
    val connection = h2.connect(h2Url, initialProperties)
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

    val indexRequests = new AtomicInteger()
    val resolveRequests = new AtomicInteger()
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/api/datasource/index", exchange => {
      indexRequests.incrementAndGet()
      respond(exchange,
        """{"code":200,"success":true,"results":[{"id":1,"type":"jdbc","alias":"sync_search","physicalNamespace":"physical_search"}]}""")
    })
    server.createContext("/api/datasource/resolve", exchange => {
      resolveRequests.incrementAndGet()
      respond(exchange,
        s"""{"code":200,"success":true,"results":{"url":"$mysqlUrl","driver":"${classOf[MysqlLikeH2Driver].getName}","user":"sa","password":""}}""")
    })
    server.start()

    val resolver = new OdepDatasourceResolver(
      s"http://127.0.0.1:${server.getAddress.getPort}",
      "app_kyuubi",
      "test-sign-key",
      2000,
      2000)
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("queryone-odep-mysql-provider-test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config(
        "spark.sql.catalog.jdbc",
        "queryone.kyuubi.odep.catalog.OdepRoutingCatalog")
      .getOrCreate()

    try {
      spark.sparkContext.setLogLevel("ERROR")
      val relation = new QueryOneMysqlDataSource(resolver).createRelation(
        spark.sqlContext,
        Map(
          "catalog" -> "jdbc",
          "alias" -> "sync_search",
          "dbtable" -> "drug_ai_drug_decision",
          "whereClauseBase64" -> "bWVudV9pZCA9ICcxXzAn",
          "partitionColumn" -> "id",
          "numPartitions" -> "2",
          "fetchsize" -> "100"))

      assertEquals("odep", relation.asInstanceOf[QueryOneMysqlRelation].queryOneAuthzMode)
      assertEquals("jdbc", relation.asInstanceOf[QueryOneMysqlRelation].queryOneAuthzCatalog)
      assertEquals("sync_search", relation.asInstanceOf[QueryOneMysqlRelation].queryOneAuthzNamespace)
      assertEquals(
        "drug_ai_drug_decision",
        relation.asInstanceOf[QueryOneMysqlRelation].queryOneAuthzTable)
      val data = spark.sqlContext.baseRelationToDataFrame(relation)
      assertEquals(2, data.rdd.getNumPartitions)
      assertEquals(Seq("alpha", "gamma"), data.orderBy("id").collect().map(_.getString(2)).toSeq)
      assertEquals(1, indexRequests.get())
      assertEquals(1, resolveRequests.get())
    } finally {
      spark.stop()
      server.stop(0)
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  @Test
  def readsStaticMysqlCatalogWithoutOdepRouting(): Unit = {
    val h2Url = "jdbc:h2:mem:static_mysql_provider;MODE=MYSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"
    val mysqlUrl = "jdbc:mysql:h2:mem:static_mysql_provider;MODE=MYSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"
    val h2 = new org.h2.Driver
    val properties = new Properties()
    properties.setProperty("user", "sa")
    properties.setProperty("password", "")
    val connection = h2.connect(h2Url, properties)
    try {
      val statement = connection.createStatement()
      try {
        statement.execute("CREATE SCHEMA `app`")
        statement.execute("CREATE TABLE `app`.`orders` (`id` BIGINT PRIMARY KEY, `name` VARCHAR(32))")
        statement.execute("INSERT INTO `app`.`orders` VALUES (1, 'alpha'), (2, 'beta')")
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("queryone-static-mysql-provider-test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.catalog.mysql_static", "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog")
      .config("spark.sql.catalog.mysql_static.url", mysqlUrl)
      .config("spark.sql.catalog.mysql_static.driver", classOf[MysqlLikeH2Driver].getName)
      .config("spark.sql.catalog.mysql_static.user", "sa")
      .config("spark.sql.catalog.mysql_static.password", "")
      .getOrCreate()

    try {
      spark.sparkContext.setLogLevel("ERROR")
      val relation = new QueryOneMysqlDataSource().createRelation(
        spark.sqlContext,
        Map(
          "catalog" -> "mysql_static",
          "dbtable" -> "app.orders",
          "partitionColumn" -> "id",
          "lowerBound" -> "1",
          "upperBound" -> "2",
          "numPartitions" -> "2"))

      val wrapped = relation.asInstanceOf[QueryOneMysqlRelation]
      assertEquals("static", wrapped.queryOneAuthzMode)
      assertEquals("mysql_static", wrapped.queryOneAuthzCatalog)
      assertEquals("app", wrapped.queryOneAuthzNamespace)
      assertEquals("orders", wrapped.queryOneAuthzTable)
      val data = spark.sqlContext.baseRelationToDataFrame(relation)
      assertEquals(Seq("alpha", "beta"), data.orderBy("id").collect().map(_.getString(1)).toSeq)
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def respond(exchange: HttpExchange, body: String): Unit = {
    val response = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.set("Content-Type", "application/json")
    exchange.sendResponseHeaders(200, response.length)
    exchange.getResponseBody.write(response)
    exchange.close()
  }
}

final class MysqlLikeH2Driver extends Driver {
  private val delegate = new org.h2.Driver

  override def connect(url: String, info: Properties): Connection = {
    if (!acceptsURL(url)) null
    else {
      val delegatedUrl = ("jdbc:h2:" + url.stripPrefix("jdbc:mysql:h2:"))
        .replace("?databaseTerm=SCHEMA", "")
        .replace("&databaseTerm=SCHEMA", "")
      delegate.connect(delegatedUrl, info)
    }
  }

  override def acceptsURL(url: String): Boolean = url.startsWith("jdbc:mysql:h2:")

  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] =
    Array.empty

  override def getMajorVersion: Int = 1

  override def getMinorVersion: Int = 0

  override def jdbcCompliant(): Boolean = false

  override def getParentLogger: Logger = Logger.getGlobal
}
