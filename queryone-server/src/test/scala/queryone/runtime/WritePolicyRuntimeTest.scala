package queryone.runtime

import queryone.identity.TenantContext
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

import java.nio.file.{Files, Path}
import java.sql.DriverManager
import scala.collection.JavaConverters._

final class WritePolicyRuntimeTest {
  @Test
  def hiveAppendMapsColumnsByNameAndRejectsIncompatibleSchema(): Unit = {
    val root = Files.createTempDirectory("queryone-write-policy-by-name-")
    val spark = localSpark(root)
    val tenant = TenantContext.development("alice")
    try {
      spark.sql("CREATE TABLE default.queryone_by_name_target (name STRING, id INT) USING parquet")
      val runtime = new QueryOneRuntime(spark)
      val success = runtime.run(
        tenant,
        """view reordered_source as select 1 as id, 'alice' as name;
          |save append reordered_source as hive.`default.queryone_by_name_target`;
          |""".stripMargin,
        10)

      assertTrue(success.statements.flatMap(_.error).mkString("\n"), success.success)
      assertEquals(
        "INSERT INTO TABLE spark_catalog.default.queryone_by_name_target (`name`, `id`) " +
          "SELECT `name`, `id` FROM reordered_source",
        success.statements.last.sql)
      assertFalse(success.statements.last.sql.contains("BY NAME"))
      val written = spark.sql(
        "SELECT name, id FROM default.queryone_by_name_target").collect().map(row => row.getString(0) -> row.getInt(1))
      assertEquals(Seq("alice" -> 1), written.toSeq)

      val incompatible = runtime.run(
        tenant,
        """view missing_column_source as select 2 as id;
          |save append missing_column_source as hive.`default.queryone_by_name_target`;
          |""".stripMargin,
        10)
      assertFalse(incompatible.success)
      assertTrue(incompatible.statements.flatMap(_.error).mkString("\n").contains("must match target columns by name"))
      assertEquals(1L, spark.table("default.queryone_by_name_target").count())

      val incompatibleTypes = runtime.run(
        tenant,
        """view incompatible_type_source as select 'not-an-int' as id, 'bob' as name;
          |save append incompatible_type_source as hive.`default.queryone_by_name_target`;
          |""".stripMargin,
        10)
      assertFalse(incompatibleTypes.success)
      assertTrue(incompatibleTypes.statements.flatMap(_.error).mkString("\n").toLowerCase
        .contains("incompatible data"))
      assertEquals(1L, spark.table("default.queryone_by_name_target").count())
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def hiveAppendStillRequiresExistingTarget(): Unit = {
    val root = Files.createTempDirectory("queryone-write-policy-hive-")
    val spark = localSpark(root)
    val tenant = TenantContext.development("alice")
    try {
      spark.sql("CREATE TABLE default.queryone_append_target (id INT) USING parquet")
      val runtime = new QueryOneRuntime(spark)
      val success = runtime.run(
        tenant,
        """view result_view as select 1 as id;
          |save append result_view as hive.`default.queryone_append_target`;
          |select count(*) as cnt from default.queryone_append_target;
          |""".stripMargin,
        10)
      assertTrue(success.statements.flatMap(_.error).mkString("\n"), success.success)
      assertEquals("1", success.statements.last.rows.head.head)

      val missing = runtime.run(
        tenant,
        """view result_view as select 1 as id;
          |save append result_view as hive.`default.queryone_missing_target`;
          |""".stripMargin,
        10)
      assertFalse(missing.success)
      assertTrue(missing.statements.flatMap(_.error).mkString("\n").contains("target table does not exist"))
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def localJdbcCatalogAppendMapsColumnsByNameAndFailsBeforeUnsafeWrites(): Unit = {
    val root = Files.createTempDirectory("queryone-write-policy-jdbc-")
    val spark = localSpark(root)
    val tenant = TenantContext.development("alice")
    val jdbcUrl = s"jdbc:derby:${root.resolve("mysql-db").toAbsolutePath};create=true"
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver")
    val setup = DriverManager.getConnection(jdbcUrl)
    try {
      setup.createStatement().executeUpdate(
        "CREATE TABLE TARGET_USERS (NAME VARCHAR(100), ID INTEGER)")
    } finally {
      setup.close()
    }

    try {
        spark.conf.set(
          "spark.sql.catalog.derby_static",
          "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog")
        spark.conf.set("spark.sql.catalog.derby_static.url", jdbcUrl)
        spark.conf.set("spark.sql.catalog.derby_static.driver", "org.apache.derby.jdbc.EmbeddedDriver")
        val runtime = new QueryOneRuntime(spark)
        val success = runtime.run(
          tenant,
          """view mysql_source as select 7 as id, 'alice' as name;
            |save append mysql_source as jdbc.`derby_static.APP.TARGET_USERS`;
            |""".stripMargin,
          10)

        assertTrue(success.statements.flatMap(_.error).mkString("\n"), success.success)
        assertEquals(Seq("alice" -> 7), readMysqlRows(jdbcUrl))

        val missingColumn = runtime.run(
          tenant,
          """view mysql_missing_column as select 8 as id;
            |save append mysql_missing_column as jdbc.`derby_static.APP.TARGET_USERS`;
            |""".stripMargin,
          10)
        assertFalse(missingColumn.success)
        assertTrue(missingColumn.statements.flatMap(_.error).mkString("\n")
          .contains("must match target columns by name"))
        assertEquals(1, readMysqlRows(jdbcUrl).size)

        val incompatibleType = runtime.run(
          tenant,
          """view mysql_incompatible_type as select 'not-an-int' as id, 'bob' as name;
            |save append mysql_incompatible_type as jdbc.`derby_static.APP.TARGET_USERS`;
            |""".stripMargin,
          10)
        assertFalse(incompatibleType.success)
        assertTrue(incompatibleType.statements.flatMap(_.error).mkString("\n").toLowerCase
          .contains("incompatible data"))
        assertEquals(1, readMysqlRows(jdbcUrl).size)

        val missingTarget = runtime.run(
          tenant,
          """view mysql_missing_target_source as select 9 as id, 'carol' as name;
            |save append mysql_missing_target_source as jdbc.`derby_static.APP.MISSING_USERS`;
            |""".stripMargin,
          10)
        assertFalse(missingTarget.success)
        assertTrue(missingTarget.statements.flatMap(_.error).mkString("\n")
          .contains("target table does not exist"))
        assertEquals(1, readMysqlRows(jdbcUrl).size)
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  private def readMysqlRows(jdbcUrl: String): Seq[(String, Int)] = {
    val connection = DriverManager.getConnection(jdbcUrl)
    try {
      val statement = connection.createStatement()
      try {
        val resultSet = statement.executeQuery("SELECT NAME, ID FROM TARGET_USERS ORDER BY ID")
        val rows = Seq.newBuilder[(String, Int)]
        while (resultSet.next()) {
          rows += resultSet.getString(1) -> resultSet.getInt(2)
        }
        resultSet.close()
        rows.result()
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  private def localSpark(root: Path): SparkSession = {
    SparkSession.builder()
      .appName("QueryOne WritePolicyRuntimeTest")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", root.resolve("warehouse").toString)
      .getOrCreate()
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      val stream = Files.walk(path)
      try {
        stream.iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
      } finally {
        stream.close()
      }
    }
  }
}
