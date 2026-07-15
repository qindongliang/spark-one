package ai.sparkone.runtime

import ai.sparkone.identity.TenantContext
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

final class WritePolicyRuntimeTest {
  @Test
  def hiveAppendMapsColumnsByNameAndRejectsIncompatibleSchema(): Unit = {
    val root = Files.createTempDirectory("sparkone-write-policy-by-name-")
    val spark = localSpark(root)
    val tenant = TenantContext.development("alice")
    try {
      spark.sql("CREATE TABLE default.sparkone_by_name_target (name STRING, id INT) USING parquet")
      val runtime = new SparkOneRuntime(spark)
      val success = runtime.run(
        tenant,
        """view reordered_source as select 1 as id, 'alice' as name;
          |save append reordered_source as hive.`default.sparkone_by_name_target`;
          |""".stripMargin,
        10)

      assertTrue(success.statements.flatMap(_.error).mkString("\n"), success.success)
      assertEquals(
        "INSERT INTO TABLE default.sparkone_by_name_target (`name`, `id`) " +
          "SELECT `name`, `id` FROM reordered_source",
        success.statements.last.sql)
      assertFalse(success.statements.last.sql.contains("BY NAME"))
      val written = spark.sql(
        "SELECT name, id FROM default.sparkone_by_name_target").collect().map(row => row.getString(0) -> row.getInt(1))
      assertEquals(Seq("alice" -> 1), written.toSeq)

      val incompatible = runtime.run(
        tenant,
        """view missing_column_source as select 2 as id;
          |save append missing_column_source as hive.`default.sparkone_by_name_target`;
          |""".stripMargin,
        10)
      assertFalse(incompatible.success)
      assertTrue(incompatible.statements.flatMap(_.error).mkString("\n").contains("must match target columns by name"))
      assertEquals(1L, spark.table("default.sparkone_by_name_target").count())

      val incompatibleTypes = runtime.run(
        tenant,
        """view incompatible_type_source as select 'not-an-int' as id, 'bob' as name;
          |save append incompatible_type_source as hive.`default.sparkone_by_name_target`;
          |""".stripMargin,
        10)
      assertFalse(incompatibleTypes.success)
      assertTrue(incompatibleTypes.statements.flatMap(_.error).mkString("\n").contains("INCOMPATIBLE_DATA_FOR_TABLE"))
      assertEquals(1L, spark.table("default.sparkone_by_name_target").count())
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def hiveAppendStillRequiresExistingTarget(): Unit = {
    val root = Files.createTempDirectory("sparkone-write-policy-hive-")
    val spark = localSpark(root)
    val tenant = TenantContext.development("alice")
    try {
      spark.sql("CREATE TABLE default.sparkone_append_target (id INT) USING parquet")
      val runtime = new SparkOneRuntime(spark)
      val success = runtime.run(
        tenant,
        """view result_view as select 1 as id;
          |save append result_view as hive.`default.sparkone_append_target`;
          |select count(*) as cnt from default.sparkone_append_target;
          |""".stripMargin,
        10)
      assertTrue(success.statements.flatMap(_.error).mkString("\n"), success.success)
      assertEquals("1", success.statements.last.rows.head.head)

      val missing = runtime.run(
        tenant,
        """view result_view as select 1 as id;
          |save append result_view as hive.`default.sparkone_missing_target`;
          |""".stripMargin,
        10)
      assertFalse(missing.success)
      assertTrue(missing.statements.flatMap(_.error).mkString("\n").contains("target table does not exist"))
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  private def localSpark(root: Path): SparkSession = {
    SparkSession.builder()
      .appName("SparkOne WritePolicyRuntimeTest")
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
