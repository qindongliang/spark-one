package ai.sparkone.runtime

import ai.sparkone.identity.TenantContext
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

final class WritePolicyRuntimeTest {
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
