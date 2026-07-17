package ai.sparkone.extension.overwrite

import org.apache.spark.sql.SparkSession
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.Test

import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

final class ManagedHdfsLoadIntegrationTest {
  @Test
  def loadsOnlyRelativePathsInsideTenantWorkspace(): Unit = {
    val workspace = Files.createTempDirectory("sparkone-load-workspace")
    val warehouse = Files.createTempDirectory("sparkone-load-warehouse")
    val spark = SparkSession.builder()
      .appName("ManagedHdfsLoadIntegrationTest")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.toString)
      .config(ManagedHdfsWorkspacePolicy.WorkspaceRootKey, workspace.toString)
      .withExtensions(new SparkOneHdfsOverwriteExtensions().apply)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try {
      import spark.implicits._

      val aliceTarget = workspace.resolve("alice/extension-test/result")
      Files.createDirectories(aliceTarget.getParent)
      Seq((1L, "beijing"), (2L, "shanghai"))
        .toDF("id", "name")
        .write
        .parquet(aliceTarget.toString)

      execute(spark, ManagedHdfsLoadRequest(
        "alice", "loaded_result", "parquet", "extension-test/result", Map.empty))

      assertEquals(
        Seq((1L, "beijing"), (2L, "shanghai")),
        spark.table("loaded_result")
          .orderBy("id")
          .collect()
          .map(row => row.getLong(0) -> row.getString(1))
          .toSeq)

      Seq("/public/sparkone/user/bob/result", "../bob/result", "extension-test/.sparkone-overwrite-x/staging")
        .foreach { path =>
          try {
            execute(spark, ManagedHdfsLoadRequest(
              "alice", "blocked_result", "parquet", path, Map.empty))
            fail(s"Expected managed load path to be rejected: $path")
          } catch {
            case e: Exception =>
              assertTrue(rootMessage(e).contains("validated relative path"))
          }
        }

      try {
        execute(spark, ManagedHdfsLoadRequest(
          "alice", "blocked_options", "parquet", "extension-test/result", Map("path" -> "../bob")))
        fail("Expected managed load path override option to be rejected")
      } catch {
        case e: Exception =>
          assertTrue(rootMessage(e).contains("option is not allowed"))
      }
    } finally {
      spark.stop()
      deleteRecursively(workspace)
      deleteRecursively(warehouse)
    }
  }

  private def execute(spark: SparkSession, request: ManagedHdfsLoadRequest): Unit = {
    spark.sql(ManagedHdfsLoadProtocol.render(request)).collect()
  }

  private def rootMessage(error: Throwable): String = {
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).toSeq.lastOption
      .flatMap(value => Option(value.getMessage))
      .getOrElse(error.getClass.getName)
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      val stream = Files.walk(path)
      try {
        stream.iterator().asScala.toSeq.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      } finally {
        stream.close()
      }
    }
  }
}
