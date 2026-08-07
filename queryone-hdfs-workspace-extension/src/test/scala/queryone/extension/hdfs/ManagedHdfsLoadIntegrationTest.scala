package queryone.extension.hdfs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.Test

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters._

final class ManagedHdfsLoadIntegrationTest {
  @Test
  def loadsOnlyRelativePathsInsideTenantWorkspace(): Unit = {
    val workspace = Files.createTempDirectory("queryone-load-workspace")
    val warehouse = Files.createTempDirectory("queryone-load-warehouse")
    val aliceTarget = workspace.resolve("alice/extension-test/result")
    val observedInternalLoadRead = new AtomicBoolean(false)
    val spark = SparkSession.builder()
      .appName("ManagedHdfsLoadIntegrationTest")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.toString)
      .config(ManagedHdfsWorkspacePolicy.WorkspaceRootKey, workspace.toString)
      .withExtensions { extensions =>
        new QueryOneHdfsWorkspaceExtensions().apply(extensions)
        extensions.injectCheckRule { session =>
          (plan: LogicalPlan) => plan.foreach {
            case relation: LogicalRelation
                if ManagedHdfsWorkspacePolicy.managedLoadWorkspaceOwner(relation).isEmpty =>
              relation.relation match {
                case hdfs: HadoopFsRelation =>
                  assertTrue(
                    ManagedHdfsWorkspacePolicy.matchesManagedLoadReadPaths(
                      session,
                      hdfs.location.rootPaths))
                  observedInternalLoadRead.set(true)
                case _ =>
              }
            case _ =>
          }
        }
      }
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try {
      import spark.implicits._

      Files.createDirectories(aliceTarget.getParent)
      Seq((1L, "beijing"), (2L, "shanghai"))
        .toDF("id", "name")
        .write
        .parquet(aliceTarget.toString)

      execute(spark, ManagedHdfsLoadRequest(
        "alice", "loaded_result", "parquet", "extension-test/result", Map.empty))

      val csvTarget = workspace.resolve("alice/extension-test/csv-result")
      Files.createDirectories(csvTarget)
      Files.write(
        csvTarget.resolve("part-1.csv"),
        "id,name\n1,beijing\n".getBytes(StandardCharsets.UTF_8))
      Files.write(
        csvTarget.resolve("part-2.csv"),
        "id,name\n2,shanghai\n".getBytes(StandardCharsets.UTF_8))
      execute(spark, ManagedHdfsLoadRequest(
        "alice",
        "loaded_csv_result",
        "csv",
        "extension-test/csv-result",
        Map("header" -> "true", "inferSchema" -> "true")))

      assertTrue(observedInternalLoadRead.get())
      assertEquals(None, ManagedHdfsWorkspacePolicy.managedLoadReadContext(spark.sparkContext))
      assertTrue(
        spark.table("loaded_result").queryExecution.analyzed.exists(plan =>
          ManagedHdfsWorkspacePolicy.managedLoadWorkspaceOwner(plan).contains("alice")))
      assertEquals(
        Seq((1L, "beijing"), (2L, "shanghai")),
        spark.table("loaded_result")
          .orderBy("id")
          .collect()
          .map(row => row.getLong(0) -> row.getString(1))
          .toSeq)
      assertEquals(2L, spark.table("loaded_csv_result").count())

      Seq("/public/odep/user/bob/result", "../bob/result", "extension-test/.queryone-overwrite-x/staging")
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
