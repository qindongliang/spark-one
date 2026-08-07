package ai.queryone.extension.overwrite

import org.apache.curator.test.TestingServer
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.sql.SparkSession
import org.junit.Assert.{assertEquals, assertFalse, assertTrue, fail}
import org.junit.Test

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path => NioPath}
import java.security.MessageDigest
import scala.collection.JavaConverters._

final class ManagedHdfsOverwriteIntegrationTest {
  @Test
  def overwritesCompleteResultAndKeepsOldTargetOnWriteFailure(): Unit = {
    val workspace = Files.createTempDirectory("queryone-overwrite-workspace")
    val warehouse = Files.createTempDirectory("queryone-overwrite-warehouse")
    val zooKeeper = new TestingServer(true)
    val spark = SparkSession.builder()
      .appName("ManagedHdfsOverwriteIntegrationTest")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.toString)
      .config(ManagedHdfsOverwriteExecutor.WorkspaceRootKey, workspace.toString)
      .config(ManagedHdfsOverwriteExecutor.ZooKeeperConnectKey, zooKeeper.getConnectString)
      .config(ManagedHdfsOverwriteExecutor.ZooKeeperRootKey, "/queryone/test-overwrite")
      .withExtensions(new QueryOneHdfsOverwriteExtensions().apply)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try {
      import spark.implicits._

      Seq(1L, 2L).toDF("id").createOrReplaceTempView("source_data")
      execute(spark, ManagedHdfsOverwriteRequest(
        "alice", "source_data", "parquet", "reports/daily", Map.empty))
      assertEquals(Seq(1L, 2L), readIds(spark, workspace))

      simulateInterruptedPublish(spark, workspace)
      Seq(10L, 20L).toDF("id").createOrReplaceTempView("source_data")
      execute(spark, ManagedHdfsOverwriteRequest(
        "alice", "source_data", "parquet", "reports/daily", Map.empty))
      assertEquals(Seq(10L, 20L), readIds(spark, workspace))
      assertNoWorkDirectory(workspace)

      Seq((30L, "beijing")).toDF("id", "city").createOrReplaceTempView("invalid_text_source")
      try {
        execute(spark, ManagedHdfsOverwriteRequest(
          "alice", "invalid_text_source", "text", "reports/daily", Map.empty))
        fail("Expected text writer schema validation to fail")
      } catch {
        case _: Exception =>
      }

      assertEquals(Seq(10L, 20L), readIds(spark, workspace))
      assertNoWorkDirectory(workspace)
    } finally {
      spark.stop()
      zooKeeper.close()
      deleteRecursively(workspace)
      deleteRecursively(warehouse)
    }
  }

  private def execute(spark: SparkSession, request: ManagedHdfsOverwriteRequest): Unit = {
    spark.sql(ManagedHdfsOverwriteProtocol.render(request)).collect()
  }

  private def readIds(spark: SparkSession, workspace: NioPath): Seq[Long] = {
    spark.read.parquet(workspace.resolve("alice/reports/daily").toString)
      .collect()
      .map(_.getLong(0))
      .sorted
      .toSeq
  }

  private def assertNoWorkDirectory(workspace: NioPath): Unit = {
    val parent = workspace.resolve("alice/reports")
    val stream = Files.list(parent)
    try {
      assertFalse(stream.iterator().asScala.exists(_.getFileName.toString.startsWith(".queryone-overwrite-")))
    } finally {
      stream.close()
    }
  }

  private def simulateInterruptedPublish(spark: SparkSession, workspace: NioPath): Unit = {
    val target = new HadoopPath(workspace.resolve("alice/reports/daily").toString)
    val fs = target.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val qualifiedTarget = fs.makeQualified(target)
    val hash = MessageDigest.getInstance("SHA-256")
      .digest(qualifiedTarget.toString.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
      .take(24)
    val work = new HadoopPath(qualifiedTarget.getParent, s".queryone-overwrite-$hash")
    val backup = new HadoopPath(work, "backup")
    val staging = new HadoopPath(work, "staging")

    assertTrue(fs.mkdirs(staging))
    assertTrue(fs.rename(qualifiedTarget, backup))
  }

  private def deleteRecursively(path: NioPath): Unit = {
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
