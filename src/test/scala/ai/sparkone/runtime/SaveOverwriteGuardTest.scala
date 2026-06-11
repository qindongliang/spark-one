package ai.sparkone.runtime

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

final class SaveOverwriteGuardTest {
  private val ProtectedPathsKey = "sparkone.save.overwrite.protected.paths"
  private val AllowNativeInsertOverwriteKey = "sparkone.save.native.insertOverwrite.enabled"
  private val AllowNativeDropTableKey = "sparkone.save.native.dropTable.enabled"

  @Test
  def sessionOverwritePolicyAllowsSaveAndKeepsBackup(): Unit = {
    val root = Files.createTempDirectory("sparkone-save-guard-")
    val target = root.resolve("result")
    val backupRoot = root.resolve("configured-backup-root")
    Files.createDirectories(target)
    Files.write(target.resolve("old.txt"), "old".getBytes("UTF-8"))

    val spark = localSpark(root)
    try {
      val runtime = new SparkOneRuntime(spark)
      val result = runtime.run(
        s"""set sparkone.save.overwrite.policy=allow;
           |set sparkone.save.overwrite.backup=rename;
           |set sparkone.save.overwrite.backup.path=${backupRoot.toUri};
           |
           |view result_view as select 1 as id;
           |
           |save overwrite result_view as parquet.`${target.toUri}`;
           |""".stripMargin)

      assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
      assertTrue(Files.exists(target))

      assertTrue(Files.isDirectory(backupRoot))
      val backupStream = Files.list(backupRoot)
      val backups =
        try {
          backupStream.iterator().asScala.toSeq
        } finally {
          backupStream.close()
        }
      assertEquals(1, backups.size)
      assertTrue(Files.exists(backups.head.resolve("old.txt")))
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def globalProtectedPathRejectsBoundaryOverwriteEvenWhenStatementAllowsIt(): Unit = {
    val root = Files.createTempDirectory("sparkone-save-guard-protected-")
    val protectedRoot = root.resolve("public").resolve("odep").resolve("user")
    val target = protectedRoot
    Files.createDirectories(target)
    Files.write(target.resolve("old.txt"), "old".getBytes("UTF-8"))

    withSystemProperty(ProtectedPathsKey, protectedRoot.toUri.toString) {
      val spark = localSpark(root)
      try {
        val runtime = new SparkOneRuntime(spark)
        val result = runtime.run(
          s"""set sparkone.save.overwrite.policy=allow;
             |
             |view result_view as select 1 as id;
             |
             |save overwrite result_view as parquet.`${target.toUri}`
             |options sparkoneOverwrite="allow"
             |and sparkoneOverwriteBackup="none";
             |""".stripMargin)

        assertFalse(result.success)
        val error = result.statements.flatMap(_.error).mkString("\n")
        assertTrue(error, error.contains("global protected path"))
        assertTrue(error, error.contains("protectedPath"))
        assertTrue(Files.exists(target.resolve("old.txt")))
      } finally {
        spark.stop()
      }
    }

    deleteRecursively(root)
  }

  @Test
  def globalProtectedPathAllowsSpecificChildDirectoryOverwrite(): Unit = {
    val root = Files.createTempDirectory("sparkone-save-guard-protected-child-")
    val protectedRoot = root.resolve("public").resolve("odep").resolve("user")
    val target = protectedRoot.resolve("userA")
    Files.createDirectories(target)
    Files.write(target.resolve("old.txt"), "old".getBytes("UTF-8"))

    withSystemProperty(ProtectedPathsKey, protectedRoot.toUri.toString) {
      val spark = localSpark(root)
      try {
        val runtime = new SparkOneRuntime(spark)
        val result = runtime.run(
          s"""set sparkone.save.overwrite.policy=allow;
             |
             |view result_view as select 1 as id;
             |
             |save overwrite result_view as parquet.`${target.toUri}`
             |options sparkoneOverwrite="allow"
             |and sparkoneOverwriteBackup="none";
             |""".stripMargin)

        assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
        assertTrue(Files.exists(target))
      } finally {
        spark.stop()
      }
    }

    deleteRecursively(root)
  }

  @Test
  def nativeInsertOverwriteIsBlockedByDefault(): Unit = {
    val root = Files.createTempDirectory("sparkone-native-insert-overwrite-block-")
    val target = root.resolve("native-result")

    val spark = localSpark(root)
    try {
      val runtime = new SparkOneRuntime(spark)
      val result = runtime.run(
        s"""insert overwrite directory '${target.toUri}' using parquet
           |select 1 as id;
           |""".stripMargin)

      assertFalse(result.success)
      val error = result.statements.flatMap(_.error).mkString("\n")
      assertTrue(error, error.contains("Native Spark SQL INSERT OVERWRITE is disabled"))
      assertFalse(Files.exists(target))
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def nativeInsertOverwriteCanBeAllowedForCompatibility(): Unit = {
    val root = Files.createTempDirectory("sparkone-native-insert-overwrite-allow-")
    val target = root.resolve("native-result")

    withSystemProperty(AllowNativeInsertOverwriteKey, "true") {
      val spark = localSpark(root)
      try {
        val runtime = new SparkOneRuntime(spark)
        val result = runtime.run(
          s"""insert overwrite directory '${target.toUri}' using parquet
             |select 1 as id;
             |""".stripMargin)

        assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
        assertTrue(Files.exists(target))
      } finally {
        spark.stop()
      }
    }

    deleteRecursively(root)
  }

  @Test
  def nativeDropTableIsBlockedByDefaultAndCannotBeAllowedBySessionSet(): Unit = {
    val root = Files.createTempDirectory("sparkone-native-drop-table-block-")
    val table = "sparkone_drop_table_block_target"

    val spark = localSpark(root)
    try {
      spark.sql(s"CREATE TABLE default.$table (id INT) USING parquet")
      val runtime = new SparkOneRuntime(spark)
      val result = runtime.run(
        s"""set sparkone.save.native.dropTable.enabled=true;
           |
           |drop table default.$table;
           |""".stripMargin)

      assertFalse(result.success)
      val error = result.statements.flatMap(_.error).mkString("\n")
      assertTrue(error, error.contains("Native Spark SQL DROP TABLE is disabled"))
      assertTrue(spark.catalog.tableExists("default", table))
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def nativeDropTableCanBeAllowedFromStartupConfiguration(): Unit = {
    val root = Files.createTempDirectory("sparkone-native-drop-table-allow-")
    val table = "sparkone_drop_table_allow_target"

    withSystemProperty(AllowNativeDropTableKey, "true") {
      val spark = localSpark(root)
      try {
        spark.sql(s"CREATE TABLE default.$table (id INT) USING parquet")
        val runtime = new SparkOneRuntime(spark)
        val result = runtime.run(s"drop table default.$table;")

        assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
        assertFalse(spark.catalog.tableExists("default", table))
      } finally {
        spark.stop()
      }
    }

    deleteRecursively(root)
  }

  @Test
  def wildcardProtectedPathRejectsConfiguredDepthAndAllowsDeeperPath(): Unit = {
    val root = Files.createTempDirectory("sparkone-save-guard-wildcard-")
    val firstLevel = root.resolve("public")
    val secondLevel = firstLevel.resolve("odep")
    val thirdLevel = secondLevel.resolve("userA")
    Files.createDirectories(firstLevel)
    Files.createDirectories(secondLevel)
    Files.createDirectories(thirdLevel)
    Files.write(firstLevel.resolve("old.txt"), "old".getBytes("UTF-8"))
    Files.write(secondLevel.resolve("old.txt"), "old".getBytes("UTF-8"))
    Files.write(thirdLevel.resolve("old.txt"), "old".getBytes("UTF-8"))

    val protectedPattern = root.toUri.toString.stripSuffix("/") + "/*/*"
    withSystemProperty(ProtectedPathsKey, protectedPattern) {
      val spark = localSpark(root)
      try {
        val runtime = new SparkOneRuntime(spark)

        val firstLevelResult = runOverwrite(runtime, firstLevel)
        assertFalse(firstLevelResult.success)
        assertTrue(firstLevelResult.statements.flatMap(_.error).mkString("\n").contains("global protected path"))

        val secondLevelResult = runOverwrite(runtime, secondLevel)
        assertFalse(secondLevelResult.success)
        assertTrue(secondLevelResult.statements.flatMap(_.error).mkString("\n").contains("global protected path"))

        val thirdLevelResult = runOverwrite(runtime, thirdLevel)
        assertTrue(thirdLevelResult.statements.flatMap(_.error).mkString("\n"), thirdLevelResult.success)
      } finally {
        spark.stop()
      }
    }

    deleteRecursively(root)
  }

  @Test
  def localFilePathRejectsTrashBackup(): Unit = {
    val root = Files.createTempDirectory("sparkone-save-guard-trash-")
    val target = root.resolve("result")
    Files.createDirectories(target)
    Files.write(target.resolve("old.txt"), "old".getBytes("UTF-8"))

    val spark = localSpark(root)
    try {
      val runtime = new SparkOneRuntime(spark)
      val result = runtime.run(
        s"""set sparkone.save.overwrite.policy=allow;
           |set sparkone.save.overwrite.backup=trash;
           |
           |view result_view as select 1 as id;
           |
           |save overwrite result_view as parquet.`${target.toUri}`;
           |""".stripMargin)

      assertFalse(result.success)
      val error = result.statements.flatMap(_.error).mkString("\n")
      assertTrue(error, error.contains("does not support"))
      assertTrue(error, error.contains("trash"))
      assertTrue(Files.exists(target.resolve("old.txt")))
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def hiveSaveAppendUsesCatalogInsert(): Unit = {
    val root = Files.createTempDirectory("sparkone-hive-save-append-")
    val spark = localSpark(root)
    try {
      spark.sql("CREATE TABLE default.sparkone_hive_append_target (id INT) USING parquet")
      val runtime = new SparkOneRuntime(spark)
      val result = runtime.run(
        """view result_view as select 1 as id;
          |
          |save append result_view as hive.`default.sparkone_hive_append_target`;
          |
          |select count(*) as cnt from default.sparkone_hive_append_target;
          |""".stripMargin)

      assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
      assertEquals("1", result.statements.last.rows.head.head)
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def hiveSaveOverwriteRequiresExplicitAllowAndDoesNotUseFileBackup(): Unit = {
    val root = Files.createTempDirectory("sparkone-hive-save-overwrite-")
    val spark = localSpark(root)
    try {
      spark.sql("CREATE TABLE default.sparkone_hive_overwrite_target (id INT) USING parquet")
      spark.sql("INSERT INTO TABLE default.sparkone_hive_overwrite_target SELECT 0")

      val runtime = new SparkOneRuntime(spark)
      val rejected = runtime.run(
        """view result_view as select 1 as id;
          |
          |save overwrite result_view as hive.`default.sparkone_hive_overwrite_target`;
          |""".stripMargin)

      assertFalse(rejected.success)
      assertTrue(rejected.statements.flatMap(_.error).mkString("\n").contains("requires explicit confirmation"))

      val allowed = runtime.run(
        """view result_view as select 2 as id;
          |
          |save overwrite result_view as hive.`default.sparkone_hive_overwrite_target`
          |options sparkoneOverwrite="allow";
          |
          |select id from default.sparkone_hive_overwrite_target;
          |""".stripMargin)

      assertTrue(allowed.statements.flatMap(_.error).mkString("\n"), allowed.success)
      assertEquals(Seq(Seq("2")), allowed.statements.last.rows)
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  private def localSpark(root: Path): SparkSession = {
    SparkSession.builder()
      .appName("SparkOne SaveOverwriteGuardTest")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", root.resolve("warehouse").toString)
      .getOrCreate()
  }

  private def runOverwrite(runtime: SparkOneRuntime, target: Path) = {
    runtime.run(
      s"""set sparkone.save.overwrite.policy=allow;
         |
         |view result_view as select 1 as id;
         |
         |save overwrite result_view as parquet.`${target.toUri}`
         |options sparkoneOverwrite="allow"
         |and sparkoneOverwriteBackup="none";
         |""".stripMargin)
  }

  private def withSystemProperty(key: String, value: String)(body: => Unit): Unit = {
    val previous = sys.props.get(key)
    sys.props.put(key, value)
    try {
      body
    } finally {
      previous match {
        case Some(oldValue) => sys.props.put(key, oldValue)
        case None => sys.props.remove(key)
      }
    }
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
