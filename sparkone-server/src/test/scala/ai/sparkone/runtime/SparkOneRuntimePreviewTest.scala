package ai.sparkone.runtime

import ai.sparkone.extension.overwrite.{ManagedHdfsWorkspacePolicy, SparkOneHdfsOverwriteExtensions}
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

final class SparkOneRuntimePreviewTest {
  @Test
  def loadStatementReturnsPreviewRowsFromRegisteredView(): Unit = {
    val root = Files.createTempDirectory("sparkone-runtime-preview-")
    val spark = localSpark(root)
    try {
      import spark.implicits._
      val source = root.resolve("local-runtime/users")
      Files.createDirectories(source.getParent)
      Seq(
        ("beijing", 1),
        ("shanghai", 2),
        ("hangzhou", 3))
        .toDF("city", "id")
        .write
        .parquet(source.toString)

      val runtime = new SparkOneRuntime(spark)
      val schemaOnly = runtime.run("load parquet.`users` as users;", limit = 2)

      assertTrue(schemaOnly.statements.flatMap(_.error).mkString("\n"), schemaOnly.success)
      assertEquals(0, schemaOnly.statements.head.rowCount)
      assertFalse(schemaOnly.statements.head.truncated)
      assertEquals(Seq("city", "id"), schemaOnly.statements.head.schema.map(_.name))
      assertEquals(Some("users"), schemaOnly.statements.head.previewTable)

      val result = runtime.previewTable("users", limit = 2)

      assertEquals(2, result.rowCount)
      assertTrue(result.truncated)
      assertEquals(Seq("city", "id"), result.schema.map(_.name))
      assertTrue(spark.catalog.tableExists("users"))
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def multipleSuccessfulLoadsOnlyReturnLastSchemaResult(): Unit = {
    val root = Files.createTempDirectory("sparkone-runtime-preview-multiple-loads-")
    val spark = localSpark(root)
    try {
      import spark.implicits._
      val first = root.resolve("local-runtime/first")
      val second = root.resolve("local-runtime/second")
      Files.createDirectories(first.getParent)
      Seq(1).toDF("id").write.parquet(first.toString)
      Seq(2).toDF("id").write.parquet(second.toString)

      val runtime = new SparkOneRuntime(spark)
      val result = runtime.run(
        """load parquet.`first` as first_users;
           |load parquet.`second` as second_users;
           |""".stripMargin)

      assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
      assertEquals(1, result.statements.size)
      assertEquals(Some("second_users"), result.statements.head.previewTable)
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def previewConfigDefaultsToTenRowsAndClampsRequestedLimit(): Unit = {
    withSystemProperties(Map(
      PreviewConfig.MaxRowsKey -> "3",
      PreviewConfig.DefaultTabKey -> "preview")) {
      val config = PreviewConfig.current

      assertEquals(3, config.maxRows)
      assertEquals("preview", config.defaultTab)
      assertEquals(3, config.clampRows(None))
      assertEquals(2, config.clampRows(Some(2)))
      assertEquals(3, config.clampRows(Some(200)))
      assertEquals(1, config.clampRows(Some(0)))
    }

    val defaults = PreviewConfig.current
    assertEquals(10, defaults.maxRows)
    assertEquals("schema", defaults.defaultTab)
  }

  @Test
  def runtimeClampsExplicitLimitToPreviewMaxRows(): Unit = {
    val root = Files.createTempDirectory("sparkone-runtime-preview-clamp-")
    val spark = localSpark(root)
    try {
      withSystemProperties(Map(PreviewConfig.MaxRowsKey -> "2")) {
        val runtime = new SparkOneRuntime(spark)
        val result = runtime.run(
          """select * from values
            |  (1),
            |  (2),
            |  (3)
            |as t(id);
            |""".stripMargin,
          limit = 100)

        assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
        assertEquals(2, result.statements.head.rowCount)
        assertTrue(result.statements.head.truncated)
      }
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def runSubstitutesLiteralSetVariablesInLaterStatements(): Unit = {
    val root = Files.createTempDirectory("sparkone-runtime-set-literal-")
    val spark = localSpark(root)
    try {
      val runtime = new SparkOneRuntime(spark)
      val result = runtime.run(
        """set biz_date = "2026-03-14";
          |select '${biz_date}' as dt;
          |""".stripMargin)

      assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
      assertEquals(2, result.statements.size)
      assertEquals(Nil, result.statements.head.schema)
      assertEquals(0, result.statements.head.rowCount)
      assertEquals(Seq("2026-03-14"), result.statements.last.rows.head)
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def runEvaluatesSqlSetVariablesBeforeLaterStatements(): Unit = {
    val root = Files.createTempDirectory("sparkone-runtime-set-sql-")
    val spark = localSpark(root)
    try {
      val runtime = new SparkOneRuntime(spark)
      val result = runtime.run(
        """set start_date as select date_sub(date '2026-03-15', 1) as dt;
          |select '${start_date}' as dt;
          |""".stripMargin)

      assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
      assertEquals(2, result.statements.size)
      assertEquals(Nil, result.statements.head.schema)
      assertEquals(0, result.statements.head.rowCount)
      assertEquals(Seq("2026-03-14"), result.statements.last.rows.head)
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def assertPassesAndContinuesWithLaterStatements(): Unit = {
    val root = Files.createTempDirectory("sparkone-runtime-assert-pass-")
    val spark = localSpark(root)
    try {
      val runtime = new SparkOneRuntime(spark)
      val result = runtime.run(
        """view quality_metrics as select 1 as row_count, 0 as null_count;
          |assert quality_metrics
          |where "row_count > 0 and null_count = 0"
          |message "quality metrics are invalid";
          |select 2 as continued;
          |""".stripMargin)

      assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
      assertEquals(3, result.statements.size)
      assertEquals(Some(AssertionStatus.Passed), result.statements(1).assertion.map(_.status))
      assertEquals(Seq("2"), result.statements.last.rows.head)
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def inlineQueryAssertPassesAndContinuesWithLaterStatements(): Unit = {
    val root = Files.createTempDirectory("sparkone-runtime-inline-assert-pass-")
    val spark = localSpark(root)
    try {
      val runtime = new SparkOneRuntime(spark)
      val result = runtime.run(
        """assert (
          |  select * from values (1), (2) as metrics(row_count)
          |)
          |where "row_count > 0"
          |message "row_count must be positive";
          |select 3 as continued;
          |""".stripMargin)

      assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
      assertEquals(2, result.statements.size)
      assertEquals(Some(AssertionStatus.Passed), result.statements.head.assertion.map(_.status))
      assertEquals(Some("inline query"), result.statements.head.assertion.map(_.table))
      assertEquals(Seq("3"), result.statements.last.rows.head)
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def inlineQueryAssertReturnsViolationsAndStopsLaterStatements(): Unit = {
    val root = Files.createTempDirectory("sparkone-runtime-inline-assert-fail-")
    val spark = localSpark(root)
    try {
      val runtime = new SparkOneRuntime(spark)
      val result = runtime.run(
        """assert (
          |  select * from values (1), (0), (-1) as metrics(row_count)
          |)
          |where "row_count > 0"
          |message "row_count must be positive";
          |select 3 as should_not_run;
          |""".stripMargin,
        limit = 1)

      assertFalse(result.success)
      assertEquals(1, result.statements.size)
      val assertion = result.statements.head
      assertEquals(Some(AssertionStatus.Failed), assertion.assertion.map(_.status))
      assertEquals(Some("row_count must be positive"), assertion.error)
      assertEquals(1, assertion.rowCount)
      assertTrue(assertion.truncated)
      assertTrue(Set("0", "-1").contains(assertion.rows.head.head))
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def assertFailureReturnsLimitedViolationsAndStopsLaterStatements(): Unit = {
    val root = Files.createTempDirectory("sparkone-runtime-assert-fail-")
    val spark = localSpark(root)
    try {
      val runtime = new SparkOneRuntime(spark)
      val result = runtime.run(
        """view quality_metrics as
          |select * from values (1), (0), (-1) as metrics(row_count);
          |assert quality_metrics
          |where "row_count > 0"
          |message "row_count must be positive";
          |select 2 as should_not_run;
          |""".stripMargin,
        limit = 1)

      assertFalse(result.success)
      assertEquals(2, result.statements.size)
      val assertion = result.statements.last
      assertEquals(Some(AssertionStatus.Failed), assertion.assertion.map(_.status))
      assertEquals(Some("row_count must be positive"), assertion.error)
      assertEquals(1, assertion.rowCount)
      assertTrue(assertion.truncated)
      assertTrue(Set("0", "-1").contains(assertion.rows.head.head))
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def assertTreatsNullPredicateAsFailure(): Unit = {
    val root = Files.createTempDirectory("sparkone-runtime-assert-null-")
    val spark = localSpark(root)
    try {
      val runtime = new SparkOneRuntime(spark)
      val result = runtime.run(
        """view quality_metrics as select cast(null as bigint) as row_count;
          |assert quality_metrics
          |where "row_count > 0"
          |message "row_count must be positive";
          |""".stripMargin)

      assertFalse(result.success)
      assertEquals(Some(AssertionStatus.Failed), result.statements.last.assertion.map(_.status))
      assertNull(result.statements.last.rows.head.head)
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def assertTreatsEmptyResultTableAsPassed(): Unit = {
    val root = Files.createTempDirectory("sparkone-runtime-assert-empty-")
    val spark = localSpark(root)
    try {
      val runtime = new SparkOneRuntime(spark)
      val result = runtime.run(
        """view empty_metrics as select 1 as row_count where false;
          |assert empty_metrics
          |where "row_count > 0"
          |message "row_count must be positive";
          |""".stripMargin)

      assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
      assertEquals(Some(AssertionStatus.Passed), result.statements.last.assertion.map(_.status))
      assertEquals(0, result.statements.last.rowCount)
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  @Test
  def assertReportsQueryExecutionErrorsSeparatelyFromBusinessFailures(): Unit = {
    val root = Files.createTempDirectory("sparkone-runtime-assert-error-")
    val spark = localSpark(root)
    try {
      val runtime = new SparkOneRuntime(spark)
      val result = runtime.run(
        """assert missing_metrics
          |where "row_count > 0"
          |message "row_count must be positive";
          |""".stripMargin)

      assertFalse(result.success)
      assertEquals(1, result.statements.size)
      val assertion = result.statements.head
      assertEquals(Some(AssertionStatus.Error), assertion.assertion.map(_.status))
      assertFalse(assertion.error.contains("row_count must be positive"))
      assertTrue(assertion.error.get.contains("TABLE_OR_VIEW_NOT_FOUND"))
    } finally {
      spark.stop()
      deleteRecursively(root)
    }
  }

  private def localSpark(root: Path): SparkSession = {
    SparkSession.builder()
      .appName("SparkOne RuntimePreviewTest")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", root.resolve("warehouse").toString)
      .config(ManagedHdfsWorkspacePolicy.WorkspaceRootKey, root.toString)
      .withExtensions(new SparkOneHdfsOverwriteExtensions().apply)
      .getOrCreate()
  }

  private def withSystemProperties[T](values: Map[String, String])(body: => T): T = {
    val previous = values.keys.map(key => key -> sys.props.get(key)).toMap
    values.foreach { case (key, value) => sys.props.put(key, value) }
    try {
      body
    } finally {
      previous.foreach {
        case (key, Some(oldValue)) => sys.props.put(key, oldValue)
        case (key, None) => sys.props.remove(key)
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
