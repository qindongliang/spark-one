package ai.sparkone.sql

import ai.sparkone.identity.TenantContext
import org.junit.Assert._
import org.junit.Test

final class WritePlanTest {
  private val tenant = TenantContext.development("alice")
  private val planner = new WritePlanner

  @Test
  def fixedCapabilityMatrixMatchesProductPolicy(): Unit = {
    import WriteMode._
    import WriteTargetKind._

    assertTrue(WriteCapabilityMatrix.supports(HiveCatalog, Append))
    assertTrue(WriteCapabilityMatrix.supports(DorisCatalog, Append))
    assertTrue(WriteCapabilityMatrix.supports(Mysql, Append))
    assertTrue(WriteCapabilityMatrix.supports(ManagedHdfs, Append))
    assertTrue(WriteCapabilityMatrix.supports(ManagedHdfs, Overwrite))
    assertTrue(WriteCapabilityMatrix.supports(ExternalPath, Append))

    assertFalse(WriteCapabilityMatrix.supports(HiveCatalog, Overwrite))
    assertFalse(WriteCapabilityMatrix.supports(DorisCatalog, Overwrite))
    assertFalse(WriteCapabilityMatrix.supports(Mysql, Overwrite))
    assertFalse(WriteCapabilityMatrix.supports(ExternalPath, Overwrite))
    assertFalse(WriteCapabilityMatrix.supports(UnknownProvider, Append))
    assertFalse(WriteCapabilityMatrix.supports(UnknownProvider, Overwrite))
  }

  @Test
  def createsTenantScopedHiveAppendPlanBeforeRenderingSql(): Unit = {
    val plan = planner.plan(
      tenant,
      mode = "append",
      sourceTable = "source_view",
      format = "hive",
      path = "default.target_table",
      providerOptions = Nil,
      partitionColumns = Seq("dt"),
      resolvedSource = CatalogSaveSource(
        "default.target_table",
        SaveTargetType.Catalog,
        supportsPartitionBy = true))

    assertEquals(tenant, plan.tenant)
    assertEquals(WriteMode.Append, plan.mode)
    assertEquals(WriteTargetKind.HiveCatalog, plan.target.kind)
    assertEquals("default.target_table", plan.target.identifier)
    assertEquals(WriteExecutionType.CatalogSql, plan.executionType)
    assertEquals(
      "INSERT INTO TABLE default.target_table PARTITION (dt) SELECT * FROM source_view",
      WriteSqlRenderer.render(plan))
  }

  @Test
  def catalogAndMysqlOverwriteArePermanentlyDenied(): Unit = {
    val targets = Seq[ResolvedSaveSource](
      CatalogSaveSource("default.target", SaveTargetType.Catalog, supportsPartitionBy = true),
      CatalogSaveSource("doris.app.target", SaveTargetType.DorisCatalog, supportsPartitionBy = false),
      MysqlSaveSource("target", Seq("url" -> "jdbc:mysql://host/db")))

    targets.foreach { target =>
      val error = expectCompileException {
        planner.plan(tenant, "overwrite", "source", targetFormat(target), targetPath(target), Nil, Nil, target)
      }
      assertTrue(error.getMessage, error.getMessage.contains("permanently denied"))
    }
  }

  @Test
  def classifiesOnlyKnownRelativeProviderPathsAsManagedHdfs(): Unit = {
    val managed = planner.plan(
      tenant, "append", "source", "parquet", "reports/daily", Nil, Nil, ProviderSaveSource("parquet"))
    val external = planner.plan(
      tenant, "append", "source", "parquet", "s3a://bucket/reports", Nil, Nil, ProviderSaveSource("parquet"))

    assertEquals(WriteTargetKind.ManagedHdfs, managed.target.kind)
    assertEquals(WriteTargetKind.ExternalPath, external.target.kind)

    val unknownError = expectCompileException {
      planner.plan(tenant, "append", "source", "custom", "reports/daily", Nil, Nil, ProviderSaveSource("custom"))
    }
    assertTrue(unknownError.getMessage.contains("unknown-provider"))
  }

  @Test
  def externalPathOverwriteIsPermanentlyDenied(): Unit = {
    val error = expectCompileException {
      planner.plan(
        tenant, "overwrite", "source", "parquet", "file:///tmp/result", Nil, Nil, ProviderSaveSource("parquet"))
    }

    assertTrue(error.getMessage.contains("external-path"))
    assertTrue(error.getMessage.contains("permanently denied"))
  }

  @Test
  def encodedOrAmbiguousPathsNeverEnterManagedHdfs(): Unit = {
    val unsafePaths = Seq(
      "reports/%2e%2e/daily",
      "reports/daily?version=1",
      "reports/daily#latest",
      "reports//daily",
      "reports\\daily")

    unsafePaths.foreach { path =>
      val error = expectCompileException {
        planner.plan(tenant, "overwrite", "source", "parquet", path, Nil, Nil, ProviderSaveSource("parquet"))
      }
      assertTrue(path, error.getMessage.contains("external-path"))
      assertTrue(path, error.getMessage.contains("permanently denied"))
    }
  }

  private def targetFormat(source: ResolvedSaveSource): String = source match {
    case CatalogSaveSource(_, SaveTargetType.Catalog, _) => "hive"
    case CatalogSaveSource(_, SaveTargetType.DorisCatalog, _) => "doris"
    case _: MysqlSaveSource => "mysql"
    case _ => "unknown"
  }

  private def targetPath(source: ResolvedSaveSource): String = source match {
    case CatalogSaveSource(identifier, _, _) => identifier
    case MysqlSaveSource(dbtable, _) => dbtable
    case _ => "unknown"
  }

  private def expectCompileException(body: => Unit): CompileException = {
    try {
      body
      fail("Expected CompileException")
      throw new AssertionError("unreachable")
    } catch {
      case e: CompileException => e
    }
  }
}
