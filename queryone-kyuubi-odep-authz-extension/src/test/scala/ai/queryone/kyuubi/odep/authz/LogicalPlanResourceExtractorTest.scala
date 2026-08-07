package ai.queryone.kyuubi.odep.authz

import ai.queryone.extension.overwrite.ManagedHdfsWorkspacePolicy
import ai.queryone.provider.mysql.QueryOneMysqlRelation
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.ResolvedDBObjectName
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, CreateTableAsSelect, LocalRelation, ReplaceTableAsSelect, TableSpec}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table, TableCapability}
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.junit.Assert.{assertEquals, assertThrows}
import org.junit.{After, Before, Test}

import java.util

final class LogicalPlanResourceExtractorTest {
  private var spark: SparkSession = _
  private var extractor: LogicalPlanResourceExtractor = _

  @Before
  def setUp(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("queryone-odep-authz-extractor-test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    extractor = new LogicalPlanResourceExtractor(spark)
  }

  @After
  def tearDown(): Unit = {
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  @Test
  def extractsOdepCatalogReadAndWriteResources(): Unit = {
    val doris = relation("doris", "analytics", "events")
    val write = AppendData.byPosition(
      relation("jdbc", "ask00", "users"),
      LocalRelation(AttributeReference("id", LongType, nullable = false)()))

    assertEquals(
      Seq(OdepAuthzResource.table("doris", "analytics", "events", "read")),
      extractor.extract(doris))
    assertEquals(
      Seq(OdepAuthzResource.table("jdbc", "ask00", "users", "write")),
      extractor.extract(write))
  }

  @Test
  def extractsCreateAndReplaceTableAsSelectResources(): Unit = {
    val catalog = new TestCatalog("doris")
    val target = ResolvedDBObjectName(catalog, Seq("analytics", "events_copy"))
    val query = relation("jdbc", "ask00", "events")
    val tableSpec = TableSpec(Map.empty, None, Map.empty, None, None, None, external = false)
    val expected = Seq(
      OdepAuthzResource.table("doris", "analytics", "events_copy", "write"),
      OdepAuthzResource.table("jdbc", "ask00", "events", "read"))

    assertEquals(
      expected,
      extractor.extract(CreateTableAsSelect(target, Nil, query, tableSpec, Map.empty, false)))
    assertEquals(
      expected,
      extractor.extract(ReplaceTableAsSelect(target, Nil, query, tableSpec, Map.empty, true)))
  }

  @Test
  def extractsHiveTableAndRawHdfsPath(): Unit = {
    val schema = StructType(Seq(StructField("id", LongType, nullable = false)))
    val hiveTable = CatalogTable(
      TableIdentifier("users", Some("default")),
      CatalogTableType.MANAGED,
      CatalogStorageFormat.empty,
      schema,
      provider = Some("parquet"))
    val hive = LogicalRelation(new TestRelation(spark.sqlContext, schema), hiveTable)
    val hdfs = LogicalRelation(HadoopFsRelation(
      new TestFileIndex("hdfs:///public/odep/user/alice/events"),
      StructType(Nil),
      schema,
      None,
      new ParquetFileFormat(),
      Map.empty)(spark))

    assertEquals(
      Seq(OdepAuthzResource.table("hive", "default", "users", "read")),
      extractor.extract(hive))
    assertEquals(
      Seq(OdepAuthzResource.hdfs("/public/odep/user/alice/events", "read")),
      extractor.extract(hdfs))

    ManagedHdfsWorkspacePolicy.markManagedLoadRelations(hdfs, "alice")
    assertEquals(
      Seq(ManagedHdfsAccess(
        "alice",
        "read",
        OdepAuthzResource.hdfs("/public/odep/user/alice/events", "read"))),
      extractor.managedHdfsAccesses(hdfs))
    assertEquals(Seq.empty, extractor.extractUnmanaged(hdfs))
  }

  @Test
  def skipsOnlyExpectedManagedLoadInternalReadAndRestoresContext(): Unit = {
    val expectedPath = new Path("/public/odep/user/alice/events")
    val hdfs = LogicalRelation(HadoopFsRelation(
      new TestFileIndex(expectedPath.toString),
      StructType(Nil),
      StructType(Seq(StructField("id", LongType, nullable = false))),
      None,
      new ParquetFileFormat(),
      Map.empty)(spark))

    assertEquals(
      Seq.empty,
      ManagedHdfsWorkspacePolicy.withManagedLoadRead(
        spark.sparkContext,
        "alice",
        expectedPath) {
        assertEquals(
          Some("alice"),
          ManagedHdfsWorkspacePolicy.managedLoadReadContext(spark.sparkContext)
            .map(_.workspaceOwner))
        extractor.extract(hdfs)
      })
    assertEquals(None, ManagedHdfsWorkspacePolicy.managedLoadReadContext(spark.sparkContext))
    assertEquals(
      Seq(OdepAuthzResource.hdfs(expectedPath.toString, "read")),
      extractor.extract(hdfs))

    val inferredCsvFiles = LogicalRelation(HadoopFsRelation(
      new TestFileIndex(Seq(
        expectedPath + "/part-1.csv",
        expectedPath + "/nested/part-2.csv")),
      StructType(Nil),
      StructType(Seq(StructField("value", LongType, nullable = false))),
      None,
      new ParquetFileFormat(),
      Map.empty)(spark))
    assertEquals(
      Seq.empty,
      ManagedHdfsWorkspacePolicy.withManagedLoadRead(
        spark.sparkContext,
        "alice",
        expectedPath) {
        extractor.extract(inferredCsvFiles)
      })

    val error = assertThrows(
      classOf[OdepAuthorizationException],
      () => ManagedHdfsWorkspacePolicy.withManagedLoadRead(
        spark.sparkContext,
        "alice",
        new Path("/public/odep/user/alice/other")) {
        extractor.extract(hdfs)
      })
    assertEquals("Managed HDFS load resolved to an unexpected HDFS path", error.getMessage)
    assertEquals(None, ManagedHdfsWorkspacePolicy.managedLoadReadContext(spark.sparkContext))
  }

  @Test
  def extractsLogicalAliasFromPartitionedMysqlRelation(): Unit = {
    val schema = StructType(Seq(StructField("id", LongType, nullable = false)))
    val constructor = classOf[QueryOneMysqlRelation].getDeclaredConstructor(
      classOf[BaseRelation], classOf[String], classOf[String], classOf[String], classOf[String])
    constructor.setAccessible(true)
    val mysql = constructor.newInstance(
      new TestScanRelation(spark.sqlContext, schema),
      "odep",
      "jdbc",
      "sync_search",
      "drug_ai_drug_decision")

    assertEquals(
      Seq(OdepAuthzResource.table(
        "jdbc", "sync_search", "drug_ai_drug_decision", "read")),
      extractor.extract(LogicalRelation(mysql)))
  }

  @Test
  def skipsStaticCatalogAndStaticPartitionedMysqlRelation(): Unit = {
    assertEquals(
      Seq.empty,
      extractor.extract(relation("mysql_static", "app", "orders")))
    assertEquals(
      Seq.empty,
      extractor.extract(relation("doris_static", "analytics", "events")))

    val schema = StructType(Seq(StructField("id", LongType, nullable = false)))
    val constructor = classOf[QueryOneMysqlRelation].getDeclaredConstructor(
      classOf[BaseRelation], classOf[String], classOf[String], classOf[String], classOf[String])
    constructor.setAccessible(true)
    val mysql = constructor.newInstance(
      new TestScanRelation(spark.sqlContext, schema),
      "static",
      "mysql_static",
      "app",
      "orders")

    assertEquals(Seq.empty, extractor.extract(LogicalRelation(mysql)))
  }

  @Test
  def rejectsUnmanagedV1Sources(): Unit = {
    val relation = LogicalRelation(new TestRelation(
      spark.sqlContext,
      StructType(Seq(StructField("id", LongType, nullable = false)))))

    assertThrows(
      classOf[OdepAuthorizationException],
      () => extractor.extract(relation))
  }

  @Test
  def extractsManagedHdfsLoadAndOverwritePaths(): Unit = {
    spark.conf.set("spark.queryone.overwrite.workspaceRoot", "/public/odep/user")

    assertEquals(
      Seq(OdepAuthzResource.hdfs("/public/odep/user/alice/imports/users", "read")),
      extractor.extract(managedHdfsCommand(
        "QueryOneManagedHdfsLoadCommand", "alice", "imports/users")))
    assertEquals(
      Seq(OdepAuthzResource.hdfs("/public/odep/user/alice/reports/daily", "write")),
      extractor.extract(managedHdfsCommand(
        "QueryOneManagedHdfsOverwriteCommand", "alice", "reports/daily")))
  }

  @Test
  def rejectsNativeHdfsPathWrites(): Unit = {
    val query = relation("jdbc", "ask00", "source_events")
    val command = InsertIntoHadoopFsRelationCommand(
      new Path("hdfs:///public/share/output"),
      Map.empty,
      ifPartitionNotExists = false,
      Seq.empty,
      None,
      new ParquetFileFormat(),
      Map.empty,
      query,
      SaveMode.Overwrite,
      None,
      None,
      Seq("id"))

    val error = assertThrows(
      classOf[OdepAuthorizationException],
      () => extractor.extract(command))
    assertEquals(
      "Native HDFS path writes are disabled; use managed HDFS overwrite in the current user workspace",
      error.getMessage)

    assertEquals(
      Seq(OdepAuthzResource.table("jdbc", "ask00", "source_events", "read")),
      ManagedHdfsWorkspacePolicy.withManagedOverwriteWrite(spark.sparkContext) {
        extractor.extract(command)
      })
    assertThrows(
      classOf[OdepAuthorizationException],
      () => extractor.extract(command))
  }

  @Test
  def rejectsUnknownCatalog(): Unit = {
    assertThrows(
      classOf[OdepAuthorizationException],
      () => extractor.extract(relation("iceberg", "warehouse", "events")))
  }

  private def relation(catalog: String, database: String, table: String): DataSourceV2Relation =
    DataSourceV2Relation.create(
      new TestTable(table),
      Some(new TestCatalog(catalog)),
      Some(Identifier.of(Array(database), table)))

  private def managedHdfsCommand(
      className: String,
      tenant: String,
      relativePath: String): org.apache.spark.sql.catalyst.plans.logical.LogicalPlan = {
    val commandClass = Class.forName(s"ai.queryone.extension.overwrite.$className")
    commandClass.getConstructors.head.newInstance(
      tenant,
      "managed_view",
      "parquet",
      relativePath,
      Map.empty[String, String]).asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]
  }
}

private final class TestCatalog(private val catalogName: String) extends CatalogPlugin {
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}
  override def name(): String = catalogName
}

private final class TestTable(private val tableName: String) extends Table {
  override def name(): String = tableName
  override def schema(): StructType =
    StructType(Seq(StructField("id", LongType, nullable = false)))
  override def capabilities(): util.Set[TableCapability] = util.Collections.emptySet()
}

private class TestRelation(
    override val sqlContext: SQLContext,
    override val schema: StructType) extends BaseRelation

private final class TestScanRelation(
    sqlContextValue: SQLContext,
    schemaValue: StructType)
  extends TestRelation(sqlContextValue, schemaValue) with PrunedFilteredScan {

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    sqlContext.sparkContext.emptyRDD[Row]
}

private final class TestFileIndex(paths: Seq[String]) extends FileIndex {
  def this(path: String) = this(Seq(path))

  override def rootPaths: Seq[Path] = paths.map(new Path(_))
  override def listFiles(
      partitionFilters: Seq[org.apache.spark.sql.catalyst.expressions.Expression],
      dataFilters: Seq[org.apache.spark.sql.catalyst.expressions.Expression]): Seq[PartitionDirectory] =
    Seq.empty
  override def inputFiles: Array[String] = paths.toArray
  override def refresh(): Unit = {}
  override def sizeInBytes: Long = 0L
  override def partitionSchema: StructType = StructType(Nil)
}
