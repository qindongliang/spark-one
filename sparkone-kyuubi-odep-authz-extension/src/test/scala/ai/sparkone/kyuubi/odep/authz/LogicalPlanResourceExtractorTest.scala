package ai.sparkone.kyuubi.odep.authz

import ai.sparkone.provider.mysql.SparkOneMysqlRelation
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.ResolvedDBObjectName
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, CreateTableAsSelect, LocalRelation, ReplaceTableAsSelect, TableSpec}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table, TableCapability}
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
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
      .appName("sparkone-odep-authz-extractor-test")
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
  }

  @Test
  def extractsLogicalAliasFromPartitionedMysqlRelation(): Unit = {
    val schema = StructType(Seq(StructField("id", LongType, nullable = false)))
    val constructor = classOf[SparkOneMysqlRelation].getDeclaredConstructor(
      classOf[BaseRelation], classOf[String], classOf[String])
    constructor.setAccessible(true)
    val mysql = constructor.newInstance(
      new TestScanRelation(spark.sqlContext, schema),
      "sync_search",
      "drug_ai_drug_decision")

    assertEquals(
      Seq(OdepAuthzResource.table(
        "jdbc", "sync_search", "drug_ai_drug_decision", "read")),
      extractor.extract(LogicalRelation(mysql)))
  }

  @Test
  def extractsManagedHdfsLoadAndOverwritePaths(): Unit = {
    spark.conf.set("spark.sparkone.overwrite.workspaceRoot", "/public/odep/user")

    assertEquals(
      Seq(OdepAuthzResource.hdfs("/public/odep/user/alice/imports/users", "read")),
      extractor.extract(managedHdfsCommand(
        "SparkOneManagedHdfsLoadCommand", "alice", "imports/users")))
    assertEquals(
      Seq(OdepAuthzResource.hdfs("/public/odep/user/alice/reports/daily", "write")),
      extractor.extract(managedHdfsCommand(
        "SparkOneManagedHdfsOverwriteCommand", "alice", "reports/daily")))
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
    val commandClass = Class.forName(s"ai.sparkone.extension.overwrite.$className")
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

private final class TestFileIndex(path: String) extends FileIndex {
  override def rootPaths: Seq[Path] = Seq(new Path(path))
  override def listFiles(
      partitionFilters: Seq[org.apache.spark.sql.catalyst.expressions.Expression],
      dataFilters: Seq[org.apache.spark.sql.catalyst.expressions.Expression]): Seq[PartitionDirectory] =
    Seq.empty
  override def inputFiles: Array[String] = Array(path)
  override def refresh(): Unit = {}
  override def sizeInBytes: Long = 0L
  override def partitionSchema: StructType = StructType(Nil)
}
