package ai.queryone.kyuubi.odep.authz

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Limit, LocalRelation}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table, TableCapability}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession
import org.junit.Assert.{assertEquals, assertThrows}
import org.junit.{After, Before, Test}

import java.nio.charset.StandardCharsets
import java.security.{KeyPairGenerator, Signature}
import java.util.{Base64, Collections}

final class OdepAuthorizationCheckTest {
  private var spark: SparkSession = _

  @Before
  def setUp(): Unit = {
    OdepPreAnalysisAuthorizationContext.clear()
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("queryone-odep-authz-rule-test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.kyuubi.session.user.sign.enabled", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  @After
  def tearDown(): Unit = {
    OdepPreAnalysisAuthorizationContext.clear()
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  @Test
  def checksSignedSessionUserBeforeAllowingPlan(): Unit = {
    setSignedUser("alice")
    var checkedSubject = ""
    var checkedResources = Seq.empty[OdepAuthzResource]
    val rule = new OdepAuthorizationCheck(spark, (subject, resources) => {
      checkedSubject = subject
      checkedResources = resources
      OdepAuthzResult(allowed = true, Seq.empty)
    })

    rule(relation("doris", "analytics", "events"))

    assertEquals("alice", checkedSubject)
    assertEquals(
      Seq(OdepAuthzResource.table("doris", "analytics", "events", "read")),
      checkedResources)
  }

  @Test
  def rejectsDeniedResourceWithConciseMessage(): Unit = {
    setSignedUser("alice")
    val resource = OdepAuthzResource.table("jdbc", "ask00", "secret", "read")
    val rule = new OdepAuthorizationCheck(
      spark,
      (_, _) => OdepAuthzResult(allowed = false, Seq(resource -> "NO_MATCHING_RESOURCE")))

    val error = assertThrows(
      classOf[OdepAuthorizationException],
      () => rule(relation("jdbc", "ask00", "secret")))
    assertEquals("Resource access denied: jdbc:ask00.secret:read", error.getMessage)
  }

  @Test
  def rejectsMissingKyuubiSignatureButAllowsResourceFreeQuery(): Unit = {
    val rule = new OdepAuthorizationCheck(
      spark,
      (_, _) => OdepAuthzResult(allowed = true, Seq.empty))

    rule(LocalRelation(AttributeReference("id", LongType, nullable = false)()))
    assertThrows(
      classOf[OdepAuthorizationException],
      () => rule(relation("hive", "default", "users")))
  }

  @Test
  def allowsStaticCatalogWithoutSubjectOrOdepCall(): Unit = {
    var authorizeCalls = 0
    val rule = new OdepAuthorizationCheck(spark, (_, _) => {
      authorizeCalls += 1
      OdepAuthzResult(allowed = true, Seq.empty)
    })

    rule(relation("mysql_static", "Dworks", "orders"))
    rule(relation("doris_static", "dataagent", "events"))

    assertEquals(0, authorizeCalls)
  }

  @Test
  def localRuleUsesScopedSubjectAndRestoresIt(): Unit = {
    var checkedSubject = ""
    val rule = new OdepAuthorizationCheck(
      spark,
      (subject, _) => {
        checkedSubject = subject
        OdepAuthzResult(allowed = true, Seq.empty)
      },
      LocalExecutionSubject.resolve)

    LocalExecutionSubject.withSubject(spark.sparkContext, "alice") {
      rule(relation("doris", "analytics", "events"))
      assertEquals(Some("alice"), LocalExecutionSubject.current(spark.sparkContext))
    }

    assertEquals("alice", checkedSubject)
    assertEquals(None, LocalExecutionSubject.current(spark.sparkContext))
  }

  @Test
  def kyuubiRuleDoesNotAcceptLocalSubject(): Unit = {
    val rule = new OdepAuthorizationCheck(
      spark,
      (_, _) => OdepAuthzResult(allowed = true, Seq.empty))

    LocalExecutionSubject.withSubject(spark.sparkContext, "alice") {
      assertThrows(
        classOf[OdepAuthorizationException],
        () => rule(relation("jdbc", "analytics", "events")))
    }
  }

  @Test
  def allowsOwnManagedWorkspaceWithoutCallingOdep(): Unit = {
    setSignedUser("alice")
    var authorizeCalls = 0
    val rule = new OdepAuthorizationCheck(spark, (_, _) => {
      authorizeCalls += 1
      OdepAuthzResult(allowed = true, Seq.empty)
    })

    rule(managedHdfsCommand("QueryOneManagedHdfsLoadCommand", "alice"))
    rule(managedHdfsCommand("QueryOneManagedHdfsOverwriteCommand", "alice"))

    assertEquals(0, authorizeCalls)
  }

  @Test
  def checksCrossOwnerManagedLoadWithOdep(): Unit = {
    setSignedUser("alice")
    var checkedResources = Seq.empty[OdepAuthzResource]
    val rule = new OdepAuthorizationCheck(spark, (_, resources) => {
      checkedResources = resources
      OdepAuthzResult(allowed = true, Seq.empty)
    })

    rule(managedHdfsCommand("QueryOneManagedHdfsLoadCommand", "bob"))

    assertEquals(
      Seq(OdepAuthzResource.hdfs("/public/odep/user/bob/reports/daily", "read")),
      checkedResources)
  }

  @Test
  def rejectsCrossOwnerManagedOverwriteWithoutCallingOdep(): Unit = {
    setSignedUser("alice")
    var authorizeCalls = 0
    val rule = new OdepAuthorizationCheck(spark, (_, _) => {
      authorizeCalls += 1
      OdepAuthzResult(allowed = true, Seq.empty)
    })

    val error = assertThrows(
      classOf[OdepAuthorizationException],
      () => rule(managedHdfsCommand("QueryOneManagedHdfsOverwriteCommand", "bob")))

    assertEquals(
      "Managed HDFS overwrite is only allowed in the current user's workspace",
      error.getMessage)
    assertEquals(0, authorizeCalls)
  }

  @Test
  def reusesPlanBoundHdfsProofAcrossCsvAnalysisAndPreviewPlans(): Unit = {
    var authorizeCalls = 0
    val authorize = (_: String, _: Seq[OdepAuthzResource]) => {
      authorizeCalls += 1
      OdepAuthzResult(allowed = true, Seq.empty)
    }
    val parser = new OdepPreAnalysisAuthorizationParser(
      spark,
      new SparkSqlParser,
      authorize,
      LocalExecutionSubject.resolve)
    val rule = new OdepAuthorizationCheck(
      spark,
      authorize,
      LocalExecutionSubject.resolve)

    LocalExecutionSubject.withSubject(spark.sparkContext, "alice") {
      val parsed = parser.parsePlan("select count(*) from csv.`hdfs:///public/events`")
      val unresolved = parsed.collect {
        case relation: UnresolvedRelation
            if relation.getTagValue(OdepPreAnalysisAuthorizationContext.ProofTag).nonEmpty =>
          relation
      }.head

      rule(hdfsRelation("hdfs:///public/events/part-00000.csv"))
      rule(hdfsRelation("hdfs:///public/events/nested/part-00001.csv"))

      val resolved = hdfsRelation("hdfs:///public/events")
      resolved.copyTagsFrom(unresolved)
      rule(resolved)
      rule(Limit(Literal(101), resolved))
    }

    assertEquals(1, authorizeCalls)
    assertEquals(Set.empty, OdepPreAnalysisAuthorizationContext.current)
  }

  @Test
  def rejectsResolvedHdfsPathThatDiffersFromPlanBoundProof(): Unit = {
    var authorizeCalls = 0
    val rule = new OdepAuthorizationCheck(
      spark,
      (_, _) => {
        authorizeCalls += 1
        OdepAuthzResult(allowed = true, Seq.empty)
      },
      LocalExecutionSubject.resolve)
    val relation = hdfsRelation("hdfs:///public/changed")
    relation.setTagValue(
      OdepPreAnalysisAuthorizationContext.ProofTag,
      OdepPreAuthorizedHdfsRead("alice", "/public/original"))
    OdepPreAnalysisAuthorizationContext.activate(
      Set(OdepPreAuthorizedHdfsRead("alice", "/public/original")))

    val error = LocalExecutionSubject.withSubject(spark.sparkContext, "alice") {
      assertThrows(classOf[OdepAuthorizationException], () => rule(relation))
    }

    assertEquals(
      "Native HDFS relation resolved to a path that was not authorized before analysis",
      error.getMessage)
    assertEquals(0, authorizeCalls)
    assertEquals(Set.empty, OdepPreAnalysisAuthorizationContext.current)
  }

  @Test
  def rejectsSiblingPathThatOnlySharesAuthorizedPrefix(): Unit = {
    var authorizeCalls = 0
    val rule = new OdepAuthorizationCheck(
      spark,
      (_, _) => {
        authorizeCalls += 1
        OdepAuthzResult(allowed = true, Seq.empty)
      },
      LocalExecutionSubject.resolve)
    OdepPreAnalysisAuthorizationContext.activate(
      Set(OdepPreAuthorizedHdfsRead("alice", "/public/events")))

    val error = LocalExecutionSubject.withSubject(spark.sparkContext, "alice") {
      assertThrows(
        classOf[OdepAuthorizationException],
        () => rule(hdfsRelation("hdfs:///public/events-other/part-00000.csv")))
    }

    assertEquals(
      "Native HDFS relation resolved to a path that was not authorized before analysis",
      error.getMessage)
    assertEquals(0, authorizeCalls)
    assertEquals(Set.empty, OdepPreAnalysisAuthorizationContext.current)
  }

  private def relation(catalog: String, database: String, table: String): DataSourceV2Relation =
    DataSourceV2Relation.create(
      new RuleTestTable(table),
      Some(new RuleTestCatalog(catalog)),
      Some(Identifier.of(Array(database), table)))

  private def hdfsRelation(path: String): LogicalRelation = {
    val schema = StructType(Seq(StructField("id", LongType, nullable = false)))
    LogicalRelation(HadoopFsRelation(
      new RuleTestFileIndex(path),
      StructType(Nil),
      schema,
      None,
      new ParquetFileFormat(),
      Map.empty)(spark))
  }

  private def managedHdfsCommand(
      className: String,
      workspaceOwner: String): org.apache.spark.sql.catalyst.plans.logical.LogicalPlan = {
    val commandClass = Class.forName(s"ai.queryone.extension.overwrite.$className")
    commandClass.getConstructors.head.newInstance(
      workspaceOwner,
      "managed_view",
      "parquet",
      "reports/daily",
      Map.empty[String, String]).asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]
  }

  private def setSignedUser(user: String): Unit = {
    val keyPair = KeyPairGenerator.getInstance("EC").generateKeyPair()
    val signer = Signature.getInstance("SHA256withECDSA")
    signer.initSign(keyPair.getPrivate)
    signer.update(user.getBytes(StandardCharsets.UTF_8))
    spark.sparkContext.setLocalProperty("kyuubi.session.user", user)
    spark.sparkContext.setLocalProperty(
      "kyuubi.session.sign.publickey",
      Base64.getEncoder.encodeToString(keyPair.getPublic.getEncoded))
    spark.sparkContext.setLocalProperty(
      "kyuubi.session.user.sign",
      Base64.getEncoder.encodeToString(signer.sign()))
  }
}

private final class RuleTestCatalog(private val catalogName: String) extends CatalogPlugin {
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}
  override def name(): String = catalogName
}

private final class RuleTestTable(private val tableName: String) extends Table {
  override def name(): String = tableName
  override def schema(): StructType =
    StructType(Seq(StructField("id", LongType, nullable = false)))
  override def capabilities(): java.util.Set[TableCapability] = Collections.emptySet()
}

private final class RuleTestFileIndex(path: String) extends FileIndex {
  override def rootPaths: Seq[Path] = Seq(new Path(path))
  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = Seq.empty
  override def inputFiles: Array[String] = Array(path)
  override def refresh(): Unit = {}
  override def sizeInBytes: Long = 0L
  override def partitionSchema: StructType = StructType(Nil)
}
