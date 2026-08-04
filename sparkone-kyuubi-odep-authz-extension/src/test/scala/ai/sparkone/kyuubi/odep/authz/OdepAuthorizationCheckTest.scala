package ai.sparkone.kyuubi.odep.authz

import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table, TableCapability}
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
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("sparkone-odep-authz-rule-test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.kyuubi.session.user.sign.enabled", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  @After
  def tearDown(): Unit = {
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

  private def relation(catalog: String, database: String, table: String): DataSourceV2Relation =
    DataSourceV2Relation.create(
      new RuleTestTable(table),
      Some(new RuleTestCatalog(catalog)),
      Some(Identifier.of(Array(database), table)))

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
