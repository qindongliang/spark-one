package ai.queryone.kyuubi.odep.authz

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkSqlParser
import org.junit.Assert.{assertEquals, assertThrows, assertTrue}
import org.junit.{After, Before, Test}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.{KeyPairGenerator, Signature}
import java.util.Base64
import java.util.concurrent.{CopyOnWriteArrayList, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._

final class OdepPreAnalysisAuthorizationParserTest {
  private var spark: SparkSession = _
  private var allow = false
  private var checkedSubject = ""
  private var checkedResources = Seq.empty[OdepAuthzResource]
  private val authorizeCalls = new AtomicInteger()
  private val events = new CopyOnWriteArrayList[String]()
  private var firstJob = new CountDownLatch(1)

  @Before
  def setUp(): Unit = {
    OdepPreAnalysisAuthorizationContext.clear()
    allow = false
    checkedSubject = ""
    checkedResources = Seq.empty
    authorizeCalls.set(0)
    events.clear()
    firstJob = new CountDownLatch(1)
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("queryone-pre-analysis-authz-parser-test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.kyuubi.session.user.sign.enabled", "true")
      .withExtensions { extensions =>
        extensions.injectParser { (currentSpark, delegate) =>
          new OdepPreAnalysisAuthorizationParser(
            currentSpark,
            delegate,
            authorize,
            LocalExecutionSubject.resolve)
        }
      }
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        events.add("job")
        firstJob.countDown()
      }
    })
  }

  @After
  def tearDown(): Unit = {
    OdepPreAnalysisAuthorizationContext.clear()
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  @Test
  def deniesBeforeFileResolutionStartsAnySparkJob(): Unit = {
    val error = LocalExecutionSubject.withSubject(spark.sparkContext, "alice") {
      assertThrows(
        classOf[OdepAuthorizationException],
        () => spark.sql("select count(*) from csv.`/path/that/must/not/be/resolved`").collect())
    }

    assertEquals("Resource access denied: hdfs:/path/that/must/not/be/resolved:read", error.getMessage)
    assertEquals("alice", checkedSubject)
    assertEquals(
      Seq(OdepAuthzResource.hdfs("/path/that/must/not/be/resolved", "read")),
      checkedResources)
    assertEquals(1, authorizeCalls.get())
    assertTrue(!events.contains("job"))
  }

  @Test
  def authorizesBeforeCsvSchemaInferenceJob(): Unit = {
    val root = Files.createTempDirectory("queryone-pre-authz-csv-")
    Files.write(
      root.resolve("events.csv"),
      "id,name\n1,alice\n2,bob\n".getBytes(StandardCharsets.UTF_8))
    allow = true

    try {
      LocalExecutionSubject.withSubject(spark.sparkContext, "alice") {
        spark.sql(s"select count(*) from csv.`${root.toString}`").collect()
      }

      assertTrue("CSV schema inference should start a Spark job", firstJob.await(5, TimeUnit.SECONDS))
      assertEquals("authorize", events.get(0))
      assertTrue(events.asScala.indexOf("authorize") < events.asScala.indexOf("job"))
    } finally {
      deleteRecursively(root)
    }
  }

  @Test
  def batchesAndDeduplicatesNativeHdfsResources(): Unit = {
    allow = true
    LocalExecutionSubject.withSubject(spark.sparkContext, "alice") {
      spark.sessionState.sqlParser.parsePlan(
        """select * from csv.`/public/a/` union all
          |select * from parquet.`hdfs:///public/b` union all
          |select * from csv.`/public/a`""".stripMargin)
    }

    assertEquals(1, authorizeCalls.get())
    assertEquals(
      Seq(
        OdepAuthzResource.hdfs("/public/a", "read"),
        OdepAuthzResource.hdfs("/public/b", "read")),
      checkedResources)
  }

  @Test
  def rejectsGlobBeforeCallingRmsOrResolvingSubject(): Unit = {
    val error = assertThrows(
      classOf[OdepAuthorizationException],
      () => spark.sessionState.sqlParser.parsePlan("select * from csv.`/public/*/events.csv`"))

    assertTrue(error.getMessage.contains("without glob patterns"))
    assertEquals(0, authorizeCalls.get())
    assertTrue(!events.contains("job"))
  }

  @Test
  def usesSignedKyuubiSubjectDuringPreAnalysisAuthorization(): Unit = {
    val parser = new OdepPreAnalysisAuthorizationParser(
      spark,
      new SparkSqlParser,
      authorize,
      KyuubiSessionSubject.resolve)
    setSignedUser("kyuubi-user")
    allow = true

    parser.parsePlan("select * from orc.`viewfs:///public/events`")

    assertEquals("kyuubi-user", checkedSubject)
    assertEquals(
      Seq(OdepAuthzResource.hdfs("/public/events", "read")),
      checkedResources)
  }

  private def authorize(
      subject: String,
      resources: Seq[OdepAuthzResource]): OdepAuthzResult = {
    authorizeCalls.incrementAndGet()
    checkedSubject = subject
    checkedResources = resources
    events.add("authorize")
    if (allow) {
      OdepAuthzResult(allowed = true, Seq.empty)
    } else {
      OdepAuthzResult(
        allowed = false,
        resources.map(resource => resource -> "NO_MATCHING_RESOURCE"))
    }
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

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      val stream = Files.walk(path)
      try stream.iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
      finally stream.close()
    }
  }
}
