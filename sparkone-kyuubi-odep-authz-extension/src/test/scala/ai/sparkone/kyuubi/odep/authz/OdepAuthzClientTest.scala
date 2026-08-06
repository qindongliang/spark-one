package ai.sparkone.kyuubi.odep.authz

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.junit.Assert.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.{After, Before, Test}

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.{InetSocketAddress, URLDecoder}
import java.nio.charset.StandardCharsets

final class OdepAuthzClientTest {
  private val appId = "app_kyuubi"
  private val signKey = "test-sign-key"
  private val objectMapper = new ObjectMapper()
  private var server: HttpServer = _
  private var apiUrl: String = _
  private var response: String = _
  private var capturedForm: Map[String, String] = Map.empty

  @Before
  def setUp(): Unit = {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/api/sparkone/authz/check", handle)
    server.start()
    apiUrl = s"http://127.0.0.1:${server.getAddress.getPort}"
  }

  @After
  def tearDown(): Unit = server.stop(0)

  @Test
  def sendsSignedDorisAndHdfsBatch(): Unit = {
    response =
      """{"code":200,"success":true,"results":{"allowed":true,"decisions":[""" +
        """{"resource":{"resourceType":"doris","database":"analytics","table":"events","action":"read"},"allowed":true,"reason":"MATCHED"},""" +
        """{"resource":{"resourceType":"hdfs","path":"/public/odep/user/alice/data","action":"write"},"allowed":true,"reason":"MATCHED"}]}}"""
    val resources = Seq(
      OdepAuthzResource.table("doris", "analytics", "events", "read"),
      OdepAuthzResource.hdfs("hdfs:///public/odep/user/alice/data", "write"))

    val result = client().check("alice", resources)

    assertTrue(result.allowed)
    assertTrue(result.denied.isEmpty)
    assertEquals("alice", capturedForm("subject"))
    val request = objectMapper.readTree(capturedForm("requests"))
    assertEquals("doris", request.get(0).get("resourceType").asText())
    assertEquals("/public/odep/user/alice/data", request.get(1).get("path").asText())
    assertFalse(capturedForm.contains("appSignKey"))
  }

  @Test
  def returnsDeniedDecision(): Unit = {
    response =
      """{"code":200,"success":true,"results":{"allowed":false,"decisions":[""" +
        """{"resource":{"resourceType":"jdbc","database":"ask00","table":"secret","action":"read"},"allowed":false,"reason":"BLACKLISTED"}]}}"""
    val resource = OdepAuthzResource.table("jdbc", "ask00", "secret", "read")

    val result = client().check("alice", Seq(resource))

    assertFalse(result.allowed)
    assertEquals(Seq(resource -> "BLACKLISTED"), result.denied)
  }

  @Test
  def rejectsIncompleteDecisionResponse(): Unit = {
    response =
      """{"code":200,"success":true,"results":{"allowed":true,"decisions":[]}}"""

    assertThrows(
      classOf[OdepAuthorizationException],
      () => client().check(
        "alice",
        Seq(OdepAuthzResource.table("hive", "default", "users", "read"))))
  }

  @Test
  def usesRuntimePropertiesWhenEnvironmentIsMissing(): Unit = {
    response = allowedJdbcResponse
    val properties = Map(
      "sparkone.odep.api.url" -> apiUrl,
      "sparkone.odep.app.id" -> appId,
      "sparkone.odep.sign.key" -> signKey,
      "sparkone.odep.connect.timeout.seconds" -> "2",
      "sparkone.odep.request.timeout.seconds" -> "2")

    val result = OdepAuthzClient.fromRuntimeConfiguration(Map.empty, properties)
      .check("alice", Seq(OdepAuthzResource.table("jdbc", "analytics", "events", "read")))

    assertTrue(result.allowed)
    assertEquals(appId, capturedForm("appId"))
  }

  @Test
  def prefersEnvironmentOverRuntimeProperties(): Unit = {
    response = allowedJdbcResponse
    val properties = Map(
      "sparkone.odep.api.url" -> "ftp://invalid.example",
      "sparkone.odep.app.id" -> "wrong-app",
      "sparkone.odep.sign.key" -> "wrong-key",
      "sparkone.odep.connect.timeout.seconds" -> "invalid",
      "sparkone.odep.request.timeout.seconds" -> "invalid")
    val environment = Map(
      "ODEP_API_URL" -> apiUrl,
      "ODEP_KYUUBI_APP_ID" -> appId,
      "ODEP_KYUUBI_SIGN_KEY" -> signKey,
      "ODEP_CONNECT_TIMEOUT_SECONDS" -> "2",
      "ODEP_REQUEST_TIMEOUT_SECONDS" -> "2")

    val result = OdepAuthzClient.fromRuntimeConfiguration(environment, properties)
      .check("alice", Seq(OdepAuthzResource.table("jdbc", "analytics", "events", "read")))

    assertTrue(result.allowed)
    assertEquals(appId, capturedForm("appId"))
  }

  private def client(): OdepAuthzClient =
    new OdepAuthzClient(apiUrl, appId, signKey, 2000, 2000)

  private def allowedJdbcResponse: String =
    """{"code":200,"success":true,"results":{"allowed":true,"decisions":[""" +
      """{"resource":{"resourceType":"jdbc","database":"analytics","table":"events","action":"read"},"allowed":true,"reason":"MATCHED"}]}}"""

  private def handle(exchange: HttpExchange): Unit = {
    capturedForm = parseForm(readAll(exchange.getRequestBody))
    val signed = capturedForm - "sign"
    val valid = appId == capturedForm.getOrElse("appId", "") &&
      capturedForm.get("sign").contains(OdepAuthzClient.sign(signed, signKey))
    if (!valid) {
      exchange.sendResponseHeaders(403, -1)
      exchange.close()
      return
    }
    val body = response.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.set("Content-Type", "application/json")
    exchange.sendResponseHeaders(200, body.length)
    exchange.getResponseBody.write(body)
    exchange.close()
  }

  private def parseForm(body: String): Map[String, String] =
    body.split("&").map { pair =>
      val parts = pair.split("=", 2)
      URLDecoder.decode(parts(0), StandardCharsets.UTF_8.name()) ->
        URLDecoder.decode(parts(1), StandardCharsets.UTF_8.name())
    }.toMap

  private def readAll(input: InputStream): String = {
    val output = new ByteArrayOutputStream()
    val buffer = new Array[Byte](1024)
    var read = input.read(buffer)
    while (read != -1) {
      output.write(buffer, 0, read)
      read = input.read(buffer)
    }
    new String(output.toByteArray, StandardCharsets.UTF_8)
  }
}
