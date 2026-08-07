package ai.queryone.kyuubi.odep.authz

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import java.io.{ByteArrayOutputStream, IOException, InputStream, OutputStream}
import java.net.{HttpURLConnection, URL, URLEncoder}
import java.nio.charset.StandardCharsets
import java.security.{MessageDigest, SecureRandom}
import java.util.{Locale, TreeMap}

private[authz] final class OdepAuthzClient(
    apiUrl: String,
    appIdValue: String,
    signKeyValue: String,
    connectTimeoutMillis: Int,
    readTimeoutMillis: Int) {

  import OdepAuthzClient._

  private val endpoint = new URL(
    trimTrailingSlash(apiUrl) + "/api/queryone/authz/check")
  private val appId = requireNonBlank(appIdValue, "ODEP appId")
  private val signKey = requireNonBlank(signKeyValue, "ODEP sign key")
  private val objectMapper = new ObjectMapper()
  private val secureRandom = new SecureRandom()

  if (endpoint.getProtocol != "http" && endpoint.getProtocol != "https") {
    throw new IllegalArgumentException("ODEP authorization API URL must use HTTP or HTTPS")
  }
  requirePositive(connectTimeoutMillis, "connect timeout")
  requirePositive(readTimeoutMillis, "read timeout")

  def check(subjectValue: String, resources: Seq[OdepAuthzResource]): OdepAuthzResult = {
    val subject = requireNonBlank(subjectValue, "subject")
    if (resources.isEmpty) {
      return OdepAuthzResult(allowed = true, Seq.empty)
    }

    val requestsJson = serialize(resources)
    val timestamp = String.valueOf(System.currentTimeMillis() / 1000)
    val nonceValue = nonce()
    val signed = Map(
      "appId" -> appId,
      "nonce" -> nonceValue,
      "timestamp" -> timestamp,
      "subject" -> subject,
      "requests" -> requestsJson)
    val form = signed ++ Map("sign" -> sign(signed, signKey))
    parseResponse(post(form), resources)
  }

  private def serialize(resources: Seq[OdepAuthzResource]): String = {
    val requests = objectMapper.createArrayNode()
    resources.foreach { resource =>
      val node = requests.addObject()
      node.put("resourceType", resource.resourceType)
      resource.database.foreach(node.put("database", _))
      resource.table.foreach(node.put("table", _))
      resource.path.foreach(node.put("path", _))
      node.put("action", resource.action)
    }
    objectMapper.writeValueAsString(requests)
  }

  private def post(form: Map[String, String]): Array[Byte] = {
    var connection: HttpURLConnection = null
    try {
      val requestBody = formBody(form)
      connection = endpoint.openConnection().asInstanceOf[HttpURLConnection]
      connection.setInstanceFollowRedirects(false)
      connection.setRequestMethod("POST")
      connection.setConnectTimeout(connectTimeoutMillis)
      connection.setReadTimeout(readTimeoutMillis)
      connection.setDoOutput(true)
      connection.setRequestProperty(
        "Content-Type",
        "application/x-www-form-urlencoded; charset=UTF-8")
      connection.setFixedLengthStreamingMode(requestBody.length)
      val output = connection.getOutputStream
      try {
        output.write(requestBody)
      } finally {
        output.close()
      }

      val status = connection.getResponseCode
      if (status != HttpURLConnection.HTTP_OK) {
        closeQuietly(connection.getErrorStream)
        throw new OdepAuthorizationException(
          s"ODEP authorization API returned HTTP $status")
      }
      val input = connection.getInputStream
      try {
        readLimited(input)
      } finally {
        input.close()
      }
    } catch {
      case error: OdepAuthorizationException => throw error
      case error: IOException =>
        throw new OdepAuthorizationException(
          s"Failed to call ODEP authorization API: $endpoint",
          error)
    } finally {
      if (connection != null) {
        connection.disconnect()
      }
    }
  }

  private def parseResponse(
      responseBody: Array[Byte],
      resources: Seq[OdepAuthzResource]): OdepAuthzResult = {
    val root = try {
      objectMapper.readTree(responseBody)
    } catch {
      case error: IOException =>
        throw new OdepAuthorizationException("ODEP authorization response is invalid JSON", error)
    }
    if (root == null || !root.isObject) {
      throw new OdepAuthorizationException("ODEP authorization response must be an object")
    }
    if (root.path("code").asInt(-1) != 200 || !root.path("success").asBoolean(false)) {
      throw new OdepAuthorizationException(
        s"ODEP authorization business request failed: code=${root.path("code").asInt(-1)}")
    }

    val result = root.get("results")
    if (result == null || !result.isObject || !result.path("allowed").isBoolean) {
      throw new OdepAuthorizationException("ODEP authorization results are invalid")
    }
    val decisions = result.get("decisions")
    if (decisions == null || !decisions.isArray || decisions.size() != resources.size) {
      throw new OdepAuthorizationException(
        "ODEP authorization decisions do not match the request count")
    }

    val denied = resources.zipWithIndex.flatMap { case (resource, index) =>
      val decision = decisions.get(index)
      val allowedNode = decision.get("allowed")
      val resourceNode = decision.get("resource")
      if (!decision.isObject || allowedNode == null || !allowedNode.isBoolean ||
          resourceNode == null || !resourceNode.isObject ||
          !sameResource(resourceNode, resource)) {
        throw new OdepAuthorizationException(
          s"ODEP authorization decision ${index + 1} is invalid")
      }
      if (allowedNode.asBoolean()) {
        None
      } else {
        val reason = text(decision.get("reason")).getOrElse("DENIED")
        Some(resource -> reason)
      }
    }

    val allowed = result.get("allowed").asBoolean()
    if (allowed != denied.isEmpty) {
      throw new OdepAuthorizationException(
        "ODEP authorization overall result conflicts with its decisions")
    }
    OdepAuthzResult(allowed, denied)
  }

  private def sameResource(node: JsonNode, resource: OdepAuthzResource): Boolean = {
    text(node.get("resourceType")).contains(resource.resourceType) &&
      text(node.get("action")).contains(resource.action) &&
      resource.database.forall(value => text(node.get("database")).contains(value)) &&
      resource.table.forall(value => text(node.get("table")).contains(value)) &&
      resource.path.forall(value => text(node.get("path")).contains(value))
  }

  private def text(node: JsonNode): Option[String] =
    Option(node).filterNot(_.isNull).map(_.asText()).filter(_.nonEmpty)

  private def formBody(form: Map[String, String]): Array[Byte] =
    form.iterator.map { case (key, value) =>
      s"${encode(key)}=${encode(value)}"
    }.mkString("&").getBytes(StandardCharsets.UTF_8)

  private def readLimited(input: InputStream): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val buffer = new Array[Byte](8192)
    var total = 0
    var read = input.read(buffer)
    while (read != -1) {
      total += read
      if (total > OdepAuthzClient.MaxResponseBytes) {
        throw new OdepAuthorizationException("ODEP authorization response is too large")
      }
      output.write(buffer, 0, read)
      read = input.read(buffer)
    }
    output.toByteArray
  }

  private def nonce(): String = {
    val value = new Array[Char](16)
    var index = 0
    while (index < value.length) {
      value(index) = OdepAuthzClient.NonceAlphabet(
        secureRandom.nextInt(OdepAuthzClient.NonceAlphabet.length))
      index += 1
    }
    new String(value)
  }
}

private[authz] object OdepAuthzClient {
  private val DefaultConnectTimeoutSeconds = 5
  private val DefaultReadTimeoutSeconds = 60
  private val MaxResponseBytes = 10 * 1024 * 1024
  private val NonceAlphabet =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray
  private val RuntimePropertyNames = Map(
    "ODEP_API_URL" -> "queryone.odep.api.url",
    "ODEP_KYUUBI_APP_ID" -> "queryone.odep.app.id",
    "ODEP_KYUUBI_SIGN_KEY" -> "queryone.odep.sign.key",
    "ODEP_CONNECT_TIMEOUT_SECONDS" -> "queryone.odep.connect.timeout.seconds",
    "ODEP_REQUEST_TIMEOUT_SECONDS" -> "queryone.odep.request.timeout.seconds")

  def fromRuntimeConfiguration(): OdepAuthzClient = {
    val properties = RuntimePropertyNames.values.flatMap { name =>
      sys.props.get(name).map(name -> _)
    }.toMap
    fromRuntimeConfiguration(sys.env, properties)
  }

  private[authz] def fromRuntimeConfiguration(
      environment: Map[String, String],
      properties: Map[String, String]): OdepAuthzClient = {
    val propertyConfiguration = RuntimePropertyNames.flatMap { case (environmentName, propertyName) =>
      properties.get(propertyName)
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(environmentName -> _)
    }
    val environmentConfiguration = RuntimePropertyNames.keys.flatMap { name =>
      environment.get(name)
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(name -> _)
    }.toMap
    fromEnvironment(propertyConfiguration ++ environmentConfiguration)
  }

  private[authz] def fromEnvironment(environment: java.util.Map[String, String]): OdepAuthzClient = {
    import scala.collection.JavaConverters._
    fromEnvironment(environment.asScala.toMap)
  }

  private[authz] def fromEnvironment(environment: Map[String, String]): OdepAuthzClient =
    new OdepAuthzClient(
      required(environment, "ODEP_API_URL"),
      required(environment, "ODEP_KYUUBI_APP_ID"),
      required(environment, "ODEP_KYUUBI_SIGN_KEY"),
      positiveSecondsMillis(
        environment,
        "ODEP_CONNECT_TIMEOUT_SECONDS",
        DefaultConnectTimeoutSeconds),
      positiveSecondsMillis(
        environment,
        "ODEP_REQUEST_TIMEOUT_SECONDS",
        DefaultReadTimeoutSeconds))

  private[authz] def sign(parameters: Map[String, String], signKey: String): String = {
    val sorted = new TreeMap[String, String]()
    parameters.foreach { case (key, value) => sorted.put(key, value) }
    sorted.put("appSignKey", signKey)
    val payload = new StringBuilder()
    val entries = sorted.entrySet().iterator()
    while (entries.hasNext) {
      val entry = entries.next()
      if (payload.nonEmpty) {
        payload.append('&')
      }
      payload.append(entry.getKey).append('=').append(entry.getValue)
    }
    val digest = MessageDigest.getInstance("SHA-1").digest(
      payload.toString().getBytes(StandardCharsets.UTF_8))
    val hex = new StringBuilder(digest.length * 2)
    digest.foreach { value =>
      hex.append(String.format(Locale.ROOT, "%02x", Integer.valueOf(value & 0xff)))
    }
    hex.toString()
  }

  private def required(environment: Map[String, String], name: String): String =
    requireNonBlank(environment.getOrElse(name, null), name)

  private def positiveSecondsMillis(
      environment: Map[String, String],
      name: String,
      defaultSeconds: Int): Int = {
    environment.get(name).filter(_.trim.nonEmpty) match {
      case None => defaultSeconds * 1000
      case Some(value) =>
        try {
          Math.multiplyExact(requirePositive(value.trim.toInt, name), 1000)
        } catch {
          case error: NumberFormatException =>
            throw new OdepAuthorizationException(s"$name must be a positive integer", error)
          case error: ArithmeticException =>
            throw new OdepAuthorizationException(s"$name is too large", error)
        }
    }
  }

  private def encode(value: String): String =
    URLEncoder.encode(value, StandardCharsets.UTF_8.name())

  private def trimTrailingSlash(value: String): String = {
    var result = requireNonBlank(value, "ODEP_API_URL")
    while (result.endsWith("/")) {
      result = result.substring(0, result.length - 1)
    }
    result
  }

  private def requireNonBlank(value: String, name: String): String = {
    if (value == null || value.trim.isEmpty) {
      throw new OdepAuthorizationException(s"$name must be configured")
    }
    value.trim
  }

  private def requirePositive(value: Int, name: String): Int = {
    if (value <= 0) {
      throw new OdepAuthorizationException(s"$name must be positive")
    }
    value
  }

  private def closeQuietly(input: InputStream): Unit = {
    if (input != null) {
      try {
        input.close()
      } catch {
        case _: IOException =>
      }
    }
  }
}
