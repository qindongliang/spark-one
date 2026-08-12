package queryone.server

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.{Locale, TreeMap}
import scala.collection.concurrent.TrieMap

final case class InternalApiPrincipal(appId: String, subject: String, requestId: String)

final class InternalApiAuthException(message: String) extends RuntimeException(message)

/**
  * Authentication for service-to-service QueryOne calls.
  *
  * The canonical payload intentionally matches the existing ODEP OpenAPI SHA-1
  * convention so ODEP System can reuse its current signing utility. The body
  * hash and request path prevent a signed request from being changed or replayed
  * against another internal endpoint.
  */
final class InternalApiAuth private[server] (
    appId: String,
    signKey: String,
    clockSkewSeconds: Long,
    nonceTtlSeconds: Long,
    nowSeconds: () => Long = () => System.currentTimeMillis() / 1000L) {

  import InternalApiAuth.{constantTimeEquals, sha256}

  private val acceptedNonces = TrieMap.empty[String, Long]

  def verify(
      path: String,
      body: String,
      headers: Map[String, String],
      mapper: ObjectMapper): InternalApiPrincipal = {
    if (body == null || body.getBytes(StandardCharsets.UTF_8).length > InternalApiAuth.MaxBodyBytes) {
      throw new InternalApiAuthException("Request body is missing or too large")
    }
    val node = try mapper.readTree(body) catch {
      case _: Exception => throw new InternalApiAuthException("Request body must be valid JSON")
    }
    if (node == null || !node.isObject) {
      throw new InternalApiAuthException("Request body must be a JSON object")
    }

    val requestedAppId = header(headers, InternalApiAuth.AppIdHeader)
    val timestamp = header(headers, InternalApiAuth.TimestampHeader)
    val nonce = header(headers, InternalApiAuth.NonceHeader)
    val signature = header(headers, InternalApiAuth.SignatureHeader)
    val bodyHash = header(headers, InternalApiAuth.BodyHashHeader)
    if (requestedAppId != appId) {
      throw new InternalApiAuthException("Unknown internal API caller")
    }
    val timestampValue = parseTimestamp(timestamp)
    val now = nowSeconds()
    if ((BigInt(now) - BigInt(timestampValue)).abs > BigInt(clockSkewSeconds)) {
      throw new InternalApiAuthException("Internal API request timestamp is outside the allowed window")
    }
    val actualBodyHash = sha256(body)
    if (!constantTimeEquals(actualBodyHash, bodyHash)) {
      throw new InternalApiAuthException("Internal API request body hash is invalid")
    }

    val subject = requiredText(node, "subject")
    val requestId = requiredText(node, "requestId")
    val signed = Map(
      "appId" -> requestedAppId,
      "bodySha256" -> bodyHash,
      "nonce" -> nonce,
      "path" -> path,
      "requestId" -> requestId,
      "subject" -> subject,
      "timestamp" -> timestamp)
    val expectedSignature = InternalApiAuth.sign(signed, signKey)
    if (!constantTimeEquals(expectedSignature, signature)) {
      throw new InternalApiAuthException("Internal API request signature is invalid")
    }

    val nonceKey = requestedAppId + ":" + nonce
    cleanupExpiredNonces(now)
    val expiresAt = Math.max(now + nonceTtlSeconds, timestampValue + clockSkewSeconds + 1L)
    acceptedNonces.putIfAbsent(nonceKey, expiresAt) match {
      case Some(_) => throw new InternalApiAuthException("Internal API request nonce has already been used")
      case None =>
    }
    InternalApiPrincipal(requestedAppId, subject, requestId)
  }

  private def cleanupExpiredNonces(now: Long): Unit = {
    acceptedNonces.foreach { case (key, expiresAt) =>
      if (expiresAt < now) acceptedNonces.remove(key, expiresAt)
    }
  }

  private def parseTimestamp(value: String): Long = {
    try value.toLong catch {
      case _: NumberFormatException => throw new InternalApiAuthException("Internal API timestamp is invalid")
    }
  }

  private def header(headers: Map[String, String], name: String): String = {
    headers.collectFirst { case (key, value) if key.equalsIgnoreCase(name) => value }
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(throw new InternalApiAuthException(s"Missing internal API header: $name"))
  }

  private def requiredText(node: JsonNode, name: String): String = {
    val value = Option(node.get(name)).filterNot(_.isNull).map(_.asText()).map(_.trim).filter(_.nonEmpty)
    value.getOrElse(throw new InternalApiAuthException(s"Missing internal API field: $name"))
  }
}

object InternalApiAuth {
  val AppIdHeader = "X-QueryOne-App-Id"
  val TimestampHeader = "X-QueryOne-Timestamp"
  val NonceHeader = "X-QueryOne-Nonce"
  val SignatureHeader = "X-QueryOne-Signature"
  val BodyHashHeader = "X-QueryOne-Body-SHA256"
  private val MaxBodyBytes = 10 * 1024 * 1024

  def fromSystemProperties(): Option[InternalApiAuth] = {
    val appId = configured("queryone.internal.auth.app.id", "QUERYONE_INTERNAL_AUTH_APP_ID")
    val signKey = configured("queryone.internal.auth.sign.key", "QUERYONE_INTERNAL_AUTH_SIGN_KEY")
    for {
      caller <- appId
      key <- signKey
    } yield new InternalApiAuth(
      caller,
      key,
      positiveLong("queryone.internal.auth.clock.skew.seconds", "QUERYONE_INTERNAL_AUTH_CLOCK_SKEW_SECONDS", 300L),
      positiveLong("queryone.internal.auth.nonce.ttl.seconds", "QUERYONE_INTERNAL_AUTH_NONCE_TTL_SECONDS", 600L))
  }

  private[server] def sign(parameters: Map[String, String], signKey: String): String = {
    val sorted = new TreeMap[String, String]()
    parameters.foreach { case (key, value) => sorted.put(key, value) }
    sorted.put("appSignKey", signKey)
    val payload = sorted.entrySet().iterator()
    val builder = new StringBuilder
    while (payload.hasNext) {
      val entry = payload.next()
      if (builder.nonEmpty) builder.append('&')
      builder.append(entry.getKey).append('=').append(entry.getValue)
    }
    hex(MessageDigest.getInstance("SHA-1").digest(builder.toString().getBytes(StandardCharsets.UTF_8)))
  }

  private[server] def sha256(value: String): String =
    hex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)))

  private[server] def constantTimeEquals(left: String, right: String): Boolean = {
    if (left == null || right == null) false
    else MessageDigest.isEqual(left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8))
  }

  private def configured(property: String, environment: String): Option[String] =
    sys.props.get(property).orElse(sys.env.get(environment)).map(_.trim).filter(_.nonEmpty)

  private def positiveLong(property: String, environment: String, defaultValue: Long): Long = {
    configured(property, environment).map { value =>
      try {
        val parsed = value.toLong
        if (parsed <= 0) throw new IllegalArgumentException
        parsed
      } catch {
        case _: Exception => throw new IllegalArgumentException(s"$property must be a positive integer")
      }
    }.getOrElse(defaultValue)
  }

  private def hex(bytes: Array[Byte]): String = {
    val builder = new StringBuilder(bytes.length * 2)
    bytes.foreach(value => builder.append(String.format(Locale.ROOT, "%02x", Integer.valueOf(value & 0xff))))
    builder.toString()
  }
}
