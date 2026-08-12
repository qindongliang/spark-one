package queryone.server

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.Test

final class InternalApiAuthTest {
  private val mapper = new ObjectMapper()
  private val auth = new InternalApiAuth(
    "odep-system",
    "test-sign-key",
    clockSkewSeconds = 60,
    nonceTtlSeconds = 600,
    nowSeconds = () => 1700000000L)

  @Test
  def acceptsSignedRequestAndRejectsReplay(): Unit = {
    val body = """{"requestId":"req-1","subject":"alice","script":"select 1","engine":"kyuubi","limit":10,"sessionMode":"tenant_shared"}"""
    val timestamp = "1700000000"
    val nonce = "nonce-1"
    val signed = Map(
      "appId" -> "odep-system",
      "bodySha256" -> InternalApiAuth.sha256(body),
      "nonce" -> nonce,
      "path" -> "/internal/v1/run",
      "requestId" -> "req-1",
      "subject" -> "alice",
      "timestamp" -> timestamp)
    val headers = Map(
      InternalApiAuth.AppIdHeader -> "odep-system",
      InternalApiAuth.TimestampHeader -> timestamp,
      InternalApiAuth.NonceHeader -> nonce,
      InternalApiAuth.BodyHashHeader -> signed("bodySha256"),
      InternalApiAuth.SignatureHeader -> InternalApiAuth.sign(signed, "test-sign-key"))

    val principal = auth.verify("/internal/v1/run", body, headers, mapper)
    assertEquals("alice", principal.subject)
    assertEquals("req-1", principal.requestId)

    try {
      auth.verify("/internal/v1/run", body, headers, mapper)
      fail("replayed nonce should be rejected")
    } catch {
      case _: InternalApiAuthException => assertTrue(true)
    }
  }

  @Test
  def rejectsChangedBody(): Unit = {
    val body = """{"requestId":"req-2","subject":"alice","script":"select 1"}"""
    val signed = Map(
      "appId" -> "odep-system",
      "bodySha256" -> InternalApiAuth.sha256(body),
      "nonce" -> "nonce-2",
      "path" -> "/internal/v1/run",
      "requestId" -> "req-2",
      "subject" -> "alice",
      "timestamp" -> "1700000000")
    val headers = Map(
      InternalApiAuth.AppIdHeader -> "odep-system",
      InternalApiAuth.TimestampHeader -> "1700000000",
      InternalApiAuth.NonceHeader -> "nonce-2",
      InternalApiAuth.BodyHashHeader -> signed("bodySha256"),
      InternalApiAuth.SignatureHeader -> InternalApiAuth.sign(signed, "test-sign-key"))

    try {
      auth.verify("/internal/v1/run", body + " ", headers, mapper)
      fail("changed body should be rejected")
    } catch {
      case _: InternalApiAuthException => assertTrue(true)
    }
  }

  @Test
  def matchesOdepOpenApiSignatureVector(): Unit = {
    val parameters = Map(
      "appId" -> "odep-system",
      "bodySha256" -> "5d41402abc4b2a76b9719d911017c592",
      "nonce" -> "nonce-3",
      "path" -> "/internal/v1/run",
      "requestId" -> "req-3",
      "subject" -> "alice",
      "timestamp" -> "1700000000")

    assertEquals(
      "73a85965d06ede6e494c704fedb32bddd2080dc2",
      InternalApiAuth.sign(parameters, "test-sign-key"))
  }

  @Test
  def keepsNonceUntilSignedTimestampExpires(): Unit = {
    var now = 1700000000L
    val shortNonceAuth = new InternalApiAuth(
      "odep-system",
      "test-sign-key",
      clockSkewSeconds = 300,
      nonceTtlSeconds = 10,
      nowSeconds = () => now)
    val body = """{"requestId":"req-4","subject":"alice","script":"select 1"}"""
    val timestamp = now.toString
    val signed = Map(
      "appId" -> "odep-system",
      "bodySha256" -> InternalApiAuth.sha256(body),
      "nonce" -> "nonce-4",
      "path" -> "/internal/v1/run",
      "requestId" -> "req-4",
      "subject" -> "alice",
      "timestamp" -> timestamp)
    val headers = Map(
      InternalApiAuth.AppIdHeader -> "odep-system",
      InternalApiAuth.TimestampHeader -> timestamp,
      InternalApiAuth.NonceHeader -> "nonce-4",
      InternalApiAuth.BodyHashHeader -> signed("bodySha256"),
      InternalApiAuth.SignatureHeader -> InternalApiAuth.sign(signed, "test-sign-key"))

    shortNonceAuth.verify("/internal/v1/run", body, headers, mapper)
    now += 20
    try {
      shortNonceAuth.verify("/internal/v1/run", body, headers, mapper)
      fail("nonce should remain blocked while its timestamp is valid")
    } catch {
      case _: InternalApiAuthException => assertTrue(true)
    }
  }
}
