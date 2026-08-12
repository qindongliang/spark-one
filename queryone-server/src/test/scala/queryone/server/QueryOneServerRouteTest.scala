package queryone.server

import io.javalin.Javalin
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import java.net.{HttpURLConnection, URL}
import scala.io.Source

final class QueryOneServerRouteTest {

  @Test
  def keepsOnlyHealthAndInternalRoutesWhenDevelopmentAccessIsDisabled(): Unit = {
    withServer(developmentAccess = false) { port =>
      val health = request(port, "GET", "/healthz")
      assertEquals(200, health.status)
      assertEquals("OK", health.body)

      assertEquals(404, request(port, "GET", "/").status)
      assertEquals(404, request(port, "GET", "/app.js").status)
      assertEquals(404, request(port, "GET", "/webjars/codemirror/5.65.19/lib/codemirror.js").status)
      assertEquals(404, request(port, "GET", "/api/session").status)
      assertEquals(404, request(port, "POST", "/api/login", "{}").status)

      val internal = request(port, "POST", "/internal/v1/engines", "{}")
      assertEquals(404, internal.status)
      assertTrue(internal.body.contains("Internal API is not configured"))
    }
  }

  @Test
  def exposesDevelopmentPageAndApisWhenDevelopmentAccessIsEnabled(): Unit = {
    withServer(developmentAccess = true) { port =>
      assertEquals(200, request(port, "GET", "/").status)
      assertEquals(200, request(port, "GET", "/api/session").status)
      assertEquals(400, request(port, "POST", "/api/login", "{}").status)
      assertEquals(200, request(port, "GET", "/healthz").status)
    }
  }

  private def withServer(developmentAccess: Boolean)(test: Int => Unit): Unit = {
    val app = QueryOneServer.createApp(developmentAccess).start("127.0.0.1", 0)
    try test(app.port()) finally app.stop()
  }

  private def request(port: Int, method: String, path: String, body: String = ""): HttpResponse = {
    val connection = new URL(s"http://127.0.0.1:$port$path").openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod(method)
    connection.setConnectTimeout(5000)
    connection.setReadTimeout(5000)
    if (body.nonEmpty) {
      connection.setDoOutput(true)
      connection.setRequestProperty("Content-Type", "application/json")
      val bytes = body.getBytes("UTF-8")
      connection.setFixedLengthStreamingMode(bytes.length)
      val output = connection.getOutputStream
      try output.write(bytes) finally output.close()
    }

    val status = connection.getResponseCode
    val stream = Option(if (status >= 400) connection.getErrorStream else connection.getInputStream)
    val responseBody = stream.map { input =>
      val source = Source.fromInputStream(input, "UTF-8")
      try source.mkString finally source.close()
    }.getOrElse("")
    connection.disconnect()
    HttpResponse(status, responseBody)
  }
}

private final case class HttpResponse(status: Int, body: String)
