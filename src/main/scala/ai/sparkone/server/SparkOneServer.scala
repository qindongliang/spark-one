package ai.sparkone.server

import ai.sparkone.runtime.SparkOneRuntime
import ai.sparkone.sql.{SparkOneCompiler, SparkSqlValidator}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.javalin.Javalin
import io.javalin.core.JavalinConfig
import io.javalin.http.Context
import io.javalin.http.staticfiles.Location

import java.util.function.Consumer

object SparkOneServer {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val compiler = new SparkOneCompiler(new SparkSqlValidator)
  @volatile private var runtimeRef: SparkOneRuntime = _

  def main(args: Array[String]): Unit = {
    sys.props.put("org.slf4j.simpleLogger.defaultLogLevel",
      sys.props.getOrElse("sparkone.logLevel", "info"))

    val host = sys.props.getOrElse("sparkone.host", "127.0.0.1")
    val port = sys.props.get("sparkone.port")
      .orElse(sys.env.get("SPARKONE_PORT"))
      .orElse(args.headOption)
      .map(_.toInt)
      .getOrElse(7070)

    val app = Javalin.create(new Consumer[JavalinConfig] {
      override def accept(config: JavalinConfig): Unit = {
        config.enableWebjars()
        config.addStaticFiles("/public", Location.CLASSPATH)
        config.addSinglePageRoot("/", "/public/index.html", Location.CLASSPATH)
      }
    }).start(host, port)
    app.post("/api/compile", (ctx: Context) => handleCompile(ctx))
    app.post("/api/run", (ctx: Context) => handleRun(ctx))

    sys.addShutdownHook {
      Option(runtimeRef).foreach(_.close())
    }

    println(s"SparkOne SQL is listening on http://$host:$port")
  }

  private def handleCompile(ctx: Context): Unit = {
    val request = readSqlRequest(ctx)
    try {
      val statements = compiler.compile(request.script).zipWithIndex.map { case (statement, index) =>
        Map(
          "index" -> (index + 1),
          "source" -> statement.source,
          "sql" -> statement.sql)
      }
      json(ctx, Map("success" -> true, "statements" -> statements))
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        json(ctx.status(400), Map("success" -> false, "error" -> errorMessage(e)))
    }
  }

  private def handleRun(ctx: Context): Unit = {
    val request = readSqlRequest(ctx)
    try {
      json(ctx, runtime.run(request.script, request.limit))
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        json(ctx.status(400), Map("success" -> false, "error" -> errorMessage(e)))
    }
  }

  private def runtime: SparkOneRuntime = {
    if (runtimeRef == null) {
      this.synchronized {
        if (runtimeRef == null) {
          runtimeRef = SparkOneRuntime.local()
        }
      }
    }
    runtimeRef
  }

  private def readSqlRequest(ctx: Context): SqlRequest = {
    val node = mapper.readTree(ctx.body())
    val script = Option(node.get("script")).map(_.asText()).getOrElse("")
    val limit = Option(node.get("limit")).map(_.asInt(200)).getOrElse(200).max(1).min(1000)
    SqlRequest(script, limit)
  }

  private def json(ctx: Context, value: Any): Unit = {
    ctx.contentType("application/json; charset=utf-8")
    ctx.result(mapper.writeValueAsString(value))
  }

  private def errorMessage(error: Throwable): String = {
    val root = Iterator.iterate(error)(_.getCause).takeWhile(_ != null).toSeq.lastOption.getOrElse(error)
    Option(root.getMessage).filter(_.nonEmpty).getOrElse(root.getClass.getName)
  }
}

final case class SqlRequest(script: String, limit: Int)
