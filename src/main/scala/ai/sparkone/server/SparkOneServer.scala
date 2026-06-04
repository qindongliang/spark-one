package ai.sparkone.server

import ai.sparkone.runtime.SparkOneRuntime
import ai.sparkone.sql.{SparkOneCompiler, SparkSqlValidator}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.javalin.Javalin
import io.javalin.core.JavalinConfig
import io.javalin.http.Context
import io.javalin.http.RequestLogger
import io.javalin.http.staticfiles.Location
import org.slf4j.LoggerFactory
import toml.derivation.auto._

import java.io.FileInputStream
import java.nio.file.Files
import java.nio.file.Paths
import java.util.Properties
import java.util.function.Consumer
import scala.collection.JavaConverters._

object SparkOneServer {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val compiler = new SparkOneCompiler(new SparkSqlValidator)
  @volatile private var runtimeRef: SparkOneRuntime = _

  def main(args: Array[String]): Unit = {
    val options = ServerOptions.parse(args)
    options.properties.foreach { case (key, value) => sys.props.put(key, value) }

    putDefaultProperty("log4j2.configurationFile", "classpath:log4j2.xml")
    putDefaultProperty("sparkone.logLevel", "info")

    val host = sys.props.getOrElse("sparkone.host", "127.0.0.1")
    val port = sys.props.get("sparkone.port")
      .orElse(sys.env.get("SPARKONE_PORT"))
      .orElse(options.port.map(_.toString))
      .map(_.toInt)
      .getOrElse(7070)

    val app = Javalin.create(new Consumer[JavalinConfig] {
      override def accept(config: JavalinConfig): Unit = {
        config.enableWebjars()
        config.addStaticFiles("/public", Location.CLASSPATH)
        config.addSinglePageRoot("/", "/public/index.html", Location.CLASSPATH)
        config.requestLogger(new RequestLogger {
          override def handle(ctx: Context, timeMs: java.lang.Float): Unit = {
            val elapsedMs = Option(timeMs).map(value => f"${value.toDouble}%.1f").getOrElse("0.0")
            logger.info(s"${ctx.method()} ${ctx.path()} -> ${ctx.status()} ($elapsedMs ms)")
          }
        })
      }
    }).start(host, port)
    app.post("/api/compile", (ctx: Context) => handleCompile(ctx))
    app.post("/api/run", (ctx: Context) => handleRun(ctx))

    sys.addShutdownHook {
      Option(runtimeRef).foreach(_.close())
    }

    logger.info(s"SparkOne SQL is listening on http://$host:$port")
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
        logger.warn("Failed to compile SQL request", e)
        json(ctx.status(400), Map("success" -> false, "error" -> errorMessage(e)))
    }
  }

  private def handleRun(ctx: Context): Unit = {
    val request = readSqlRequest(ctx)
    try {
      json(ctx, runtime.run(request.script, request.limit))
    } catch {
      case e: Throwable =>
        logger.warn("Failed to run SQL request", e)
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

  private def putDefaultProperty(key: String, value: String): Unit = {
    if (!sys.props.contains(key)) {
      sys.props.put(key, value)
    }
  }
}

final case class SqlRequest(script: String, limit: Int)

private final case class ServerOptions(port: Option[Int], properties: Map[String, String])

private object ServerOptions {
  def parse(args: Array[String]): ServerOptions = {
    val arguments = args.toSeq
    val configProperties = configFiles(arguments).flatMap(ServerConfigFile.load).toMap
    val parser = new Parser(withoutConfigArgs(arguments), configProperties)
    parser.parse()
  }

  private def configFiles(args: Seq[String]): Seq[String] = {
    val files = scala.collection.mutable.ArrayBuffer.empty[String]
    var remaining = args
    while (remaining.nonEmpty) {
      val arg = remaining.head
      remaining = remaining.tail
      if (arg == "--conf") {
        if (remaining.isEmpty || remaining.head.startsWith("--")) {
          throw new IllegalArgumentException("Missing value for server argument: --conf")
        }
        files += remaining.head
        remaining = remaining.tail
      } else if (arg.startsWith("--conf=")) {
        files += arg.stripPrefix("--conf=")
      }
    }
    files.toSeq
  }

  private def withoutConfigArgs(args: Seq[String]): Seq[String] = {
    val kept = scala.collection.mutable.ArrayBuffer.empty[String]
    var remaining = args
    while (remaining.nonEmpty) {
      val arg = remaining.head
      remaining = remaining.tail
      if (arg == "--conf") {
        if (remaining.isEmpty || remaining.head.startsWith("--")) {
          throw new IllegalArgumentException("Missing value for server argument: --conf")
        }
        remaining = remaining.tail
      } else if (!arg.startsWith("--conf=")) {
        kept += arg
      }
    }
    kept.toSeq
  }

  private final class Parser(args: Seq[String], defaults: Map[String, String]) {
    private val properties = scala.collection.mutable.LinkedHashMap.empty[String, String]
    properties ++= defaults
    private var port: Option[Int] = None
    private var remaining = args

    def parse(): ServerOptions = {
      while (remaining.nonEmpty) {
        val arg = next()
        arg match {
          case "--hive" | "--hive-enabled" =>
            properties += "sparkone.hive.enabled" -> "true"
          case value if value.startsWith("--") && value.contains("=") =>
            val Array(name, rawValue) = value.split("=", 2)
            setOption(name, rawValue)
          case "--host" =>
            properties += "sparkone.host" -> requireValue(arg)
          case "--port" =>
            properties += "sparkone.port" -> requireValue(arg)
          case "--log-level" =>
            properties += "sparkone.logLevel" -> requireValue(arg)
          case "--hadoop-conf-dir" =>
            properties += "sparkone.hadoop.conf.dir" -> requireValue(arg)
          case "--hadoop-conf-files" =>
            properties += "sparkone.hadoop.conf.files" -> requireValue(arg)
          case "--hive-conf" =>
            properties += "sparkone.hive.conf.file" -> requireValue(arg)
          case "--hive-conf-dir" =>
            properties += "sparkone.hive.conf.dir" -> requireValue(arg)
          case "--principal" =>
            properties += "sparkone.kerberos.principal" -> requireValue(arg)
          case "--keytab" =>
            properties += "sparkone.kerberos.keytab" -> requireValue(arg)
          case value if value.forall(_.isDigit) =>
            port = Some(value.toInt)
          case value =>
            throw new IllegalArgumentException(s"Unknown server argument: $value")
        }
      }

      ServerOptions(port, properties.toMap)
    }

    private def setOption(name: String, value: String): Unit = {
      name match {
        case "--host" => properties += "sparkone.host" -> value
        case "--port" => properties += "sparkone.port" -> value
        case "--log-level" => properties += "sparkone.logLevel" -> value
        case "--hadoop-conf-dir" => properties += "sparkone.hadoop.conf.dir" -> value
        case "--hadoop-conf-files" => properties += "sparkone.hadoop.conf.files" -> value
        case "--hive-conf" => properties += "sparkone.hive.conf.file" -> value
        case "--hive-conf-dir" => properties += "sparkone.hive.conf.dir" -> value
        case "--principal" => properties += "sparkone.kerberos.principal" -> value
        case "--keytab" => properties += "sparkone.kerberos.keytab" -> value
        case other => throw new IllegalArgumentException(s"Unknown server argument: $other")
      }
    }

    private def next(): String = {
      val value = remaining.head
      remaining = remaining.tail
      value
    }

    private def requireValue(option: String): String = {
      if (remaining.isEmpty || remaining.head.startsWith("--")) {
        throw new IllegalArgumentException(s"Missing value for server argument: $option")
      }
      next()
    }
  }
}

private object ServerConfigFile {
  def load(path: String): Map[String, String] = {
    if (path.endsWith(".toml")) {
      loadToml(path)
    } else {
      loadProperties(path)
    }
  }

  private def loadToml(path: String): Map[String, String] = {
    val content = new String(Files.readAllBytes(Paths.get(path)), "UTF-8")
    toml.Toml.parseAs[SparkOneTomlConfig](content) match {
      case Right(config) => config.toProperties
      case Left((path, message)) =>
        val location = if (path.isEmpty) "<root>" else path.mkString(".")
        throw new IllegalArgumentException(s"Invalid TOML config file $path: $location $message")
    }
  }

  private def loadProperties(path: String): Map[String, String] = {
    val properties = new Properties()
    val input = new FileInputStream(path)
    try {
      properties.load(input)
    } finally {
      input.close()
    }

    properties.stringPropertyNames().asScala
      .filter(_.startsWith("sparkone."))
      .map(name => name -> properties.getProperty(name).trim)
      .filter(_._2.nonEmpty)
      .toMap
  }
}

private final case class SparkOneTomlConfig(
    server: Option[ServerTomlSection] = None,
    spark: Option[SparkTomlSection] = None,
    hadoop: Option[HadoopTomlSection] = None,
    hive: Option[HiveTomlSection] = None,
    kerberos: Option[KerberosTomlSection] = None,
    jars: Option[JarsTomlSection] = None) {

  def toProperties: Map[String, String] = {
    Seq(
      server.toSeq.flatMap(_.toProperties),
      spark.toSeq.flatMap(_.toProperties),
      hadoop.toSeq.flatMap(_.toProperties),
      hive.toSeq.flatMap(_.toProperties),
      kerberos.toSeq.flatMap(_.toProperties),
      jars.toSeq.flatMap(_.toProperties)).flatten.toMap
  }
}

private final case class ServerTomlSection(
    host: Option[String] = None,
    port: Option[Int] = None,
    logLevel: Option[String] = None) {
  def toProperties: Map[String, String] = {
    Seq(
      host.map("sparkone.host" -> _),
      port.map(value => "sparkone.port" -> value.toString),
      logLevel.map("sparkone.logLevel" -> _)).flatten.toMap
  }
}

private final case class SparkTomlSection(master: Option[String] = None) {
  def toProperties: Map[String, String] = {
    Seq(master.map("sparkone.master" -> _)).flatten.toMap
  }
}

private final case class HadoopTomlSection(
    confDir: Option[String] = None,
    confFiles: Option[String] = None) {
  def toProperties: Map[String, String] = {
    Seq(
      confDir.map("sparkone.hadoop.conf.dir" -> _),
      confFiles.map("sparkone.hadoop.conf.files" -> _)).flatten.toMap
  }
}

private final case class HiveTomlSection(
    enabled: Option[Boolean] = None,
    confFile: Option[String] = None,
    confDir: Option[String] = None) {
  def toProperties: Map[String, String] = {
    Seq(
      enabled.map(value => "sparkone.hive.enabled" -> value.toString),
      confFile.map("sparkone.hive.conf.file" -> _),
      confDir.map("sparkone.hive.conf.dir" -> _)).flatten.toMap
  }
}

private final case class KerberosTomlSection(
    principal: Option[String] = None,
    keytab: Option[String] = None) {
  def toProperties: Map[String, String] = {
    Seq(
      principal.map("sparkone.kerberos.principal" -> _),
      keytab.map("sparkone.kerberos.keytab" -> _)).flatten.toMap
  }
}

private final case class JarsTomlSection(
    packages: Option[String] = None,
    files: Option[String] = None,
    repositories: Option[String] = None) {
  def toProperties: Map[String, String] = {
    Seq(
      packages.map("sparkone.jars.packages" -> _),
      files.map("sparkone.jars" -> _),
      repositories.map("sparkone.jars.repositories" -> _)).flatten.toMap
  }
}
