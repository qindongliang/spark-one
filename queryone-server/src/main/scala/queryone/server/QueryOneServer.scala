package queryone.server

import queryone.extension.hdfs.{ManagedHdfsLoadProtocol, ManagedHdfsOverwriteProtocol}
import queryone.identity.{DevelopmentSessionStore, TenantContext}
import queryone.runtime.{EngineInfo, PreviewConfig, QueryOneEngine, SessionMode, QueryOneEngineRegistry}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.javalin.Javalin
import io.javalin.core.JavalinConfig
import io.javalin.http.{Context, Cookie, RequestLogger, SameSite}
import io.javalin.http.staticfiles.Location
import org.slf4j.LoggerFactory
import com.typesafe.config.{Config, ConfigException, ConfigFactory}

import java.nio.file.Files
import java.nio.file.Paths
import java.util.function.Consumer
import scala.collection.JavaConverters._

object QueryOneServer {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val SimpleIdentifier = "^[A-Za-z_][A-Za-z0-9_]*$".r
  private val SessionCookieName = "queryone_session"
  private val sessions = new DevelopmentSessionStore
  @volatile private var enginesRef: QueryOneEngineRegistry = _
  @volatile private var internalApiAuthRef: Option[InternalApiAuth] = None

  def main(args: Array[String]): Unit = {
    val options = ServerOptions.parse(args)
    options.properties.foreach { case (key, value) => sys.props.put(key, value) }
    internalApiAuthRef = InternalApiAuth.fromSystemProperties()

    putDefaultProperty("log4j2.configurationFile", "classpath:log4j2.xml")
    putDefaultProperty("queryone.logLevel", "info")

    val host = sys.props.getOrElse("queryone.host", "127.0.0.1")
    val port = sys.props.get("queryone.port")
      .orElse(sys.env.get("QUERYONE_PORT"))
      .orElse(options.port.map(_.toString))
      .map(_.toInt)
      .getOrElse(7070)
    val developmentAccess = developmentAccessEnabled

    val app = createApp(developmentAccess).start(host, port)

    sys.addShutdownHook {
      Option(enginesRef).foreach(_.close())
    }

    logger.info(s"QueryOne SQL is listening on http://$host:$port")
    logger.info(s"QueryOne development access is ${if (developmentAccess) "enabled" else "disabled"}")
  }

  private[server] def createApp(developmentAccess: Boolean): Javalin = {
    val app = Javalin.create(new Consumer[JavalinConfig] {
      override def accept(config: JavalinConfig): Unit = {
        if (developmentAccess) {
          config.enableWebjars()
          config.addStaticFiles("/public", Location.CLASSPATH)
          config.addSinglePageRoot("/", "/public/index.html", Location.CLASSPATH)
        }
        config.requestLogger(new RequestLogger {
          override def handle(ctx: Context, timeMs: java.lang.Float): Unit = {
            val elapsedMs = Option(timeMs).map(value => f"${value.toDouble}%.1f").getOrElse("0.0")
            logger.info(s"${ctx.method()} ${ctx.path()} -> ${ctx.status()} ($elapsedMs ms)")
          }
        })
      }
    })

    app.get("/healthz", (ctx: Context) => {
      ctx.contentType("text/plain; charset=utf-8")
      ctx.result("OK")
      ()
    })
    if (developmentAccess) {
      app.get("/api/config", (ctx: Context) => handleConfig(ctx))
      app.get("/api/session", (ctx: Context) => handleSession(ctx))
      app.post("/api/login", (ctx: Context) => handleLogin(ctx))
      app.post("/api/logout", (ctx: Context) => handleLogout(ctx))
      app.post("/api/compile", (ctx: Context) => handleCompile(ctx))
      app.post("/api/run", (ctx: Context) => handleRun(ctx))
      app.post("/api/preview", (ctx: Context) => handlePreview(ctx))
    }
    app.post("/internal/v1/engines", (ctx: Context) => handleInternalEngines(ctx))
    app.post("/internal/v1/compile", (ctx: Context) => handleInternalCompile(ctx))
    app.post("/internal/v1/run", (ctx: Context) => handleInternalRun(ctx))
    app
  }

  private def handleConfig(ctx: Context): Unit = {
    json(ctx, uiConfig)
  }

  private def handleSession(ctx: Context): Unit = {
    sessions.resolve(sessionToken(ctx)) match {
      case Some(tenant) =>
        json(ctx, Map(
          "authenticated" -> true,
          "username" -> tenant.username,
          "identitySource" -> tenant.identitySource))
      case None =>
        json(ctx, Map("authenticated" -> false))
    }
  }

  private def handleLogin(ctx: Context): Unit = {
    try {
      val node = mapper.readTree(ctx.body())
      val username = Option(node.get("username")).filterNot(_.isNull).map(_.asText()).getOrElse("")
      val previousToken = sessionToken(ctx)
      val session = sessions.create(username)
      sessions.remove(previousToken)
      val cookie = new Cookie(SessionCookieName, session.token)
      cookie.setPath("/")
      cookie.setHttpOnly(true)
      cookie.setSameSite(SameSite.LAX)
      ctx.cookie(cookie)
      logger.info(s"Development tenant login: username=${session.tenant.username}")
      json(ctx, Map(
        "success" -> true,
        "authenticated" -> true,
        "username" -> session.tenant.username,
        "identitySource" -> session.tenant.identitySource))
    } catch {
      case e: IllegalArgumentException =>
        logger.warn(s"Development tenant login rejected: ${errorMessage(e)}")
        json(ctx.status(400), Map("success" -> false, "error" -> errorMessage(e)))
      case e: Throwable =>
        logger.error("Failed to process development tenant login", e)
        json(ctx.status(400), Map("success" -> false, "error" -> "Invalid login request"))
    }
  }

  private def handleLogout(ctx: Context): Unit = {
    sessions.remove(sessionToken(ctx))
    ctx.removeCookie(SessionCookieName, "/")
    json(ctx, Map("success" -> true, "authenticated" -> false))
  }

  private def handleCompile(ctx: Context): Unit = {
    withTenant(ctx) { tenant =>
      try {
        val request = readSqlRequest(ctx)
        val engine = engines.get(request.engine)
        val statements = engine.compile(tenant, request.script).zipWithIndex.map { case (statement, index) =>
          Map(
            "index" -> (index + 1),
            "source" -> statement.source,
            "sql" -> displaySql(statement.sql))
        }
        json(ctx, Map(
          "success" -> true,
          "engine" -> engine.id,
          "diagnostics" -> engine.capabilities.compileDiagnostics,
          "statements" -> statements))
      } catch {
        case e: Throwable =>
          logger.warn(s"Failed to compile SQL request for tenant ${tenant.username}", e)
          json(ctx.status(400), Map("success" -> false, "error" -> errorMessage(e)))
      }
    }
  }

  private def handleRun(ctx: Context): Unit = {
    withTenant(ctx) { tenant =>
      try {
        val request = readSqlRequest(ctx)
        val engine = engines.get(request.engine)
        val result = engine.run(tenant, request.script, request.limit, request.sessionMode)
        json(ctx, Map(
          "success" -> result.success,
          "outcome" -> result.outcome,
          "stoppedEarly" -> result.stoppedEarly,
          "engine" -> engine.id,
          "showCompiledSql" -> showCompiledSql,
          "statements" -> result.statements.map(statement =>
            statement.copy(sql = displaySql(statement.sql)))))
      } catch {
        case e: Throwable =>
          logger.warn(s"Failed to run SQL request for tenant ${tenant.username}", e)
          json(ctx.status(400), Map("success" -> false, "error" -> errorMessage(e)))
      }
    }
  }

  private def handlePreview(ctx: Context): Unit = {
    withTenant(ctx) { tenant =>
      try {
        val request = readPreviewRequest(ctx)
        val engine = engines.get(request.engine)
        val result = engine.previewTable(tenant, request.table, request.limit)
        json(ctx, Map(
          "success" -> true,
          "engine" -> engine.id,
          "statement" -> result))
      } catch {
        case e: Throwable =>
          logger.warn(s"Failed to preview table for tenant ${tenant.username}", e)
          json(ctx.status(400), Map("success" -> false, "error" -> errorMessage(e)))
      }
    }
  }

  private def handleInternalCompile(ctx: Context): Unit = {
    handleInternalRequest(ctx, "/internal/v1/compile") { case (principal, node) =>
      val request = readInternalSqlRequest(node)
      val tenant = TenantContext(principal.subject, "odep_signed")
      val engine = internalEngine(request.engine)
      val statements = engine.compile(tenant, request.script).zipWithIndex.map { case (statement, index) =>
        Map(
          "index" -> (index + 1),
          "source" -> statement.source,
          "sql" -> displaySql(statement.sql))
      }
      json(ctx, Map(
        "success" -> true,
        "requestId" -> principal.requestId,
        "engine" -> engine.id,
        "diagnostics" -> engine.capabilities.compileDiagnostics,
        "statements" -> statements))
    }
  }

  private def handleInternalEngines(ctx: Context): Unit = {
    handleInternalRequest(ctx, "/internal/v1/engines") { case (principal, _) =>
      val catalog = internalEngineCatalog(engines.defaultId, engines.infos)
      json(ctx, Map(
        "success" -> true,
        "requestId" -> principal.requestId,
        "defaultEngine" -> catalog.defaultEngine,
        "engines" -> catalog.engines))
    }
  }

  private def handleInternalRun(ctx: Context): Unit = {
    handleInternalRequest(ctx, "/internal/v1/run") { case (principal, node) =>
      val request = readInternalSqlRequest(node)
      val tenant = TenantContext(principal.subject, "odep_signed")
      val engine = internalEngine(request.engine)
      val result = engine.run(tenant, request.script, request.limit, request.sessionMode)
      json(ctx, Map(
        "success" -> result.success,
        "requestId" -> principal.requestId,
        "outcome" -> result.outcome,
        "stoppedEarly" -> result.stoppedEarly,
        "engine" -> engine.id,
        "showCompiledSql" -> showCompiledSql,
        "statements" -> result.statements.map(statement =>
          statement.copy(sql = displaySql(statement.sql)))))
    }
  }

  private def handleInternalRequest(
      ctx: Context,
      path: String)(body: ((InternalApiPrincipal, com.fasterxml.jackson.databind.JsonNode)) => Unit): Unit = {
    internalApiAuthRef match {
      case None =>
        json(ctx.status(404), Map("success" -> false, "error" -> "Internal API is not configured"))
      case Some(auth) =>
        val rawBody = ctx.body()
        try {
          val node = mapper.readTree(rawBody)
          val headers = Seq(
            InternalApiAuth.AppIdHeader,
            InternalApiAuth.TimestampHeader,
            InternalApiAuth.NonceHeader,
            InternalApiAuth.SignatureHeader,
            InternalApiAuth.BodyHashHeader).flatMap(name => Option(ctx.header(name)).map(name -> _)).toMap
          val principal = auth.verify(path, rawBody, headers, mapper)
          body(principal -> node)
        } catch {
          case error: InternalApiAuthException =>
            logger.warn(s"Rejected QueryOne internal API request: ${error.getMessage}")
            json(ctx.status(401), Map("success" -> false, "error" -> error.getMessage))
          case error: Throwable =>
            logger.error("Failed to process QueryOne internal API request", error)
            json(ctx.status(400), Map("success" -> false, "error" -> errorMessage(error)))
        }
    }
  }

  private def withTenant(ctx: Context)(body: TenantContext => Unit): Unit = {
    sessions.resolve(sessionToken(ctx)) match {
      case Some(tenant) => body(tenant)
      case None =>
        json(ctx.status(401), Map(
          "success" -> false,
          "authenticated" -> false,
          "error" -> "Login required"))
    }
  }

  private def sessionToken(ctx: Context): Option[String] = {
    Option(ctx.cookie(SessionCookieName)).map(_.trim).filter(_.nonEmpty)
  }

  private def uiConfig: Map[String, Any] = {
    val preview = PreviewConfig.current
    Map(
      "showCompiledSql" -> showCompiledSql,
      "previewMaxRows" -> preview.maxRows,
      "defaultResultTab" -> preview.defaultTab,
      "defaultEngine" -> engines.defaultId,
      "engines" -> engines.infos)
  }

  private[server] def internalEngineCatalog(
      defaultId: String,
      infos: Seq[EngineInfo]): InternalEngineCatalog = {
    val available = infos.filter(_.engineType == "kyuubi").sortBy(_.id)
    val defaultEngine = available.find(_.id == defaultId).orElse(available.headOption).map(_.id)
    InternalEngineCatalog(defaultEngine, available)
  }

  private def internalEngine(requestedId: Option[String]): QueryOneEngine = {
    val engine = engines.get(requestedId)
    if (engine.engineType != "kyuubi") {
      throw new IllegalArgumentException(s"ODEP internal API does not expose engine: ${engine.id}")
    }
    engine
  }

  private def showCompiledSql: Boolean = {
    enabled("queryone.ui.showCompiledSql", defaultValue = false)
  }

  private[server] def developmentAccessEnabled: Boolean = {
    enabled("queryone.development.access.enabled", defaultValue = false)
  }

  private def engines: QueryOneEngineRegistry = {
    if (enginesRef == null) {
      this.synchronized {
        if (enginesRef == null) {
          enginesRef = QueryOneEngineRegistry.fromSystemProperties()
        }
      }
    }
    enginesRef
  }

  private def readSqlRequest(ctx: Context): SqlRequest = {
    val node = mapper.readTree(ctx.body())
    val script = Option(node.get("script")).map(_.asText()).getOrElse("")
    val preview = PreviewConfig.current
    val requestedLimit = Option(node.get("limit"))
      .filterNot(_.isNull)
      .map(_.asInt(preview.maxRows))
    val limit = preview.clampRows(requestedLimit)
    val engine = Option(node.get("engine")).filterNot(_.isNull).map(_.asText()).map(_.trim).filter(_.nonEmpty)
    val sessionMode = SessionMode.parse(
      Option(node.get("sessionMode")).filterNot(_.isNull).map(_.asText()))
    SqlRequest(script, limit, engine, sessionMode)
  }

  private def readPreviewRequest(ctx: Context): PreviewRequest = {
    val node = mapper.readTree(ctx.body())
    val table = Option(node.get("table")).map(_.asText()).getOrElse("").trim
    if (SimpleIdentifier.findFirstIn(table).isEmpty) {
      throw new IllegalArgumentException(s"Preview table must be a simple identifier: $table")
    }
    val preview = PreviewConfig.current
    val requestedLimit = Option(node.get("limit"))
      .filterNot(_.isNull)
      .map(_.asInt(preview.maxRows))
    val engine = Option(node.get("engine")).filterNot(_.isNull).map(_.asText()).map(_.trim).filter(_.nonEmpty)
    PreviewRequest(table, preview.clampRows(requestedLimit), engine)
  }

  private def readInternalSqlRequest(node: com.fasterxml.jackson.databind.JsonNode): SqlRequest = {
    val script = Option(node.get("script")).map(_.asText()).getOrElse("")
    if (script.trim.isEmpty) throw new IllegalArgumentException("SQL script must not be empty")
    val preview = PreviewConfig.current
    val requestedLimit = Option(node.get("limit")).filterNot(_.isNull).map(_.asInt(preview.maxRows))
    val limit = preview.clampRows(requestedLimit)
    val engine = Option(node.get("engine")).filterNot(_.isNull).map(_.asText()).map(_.trim).filter(_.nonEmpty)
    val sessionMode = SessionMode.parse(
      Option(node.get("sessionMode")).filterNot(_.isNull).map(_.asText()))
    SqlRequest(script, limit, engine, sessionMode)
  }

  private def json(ctx: Context, value: Any): Unit = {
    ctx.contentType("application/json; charset=utf-8")
    ctx.result(mapper.writeValueAsString(value))
  }

  private[server] def displaySql(sql: String): String = {
    ManagedHdfsLoadProtocol.parse(sql).map { request =>
      Seq(
        "MANAGED HDFS LOAD",
        s"  workspace owner: ${request.workspaceOwner}",
        s"  view: ${request.targetTable}",
        s"  format: ${request.format}",
        s"  source: ${request.relativePath}",
        s"  options: ${displayOptions(request.options)}").mkString("\n")
    }.orElse(ManagedHdfsOverwriteProtocol.parse(sql).map { request =>
      Seq(
        "MANAGED HDFS OVERWRITE",
        s"  tenant: ${request.tenant}",
        s"  source: ${request.sourceTable}",
        s"  format: ${request.format}",
        s"  target: ${request.relativePath}",
        s"  options: ${displayOptions(request.options)}").mkString("\n")
    }).getOrElse(sql)
  }

  private def displayOptions(options: Map[String, String]): String = {
    options.toSeq.sortBy(_._1).map { case (key, value) =>
      s"$key=${displayLiteral(value)}"
    }.mkString("{", ", ", "}")
  }

  private def displayLiteral(value: String): String = {
    val escaped = value.flatMap {
      case '\\' => "\\\\"
      case '\'' => "\\'"
      case '\r' => "\\r"
      case '\n' => "\\n"
      case '\t' => "\\t"
      case char => char.toString
    }
    s"'$escaped'"
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

  private def enabled(key: String, defaultValue: Boolean): Boolean = {
    sys.props.get(key)
      .map(value => Set("1", "true", "yes", "on").contains(value.trim.toLowerCase))
      .getOrElse(defaultValue)
  }
}

final case class SqlRequest(
    script: String,
    limit: Int,
    engine: Option[String],
    sessionMode: SessionMode)

final case class PreviewRequest(table: String, limit: Int, engine: Option[String])

private[server] final case class InternalEngineCatalog(
    defaultEngine: Option[String],
    engines: Seq[EngineInfo])

private final case class ServerOptions(port: Option[Int], properties: Map[String, String])

private object ServerOptions {
  private val DefaultConfigFile = "conf/queryone.conf"
  private val DefaultConfigFiles = Seq(DefaultConfigFile, s"../$DefaultConfigFile")

  def parse(args: Array[String]): ServerOptions = {
    val arguments = args.toSeq
    val configProperties = configFiles(arguments).flatMap(ServerConfigFile.load).toMap
    val parser = new Parser(withoutConfigArgs(arguments), configProperties)
    parser.parse()
  }

  private def configFiles(args: Seq[String]): Seq[String] = {
    val explicitFiles = scala.collection.mutable.ArrayBuffer.empty[String]
    var remaining = args
    while (remaining.nonEmpty) {
      val arg = remaining.head
      remaining = remaining.tail
      if (arg == "--conf") {
        if (remaining.isEmpty || remaining.head.startsWith("--")) {
          throw new IllegalArgumentException("Missing value for server argument: --conf")
        }
        explicitFiles += remaining.head
        remaining = remaining.tail
      } else if (arg.startsWith("--conf=")) {
        explicitFiles += arg.stripPrefix("--conf=")
      }
    }

    if (explicitFiles.nonEmpty) {
      explicitFiles.toSeq
    } else {
      DefaultConfigFiles.find(path => Files.isRegularFile(Paths.get(path))).toSeq
    }
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
            properties += localProperty("queryone.hive.enabled") -> "true"
          case value if value.startsWith("--") && value.contains("=") =>
            val Array(name, rawValue) = value.split("=", 2)
            setOption(name, rawValue)
          case "--host" =>
            properties += "queryone.host" -> requireValue(arg)
          case "--port" =>
            properties += "queryone.port" -> requireValue(arg)
          case "--log-level" =>
            properties += "queryone.logLevel" -> requireValue(arg)
          case "--hadoop-conf-dir" =>
            properties += localProperty("queryone.hadoop.conf.dir") -> requireValue(arg)
          case "--hadoop-conf-files" =>
            properties += localProperty("queryone.hadoop.conf.files") -> requireValue(arg)
          case "--hadoop-group-static-overrides" =>
            properties += localProperty("queryone.hadoop.group.static.mapping.overrides") -> requireValue(arg)
          case "--hive-conf" =>
            properties += localProperty("queryone.hive.conf.file") -> requireValue(arg)
          case "--hive-conf-dir" =>
            properties += localProperty("queryone.hive.conf.dir") -> requireValue(arg)
          case "--principal" =>
            properties += localProperty("spark.kerberos.principal") -> requireValue(arg)
          case "--keytab" =>
            properties += localProperty("spark.kerberos.keytab") -> requireValue(arg)
          case "--krb5-conf" =>
            properties += localProperty("java.security.krb5.conf") -> requireValue(arg)
          case value if value.forall(_.isDigit) =>
            properties += "queryone.port" -> value
            port = Some(value.toInt)
          case value =>
            throw new IllegalArgumentException(s"Unknown server argument: $value")
        }
      }

      ServerOptions(port, properties.toMap)
    }

    private def setOption(name: String, value: String): Unit = {
      name match {
        case "--host" => properties += "queryone.host" -> value
        case "--port" => properties += "queryone.port" -> value
        case "--log-level" => properties += "queryone.logLevel" -> value
        case "--hadoop-conf-dir" => properties += localProperty("queryone.hadoop.conf.dir") -> value
        case "--hadoop-conf-files" => properties += localProperty("queryone.hadoop.conf.files") -> value
        case "--hadoop-group-static-overrides" => properties += localProperty("queryone.hadoop.group.static.mapping.overrides") -> value
        case "--hive-conf" => properties += localProperty("queryone.hive.conf.file") -> value
        case "--hive-conf-dir" => properties += localProperty("queryone.hive.conf.dir") -> value
        case "--principal" => properties += localProperty("spark.kerberos.principal") -> value
        case "--keytab" => properties += localProperty("spark.kerberos.keytab") -> value
        case "--krb5-conf" => properties += localProperty("java.security.krb5.conf") -> value
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

    private def localProperty(name: String): String = {
      s"queryone.engine.local.local.property.$name"
    }
  }
}

private[server] object ServerConfigFile {
  def load(path: String): Map[String, String] = {
    loadHocon(path)
  }

  private def loadHocon(path: String): Map[String, String] = {
    try {
      QueryOneHoconConfig.toProperties(ConfigFactory.parseFile(Paths.get(path).toFile).resolve())
    } catch {
      case e: ConfigException =>
        throw new IllegalArgumentException(s"Invalid HOCON config file $path: ${e.getMessage}", e)
    }
  }
}

private[server] object QueryOneHoconConfig {
  def toProperties(config: Config): Map[String, String] = {
    Seq(
      serverProperties(config),
      internalApiProperties(config),
      previewProperties(config),
      engineProperties(config),
      passthroughProperties(config)).flatten.toMap
  }

  private def serverProperties(config: Config): Map[String, String] = {
    Seq(
      string(config, "server.host").map("queryone.host" -> _),
      int(config, "server.port").map(value => "queryone.port" -> value.toString),
      string(config, "server.logLevel").map("queryone.logLevel" -> _),
      boolean(config, "server.developmentAccessEnabled")
        .map(value => "queryone.development.access.enabled" -> value.toString),
      boolean(config, "server.showCompiledSql").map(value => "queryone.ui.showCompiledSql" -> value.toString)).flatten.toMap
  }

  private def previewProperties(config: Config): Map[String, String] = {
    Seq(
      int(config, "preview.maxRows").map(value => PreviewConfig.MaxRowsKey -> value.toString),
      string(config, "preview.defaultTab").map(PreviewConfig.DefaultTabKey -> _)).flatten.toMap
  }

  private def internalApiProperties(config: Config): Map[String, String] = {
    Seq(
      string(config, "internalApi.auth.appId").map("queryone.internal.auth.app.id" -> _),
      string(config, "internalApi.auth.signKey").map("queryone.internal.auth.sign.key" -> _),
      long(config, "internalApi.auth.clockSkewSeconds")
        .map(value => "queryone.internal.auth.clock.skew.seconds" -> value.toString),
      long(config, "internalApi.auth.nonceTtlSeconds")
        .map(value => "queryone.internal.auth.nonce.ttl.seconds" -> value.toString)).flatten.toMap
  }

  private def engineProperties(config: Config): Map[String, String] = {
    val defaultEngine = string(config, "engines.default").map("queryone.engine.default" -> _).toMap
    if (!config.hasPath("engines")) {
      defaultEngine
    } else {
      val engines = config.getConfig("engines").root().keySet().asScala
        .filter(_ != "default")
        .flatMap { id =>
          val engineConfig = config.getConfig(s"engines.$id")
          singleEngineProperties(id, engineConfig)
        }
        .toMap
      defaultEngine ++ engines
    }
  }

  private def singleEngineProperties(id: String, config: Config): Map[String, String] = {
    val prefix = s"queryone.engine.$id"
    val common = Seq(
      string(config, "type").map(s"$prefix.type" -> _),
      string(config, "label").map(s"$prefix.label" -> _),
      boolean(config, "enabled").map(value => s"$prefix.enabled" -> value.toString)).flatten

    val kyuubi =
      if (!string(config, "type").exists(_.equalsIgnoreCase("kyuubi"))) Seq.empty
      else Seq(
        string(config, "url").map(s"$prefix.kyuubi.url" -> _),
        string(config, "user").map(s"$prefix.kyuubi.user" -> _),
        string(config, "password").map(s"$prefix.kyuubi.password" -> _),
        string(config, "driver").map(s"$prefix.kyuubi.driver" -> _)).flatten ++
        stringMap(config, "options").map { case (key, value) =>
          s"$prefix.kyuubi.option.$key" -> value
        }

    val local =
      if (string(config, "type").exists(_.equalsIgnoreCase("kyuubi"))) Seq.empty
      else localEngineProperties(prefix, config)

    (common ++ local ++ kyuubi).toMap
  }

  private def localEngineProperties(prefix: String, config: Config): Seq[(String, String)] = {
    val propertyPrefix = s"$prefix.local.property"
    localSparkProperties(propertyPrefix, config) ++
      localOdepProperties(propertyPrefix, config) ++
      localOverwriteProperties(propertyPrefix, config) ++
      localHadoopProperties(propertyPrefix, config) ++
      localHiveProperties(propertyPrefix, config) ++
      localKerberosProperties(propertyPrefix, config) ++
      localJarsProperties(propertyPrefix, config) ++
      localCatalogProperties(propertyPrefix, config)
  }

  private def localOdepProperties(prefix: String, config: Config): Seq[(String, String)] = {
    Seq(
      string(config, "odep.apiUrl").map(s"$prefix.queryone.odep.api.url" -> _),
      string(config, "odep.appId").map(s"$prefix.queryone.odep.app.id" -> _),
      string(config, "odep.signKey").map(s"$prefix.queryone.odep.sign.key" -> _),
      int(config, "odep.connectTimeoutSeconds")
        .map(value => s"$prefix.queryone.odep.connect.timeout.seconds" -> value.toString),
      int(config, "odep.requestTimeoutSeconds")
        .map(value => s"$prefix.queryone.odep.request.timeout.seconds" -> value.toString)).flatten
  }

  private def localSparkProperties(prefix: String, config: Config): Seq[(String, String)] = {
    Seq(
      string(config, "spark.master").map(s"$prefix.spark.master" -> _),
      string(config, "spark.driverHost").map(s"$prefix.spark.driver.host" -> _),
      string(config, "spark.driverBindAddress").map(s"$prefix.spark.driver.bindAddress" -> _),
      string(config, "spark.kerberos.principal").map(s"$prefix.spark.kerberos.principal" -> _),
      string(config, "spark.kerberos.keytab").map(s"$prefix.spark.kerberos.keytab" -> _)).flatten
  }

  private def localOverwriteProperties(prefix: String, config: Config): Seq[(String, String)] = {
    Seq(
      string(config, "overwrite.zkConnect").map(s"$prefix.spark.queryone.overwrite.zk.connect" -> _),
      string(config, "overwrite.zkRoot").map(s"$prefix.spark.queryone.overwrite.zk.root" -> _),
      string(config, "overwrite.workspaceRoot").map(s"$prefix.spark.queryone.overwrite.workspaceRoot" -> _),
      int(config, "overwrite.zkSessionTimeoutMs")
        .map(value => s"$prefix.spark.queryone.overwrite.zk.sessionTimeoutMs" -> value.toString),
      int(config, "overwrite.zkConnectionTimeoutMs")
        .map(value => s"$prefix.spark.queryone.overwrite.zk.connectionTimeoutMs" -> value.toString)).flatten
  }

  private def localHadoopProperties(prefix: String, config: Config): Seq[(String, String)] = {
    Seq(
      string(config, "hadoop.confDir").map(s"$prefix.queryone.hadoop.conf.dir" -> _),
      string(config, "hadoop.confFiles").map(s"$prefix.queryone.hadoop.conf.files" -> _),
      string(config, "hadoop.groupStaticOverrides").map(s"$prefix.queryone.hadoop.group.static.mapping.overrides" -> _)).flatten
  }

  private def localHiveProperties(prefix: String, config: Config): Seq[(String, String)] = {
    Seq(
      boolean(config, "hive.enabled").map(value => s"$prefix.queryone.hive.enabled" -> value.toString),
      string(config, "hive.confFile").map(s"$prefix.queryone.hive.conf.file" -> _),
      string(config, "hive.confDir").map(s"$prefix.queryone.hive.conf.dir" -> _)).flatten
  }

  private def localKerberosProperties(prefix: String, config: Config): Seq[(String, String)] = {
    Seq(string(config, "kerberos.krb5Conf").map(s"$prefix.java.security.krb5.conf" -> _)).flatten
  }

  private def localJarsProperties(prefix: String, config: Config): Seq[(String, String)] = {
    Seq(
      string(config, "jars.packages").map(s"$prefix.spark.jars.packages" -> _),
      string(config, "jars.jars").map(s"$prefix.spark.jars" -> _),
      string(config, "jars.files").map(s"$prefix.spark.files" -> _),
      string(config, "jars.repositories").map(s"$prefix.spark.jars.repositories" -> _)).flatten
  }

  private def localCatalogProperties(prefix: String, config: Config): Seq[(String, String)] = {
    val doris =
      if (!config.hasPath("catalogs.doris_static")) Map.empty[String, String]
      else dorisCatalogProperties(prefix, config.getConfig("catalogs.doris_static"))
    val mysql =
      if (!config.hasPath("catalogs.mysql_static")) Map.empty[String, String]
      else mysqlCatalogProperties(prefix, config.getConfig("catalogs.mysql_static"))

    (doris ++ mysql).toSeq
  }

  private def mysqlCatalogProperties(propertyPrefix: String, config: Config): Map[String, String] = {
    val prefix = s"$propertyPrefix.spark.sql.catalog.mysql_static"
    (Seq(
      string(config, "class").orElse(Some("org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog")).map(prefix -> _),
      string(config, "url").map(s"$prefix.url" -> _),
      string(config, "driver").map(s"$prefix.driver" -> _),
      string(config, "user").map(s"$prefix.user" -> _),
      string(config, "password").map(s"$prefix.password" -> _)).flatten ++
      stringMap(config, "options").collect {
        case (key, value) if !Set("dbtable", "query").contains(key.toLowerCase) =>
          s"$prefix.$key" -> value
      }).toMap
  }

  private def dorisCatalogProperties(propertyPrefix: String, config: Config): Map[String, String] = {
    val prefix = s"$propertyPrefix.spark.sql.catalog.doris_static"
    (Seq(
      string(config, "class").orElse(Some("org.apache.doris.spark.catalog.DorisTableCatalog")).map(prefix -> _),
      string(config, "fenodes").map(s"$prefix.doris.fenodes" -> _),
      string(config, "queryPort").map(s"$prefix.doris.query.port" -> _),
      string(config, "user").map(s"$prefix.doris.user" -> _),
      string(config, "password").map(s"$prefix.doris.password" -> _)).flatten ++
      stringMap(config, "options").map { case (key, value) =>
        s"$prefix.$key" -> value
      }).toMap
  }

  private def passthroughProperties(config: Config): Map[String, String] = {
    config.entrySet().asScala
      .map(entry => entry.getKey -> unwrappedString(entry.getValue.unwrapped()))
      .collect {
        case (key, value) if value.nonEmpty &&
          key.startsWith("queryone.") =>
          key -> value
      }
      .toMap
  }

  private def string(config: Config, path: String): Option[String] = {
    if (config.hasPath(path)) {
      Some(config.getString(path).trim).filter(_.nonEmpty)
    } else {
      None
    }
  }

  private def int(config: Config, path: String): Option[Int] = {
    if (config.hasPath(path)) Some(config.getInt(path)) else None
  }

  private def long(config: Config, path: String): Option[Long] = {
    if (config.hasPath(path)) Some(config.getLong(path)) else None
  }

  private def boolean(config: Config, path: String): Option[Boolean] = {
    if (config.hasPath(path)) Some(config.getBoolean(path)) else None
  }

  private def stringMap(config: Config, path: String): Map[String, String] = {
    if (config.hasPath(path)) {
      config.getConfig(path).entrySet().asScala.map { entry =>
        entry.getKey -> unwrappedString(entry.getValue.unwrapped())
      }.filter(_._2.nonEmpty).toMap
    } else {
      Map.empty
    }
  }

  private def unwrappedString(value: Any): String = {
    Option(value).map(_.toString.trim).getOrElse("")
  }
}
