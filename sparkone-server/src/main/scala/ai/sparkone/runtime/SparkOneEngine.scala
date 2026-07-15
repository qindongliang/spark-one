package ai.sparkone.runtime

import ai.sparkone.identity.TenantContext
import ai.sparkone.sql.{CatalogWriteSqlRenderer, CompileException, CompiledStatement, DataSourceResolver, LoadTargetType, MysqlLoadMode, MysqlLoadProfile, SetValueType, SparkOneCompiler, SparkSqlValidator, WriteExecutionType, WriteMode, WritePlan}
import org.slf4j.LoggerFactory

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}
import java.util.Properties
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.util.control.NonFatal

trait SparkOneEngine extends AutoCloseable {
  def id: String
  def label: String
  def engineType: String
  def capabilities: EngineCapabilities
  def compile(tenant: TenantContext, script: String): Seq[CompiledStatement]
  def run(tenant: TenantContext, script: String, limit: Int = PreviewConfig.current.maxRows): RunResult
  def previewTable(tenant: TenantContext, table: String, limit: Int = PreviewConfig.current.maxRows): StatementResult
}

final case class EngineCapabilities(
    mysqlAdapter: Boolean,
    externalCatalogConfiguredBySparkOne: Boolean,
    sessionScopedTempViews: Boolean,
    kyuubiExternalEngineConfig: Boolean,
    compileDiagnostics: Seq[String])

object EngineCapabilities {
  val Local: EngineCapabilities = EngineCapabilities(
    mysqlAdapter = true,
    externalCatalogConfiguredBySparkOne = true,
    sessionScopedTempViews = true,
    kyuubiExternalEngineConfig = false,
    compileDiagnostics = Nil)

  val Kyuubi: EngineCapabilities = EngineCapabilities(
    mysqlAdapter = false,
    externalCatalogConfiguredBySparkOne = false,
    sessionScopedTempViews = true,
    kyuubiExternalEngineConfig = true,
    compileDiagnostics = Seq(
      "Kyuubi engine does not read engines.local catalog, datasource, or jars config; configure catalogs and provider jars in Kyuubi/Spark engine.",
      "Kyuubi load mysql can use mysql.`catalog.db.table`; big-table options require sparkone_mysql provider in Kyuubi/Spark engine.",
      "SparkOne save mysql adapter is local-only; Kyuubi save mysql is unavailable."))
}

final case class EngineInfo(id: String, label: String, engineType: String, capabilities: EngineCapabilities)

final class LocalSparkEngine(
    val id: String,
    val label: String,
    runtime: => SparkOneRuntime,
    properties: Map[String, String])
  extends SparkOneEngine {

  override val engineType: String = "local"
  override val capabilities: EngineCapabilities = EngineCapabilities.Local
  private val compiler = new SparkOneCompiler(new SparkSqlValidator)
  @volatile private var delegateRef: SparkOneRuntime = _

  override def compile(tenant: TenantContext, script: String): Seq[CompiledStatement] = {
    withLocalProperties {
      compiler.compile(tenant, script)
    }
  }

  override def run(tenant: TenantContext, script: String, limit: Int): RunResult = {
    withLocalProperties {
      delegate.run(tenant, script, limit)
    }
  }

  override def previewTable(tenant: TenantContext, table: String, limit: Int): StatementResult = {
    withLocalProperties {
      delegate.previewTable(table, limit)
    }
  }

  override def close(): Unit = {
    val current = delegateRef
    if (current != null) {
      current.close()
      delegateRef = null
    }
  }

  private def delegate: SparkOneRuntime = {
    if (delegateRef == null) {
      this.synchronized {
        if (delegateRef == null) {
          delegateRef = runtime
        }
      }
    }
    delegateRef
  }

  private def withLocalProperties[T](body: => T): T = LocalSparkEngine.PropertyLock.synchronized {
    val previous = properties.keys.map(key => key -> sys.props.get(key)).toMap
    properties.foreach { case (key, value) => sys.props.put(key, value) }
    try {
      body
    } finally {
      previous.foreach {
        case (key, Some(value)) => sys.props.put(key, value)
        case (key, None) => sys.props.remove(key)
      }
    }
  }
}

private object LocalSparkEngine {
  private val PropertyLock = new AnyRef
}

final class KyuubiJdbcEngine(
    val id: String,
    val label: String,
    config: KyuubiJdbcConfig,
    mysqlLoadProfiles: Map[String, MysqlLoadProfile] = Map.empty,
    compilerOverride: Option[SparkOneCompiler] = None,
    connectionFactory: KyuubiJdbcConfig => Connection = KyuubiJdbcEngine.openConnection)
  extends SparkOneEngine {

  override val engineType: String = "kyuubi"
  override val capabilities: EngineCapabilities = EngineCapabilities.Kyuubi

  private val logger = LoggerFactory.getLogger(getClass)
  private val compiler = compilerOverride.getOrElse {
    new SparkOneCompiler(
      new SparkSqlValidator,
      new DataSourceResolver(
        mysqlLoadMode = MysqlLoadMode.KyuubiProfile,
        mysqlLoadProfiles = mysqlLoadProfiles))
  }
  private val tenantSessions = TrieMap.empty[String, TenantJdbcSession]

  override def compile(tenant: TenantContext, script: String): Seq[CompiledStatement] = {
    compiler.compile(tenant, script).map { statement =>
      validateSupported(statement)
      statement
    }
  }

  override def run(tenant: TenantContext, script: String, limit: Int): RunResult = {
    val session = tenantSession(tenant)
    session.lock.synchronized {
      val previewLimit = PreviewConfig.current.clampRows(Some(limit))
      val sources = compiler.splitStatements(script)
      val variables = mutable.LinkedHashMap[String, String]()
      val results: Seq[StatementResult] = sources.zipWithIndex.map { case (source, offset) =>
        val started = System.nanoTime()
        var statement: Option[CompiledStatement] = None
        try {
          val compiledStatement = compiler.compileStatementWithVariables(tenant, source, variables.toMap)
          statement = Some(compiledStatement)
          validateSupported(compiledStatement)
          val executableStatement = prepareWriteStatement(session, compiledStatement)
          statement = Some(executableStatement)
          execute(session, executableStatement, variables, previewLimit, offset + 1, started)
        } catch {
          case e: Exception =>
            val sourceSummary = statement.map(_.source).getOrElse(source)
            val sqlSummary = statement.map(_.sql).getOrElse(source)
            logger.error(
              s"Kyuubi statement ${offset + 1} failed, engine=$id, tenant=${session.tenant.username}, " +
                s"source=${summarizeSql(sourceSummary)}, " +
                s"sql=${summarizeSql(sqlSummary)}, reason=${errorMessage(e)}",
              e)
            StatementResult(
              index = offset + 1,
              source = sourceSummary,
              sql = sqlSummary,
              success = false,
              schema = Nil,
              rows = Nil,
              rowCount = 0,
              truncated = false,
              previewTable = None,
              durationMs = elapsedMs(started),
              error = Some(errorMessage(e)))
        }
      }

      val success = results.forall(_.success)
      val visibleResults =
        if (success && results.nonEmpty && results.forall(_.previewTable.nonEmpty)) results.takeRight(1)
        else results
      RunResult(success, visibleResults)
    }
  }

  override def previewTable(tenant: TenantContext, table: String, limit: Int): StatementResult = {
    val session = tenantSession(tenant)
    session.lock.synchronized {
      val started = System.nanoTime()
      val previewLimit = PreviewConfig.current.clampRows(Some(limit))
      val sql = s"SELECT * FROM `${table.replace("`", "``")}` LIMIT ${previewLimit + 1}"
      withStatement(session) { statement =>
        val resultSet = statement.executeQuery(sql)
        try {
          val result = collectResultSet(resultSet, previewLimit)
          StatementResult(
            index = 1,
            source = table,
            sql = sql,
            success = true,
            schema = result.schema,
            rows = result.rows,
            rowCount = result.rows.size,
            truncated = result.truncated,
            previewTable = Some(table),
            durationMs = elapsedMs(started),
            error = None)
        } finally {
          resultSet.close()
        }
      }
    }
  }

  override def close(): Unit = {
    tenantSessions.values.foreach { session =>
      session.lock.synchronized {
        closeConnection(session)
      }
    }
    tenantSessions.clear()
  }

  private def execute(
      session: TenantJdbcSession,
      statement: CompiledStatement,
      variables: mutable.Map[String, String],
      previewLimit: Int,
      index: Int,
      started: Long): StatementResult = {
    statement.set match {
      case Some(metadata) =>
        val value = metadata.valueType match {
          case SetValueType.Literal =>
            metadata.value
          case SetValueType.Sql =>
            firstValue(session, metadata.value).getOrElse("")
        }
        variables(metadata.key) = value
        StatementResult(index, statement.source, statement.sql, success = true, Nil, Nil, 0,
          truncated = false, None, elapsedMs(started), None)
      case None =>
        try {
          withStatement(session, retryOnReconnect = statement.writePlan.isEmpty) { jdbcStatement =>
            jdbcStatement.setMaxRows(previewLimit + 1)
            jdbcStatement.setFetchSize(previewLimit + 1)
            val hasResultSet = jdbcStatement.execute(statement.sql)
            val collected =
              if (hasResultSet) {
                val resultSet = jdbcStatement.getResultSet
                try {
                  collectResultSet(resultSet, previewLimit)
                } finally {
                  resultSet.close()
                }
              } else if (statement.load.nonEmpty) {
                collectSchema(session, statement.load.get.table, previewLimit)
              } else {
                JdbcResult(Nil, Nil, truncated = false)
              }

            StatementResult(
              index = index,
              source = statement.source,
              sql = statement.sql,
              success = true,
              schema = collected.schema,
              rows = collected.rows,
              rowCount = collected.rows.size,
              truncated = collected.truncated,
              previewTable = statement.load.map(_.table),
              durationMs = elapsedMs(started),
              error = None)
          }
        } catch {
          case NonFatal(e) if statement.writePlan.nonEmpty && shouldReconnect(e) =>
            val target = statement.writePlan.map(_.target.identifier).getOrElse("unknown")
            throw new CompileException(
              s"SAVE connection was interrupted; write status is unknown and SparkOne did not retry. " +
                s"Verify target before submitting again: $target",
              e)
        }
    }
  }

  private def prepareWriteStatement(
      session: TenantJdbcSession,
      statement: CompiledStatement): CompiledStatement = {
    statement.writePlan match {
      case Some(plan) if plan.executionType == WriteExecutionType.CatalogSql && plan.mode == WriteMode.Append =>
        statement.copy(sql = prepareCatalogAppend(session, plan))
      case _ => statement
    }
  }

  private def prepareCatalogAppend(
      session: TenantJdbcSession,
      plan: WritePlan): String = {
    val targetColumns = try {
      queryColumnNames(session, s"SELECT * FROM ${plan.target.identifier} LIMIT 0")
    } catch {
      case NonFatal(e) if shouldReconnect(e) =>
        throw new CompileException(
          s"SAVE target preflight was interrupted before writing: ${plan.target.identifier}",
          e)
      case NonFatal(e) =>
        throw new CompileException(
          s"SAVE target table does not exist or cannot be resolved: ${plan.target.identifier}. " +
            s"Create the target table explicitly before SAVE ${plan.mode.name}.",
          e)
    }

    val sourceColumns = try {
      queryColumnNames(session, s"SELECT * FROM ${plan.sourceTable} LIMIT 0")
    } catch {
      case NonFatal(e) if shouldReconnect(e) =>
        throw new CompileException(
          s"SAVE source preflight was interrupted before writing: ${plan.sourceTable}",
          e)
      case NonFatal(e) =>
        throw new CompileException(
          s"SAVE source table or view does not exist or cannot be resolved: ${plan.sourceTable}",
          e)
    }
    val sql = CatalogWriteSqlRenderer.render(plan, sourceColumns, targetColumns)

    try {
      queryColumnNames(session, s"EXPLAIN $sql")
    } catch {
      case NonFatal(e) if shouldReconnect(e) =>
        throw new CompileException(
          s"SAVE schema preflight was interrupted before writing: ${plan.target.identifier}",
          e)
      case NonFatal(e) =>
        throw new CompileException(
          s"SAVE source schema is incompatible with target table: ${plan.target.identifier}",
          e)
    }
    sql
  }

  private def queryColumnNames(session: TenantJdbcSession, sql: String): Seq[String] = {
    withStatement(session) { statement =>
      val resultSet = statement.executeQuery(sql)
      try {
        schemaInfo(resultSet.getMetaData).map(_.name)
      } finally {
        resultSet.close()
      }
    }
  }

  private def validateSupported(statement: CompiledStatement): Unit = {
    statement.load.foreach { metadata =>
      if (metadata.targetType == LoadTargetType.Mysql) {
        throw new CompileException("Kyuubi engine does not support SparkOne load mysql adapter yet; use catalog SQL or local engine")
      }
    }
    statement.writePlan.foreach { plan =>
      if (plan.executionType == WriteExecutionType.MysqlAdapter) {
        throw new CompileException("Kyuubi engine does not support SparkOne save mysql adapter; use the local engine for MySQL append")
      }
    }
  }

  private def firstValue(session: TenantJdbcSession, sql: String): Option[String] = {
    withStatement(session) { statement =>
      statement.setMaxRows(1)
      val resultSet = statement.executeQuery(sql)
      try {
        if (resultSet.next()) Option(resultSet.getObject(1)).map(_.toString) else None
      } finally {
        resultSet.close()
      }
    }
  }

  private def collectSchema(session: TenantJdbcSession, table: String, previewLimit: Int): JdbcResult = {
    val sql = s"SELECT * FROM `${table.replace("`", "``")}` LIMIT 0"
    withStatement(session) { statement =>
      statement.setMaxRows(previewLimit + 1)
      val resultSet = statement.executeQuery(sql)
      try {
        JdbcResult(schemaInfo(resultSet.getMetaData), Nil, truncated = false)
      } finally {
        resultSet.close()
      }
    }
  }

  private def collectResultSet(resultSet: ResultSet, previewLimit: Int): JdbcResult = {
    val metadata = resultSet.getMetaData
    val columnCount = metadata.getColumnCount
    val rows = mutable.ArrayBuffer.empty[Seq[String]]
    var count = 0
    while (count < previewLimit + 1 && resultSet.next()) {
      rows += (1 to columnCount).map { index =>
        val value = resultSet.getString(index)
        if (resultSet.wasNull()) null else value
      }
      count += 1
    }
    JdbcResult(schemaInfo(metadata), rows.take(previewLimit).toSeq, count > previewLimit)
  }

  private def schemaInfo(metadata: ResultSetMetaData): Seq[FieldInfo] = {
    (1 to metadata.getColumnCount).map { index =>
      val name = Option(metadata.getColumnLabel(index)).filter(_.nonEmpty).getOrElse(metadata.getColumnName(index))
      val dataType = Option(metadata.getColumnTypeName(index)).filter(_.nonEmpty).getOrElse(metadata.getColumnType(index).toString)
      val nullable = metadata.isNullable(index) != ResultSetMetaData.columnNoNulls
      FieldInfo(name, dataType.toLowerCase, nullable)
    }
  }

  private def withStatement[T](session: TenantJdbcSession)(body: Statement => T): T = {
    withStatement(session, body, retryOnReconnect = true)
  }

  private def withStatement[T](
      session: TenantJdbcSession,
      retryOnReconnect: Boolean)(body: Statement => T): T = {
    withStatement(session, body, retryOnReconnect)
  }

  private def withStatement[T](
      session: TenantJdbcSession,
      body: Statement => T,
      retryOnReconnect: Boolean): T = {
    var statement: Statement = null
    val hadConnection = session.connectionRef != null
    try {
      statement = connection(session).createStatement()
      body(statement)
    } catch {
      case NonFatal(e) if retryOnReconnect && hadConnection && shouldReconnect(e) =>
        logger.warn(
          s"Kyuubi connection for engine $id and tenant ${session.tenant.username} is stale, " +
            s"reconnecting once: ${errorMessage(e)}")
        closeConnection(session)
        withStatement(session, body, retryOnReconnect = false)
      case NonFatal(e) =>
        if (shouldReconnect(e)) {
          closeConnection(session)
        }
        throw e
    } finally {
      closeStatement(session, statement)
    }
  }

  private def connection(session: TenantJdbcSession): Connection = {
    if (session.connectionRef == null || session.connectionRef.isClosed) {
      session.connectionRef = connectionFactory(config)
      logger.info(
        s"Connected logical tenant ${session.tenant.username} to Kyuubi engine $id at ${redactJdbcUrl(config.url)}")
    }
    session.connectionRef
  }

  private def closeConnection(session: TenantJdbcSession): Unit = {
    val current = session.connectionRef
    session.connectionRef = null
    if (current != null) {
      try {
        current.close()
      } catch {
        case NonFatal(e) =>
          logger.warn(s"Failed to close Kyuubi connection for engine $id: ${errorMessage(e)}")
      }
    }
  }

  private def closeStatement(session: TenantJdbcSession, statement: Statement): Unit = {
    if (statement != null) {
      try {
        statement.close()
      } catch {
        case NonFatal(e) =>
          logger.warn(s"Failed to close Kyuubi statement for engine $id: ${errorMessage(e)}")
          if (shouldReconnect(e)) {
            closeConnection(session)
          }
      }
    }
  }

  private def shouldReconnect(error: Throwable): Boolean = {
    def loop(current: Throwable): Boolean = {
      if (current == null) false
      else {
        val message = errorMessage(current).toLowerCase
        message.contains("could not send message") ||
        message.contains("connection reset") ||
        message.contains("broken pipe") ||
        message.contains("invalid session") ||
        (message.contains("session") && message.contains("closed")) ||
        loop(current.getCause)
      }
    }
    loop(error)
  }

  private def elapsedMs(started: Long): Long = {
    (System.nanoTime() - started) / 1000000L
  }

  private def errorMessage(error: Throwable): String = {
    Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getName)
  }

  private def summarizeSql(sql: String): String = {
    val normalized = sql.replaceAll("\\s+", " ").trim
    if (normalized.length <= 240) normalized else normalized.take(237) + "..."
  }

  private def redactJdbcUrl(url: String): String = {
    url.replaceAll("(?i)(password|pwd)=[^;]+", "$1=***")
  }

  private def tenantSession(tenant: TenantContext): TenantJdbcSession = {
    tenantSessions.getOrElseUpdate(tenant.username, new TenantJdbcSession(tenant))
  }

}

private final class TenantJdbcSession(val tenant: TenantContext) {
  val lock: AnyRef = new AnyRef
  @volatile var connectionRef: Connection = _
}

private object KyuubiJdbcEngine {
  def openConnection(config: KyuubiJdbcConfig): Connection = {
    Class.forName(config.driver)
    val properties = new Properties()
    config.user.foreach(properties.setProperty("user", _))
    config.password.foreach(properties.setProperty("password", _))
    config.properties.foreach { case (key, value) => properties.setProperty(key, value) }
    DriverManager.getConnection(config.url, properties)
  }
}

private final case class JdbcResult(schema: Seq[FieldInfo], rows: Seq[Seq[String]], truncated: Boolean)

final case class KyuubiJdbcConfig(
    url: String,
    user: Option[String],
    password: Option[String],
    driver: String,
    properties: Map[String, String])
