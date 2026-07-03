package ai.sparkone.runtime

import ai.sparkone.sql.{CompileException, CompiledStatement, LoadTargetType, SaveTargetType, SetValueType, SparkOneCompiler, SparkSqlValidator}
import org.slf4j.LoggerFactory

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}
import java.util.Properties
import scala.collection.mutable
import scala.util.control.NonFatal

trait SparkOneEngine extends AutoCloseable {
  def id: String
  def label: String
  def engineType: String
  def run(script: String, limit: Int = PreviewConfig.current.maxRows): RunResult
  def previewTable(table: String, limit: Int = PreviewConfig.current.maxRows): StatementResult
}

final case class EngineInfo(id: String, label: String, engineType: String)

final class LocalSparkEngine(
    val id: String,
    val label: String,
    runtime: => SparkOneRuntime,
    properties: Map[String, String])
  extends SparkOneEngine {

  override val engineType: String = "local"
  private lazy val delegate = withLocalProperties {
    runtime
  }

  override def run(script: String, limit: Int): RunResult = {
    withLocalProperties {
      delegate.run(script, limit)
    }
  }

  override def previewTable(table: String, limit: Int): StatementResult = {
    withLocalProperties {
      delegate.previewTable(table, limit)
    }
  }

  override def close(): Unit = {
    delegate.close()
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
    compiler: SparkOneCompiler = new SparkOneCompiler(new SparkSqlValidator))
  extends SparkOneEngine {

  override val engineType: String = "kyuubi"

  private val logger = LoggerFactory.getLogger(getClass)
  private val runLock = new AnyRef
  private val nativeSqlSafetyGuard = new NativeSqlSafetyGuard
  private val saveSafetyGuard = new RemoteSaveSafetyGuard
  @volatile private var connectionRef: Connection = _

  override def run(script: String, limit: Int): RunResult = runLock.synchronized {
    val previewLimit = PreviewConfig.current.clampRows(Some(limit))
    val sources = compiler.splitStatements(script)
    val variables = mutable.LinkedHashMap[String, String]()
    val results: Seq[StatementResult] = sources.zipWithIndex.map { case (source, offset) =>
      val started = System.nanoTime()
      var statement: Option[CompiledStatement] = None
      try {
        val compiledStatement = compiler.compileStatementWithVariables(source, variables.toMap)
        statement = Some(compiledStatement)
        validateSupported(compiledStatement)
        nativeSqlSafetyGuard.validate(compiledStatement)
        saveSafetyGuard.validate(compiledStatement.save)
        execute(compiledStatement, variables, previewLimit, offset + 1, started)
      } catch {
        case e: Exception =>
          val sourceSummary = statement.map(_.source).getOrElse(source)
          val sqlSummary = statement.map(_.sql).getOrElse(source)
          logger.error(
            s"Kyuubi statement ${offset + 1} failed, engine=$id, source=${summarizeSql(sourceSummary)}, " +
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

  override def previewTable(table: String, limit: Int): StatementResult = runLock.synchronized {
    val started = System.nanoTime()
    val previewLimit = PreviewConfig.current.clampRows(Some(limit))
    val sql = s"SELECT * FROM `${table.replace("`", "``")}` LIMIT ${previewLimit + 1}"
    withStatement { statement =>
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

  override def close(): Unit = runLock.synchronized {
    if (connectionRef != null) {
      connectionRef.close()
      connectionRef = null
    }
  }

  private def execute(
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
            firstValue(metadata.value).getOrElse("")
        }
        variables(metadata.key) = value
        StatementResult(index, statement.source, statement.sql, success = true, Nil, Nil, 0,
          truncated = false, None, elapsedMs(started), None)
      case None =>
        withStatement { jdbcStatement =>
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
              collectSchema(statement.load.get.table, previewLimit)
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
    }
  }

  private def validateSupported(statement: CompiledStatement): Unit = {
    statement.load.foreach { metadata =>
      if (metadata.targetType == LoadTargetType.Mysql) {
        throw new CompileException("Kyuubi engine does not support SparkOne load mysql adapter yet; use catalog SQL or local engine")
      }
    }
    statement.save.foreach { metadata =>
      if (metadata.targetType == SaveTargetType.Mysql) {
        throw new CompileException("Kyuubi engine does not support SparkOne save mysql adapter yet; use catalog SQL or local engine")
      }
    }
  }

  private def firstValue(sql: String): Option[String] = {
    withStatement { statement =>
      statement.setMaxRows(1)
      val resultSet = statement.executeQuery(sql)
      try {
        if (resultSet.next()) Option(resultSet.getObject(1)).map(_.toString) else None
      } finally {
        resultSet.close()
      }
    }
  }

  private def collectSchema(table: String, previewLimit: Int): JdbcResult = {
    val sql = s"SELECT * FROM `${table.replace("`", "``")}` LIMIT 0"
    withStatement { statement =>
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

  private def withStatement[T](body: Statement => T): T = {
    val statement = connection.createStatement()
    try {
      body(statement)
    } catch {
      case NonFatal(e) =>
        if (connectionRef != null && !connectionRef.isValid(1)) {
          close()
        }
        throw e
    } finally {
      statement.close()
    }
  }

  private def connection: Connection = {
    if (connectionRef == null || connectionRef.isClosed) {
      Class.forName(config.driver)
      val properties = new Properties()
      config.user.foreach(properties.setProperty("user", _))
      config.password.foreach(properties.setProperty("password", _))
      config.properties.foreach { case (key, value) => properties.setProperty(key, value) }
      connectionRef = DriverManager.getConnection(config.url, properties)
      logger.info(s"Connected to Kyuubi engine $id at ${redactJdbcUrl(config.url)}")
    }
    connectionRef
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

}

private final case class JdbcResult(schema: Seq[FieldInfo], rows: Seq[Seq[String]], truncated: Boolean)

final case class KyuubiJdbcConfig(
    url: String,
    user: Option[String],
    password: Option[String],
    driver: String,
    properties: Map[String, String])

private final class RemoteSaveSafetyGuard {
  private val logger = LoggerFactory.getLogger(getClass)

  def validate(save: Option[ai.sparkone.sql.SaveStatementMetadata]): Unit = {
    save.foreach { metadata =>
      if (metadata.mode.equalsIgnoreCase("overwrite")) {
        validateOverwrite(metadata)
      }
    }
  }

  private def validateOverwrite(metadata: ai.sparkone.sql.SaveStatementMetadata): Unit = {
    metadata.targetType match {
      case SaveTargetType.Mysql if !enabled("sparkone.save.mysql.overwrite.enabled") =>
        throw new CompileException("SAVE overwrite mysql is disabled by SparkOne Safe Save policy")
      case SaveTargetType.DorisCatalog if !enabled("sparkone.save.doris.overwrite.enabled") =>
        throw new CompileException("SAVE overwrite doris is disabled by SparkOne Safe Save policy")
      case _ =>
    }

    policy match {
      case "deny" =>
        logger.warn(s"Remote Safe Save: overwrite denied, target=${metadata.path}, format=${metadata.format}")
        throw new CompileException("SAVE overwrite is disabled by SparkOne Safe Save policy")
      case "requireexplicit" if !hasStatementConfirmation(metadata) =>
        logger.warn(s"Remote Safe Save: overwrite requires confirmation, target=${metadata.path}, format=${metadata.format}")
        throw new CompileException("SAVE overwrite requires options sparkoneOverwrite=\"allow\"")
      case _ =>
    }
  }

  private def hasStatementConfirmation(metadata: ai.sparkone.sql.SaveStatementMetadata): Boolean = {
    metadata.options.get("sparkoneoverwrite").exists(_.equalsIgnoreCase("allow"))
  }

  private def policy: String = {
    sys.props.getOrElse("sparkone.save.overwrite.policy", "requireExplicit").trim.toLowerCase
  }

  private def enabled(key: String): Boolean = {
    sys.props.get(key).exists(value => Set("1", "true", "yes", "on").contains(value.trim.toLowerCase))
  }
}
