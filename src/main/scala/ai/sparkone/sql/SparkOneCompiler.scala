package ai.sparkone.sql

import ai.sparkone.sql.parser.{SparkOneDslLexer, SparkOneDslParser}
import org.antlr.v4.runtime.{BaseErrorListener, CharStreams, CommonTokenStream, Parser, RecognitionException, Recognizer}

import scala.collection.JavaConverters._
import scala.collection.mutable

final class SparkOneCompiler(
    sqlValidator: SqlValidator = SqlValidator.Noop,
    dataSourceResolver: DataSourceResolver = new DataSourceResolver()) {
  def compile(script: String): Seq[CompiledStatement] = {
    compileWithVariables(script, Map.empty, allowUnresolvedVariables = true)
  }

  def compileStatementWithVariables(script: String, variables: Map[String, String]): CompiledStatement = {
    val compiled = compileWithVariables(script, variables, allowUnresolvedVariables = false)
    if (compiled.size != 1) {
      throw new CompileException(s"Expected exactly one statement after variable substitution, got ${compiled.size}")
    }
    compiled.head
  }

  def splitStatements(script: String): Seq[String] = {
    rejectLegacyDslWhere(script)
    val tree = parse(script)
    tree.statement().asScala.map(statement => originalText(script, statement).trim)
  }

  private def compileWithVariables(
      script: String,
      initialVariables: Map[String, String],
      allowUnresolvedVariables: Boolean): Seq[CompiledStatement] = {
    val variables = mutable.LinkedHashMap[String, String]() ++ initialVariables
    splitStatements(script).map { source =>
      val renderedSource = SparkOneVariableSubstitutor.render(
        source,
        variables.toMap,
        allowUnresolved = allowUnresolvedVariables)
      val tree = parse(renderedSource)
      val statements = tree.statement().asScala
      if (statements.size != 1) {
        throw new CompileException(s"Expected exactly one statement after variable substitution, got ${statements.size}")
      }
      val compiled = compileParsedStatement(renderedSource, statements.head)
      sqlValidator.validate(compiled.sql)
      compiled.set.foreach { metadata =>
        if (metadata.valueType == SetValueType.Literal) {
          variables(metadata.key) = metadata.value
        }
      }
      CompiledStatement(renderedSource, compiled.sql, compiled.load, compiled.save, compiled.set)
    }
  }

  private def parse(script: String): SparkOneDslParser.ScriptContext = {
    val lexer = new SparkOneDslLexer(CharStreams.fromString(script))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ThrowingErrorListener)

    val parser = new SparkOneDslParser(new CommonTokenStream(lexer))
    parser.removeErrorListeners()
    parser.addErrorListener(ThrowingErrorListener)
    parser.script()
  }

  private def compileParsedStatement(script: String, statement: SparkOneDslParser.StatementContext): CompileResult = {
    if (statement.loadStatement() != null) {
      compileLoad(statement.loadStatement())
    } else if (statement.saveStatement() != null) {
      compileSave(statement.saveStatement())
    } else if (statement.setStatement() != null) {
      compileSet(script, statement.setStatement())
    } else if (statement.viewStatement() != null) {
      CompileResult(compileView(script, statement.viewStatement()))
    } else {
      val sql = originalText(script, statement).trim
      rejectMalformedSparkOneDsl(sql)
      CompileResult(sql)
    }
  }

  private def compileSet(script: String, set: SparkOneDslParser.SetStatementContext): CompileResult = {
    val key = SparkOneSqlRender.requireIdentifier(set.key.getText, "SET key")
    val valueType =
      if (set.query != null) SetValueType.Sql else SetValueType.Literal
    val value =
      if (set.query != null) originalText(script, set.query).trim
      else stripQuoted(set.value.getText).trim
    if (value.isEmpty) {
      throw new CompileException("SET value must not be empty")
    }
    CompileResult(
      SparkOneSqlRender.renderSparkOneAction("SET", key),
      set = Some(SetStatementMetadata(key, value, valueType)))
  }

  private def rejectLegacyDslWhere(sql: String): Unit = {
    if (SparkOneCompiler.LegacyLoadWherePattern.findFirstIn(sql).nonEmpty ||
        SparkOneCompiler.LegacySaveWherePattern.findFirstIn(sql).nonEmpty) {
      throw new CompileException("SparkOne DSL options must use OPTIONS, not WHERE.")
    }
    if (SparkOneCompiler.LegacySetSqlTypePattern.findFirstIn(sql).nonEmpty) {
      throw new CompileException("SparkOne SQL variables must use `set name as select ...`, not `where type=\"sql\"`.")
    }
  }

  private def rejectMalformedSparkOneDsl(sql: String): Unit = {
    sql match {
      case SparkOneCompiler.LoadWithoutAsPattern(format, rawPath, rest)
          if SparkOneCompiler.ContainsAsPattern.findFirstIn(rest).isEmpty =>
        val path = rawPath.replace("``", "`")
        val alias = SparkOneCompiler.suggestAlias(path)
        throw new CompileException(
          s"SparkOne LOAD requires a target temp view: load $format.`$path` as $alias. " +
            s"Add `as $alias` to the statement.")
      case _ =>
    }
  }

  private def compileLoad(load: SparkOneDslParser.LoadStatementContext): CompileResult = {
    val (format, path) = parseSource(load.source(), "LOAD")
    val table = SparkOneSqlRender.requireIdentifier(load.table.getText, "LOAD target table")
    val options = parseOptions(load.optionClause())
    val filter = parseLoadFilter(load.whereClause())
    dataSourceResolver.resolveLoad(format, path, options, filter) match {
      case ProviderLoadSource(provider, providerOptions) =>
        CompileResult(
          SparkOneSqlRender.renderCreateTempViewUsing(table, provider, providerOptions),
          load = Some(LoadStatementMetadata(table, format, path, providerOptions.toMap)))
      case CatalogTableSource(identifier) =>
        CompileResult(
          SparkOneSqlRender.renderCreateTempViewAsSelect(table, identifier),
          load = Some(LoadStatementMetadata(table, format, identifier, Map.empty)))
      case MysqlLoadSource(dbtable, jdbcOptions) =>
        CompileResult(
          SparkOneSqlRender.renderSparkOneAction("LOAD MYSQL", s"$dbtable AS $table"),
          load = Some(LoadStatementMetadata(table, format, dbtable, jdbcOptions.toMap, LoadTargetType.Mysql)))
    }
  }

  private def compileSave(save: SparkOneDslParser.SaveStatementContext): CompileResult = {
    val mode = Option(save.saveMode()).map(_.getText.toLowerCase).getOrElse("errorifexists")
    val table = SparkOneSqlRender.requireIdentifier(save.table.getText, "SAVE source table")
    val (format, path) = parseSource(save.source(), "SAVE")
    val options = parseOptions(save.optionClause())
    val partitionColumns = parsePartitionColumns(save.partitionClause())
    val (runtimeOptions, providerOptions) = SaveControlOptions.partition(options)
    val forbiddenStatementControls = runtimeOptions.collect {
      case (key, _) if !key.equalsIgnoreCase(SaveControlOptions.Overwrite) => key
    }
    if (forbiddenStatementControls.nonEmpty) {
      throw new CompileException(
        s"SparkOne save option '${forbiddenStatementControls.head}' must be configured in HOCON save.*, not SQL OPTIONS")
    }
    val runtimeOptionMap = runtimeOptions.map { case (key, value) => key.toLowerCase -> value }.toMap

    dataSourceResolver.resolveSave(format, path, providerOptions) match {
      case ProviderSaveSource(provider) =>
        if (mode != "overwrite") {
          throw new CompileException(s"SAVE mode '$mode' is not supported for file/provider source '$format' yet")
        }
        if (partitionColumns.nonEmpty) {
          throw new CompileException(s"SAVE partitionBy is only supported for catalog source in the MVP compiler")
        }
        CompileResult(
          SparkOneSqlRender.renderInsertOverwriteDirectory(path, provider, providerOptions, table),
          Some(SaveStatementMetadata(mode, table, format, path, runtimeOptionMap)))
      case CatalogSaveSource(targetTable, targetType, supportsPartitionBy) =>
        if (mode != "overwrite" && mode != "append") {
          throw new CompileException(s"SAVE mode '$mode' is not supported for catalog source '$format'")
        }
        if (providerOptions.nonEmpty) {
          throw new CompileException(s"SAVE to catalog source '$format' does not support provider OPTIONS yet")
        }
        if (partitionColumns.nonEmpty && !supportsPartitionBy) {
          throw new CompileException(s"SAVE partitionBy is not supported for $format source")
        }
        CompileResult(
          SparkOneSqlRender.renderInsertTable(mode, targetTable, table, partitionColumns),
          Some(SaveStatementMetadata(mode, table, format, targetTable, runtimeOptionMap, targetType)))
      case MysqlSaveSource(dbtable, jdbcOptions) =>
        if (mode != "overwrite" && mode != "append") {
          throw new CompileException(s"SAVE mode '$mode' is not supported for mysql source")
        }
        if (partitionColumns.nonEmpty) {
          throw new CompileException("SAVE partitionBy is not supported for mysql source")
        }
        CompileResult(
          SparkOneSqlRender.renderSparkOneAction("SAVE MYSQL", s"$table TO $dbtable"),
          Some(SaveStatementMetadata(mode, table, format, dbtable, runtimeOptionMap, SaveTargetType.Mysql, jdbcOptions.toMap)))
    }
  }

  private def compileView(script: String, view: SparkOneDslParser.ViewStatementContext): String = {
    val table = SparkOneSqlRender.requireIdentifier(view.table.getText, "VIEW target table")
    val query = originalText(script, view.sqlStatement()).trim
    SparkOneSqlRender.renderCreateTempViewAsQuery(table, query)
  }

  private def parseSource(source: SparkOneDslParser.SourceContext, statementType: String): (String, String) = {
    val format = SparkOneSqlRender.requireIdentifier(source.format.getText, s"$statementType format")
    val path = stripQuoted(source.path.getText)
    (format, path)
  }

  private def parseOptions(optionClause: SparkOneDslParser.OptionClauseContext): Seq[(String, String)] = {
    Option(optionClause).toSeq.flatMap { clause =>
      clause.option().asScala.map { option =>
        val key = SparkOneSqlRender.requireIdentifier(option.key.getText, "option key")
        val value = stripQuoted(option.value.getText)
        key -> value
      }
    }
  }

  private def parseLoadFilter(whereClause: SparkOneDslParser.WhereClauseContext): Option[String] = {
    Option(whereClause).map { clause =>
      val filter = stripQuoted(clause.condition.getText).trim
      if (filter.isEmpty) {
        throw new CompileException("LOAD WHERE filter must not be empty")
      }
      if (filter.contains(";")) {
        throw new CompileException("LOAD WHERE filter must not contain semicolons")
      }
      filter
    }
  }

  private def parsePartitionColumns(partitionClause: SparkOneDslParser.PartitionClauseContext): Seq[String] = {
    Option(partitionClause).toSeq.flatMap { clause =>
      clause.identifier().asScala.map { identifier =>
        SparkOneSqlRender.requireIdentifier(identifier.getText, "partition column")
      }
    }
  }

  private def stripQuoted(value: String): String = {
    if (value.length >= 2 && value.head == '`' && value.last == '`') {
      value.substring(1, value.length - 1).replace("``", "`")
    } else if (value.length >= 2 && value.head == '\'' && value.last == '\'') {
      value.substring(1, value.length - 1).replace("\\'", "'")
    } else if (value.length >= 2 && value.head == '"' && value.last == '"') {
      value.substring(1, value.length - 1).replace("\\\"", "\"")
    } else {
      value
    }
  }

  private def originalText(script: String, context: org.antlr.v4.runtime.ParserRuleContext): String = {
    val start = context.getStart.getStartIndex
    val stop = context.getStop.getStopIndex
    if (start < 0 || stop < start) "" else script.substring(start, stop + 1)
  }
}

private final case class CompileResult(
    sql: String,
    save: Option[SaveStatementMetadata] = None,
    load: Option[LoadStatementMetadata] = None,
    set: Option[SetStatementMetadata] = None)

private object SparkOneCompiler {
  private val DslSource = """[A-Za-z_][A-Za-z0-9_]*\s*\.\s*`(?:``|[^`])*`"""
  private val LoadWithoutAsPattern =
    """(?is)^\s*load\s+([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*`((?:``|[^`])*)`(.*)$""".r
  private val ContainsAsPattern = """(?is)\bas\b""".r

  private val LegacyLoadWherePattern =
    ("""(?is)(?:^|;)\s*load\s+""" + DslSource + """\s+where\s+[A-Za-z_][A-Za-z0-9_]*\s*=""").r

  private val LegacySaveWherePattern =
    ("""(?is)(?:^|;)\s*save\s+(?:(?:overwrite|append|errorifexists|ignore)\s+)?""" +
      """[A-Za-z_][A-Za-z0-9_]*\s+as\s+""" + DslSource + """\s+where\b""").r

  private val LegacySetSqlTypePattern =
    """(?is)(?:^|;)\s*set\s+[A-Za-z_][A-Za-z0-9_]*\s*=.*\bwhere\s+type\s*=\s*['"]sql['"]""".r

  private def suggestAlias(path: String): String = {
    val candidate = path.split("\\.").lastOption.getOrElse("loaded_table")
      .replaceAll("[^A-Za-z0-9_]", "_")
      .replaceAll("_+", "_")
      .stripPrefix("_")
      .stripSuffix("_")
    val normalized = if (candidate.nonEmpty) candidate else "loaded_table"
    if (normalized.headOption.exists(_.isDigit)) s"t_$normalized" else normalized
  }

}

object SaveControlOptions {
  val Overwrite: String = "sparkoneOverwrite"
  val OverwriteBackup: String = "sparkoneOverwriteBackup"
  val OverwriteBackupPath: String = "sparkoneOverwriteBackupPath"

  private val Names = Set(Overwrite, OverwriteBackup, OverwriteBackupPath).map(_.toLowerCase)

  def partition(options: Seq[(String, String)]): (Seq[(String, String)], Seq[(String, String)]) = {
    options.partition { case (key, _) => Names.contains(key.toLowerCase) }
  }
}

private[sql] object SparkOneSqlRender {
  def renderCreateTempViewUsing(
      table: String,
      provider: String,
      options: Seq[(String, String)]): String = {
    val renderedOptions = renderOptions(options)
    s"CREATE OR REPLACE TEMPORARY VIEW $table USING $provider OPTIONS ($renderedOptions)"
  }

  def renderCreateTempViewAsSelect(table: String, sourceTable: String): String = {
    s"CREATE OR REPLACE TEMPORARY VIEW $table AS SELECT * FROM $sourceTable"
  }

  def renderCreateTempViewAsQuery(table: String, query: String): String = {
    s"CREATE OR REPLACE TEMPORARY VIEW $table AS $query"
  }

  def renderInsertOverwriteDirectory(
      path: String,
      provider: String,
      options: Seq[(String, String)],
      table: String): String = {
    val optionSql =
      if (options.isEmpty) ""
      else s" OPTIONS (${renderOptions(options)})"
    s"INSERT OVERWRITE DIRECTORY '${escapeSql(path)}' USING $provider$optionSql SELECT * FROM $table"
  }

  def renderInsertTable(
      mode: String,
      targetTable: String,
      sourceTable: String,
      partitionColumns: Seq[String]): String = {
    val command = mode.toLowerCase match {
      case "overwrite" => "INSERT OVERWRITE TABLE"
      case "append" => "INSERT INTO TABLE"
      case other => throw new CompileException(s"SAVE mode '$other' is not supported for catalog table")
    }
    val partitionSql =
      if (partitionColumns.isEmpty) ""
      else s" PARTITION (${partitionColumns.mkString(", ")})"
    s"$command $targetTable$partitionSql SELECT * FROM $sourceTable"
  }

  def renderSparkOneAction(action: String, target: String): String = {
    s"SELECT '${escapeSql(action)}' AS sparkone_action, '${escapeSql(target)}' AS sparkone_target"
  }

  def renderMultipartIdentifier(value: String, label: String): String = {
    val parts = value.split("\\.", -1).toSeq
    if (parts.isEmpty || parts.exists(_.isEmpty)) {
      throw new CompileException(s"$label must be a non-empty multipart identifier: $value")
    }
    parts.map(part => requireIdentifier(part, "catalog identifier part")).mkString(".")
  }

  def requireIdentifier(value: String, label: String): String = {
    if (!value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      throw new CompileException(s"$label must be a simple identifier: $value")
    }
    value
  }

  private def escapeSql(value: String): String = {
    value.replace("'", "''")
  }

  private def renderOptions(options: Seq[(String, String)]): String = {
    options.map { case (key, value) => s"$key '${escapeSql(value)}'" }.mkString(", ")
  }
}

private[sql] object ThrowingErrorListener extends BaseErrorListener {
  override def syntaxError(
      recognizer: Recognizer[_, _],
      offendingSymbol: Any,
      line: Int,
      charPositionInLine: Int,
      msg: String,
      e: RecognitionException): Unit = {
    val source = recognizer match {
      case parser: Parser => Option(parser.getTokenStream).map(_.getSourceName).getOrElse("script")
      case _ => "script"
    }
    throw new CompileException(s"Parse error at $source:$line:$charPositionInLine: $msg", e)
  }
}
