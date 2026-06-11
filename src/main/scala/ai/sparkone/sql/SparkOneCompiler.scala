package ai.sparkone.sql

import ai.sparkone.sql.parser.{SparkOneDslLexer, SparkOneDslParser}
import org.antlr.v4.runtime.{BaseErrorListener, CharStreams, CommonTokenStream, Parser, RecognitionException, Recognizer}

import scala.collection.JavaConverters._

final class SparkOneCompiler(
    sqlValidator: SqlValidator = SqlValidator.Noop,
    dataSourceResolver: DataSourceResolver = new DataSourceResolver()) {
  def compile(script: String): Seq[CompiledStatement] = {
    rejectLegacyDslWhere(script)
    val tree = parse(script)
    tree.statement().asScala.map { statement =>
      val source = originalText(script, statement).trim
      val compiled = compileStatement(script, statement)
      sqlValidator.validate(compiled.sql)
      CompiledStatement(source, compiled.sql, compiled.save)
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

  private def compileStatement(script: String, statement: SparkOneDslParser.StatementContext): CompileResult = {
    if (statement.loadStatement() != null) {
      CompileResult(compileLoad(statement.loadStatement()))
    } else if (statement.saveStatement() != null) {
      compileSave(statement.saveStatement())
    } else if (statement.viewStatement() != null) {
      CompileResult(compileView(script, statement.viewStatement()))
    } else {
      val sql = originalText(script, statement).trim
      CompileResult(sql)
    }
  }

  private def rejectLegacyDslWhere(sql: String): Unit = {
    if (SparkOneCompiler.LegacyLoadWherePattern.findFirstIn(sql).nonEmpty ||
        SparkOneCompiler.LegacySaveWherePattern.findFirstIn(sql).nonEmpty) {
      throw new CompileException("SparkOne DSL options must use OPTIONS, not WHERE.")
    }
  }

  private def compileLoad(load: SparkOneDslParser.LoadStatementContext): String = {
    val (format, path) = parseSource(load.source(), "LOAD")
    val table = SparkOneSqlRender.requireIdentifier(load.table.getText, "LOAD target table")
    val options = parseOptions(load.optionClause())
    dataSourceResolver.resolveLoad(format, path, options) match {
      case ProviderLoadSource(provider, providerOptions) =>
        SparkOneSqlRender.renderCreateTempViewUsing(table, provider, providerOptions)
      case CatalogTableSource(identifier) =>
        SparkOneSqlRender.renderCreateTempViewAsSelect(table, identifier)
    }
  }

  private def compileSave(save: SparkOneDslParser.SaveStatementContext): CompileResult = {
    val mode = Option(save.saveMode()).map(_.getText.toLowerCase).getOrElse("errorifexists")
    val table = SparkOneSqlRender.requireIdentifier(save.table.getText, "SAVE source table")
    val (format, path) = parseSource(save.source(), "SAVE")
    val options = parseOptions(save.optionClause())
    val partitionColumns = parsePartitionColumns(save.partitionClause())
    val (runtimeOptions, providerOptions) = SaveControlOptions.partition(options)
    val runtimeOptionMap = runtimeOptions.map { case (key, value) => key.toLowerCase -> value }.toMap

    dataSourceResolver.resolveSave(format) match {
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
      case CatalogSaveSource(_) =>
        if (mode != "overwrite" && mode != "append") {
          throw new CompileException(s"SAVE mode '$mode' is not supported for catalog source '$format'")
        }
        if (providerOptions.nonEmpty) {
          throw new CompileException(s"SAVE to catalog source '$format' does not support provider OPTIONS yet")
        }
        val targetTable = SparkOneSqlRender.renderMultipartIdentifier(path, "SAVE catalog table")
        CompileResult(
          SparkOneSqlRender.renderInsertTable(mode, targetTable, table, partitionColumns),
          Some(SaveStatementMetadata(mode, table, format, targetTable, runtimeOptionMap, SaveTargetType.Catalog)))
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

private final case class CompileResult(sql: String, save: Option[SaveStatementMetadata] = None)

private object SparkOneCompiler {
  private val DslSource = """[A-Za-z_][A-Za-z0-9_]*\s*\.\s*`(?:``|[^`])*`"""

  private val LegacyLoadWherePattern =
    ("""(?is)(?:^|;)\s*load\s+""" + DslSource + """\s+where\b""").r

  private val LegacySaveWherePattern =
    ("""(?is)(?:^|;)\s*save\s+(?:(?:overwrite|append|errorifexists|ignore)\s+)?""" +
      """[A-Za-z_][A-Za-z0-9_]*\s+as\s+""" + DslSource + """\s+where\b""").r
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
