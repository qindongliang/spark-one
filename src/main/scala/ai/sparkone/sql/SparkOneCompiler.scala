package ai.sparkone.sql

import ai.sparkone.sql.parser.{SparkOneDslLexer, SparkOneDslParser}
import org.antlr.v4.runtime.{BaseErrorListener, CharStreams, CommonTokenStream, Parser, RecognitionException, Recognizer}

import scala.collection.JavaConverters._

final class SparkOneCompiler(sqlValidator: SqlValidator = SqlValidator.Noop) {
  def compile(script: String): Seq[CompiledStatement] = {
    val tree = parse(script)
    tree.statement().asScala.map { statement =>
      val source = originalText(script, statement).trim
      val sql = compileStatement(script, statement)
      sqlValidator.validate(sql)
      CompiledStatement(source, sql)
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

  private def compileStatement(script: String, statement: SparkOneDslParser.StatementContext): String = {
    if (statement.loadStatement() != null) {
      compileLoad(statement.loadStatement())
    } else if (statement.saveStatement() != null) {
      compileSave(statement.saveStatement())
    } else {
      originalText(script, statement).trim
    }
  }

  private def compileLoad(load: SparkOneDslParser.LoadStatementContext): String = {
    val (format, path) = parseSource(load.source(), "LOAD")
    val table = requireIdentifier(load.table.getText, "LOAD target table")
    val options = parseOptions(load.optionClause())
    renderCreateTempView(table, format, ("path" -> path) +: options)
  }

  private def compileSave(save: SparkOneDslParser.SaveStatementContext): String = {
    val mode = Option(save.saveMode()).map(_.getText.toLowerCase).getOrElse("errorifexists")
    if (mode != "overwrite") {
      throw new CompileException(s"SAVE mode '$mode' is not supported by the MVP Spark SQL compiler yet")
    }

    val table = requireIdentifier(save.table.getText, "SAVE source table")
    val (format, path) = parseSource(save.source(), "SAVE")
    val options = parseOptions(save.optionClause())
    renderInsertOverwriteDirectory(path, format, options, table)
  }

  private def parseSource(source: SparkOneDslParser.SourceContext, statementType: String): (String, String) = {
    val format = requireIdentifier(source.format.getText, s"$statementType format")
    val path = stripQuoted(source.path.getText)
    (format, path)
  }

  private def parseOptions(optionClause: SparkOneDslParser.OptionClauseContext): Seq[(String, String)] = {
    Option(optionClause).toSeq.flatMap { clause =>
      clause.option().asScala.map { option =>
        val key = requireIdentifier(option.key.getText, "option key")
        val value = stripQuoted(option.value.getText)
        key -> value
      }
    }
  }

  private def renderCreateTempView(
      table: String,
      format: String,
      options: Seq[(String, String)]): String = {
    val renderedOptions = options.map { case (key, value) => s"$key '${escapeSql(value)}'" }.mkString(", ")
    s"CREATE OR REPLACE TEMPORARY VIEW $table USING $format OPTIONS ($renderedOptions)"
  }

  private def renderInsertOverwriteDirectory(
      path: String,
      format: String,
      options: Seq[(String, String)],
      table: String): String = {
    val optionSql =
      if (options.isEmpty) ""
      else options.map { case (key, value) => s"$key '${escapeSql(value)}'" }.mkString(" OPTIONS (", ", ", ")")
    s"INSERT OVERWRITE DIRECTORY '${escapeSql(path)}' USING $format$optionSql SELECT * FROM $table"
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

  private def requireIdentifier(value: String, label: String): String = {
    if (!value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      throw new CompileException(s"$label must be a simple identifier: $value")
    }
    value
  }

  private def escapeSql(value: String): String = {
    value.replace("'", "''")
  }

  private def originalText(script: String, context: org.antlr.v4.runtime.ParserRuleContext): String = {
    val start = context.getStart.getStartIndex
    val stop = context.getStop.getStopIndex
    if (start < 0 || stop < start) "" else script.substring(start, stop + 1)
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
