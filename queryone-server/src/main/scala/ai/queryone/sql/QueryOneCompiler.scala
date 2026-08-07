package ai.queryone.sql

import ai.queryone.extension.overwrite.{ManagedHdfsLoadProtocol, ManagedHdfsLoadRequest}
import ai.queryone.identity.TenantContext
import ai.queryone.sql.parser.{QueryOneDslLexer, QueryOneDslParser}
import org.antlr.v4.runtime.{BaseErrorListener, CharStreams, CommonTokenStream, Parser, RecognitionException, Recognizer}

import scala.collection.JavaConverters._
import scala.collection.mutable

final class QueryOneCompiler(
    sqlValidator: SqlValidator = SqlValidator.Noop,
    dataSourceResolver: DataSourceResolver = new DataSourceResolver(),
    writePlanner: WritePlanner = new WritePlanner,
    statementPolicy: StatementPolicy = new StatementPolicy,
    hiveCatalogAliasRewriter: HiveCatalogAliasRewriter = new HiveCatalogAliasRewriter) {
  def compile(script: String): Seq[CompiledStatement] = {
    compile(QueryOneCompiler.CompilerTenant, script)
  }

  def compile(tenant: TenantContext, script: String): Seq[CompiledStatement] = {
    compileWithVariables(tenant, script, Map.empty, allowUnresolvedVariables = true)
  }

  def compileStatementWithVariables(script: String, variables: Map[String, String]): CompiledStatement = {
    compileStatementWithVariables(QueryOneCompiler.CompilerTenant, script, variables)
  }

  def compileStatementWithVariables(
      tenant: TenantContext,
      script: String,
      variables: Map[String, String]): CompiledStatement = {
    val compiled = compileWithVariables(tenant, script, variables, allowUnresolvedVariables = false)
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
      tenant: TenantContext,
      script: String,
      initialVariables: Map[String, String],
      allowUnresolvedVariables: Boolean): Seq[CompiledStatement] = {
    val variables = mutable.LinkedHashMap[String, String]() ++ initialVariables
    splitStatements(script).map { source =>
      val renderedSource = QueryOneVariableSubstitutor.render(
        source,
        variables.toMap,
        allowUnresolved = allowUnresolvedVariables)
      val tree = parse(renderedSource)
      val statements = tree.statement().asScala
      if (statements.size != 1) {
        throw new CompileException(s"Expected exactly one statement after variable substitution, got ${statements.size}")
      }
      val compiled = compileParsedStatement(tenant, renderedSource, statements.head)
      sqlValidator.validate(compiled.sql)
      val statement = CompiledStatement(
        renderedSource,
        compiled.sql,
        compiled.load,
        compiled.writePlan,
        compiled.set,
        compiled.intent,
        compiled.assertion)
      statementPolicy.validate(statement)
      compiled.set.foreach { metadata =>
        if (metadata.valueType == SetValueType.Literal) {
          variables(metadata.key) = metadata.value
        }
      }
      statement
    }
  }

  private def parse(script: String): QueryOneDslParser.ScriptContext = {
    val lexer = new QueryOneDslLexer(CharStreams.fromString(script))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ThrowingErrorListener)

    val parser = new QueryOneDslParser(new CommonTokenStream(lexer))
    parser.removeErrorListeners()
    parser.addErrorListener(ThrowingErrorListener)
    parser.script()
  }

  private def compileParsedStatement(
      tenant: TenantContext,
      script: String,
      statement: QueryOneDslParser.StatementContext): CompileResult = {
    if (statement.loadStatement() != null) {
      compileLoad(tenant, statement.loadStatement())
    } else if (statement.saveStatement() != null) {
      compileSave(tenant, statement.saveStatement())
    } else if (statement.setStatement() != null) {
      compileSet(script, statement.setStatement())
    } else if (statement.viewStatement() != null) {
      CompileResult(compileView(script, statement.viewStatement()), intent = StatementIntent.View)
    } else if (statement.assertStatement() != null) {
      compileAssert(script, statement.assertStatement())
    } else {
      val sql = originalText(script, statement).trim
      rejectMalformedQueryOneDsl(sql)
      CompileResult(hiveCatalogAliasRewriter.rewrite(sql))
    }
  }

  private def compileSet(script: String, set: QueryOneDslParser.SetStatementContext): CompileResult = {
    val key = QueryOneSqlRender.requireIdentifier(set.key.getText, "SET key")
    val valueType =
      if (set.query != null) SetValueType.Sql else SetValueType.Literal
    val value =
      if (set.query != null) hiveCatalogAliasRewriter.rewrite(originalText(script, set.query).trim)
      else stripQuoted(set.value.getText).trim
    if (value.isEmpty) {
      throw new CompileException("SET value must not be empty")
    }
    CompileResult(
      QueryOneSqlRender.renderQueryOneAction("SET", key),
      set = Some(SetStatementMetadata(key, value, valueType)),
      intent = StatementIntent.SetVariable)
  }

  private def compileAssert(
      script: String,
      assertion: QueryOneDslParser.AssertStatementContext): CompileResult = {
    val source =
      if (assertion.table != null) {
        AssertionSource.Table(
          QueryOneSqlRender.requireIdentifier(assertion.table.getText, "ASSERT result table"))
      } else {
        val query = hiveCatalogAliasRewriter.rewrite(originalText(script, assertion.query).trim)
        AssertionSource.InlineQuery(query)
      }
    val predicate = stripQuoted(assertion.predicate.getText).trim
    val message = stripQuoted(assertion.message.getText).trim
    if (predicate.isEmpty) {
      throw new CompileException("ASSERT predicate must not be empty")
    }
    if (predicate.contains(";")) {
      throw new CompileException("ASSERT predicate must not contain semicolons")
    }
    if (message.isEmpty) {
      throw new CompileException("ASSERT message must not be empty")
    }
    val failureAction = Option(assertion.failureAction).map(_.getText.toLowerCase) match {
      case None | Some(AssertionFailureAction.Fail.name) => AssertionFailureAction.Fail
      case Some(AssertionFailureAction.Stop.name) => AssertionFailureAction.Stop
      case Some(other) => throw new CompileException(s"Unsupported ASSERT failure action: $other")
    }
    val plan = AssertionPlan(source, predicate, message, failureAction)
    CompileResult(
      QueryOneSqlRender.renderAssertionFailures(plan),
      assertion = Some(plan),
      intent = StatementIntent.Assert)
  }

  private def rejectLegacyDslWhere(sql: String): Unit = {
    if (QueryOneCompiler.LegacyLoadWherePattern.findFirstIn(sql).nonEmpty ||
        QueryOneCompiler.LegacySaveWherePattern.findFirstIn(sql).nonEmpty) {
      throw new CompileException("QueryOne DSL options must use OPTIONS, not WHERE.")
    }
    if (QueryOneCompiler.LegacySetSqlTypePattern.findFirstIn(sql).nonEmpty) {
      throw new CompileException("QueryOne SQL variables must use `set name as select ...`, not `where type=\"sql\"`.")
    }
  }

  private def rejectMalformedQueryOneDsl(sql: String): Unit = {
    sql match {
      case QueryOneCompiler.LoadWithoutAsPattern(format, rawPath, rest)
          if QueryOneCompiler.ContainsAsPattern.findFirstIn(rest).isEmpty =>
        val path = rawPath.replace("``", "`")
        val alias = QueryOneCompiler.suggestAlias(path)
        throw new CompileException(
          s"QueryOne LOAD requires a target temp view: load $format.`$path` as $alias. " +
            s"Add `as $alias` to the statement.")
      case _ =>
    }
  }

  private def compileLoad(
      tenant: TenantContext,
      load: QueryOneDslParser.LoadStatementContext): CompileResult = {
    val (format, path) = parseSource(load.source(), "LOAD")
    val table = QueryOneSqlRender.requireIdentifier(load.table.getText, "LOAD target table")
    val options = parseOptions(load.optionClause())
    val filter = parseLoadFilter(load.whereClause())
    dataSourceResolver.resolveLoad(format, path, options, filter) match {
      case ManagedHdfsLoadSource(provider, relativePath, providerOptions, workspaceOwner) =>
        CompileResult(
          ManagedHdfsLoadProtocol.render(ManagedHdfsLoadRequest(
            workspaceOwner.getOrElse(tenant.username),
            table,
            provider,
            relativePath,
            providerOptions.toMap)),
          load = Some(LoadStatementMetadata(
            table,
            format,
            relativePath,
            providerOptions.toMap,
            LoadTargetType.ManagedHdfs)),
          intent = StatementIntent.Load)
      case ProviderLoadSource(provider, providerOptions) =>
        CompileResult(
          QueryOneSqlRender.renderCreateTempViewUsing(table, provider, providerOptions),
          load = Some(LoadStatementMetadata(table, format, path, providerOptions.toMap)),
          intent = StatementIntent.Load)
      case CatalogTableSource(identifier) =>
        CompileResult(
          QueryOneSqlRender.renderCreateTempViewAsSelect(table, identifier),
          load = Some(LoadStatementMetadata(table, format, identifier, Map.empty)),
          intent = StatementIntent.Load)
    }
  }

  private def compileSave(tenant: TenantContext, save: QueryOneDslParser.SaveStatementContext): CompileResult = {
    val mode = Option(save.saveMode()).map(_.getText.toLowerCase).getOrElse("errorifexists")
    val table = QueryOneSqlRender.requireIdentifier(save.table.getText, "SAVE source table")
    val (format, path) = parseSource(save.source(), "SAVE")
    val options = parseOptions(save.optionClause())
    val partitionColumns = parsePartitionColumns(save.partitionClause())
    val resolvedSource = dataSourceResolver.resolveSave(format, path, options)
    val plan = writePlanner.plan(
      tenant,
      mode,
      table,
      format,
      path,
      options,
      partitionColumns,
      resolvedSource)
    CompileResult(
      WriteSqlRenderer.render(plan),
      writePlan = Some(plan),
      intent = StatementIntent.Save)
  }

  private def compileView(script: String, view: QueryOneDslParser.ViewStatementContext): String = {
    val table = QueryOneSqlRender.requireIdentifier(view.table.getText, "VIEW target table")
    val query = hiveCatalogAliasRewriter.rewrite(originalText(script, view.sqlStatement()).trim)
    QueryOneSqlRender.renderCreateTempViewAsQuery(table, query)
  }

  private def parseSource(source: QueryOneDslParser.SourceContext, statementType: String): (String, String) = {
    val format = QueryOneSqlRender.requireIdentifier(source.format.getText, s"$statementType format")
    val path = stripQuoted(source.path.getText)
    (format, path)
  }

  private def parseOptions(optionClause: QueryOneDslParser.OptionClauseContext): Seq[(String, String)] = {
    Option(optionClause).toSeq.flatMap { clause =>
      clause.option().asScala.map { option =>
        val key = QueryOneSqlRender.requireIdentifier(option.key.getText, "option key")
        val value = stripQuoted(option.value.getText)
        key -> value
      }
    }
  }

  private def parseLoadFilter(whereClause: QueryOneDslParser.WhereClauseContext): Option[String] = {
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

  private def parsePartitionColumns(partitionClause: QueryOneDslParser.PartitionClauseContext): Seq[String] = {
    Option(partitionClause).toSeq.flatMap { clause =>
      clause.identifier().asScala.map { identifier =>
        QueryOneSqlRender.requireIdentifier(identifier.getText, "partition column")
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
    writePlan: Option[WritePlan] = None,
    load: Option[LoadStatementMetadata] = None,
    set: Option[SetStatementMetadata] = None,
    assertion: Option[AssertionPlan] = None,
    intent: StatementIntent = StatementIntent.NativeSql)

private object QueryOneCompiler {
  private val CompilerTenant = TenantContext.development("compiler")
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

private[sql] object QueryOneSqlRender {
  def renderAssertionFailures(plan: AssertionPlan): String = {
    val sourceSql = plan.source match {
      case AssertionSource.Table(name) => name
      case AssertionSource.InlineQuery(sql) => s"($sql) queryone_assert_input"
    }
    s"SELECT * FROM $sourceSql WHERE NOT COALESCE((${plan.predicate}), FALSE)"
  }

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

  def renderInsertTable(
      mode: String,
      targetTable: String,
      sourceTable: String,
      partitionColumns: Seq[String],
      targetColumns: Seq[String],
      sourceColumns: Seq[String]): String = {
    val command = mode.toLowerCase match {
      case "overwrite" => "INSERT OVERWRITE TABLE"
      case "append" => "INSERT INTO TABLE"
      case other => throw new CompileException(s"SAVE mode '$other' is not supported for catalog table")
    }
    val partitionSql =
      if (partitionColumns.isEmpty) ""
      else s" PARTITION (${partitionColumns.map(quoteIdentifier).mkString(", ")})"
    val targetColumnSql = targetColumns.map(quoteIdentifier).mkString(", ")
    val sourceColumnSql = sourceColumns.map(quoteIdentifier).mkString(", ")
    s"$command $targetTable$partitionSql ($targetColumnSql) SELECT $sourceColumnSql FROM $sourceTable"
  }

  def renderQueryOneAction(action: String, target: String): String = {
    s"SELECT '${escapeSql(action)}' AS queryone_action, '${escapeSql(target)}' AS queryone_target"
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

  private def quoteIdentifier(value: String): String = {
    s"`${value.replace("`", "``")}`"
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
