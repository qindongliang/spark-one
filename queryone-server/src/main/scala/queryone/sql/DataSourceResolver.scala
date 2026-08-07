package queryone.sql

import queryone.extension.hdfs.ManagedHdfsWorkspacePolicy

import java.nio.charset.StandardCharsets
import java.util.{Base64, Locale}

final class DataSourceResolver(
    providerAliases: Map[String, String] = DataSourceResolver.DefaultProviderAliases,
    catalogFormats: Set[String] = DataSourceResolver.DefaultCatalogFormats) {

  private val normalizedProviderAliases = providerAliases.map { case (key, value) =>
    key.toLowerCase -> value
  }
  private val normalizedCatalogFormats = catalogFormats.map(_.toLowerCase)

  def resolveLoad(
      format: String,
      path: String,
      options: Seq[(String, String)],
      filter: Option[String] = None): ResolvedLoadSource = {
    val normalized = format.toLowerCase(Locale.ROOT)
    if (normalized == "jdbc") {
      val target = jdbcCatalogTarget(path, "LOAD")
      val normalizedOptions = normalizeJdbcLoadOptions(options)
      if (normalizedOptions.isEmpty) {
        val tableExpression = filter.map(condition => s"${target.catalogTable} WHERE $condition").getOrElse(target.catalogTable)
        CatalogTableSource(tableExpression)
      } else {
        val providerOptions = target.providerOptions ++
            filter.map(value => "whereClauseBase64" -> base64(value)).toSeq ++
            normalizedOptions
        ProviderLoadSource("queryone_mysql", providerOptions)
      }
    } else if (normalized == "mysql") {
      throw new CompileException(
        "LOAD mysql has been removed; use LOAD jdbc with alias.table for ODEP or catalog_static.database.table for a static Catalog.")
    } else if (normalized == "doris") {
      if (options.nonEmpty) {
        throw new CompileException("LOAD doris does not support SQL OPTIONS. Configure the ODEP Doris routing Catalog in the selected Spark engine.")
      }
      val identifier = dorisCatalogTable(path, "LOAD")
      val tableExpression = filter.map(condition => s"$identifier WHERE $condition").getOrElse(identifier)
      CatalogTableSource(tableExpression)
    } else if (normalized == "hive") {
      if (options.nonEmpty) {
        throw new CompileException("LOAD hive does not support Spark SQL OPTIONS in the MVP compiler")
      }
      val identifier = hiveCatalogTable(path, "LOAD")
      val tableExpression = filter.map(condition => s"$identifier WHERE $condition").getOrElse(identifier)
      CatalogTableSource(tableExpression)
    } else if (normalizedCatalogFormats.contains(normalized)) {
      if (options.nonEmpty) {
        throw new CompileException(s"LOAD source '$format' does not support Spark SQL OPTIONS in the MVP compiler")
      }
      val identifier = QueryOneSqlRender.renderMultipartIdentifier(path, "LOAD catalog table")
      val tableExpression = filter.map(condition => s"$identifier WHERE $condition").getOrElse(identifier)
      CatalogTableSource(tableExpression)
    } else if (ManagedHdfsWorkspacePolicy.ReadFormats.contains(normalized)) {
      if (filter.nonEmpty) {
        throw new CompileException(s"LOAD source '$format' does not support WHERE filter in the MVP compiler")
      }
      if (!ManagedHdfsWorkspacePolicy.isManagedRelativePath(path)) {
        throw new CompileException(
          s"LOAD managed HDFS requires a relative tenant workspace path: $path")
      }
      val ownerOptions = options.filter { case (key, _) => key.equalsIgnoreCase("owner") }
      if (ownerOptions.size > 1) {
        throw new CompileException("LOAD managed HDFS option 'owner' must be specified only once")
      }
      val workspaceOwner = ownerOptions.headOption.map(_._2.trim)
      workspaceOwner.foreach { owner =>
        if (!ManagedHdfsWorkspacePolicy.isValidWorkspaceOwner(owner)) {
          throw new CompileException("LOAD managed HDFS option 'owner' is invalid")
        }
      }
      val providerOptions = options.filterNot { case (key, _) => key.equalsIgnoreCase("owner") }
      providerOptions.foreach { case (key, _) =>
        if (!ManagedHdfsWorkspacePolicy.isAllowedOption(key)) {
          throw new CompileException(s"LOAD managed HDFS option is not allowed: $key")
        }
      }
      ManagedHdfsLoadSource(resolveProvider(format), path, providerOptions, workspaceOwner)
    } else {
      if (filter.nonEmpty) {
        throw new CompileException(s"LOAD source '$format' does not support WHERE filter in the MVP compiler")
      }
      throw new CompileException(
        s"LOAD provider '$format' is not supported; use a managed file provider or a configured catalog source")
    }
  }

  def resolveSave(format: String, path: String, options: Seq[(String, String)]): ResolvedSaveSource = {
    val normalized = format.toLowerCase
    if (normalized == "jdbc") {
      val target = jdbcCatalogTarget(path, "SAVE")
      if (target.odepRouted) {
        throw new CompileException(
          "SAVE jdbc does not support ODEP alias targets; use catalog_static.database.table for a static JDBC Catalog.")
      }
      if (options.nonEmpty) {
        throw new CompileException(
          "SAVE jdbc does not support SQL OPTIONS; configure write options in the static Spark JDBC Catalog.")
      }
      CatalogSaveSource(target.catalogTable, SaveTargetType.JdbcCatalog, supportsPartitionBy = false)
    } else if (normalized == "mysql") {
      throw new CompileException(
        "SAVE mysql has been removed; use SAVE jdbc with catalog_static.database.table.")
    } else if (normalized == "doris") {
      if (options.nonEmpty) {
        throw new CompileException("SAVE doris does not support SQL OPTIONS. Configure the ODEP Doris routing Catalog in the selected Spark engine.")
      }
      val identifier = dorisCatalogTable(path, "SAVE")
      CatalogSaveSource(identifier, SaveTargetType.DorisCatalog, supportsPartitionBy = false)
    } else if (normalized == "hive") {
      val identifier = hiveCatalogTable(path, "SAVE")
      CatalogSaveSource(identifier, SaveTargetType.Catalog, supportsPartitionBy = true)
    } else if (normalizedCatalogFormats.contains(normalized)) {
      val identifier = QueryOneSqlRender.renderMultipartIdentifier(path, "SAVE catalog table")
      CatalogSaveSource(identifier, SaveTargetType.Catalog, supportsPartitionBy = true)
    } else {
      ProviderSaveSource(resolveProvider(format))
    }
  }

  private def resolveProvider(format: String): String = {
    normalizedProviderAliases.getOrElse(format.toLowerCase, format)
  }

  private def dorisCatalogTable(path: String, statementType: String): String = {
    val parts = path.split("\\.", -1)
    val identifier = parts.length match {
      case 2 => s"doris.$path"
      case 3 if parts.head.equalsIgnoreCase("doris") ||
          parts.head.toLowerCase(Locale.ROOT).startsWith("doris_") => path
      case _ =>
        throw new CompileException(
          s"$statementType doris path must be database.table or doris_<instance>.database.table")
    }
    QueryOneSqlRender.renderMultipartIdentifier(identifier, s"$statementType doris table")
  }

  private def jdbcCatalogTarget(path: String, statementType: String): JdbcCatalogTarget = {
    val parts = path.split("\\.", -1)
    parts.length match {
      case 2 =>
        val alias = QueryOneSqlRender.requireIdentifier(parts(0), s"$statementType jdbc alias")
        val table = QueryOneSqlRender.requireIdentifier(parts(1), s"$statementType jdbc table")
        JdbcCatalogTarget(
          catalog = "jdbc",
          namespace = alias,
          table = table,
          odepRouted = true)
      case 3 =>
        val catalog = QueryOneSqlRender.requireIdentifier(parts(0), s"$statementType jdbc static catalog")
        if (!catalog.toLowerCase(Locale.ROOT).endsWith("_static")) {
          throw new CompileException(
            s"$statementType jdbc three-part path must use a catalog name ending in _static: $path")
        }
        val database = QueryOneSqlRender.requireIdentifier(parts(1), s"$statementType jdbc database")
        val table = QueryOneSqlRender.requireIdentifier(parts(2), s"$statementType jdbc table")
        JdbcCatalogTarget(
          catalog = catalog,
          namespace = database,
          table = table,
          odepRouted = false)
      case _ =>
        throw new CompileException(
          s"$statementType jdbc path must be alias.table or catalog_static.database.table")
    }
  }

  private def hiveCatalogTable(path: String, statementType: String): String = {
    if (path.split("\\.", -1).length != 2) {
      throw new CompileException(s"$statementType hive path must be database.table")
    }
    QueryOneSqlRender.renderMultipartIdentifier(
      s"${HiveCatalogAliasRewriter.SessionCatalog}.$path",
      s"$statementType hive table")
  }

  private def base64(value: String): String = {
    Base64.getEncoder.encodeToString(value.getBytes(StandardCharsets.UTF_8))
  }

  private def normalizeJdbcLoadOptions(options: Seq[(String, String)]): Seq[(String, String)] = {
    val canonical = Map(
      "partitioncolumn" -> "partitionColumn",
      "lowerbound" -> "lowerBound",
      "upperbound" -> "upperBound",
      "numpartitions" -> "numPartitions",
      "fetchsize" -> "fetchsize")
    val forbidden = Set("url", "driver", "user", "password", "dbtable", "query")
    val normalized = options.map { case (key, value) =>
      val lower = key.toLowerCase
      if (forbidden.contains(lower)) {
        throw new CompileException(s"JDBC load option '$key' is not allowed; credentials and connection targets must stay in trusted engine config.")
      }
      val canonicalKey = canonical.getOrElse(lower,
        throw new CompileException(
          s"JDBC load option '$key' is not supported. Allowed options: partitionColumn, lowerBound, upperBound, numPartitions, fetchsize."))
      val trimmed = value.trim
      if (trimmed.isEmpty || trimmed.contains(";")) {
        throw new CompileException(s"JDBC load option '$key' must be non-empty and must not contain semicolons.")
      }
      canonicalKey -> trimmed
    }
    val byKey = normalized.groupBy(_._1)
    byKey.collectFirst { case (key, values) if values.size > 1 => key }.foreach { key =>
      throw new CompileException(s"JDBC load option '$key' must be specified only once.")
    }
    val optionMap = normalized.toMap
    val hasPartitionColumn = optionMap.contains("partitionColumn")
    val hasLowerBound = optionMap.contains("lowerBound")
    val hasUpperBound = optionMap.contains("upperBound")
    if (hasLowerBound != hasUpperBound) {
      throw new CompileException("JDBC partition load requires lowerBound and upperBound together.")
    }
    if ((hasLowerBound || optionMap.contains("numPartitions")) && !hasPartitionColumn) {
      throw new CompileException("JDBC partition load requires partitionColumn when lowerBound, upperBound, or numPartitions is specified.")
    }
    optionMap.get("partitionColumn").foreach { value =>
      QueryOneSqlRender.requireIdentifier(value, "JDBC partitionColumn")
    }
    val withDefaultNumPartitions =
      if (hasPartitionColumn && !optionMap.contains("numPartitions")) {
        normalized :+ ("numPartitions" -> "10")
      } else {
        normalized
      }
    val withDefaultFetchSize =
      if (hasPartitionColumn && !optionMap.contains("fetchsize")) {
        withDefaultNumPartitions :+ ("fetchsize" -> "10000")
      } else {
        withDefaultNumPartitions
      }
    withDefaultFetchSize.toMap.get("numPartitions").foreach { value =>
      positiveInt(value, "numPartitions")
    }
    withDefaultFetchSize.toMap.get("fetchsize").foreach(value => positiveInt(value, "fetchsize"))
    withDefaultFetchSize
  }

  private def positiveInt(value: String, label: String): Int = {
    try {
      val parsed = value.toInt
      if (parsed <= 0) {
        throw new NumberFormatException(label)
      }
      parsed
    } catch {
      case _: NumberFormatException =>
        throw new CompileException(s"JDBC load option '$label' must be a positive integer: $value")
    }
  }

}

object DataSourceResolver {
  val DefaultProviderAliases: Map[String, String] = Map(
    "excel" -> "excel")

  val DefaultCatalogFormats: Set[String] = Set("hive")
}

sealed trait ResolvedLoadSource
final case class ManagedHdfsLoadSource(
    provider: String,
    relativePath: String,
    options: Seq[(String, String)],
    workspaceOwner: Option[String]) extends ResolvedLoadSource
final case class ProviderLoadSource(provider: String, options: Seq[(String, String)]) extends ResolvedLoadSource
final case class CatalogTableSource(identifier: String) extends ResolvedLoadSource

sealed trait ResolvedSaveSource
final case class ProviderSaveSource(provider: String) extends ResolvedSaveSource
final case class CatalogSaveSource(identifier: String, targetType: SaveTargetType, supportsPartitionBy: Boolean)
  extends ResolvedSaveSource

private final case class JdbcCatalogTarget(
    catalog: String,
    namespace: String,
    table: String,
    odepRouted: Boolean) {

  val catalogTable: String = s"$catalog.$namespace.$table"

  val providerOptions: Seq[(String, String)] =
    if (odepRouted) {
      Seq("catalog" -> catalog, "alias" -> namespace, "dbtable" -> table)
    } else {
      Seq("catalog" -> catalog, "dbtable" -> s"$namespace.$table")
    }
}
