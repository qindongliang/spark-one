package ai.sparkone.sql

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
    val normalized = format.toLowerCase
    if (normalized == "jdbc") {
      throw new CompileException("SparkOne does not support LOAD jdbc. Use LOAD mysql with HOCON datasource config.")
    } else if (normalized == "mysql") {
      val target = mysqlTarget(path, "LOAD")
      val dbtable = filter.map(renderMysqlFilteredDbtable(target.dbtable, _)).getOrElse(target.dbtable)
      MysqlLoadSource(dbtable, mysqlOptions(target.connection, dbtable, options))
    } else if (normalizedCatalogFormats.contains(normalized)) {
      if (filter.nonEmpty) {
        throw new CompileException(s"LOAD source '$format' does not support WHERE filter in the MVP compiler")
      }
      if (options.nonEmpty) {
        throw new CompileException(s"LOAD source '$format' does not support Spark SQL OPTIONS in the MVP compiler")
      }
      CatalogTableSource(SparkOneSqlRender.renderMultipartIdentifier(path, "LOAD catalog table"))
    } else {
      if (filter.nonEmpty) {
        throw new CompileException(s"LOAD source '$format' does not support WHERE filter in the MVP compiler")
      }
      ProviderLoadSource(resolveProvider(format), ("path" -> path) +: options)
    }
  }

  def resolveSave(format: String, path: String, options: Seq[(String, String)]): ResolvedSaveSource = {
    val normalized = format.toLowerCase
    if (normalized == "jdbc") {
      throw new CompileException("SparkOne does not support SAVE jdbc. Use SAVE mysql with HOCON datasource config.")
    } else if (normalized == "mysql") {
      val target = mysqlTarget(path, "SAVE")
      MysqlSaveSource(target.dbtable, mysqlOptions(target.connection, target.dbtable, options))
    } else if (normalizedCatalogFormats.contains(normalized)) {
      CatalogSaveSource(format)
    } else {
      ProviderSaveSource(resolveProvider(format))
    }
  }

  private def resolveProvider(format: String): String = {
    normalizedProviderAliases.getOrElse(format.toLowerCase, format)
  }

  private def mysqlTarget(path: String, statementType: String): MysqlTarget = {
    val split = path.split("\\.", 2)
    if (split.length != 2 || split.exists(_.trim.isEmpty)) {
      throw new CompileException(s"$statementType mysql path must be connection.table, for example mysql.`analytics.users`")
    }
    val connection = SparkOneSqlRender.requireIdentifier(split(0), s"$statementType mysql connection")
    val dbtable = SparkOneSqlRender.renderMultipartIdentifier(split(1), s"$statementType mysql table")
    MysqlTarget(connection, dbtable)
  }

  private def mysqlOptions(
      connection: String,
      dbtable: String,
      statementOptions: Seq[(String, String)]): Seq[(String, String)] = {
    val forbidden = Set("url", "driver", "user", "password", "dbtable", "query")
    statementOptions.find { case (key, _) => forbidden.contains(key.toLowerCase) }.foreach { case (key, _) =>
      throw new CompileException(s"MySQL connection option '$key' must be configured in HOCON, not SQL OPTIONS")
    }

    val base = mysqlConnectionOptions(connection)
    val merged = base ++ statementOptions.map { case (key, value) => key -> value } :+ ("dbtable" -> dbtable)
    merged
  }

  private def renderMysqlFilteredDbtable(dbtable: String, filter: String): String = {
    s"(select * from $dbtable where $filter) as sparkone_mysql_load"
  }

  private def mysqlConnectionOptions(connection: String): Seq[(String, String)] = {
    val prefix = s"sparkone.datasource.mysql.$connection."
    val url = requiredMysqlProperty(connection, "url")
    val driver = sys.props.get(prefix + "driver").map(_.trim).filter(_.nonEmpty).getOrElse("com.mysql.cj.jdbc.Driver")
    val common = Seq(
      "url" -> url,
      "driver" -> driver) ++
      optionalMysqlProperty(connection, "user").map("user" -> _).toSeq ++
      optionalMysqlProperty(connection, "password").map("password" -> _).toSeq

    val extraPrefix = prefix + "option."
    val extras = sys.props.toSeq.collect {
      case (key, value) if key.startsWith(extraPrefix) && value.trim.nonEmpty =>
        key.stripPrefix(extraPrefix) -> value.trim
    }.sortBy(_._1)

    common ++ extras
  }

  private def requiredMysqlProperty(connection: String, key: String): String = {
    optionalMysqlProperty(connection, key).getOrElse {
      throw new CompileException(s"Missing HOCON config datasources.mysql.$connection.$key for MySQL datasource")
    }
  }

  private def optionalMysqlProperty(connection: String, key: String): Option[String] = {
    sys.props.get(s"sparkone.datasource.mysql.$connection.$key").map(_.trim).filter(_.nonEmpty)
  }

}

object DataSourceResolver {
  val DefaultProviderAliases: Map[String, String] = Map(
    "excel" -> "excel")

  val DefaultCatalogFormats: Set[String] = Set("hive")
}

sealed trait ResolvedLoadSource
final case class ProviderLoadSource(provider: String, options: Seq[(String, String)]) extends ResolvedLoadSource
final case class CatalogTableSource(identifier: String) extends ResolvedLoadSource
final case class MysqlLoadSource(dbtable: String, options: Seq[(String, String)]) extends ResolvedLoadSource

sealed trait ResolvedSaveSource
final case class ProviderSaveSource(provider: String) extends ResolvedSaveSource
final case class CatalogSaveSource(format: String) extends ResolvedSaveSource
final case class MysqlSaveSource(dbtable: String, options: Seq[(String, String)]) extends ResolvedSaveSource

private final case class MysqlTarget(connection: String, dbtable: String)
