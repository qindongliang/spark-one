package ai.sparkone.sql

import java.nio.charset.StandardCharsets
import java.util.Base64

final class DataSourceResolver(
    providerAliases: Map[String, String] = DataSourceResolver.DefaultProviderAliases,
    catalogFormats: Set[String] = DataSourceResolver.DefaultCatalogFormats,
    mysqlLoadMode: MysqlLoadMode = MysqlLoadMode.LocalAdapter,
    mysqlLoadProfiles: Map[String, MysqlLoadProfile] = Map.empty) {

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
      throw new CompileException("SparkOne does not support LOAD jdbc. Use LOAD mysql with the selected local engine's datasources.mysql config.")
    } else if (normalized == "mysql") {
      mysqlLoadMode match {
        case MysqlLoadMode.LocalAdapter =>
          val target = mysqlTarget(path, "LOAD")
          val dbtable = filter.map(renderMysqlFilteredDbtable(target.dbtable, _)).getOrElse(target.dbtable)
          MysqlLoadSource(dbtable, mysqlOptions(target.connection, dbtable, options))
        case MysqlLoadMode.KyuubiProfile =>
          resolveKyuubiMysqlLoad(path, options, filter)
      }
    } else if (normalized == "doris") {
      if (options.nonEmpty) {
        throw new CompileException("LOAD doris does not support SQL OPTIONS. Configure Spark Doris Catalog in the selected local engine's catalogs.doris or Kyuubi/Spark engine config.")
      }
      val identifier = SparkOneSqlRender.renderMultipartIdentifier(s"doris.$path", "LOAD doris table")
      val tableExpression = filter.map(condition => s"$identifier WHERE $condition").getOrElse(identifier)
      CatalogTableSource(tableExpression)
    } else if (normalizedCatalogFormats.contains(normalized)) {
      if (options.nonEmpty) {
        throw new CompileException(s"LOAD source '$format' does not support Spark SQL OPTIONS in the MVP compiler")
      }
      val identifier = SparkOneSqlRender.renderMultipartIdentifier(path, "LOAD catalog table")
      val tableExpression = filter.map(condition => s"$identifier WHERE $condition").getOrElse(identifier)
      CatalogTableSource(tableExpression)
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
      throw new CompileException("SparkOne does not support SAVE jdbc. Use SAVE mysql with the selected local engine's datasources.mysql config.")
    } else if (normalized == "mysql") {
      if (mysqlLoadMode == MysqlLoadMode.KyuubiProfile) {
        throw new CompileException("Kyuubi engine does not support SparkOne save mysql adapter; use remote catalog SQL or a Kyuubi/Spark-side write path.")
      }
      val target = mysqlTarget(path, "SAVE")
      MysqlSaveSource(target.dbtable, mysqlOptions(target.connection, target.dbtable, options))
    } else if (normalized == "doris") {
      if (options.nonEmpty) {
        throw new CompileException("SAVE doris does not support SQL OPTIONS. Configure Spark Doris Catalog in the selected local engine's catalogs.doris or Kyuubi/Spark engine config.")
      }
      val identifier = SparkOneSqlRender.renderMultipartIdentifier(s"doris.$path", "SAVE doris table")
      CatalogSaveSource(identifier, SaveTargetType.DorisCatalog, supportsPartitionBy = false)
    } else if (normalizedCatalogFormats.contains(normalized)) {
      val identifier = SparkOneSqlRender.renderMultipartIdentifier(path, "SAVE catalog table")
      CatalogSaveSource(identifier, SaveTargetType.Catalog, supportsPartitionBy = true)
    } else {
      ProviderSaveSource(resolveProvider(format))
    }
  }

  private def resolveProvider(format: String): String = {
    normalizedProviderAliases.getOrElse(format.toLowerCase, format)
  }

  private def resolveKyuubiMysqlLoad(
      path: String,
      options: Seq[(String, String)],
      filter: Option[String]): ResolvedLoadSource = {
    parseKyuubiMysqlCatalogPath(path).foreach { target =>
      val normalizedOptions = normalizeKyuubiMysqlLoadOptions(options, None)
      if (normalizedOptions.isEmpty) {
        val source = filter.map(condition => s"${target.catalogTable} WHERE $condition").getOrElse(target.catalogTable)
        return CatalogTableSource(source)
      }
      val providerOptions =
        Seq(
          "catalog" -> target.catalog,
          "dbtable" -> target.dbtable) ++
          filter.map(value => "whereClauseBase64" -> base64(value)).toSeq ++
          normalizedOptions
      return ProviderLoadSource("sparkone_mysql", providerOptions)
    }

    val target = mysqlTarget(path, "LOAD")
    val profile = mysqlLoadProfiles.getOrElse(target.connection,
      throw new CompileException(
        s"Kyuubi MySQL load profile '${target.connection}' is not configured. " +
          s"Use mysql.`catalog.db.table` to reuse Kyuubi/Spark catalog config, or configure " +
          s"engines.<kyuubi>.mysqlLoadProfiles.${target.connection}."))
    profile.validateTable(target.dbtable)
    val normalizedOptions = normalizeKyuubiMysqlLoadOptions(options, Some(profile))
    profile.strategy match {
      case MysqlLoadProfileStrategy.Catalog =>
        if (normalizedOptions.nonEmpty) {
          throw new CompileException(
            s"Kyuubi MySQL load profile '${profile.name}' uses catalog strategy, which does not support per-load JDBC partition options. " +
              "Use a provider strategy profile when this load needs partitionColumn/lowerBound/upperBound/numPartitions.")
        }
        val table = profile.renderCatalogTable(target.dbtable)
        val source = filter.map(condition => s"$table WHERE $condition").getOrElse(table)
        CatalogTableSource(source)
      case MysqlLoadProfileStrategy.Provider =>
        val providerOptions =
          Seq(
            "profile" -> profile.remoteProfile,
            "dbtable" -> profile.renderRemoteTable(target.dbtable)) ++
            filter.map(value => "whereClauseBase64" -> base64(value)).toSeq ++
            withDefaultFetchSize(normalizedOptions, profile)
        ProviderLoadSource(profile.provider, providerOptions)
    }
  }

  private def base64(value: String): String = {
    Base64.getEncoder.encodeToString(value.getBytes(StandardCharsets.UTF_8))
  }

  private def normalizeKyuubiMysqlLoadOptions(
      options: Seq[(String, String)],
      profile: Option[MysqlLoadProfile]): Seq[(String, String)] = {
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
        throw new CompileException(s"Kyuubi MySQL load option '$key' is not allowed; credentials and connection targets must stay in Kyuubi/Spark engine config.")
      }
      val canonicalKey = canonical.getOrElse(lower,
        throw new CompileException(
          s"Kyuubi MySQL load option '$key' is not supported. Allowed options: partitionColumn, lowerBound, upperBound, numPartitions, fetchsize."))
      val trimmed = value.trim
      if (trimmed.isEmpty || trimmed.contains(";")) {
        throw new CompileException(s"Kyuubi MySQL load option '$key' must be non-empty and must not contain semicolons.")
      }
      canonicalKey -> trimmed
    }
    val byKey = normalized.groupBy(_._1)
    byKey.collectFirst { case (key, values) if values.size > 1 => key }.foreach { key =>
      throw new CompileException(s"Kyuubi MySQL load option '$key' must be specified only once.")
    }
    val optionMap = normalized.toMap
    val hasPartitionColumn = optionMap.contains("partitionColumn")
    val hasLowerBound = optionMap.contains("lowerBound")
    val hasUpperBound = optionMap.contains("upperBound")
    if (hasLowerBound != hasUpperBound) {
      throw new CompileException("Kyuubi MySQL partition load requires lowerBound and upperBound together.")
    }
    if ((hasLowerBound || optionMap.contains("numPartitions")) && !hasPartitionColumn) {
      throw new CompileException("Kyuubi MySQL partition load requires partitionColumn when lowerBound, upperBound, or numPartitions is specified.")
    }
    optionMap.get("partitionColumn").foreach { value =>
      SparkOneSqlRender.requireIdentifier(value, "Kyuubi MySQL partitionColumn")
    }
    val withDefaultNumPartitions =
      if (hasPartitionColumn && !optionMap.contains("numPartitions")) {
        normalized :+ ("numPartitions" -> defaultNumPartitions(profile))
      } else {
        normalized
      }
    val withDefaultFetchSize =
      if (hasPartitionColumn && !optionMap.contains("fetchsize")) {
        withDefaultNumPartitions :+ ("fetchsize" -> profile.flatMap(_.defaultFetchSize).getOrElse("10000"))
      } else {
        withDefaultNumPartitions
      }
    withDefaultFetchSize.toMap.get("numPartitions").foreach { value =>
      val count = positiveInt(value, "numPartitions")
      profile.flatMap(_.maxNumPartitions).foreach { max =>
        if (count > max) {
          throw new CompileException(
            s"Kyuubi MySQL numPartitions=$count exceeds profile '${profile.get.name}' maxNumPartitions=$max.")
        }
      }
    }
    withDefaultFetchSize.toMap.get("fetchsize").foreach(value => positiveInt(value, "fetchsize"))
    withDefaultFetchSize
  }

  private def withDefaultFetchSize(
      options: Seq[(String, String)],
      profile: MysqlLoadProfile): Seq[(String, String)] = {
    if (options.exists(_._1 == "fetchsize")) options
    else profile.defaultFetchSize.map(value => options :+ ("fetchsize" -> value)).getOrElse(options)
  }

  private def defaultNumPartitions(profile: Option[MysqlLoadProfile]): String = {
    profile.flatMap(_.maxNumPartitions).map(max => math.min(10, max).toString).getOrElse("10")
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
        throw new CompileException(s"Kyuubi MySQL load option '$label' must be a positive integer: $value")
    }
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

  private def parseKyuubiMysqlCatalogPath(path: String): Option[KyuubiMysqlCatalogTarget] = {
    val split = path.split("\\.", -1)
    if (split.length != 3) None
    else {
      val catalog = SparkOneSqlRender.requireIdentifier(split(0), "Kyuubi MySQL catalog")
      val db = SparkOneSqlRender.requireIdentifier(split(1), "Kyuubi MySQL database")
      val table = SparkOneSqlRender.requireIdentifier(split(2), "Kyuubi MySQL table")
      val dbtable = s"$db.$table"
      Some(KyuubiMysqlCatalogTarget(catalog, dbtable, s"$catalog.$dbtable"))
    }
  }

  private def mysqlOptions(
      connection: String,
      dbtable: String,
      statementOptions: Seq[(String, String)]): Seq[(String, String)] = {
    val forbidden = Set("url", "driver", "user", "password", "dbtable", "query")
    statementOptions.find { case (key, _) => forbidden.contains(key.toLowerCase) }.foreach { case (key, _) =>
      throw new CompileException(s"MySQL connection option '$key' must be configured in the selected local engine's datasources.mysql, not SQL OPTIONS")
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
      throw new CompileException(
        s"Missing MySQL datasource config for connection '$connection' key '$key' in the selected local engine, " +
          s"for example engines.local.datasources.mysql.$connection.$key")
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
final case class CatalogSaveSource(identifier: String, targetType: SaveTargetType, supportsPartitionBy: Boolean)
  extends ResolvedSaveSource
final case class MysqlSaveSource(dbtable: String, options: Seq[(String, String)]) extends ResolvedSaveSource

private final case class MysqlTarget(connection: String, dbtable: String)
private final case class KyuubiMysqlCatalogTarget(catalog: String, dbtable: String, catalogTable: String)

sealed trait MysqlLoadMode

object MysqlLoadMode {
  case object LocalAdapter extends MysqlLoadMode
  case object KyuubiProfile extends MysqlLoadMode
}

sealed trait MysqlLoadProfileStrategy

object MysqlLoadProfileStrategy {
  case object Catalog extends MysqlLoadProfileStrategy
  case object Provider extends MysqlLoadProfileStrategy

  def fromString(value: String): MysqlLoadProfileStrategy = value.trim.toLowerCase match {
    case "" | "catalog" => Catalog
    case "provider" | "profile-provider" | "profileprovider" => Provider
    case other => throw new CompileException(s"Unsupported Kyuubi MySQL load profile strategy: $other")
  }
}

final case class MysqlLoadProfile(
    name: String,
    strategy: MysqlLoadProfileStrategy = MysqlLoadProfileStrategy.Catalog,
    catalog: Option[String] = None,
    namespace: Option[String] = None,
    provider: String = "sparkone_mysql",
    remoteProfileName: Option[String] = None,
    allowedTables: Set[String] = Set.empty,
    maxNumPartitions: Option[Int] = None,
    defaultFetchSize: Option[String] = None) {

  val remoteProfile: String = remoteProfileName.orElse(catalog).getOrElse(name)

  def validate(): MysqlLoadProfile = {
    SparkOneSqlRender.requireIdentifier(name, "Kyuubi MySQL load profile name")
    strategy match {
      case MysqlLoadProfileStrategy.Catalog =>
        catalog.getOrElse {
          throw new CompileException(s"Kyuubi MySQL load profile '$name' with catalog strategy requires catalog.")
        }
      case MysqlLoadProfileStrategy.Provider =>
        SparkOneSqlRender.requireIdentifier(provider, s"Kyuubi MySQL load profile '$name' provider")
    }
    catalog.foreach(SparkOneSqlRender.requireIdentifier(_, s"Kyuubi MySQL load profile '$name' catalog"))
    namespace.foreach(SparkOneSqlRender.renderMultipartIdentifier(_, s"Kyuubi MySQL load profile '$name' namespace"))
    remoteProfileName.foreach(SparkOneSqlRender.requireIdentifier(_, s"Kyuubi MySQL load profile '$name' remoteProfile"))
    allowedTables.foreach(SparkOneSqlRender.renderMultipartIdentifier(_, s"Kyuubi MySQL load profile '$name' allowed table"))
    defaultFetchSize.foreach { value =>
      if (!value.matches("[1-9][0-9]*")) {
        throw new CompileException(s"Kyuubi MySQL load profile '$name' defaultFetchSize must be a positive integer: $value")
      }
    }
    maxNumPartitions.foreach { value =>
      if (value <= 0) {
        throw new CompileException(s"Kyuubi MySQL load profile '$name' maxNumPartitions must be positive: $value")
      }
    }
    this
  }

  def validateTable(dbtable: String): Unit = {
    SparkOneSqlRender.renderMultipartIdentifier(dbtable, s"Kyuubi MySQL load profile '$name' table")
    if (namespace.nonEmpty && dbtable.contains(".")) {
      throw new CompileException(
        s"Kyuubi MySQL load profile '$name' already defines namespace '${namespace.get}'; use mysql.`$name.table` or remove the profile namespace.")
    }
    if (allowedTables.nonEmpty && !allowedTables.map(_.toLowerCase).contains(dbtable.toLowerCase)) {
      throw new CompileException(s"Kyuubi MySQL load profile '$name' does not allow table: $dbtable")
    }
  }

  def renderCatalogTable(dbtable: String): String = {
    val catalogName = catalog.getOrElse(name)
    Seq(Some(catalogName), namespace, Some(dbtable)).flatten.mkString(".")
  }

  def renderRemoteTable(dbtable: String): String = {
    namespace.map(ns => s"$ns.$dbtable").getOrElse(dbtable)
  }
}
