package ai.sparkone.runtime

import ai.sparkone.sql.{MysqlLoadProfile, MysqlLoadProfileStrategy}

final class SparkOneEngineRegistry private (
    val defaultId: String,
    private val engines: Map[String, SparkOneEngine])
  extends AutoCloseable {

  def infos: Seq[EngineInfo] = {
    engines.values.toSeq.sortBy(_.id).map { engine =>
      EngineInfo(engine.id, engine.label, engine.engineType, engine.capabilities)
    }
  }

  def get(requestedId: Option[String]): SparkOneEngine = {
    val id = requestedId.map(_.trim).filter(_.nonEmpty).getOrElse(defaultId)
    engines.getOrElse(id, throw new IllegalArgumentException(s"Unknown execution engine: $id"))
  }

  override def close(): Unit = {
    engines.values.foreach(_.close())
  }
}

object SparkOneEngineRegistry {
  private val DefaultLocalId = "local"
  private val DefaultKyuubiId = "kyuubi"
  private val SimpleId = "^[A-Za-z_][A-Za-z0-9_-]*$".r

  def fromSystemProperties(): SparkOneEngineRegistry = {
    val ids = configuredIds
    val engines = ids.flatMap(createEngine).map(engine => engine.id -> engine).toMap
    val withLocal =
      if (engines.isEmpty && localEnabled(DefaultLocalId)) {
        Map(DefaultLocalId -> localEngine(DefaultLocalId))
      } else {
        engines
      }

    val defaultId = property("sparkone.engine.default")
      .filter(withLocal.contains)
      .orElse(withLocal.keys.toSeq.sorted.headOption)
      .getOrElse(throw new IllegalArgumentException("No SparkOne execution engine is enabled"))

    new SparkOneEngineRegistry(defaultId, withLocal)
  }

  private def configuredIds: Seq[String] = {
    val prefix = "sparkone.engine."
    val ids = sys.props.keys.flatMap { key =>
      if (key.startsWith(prefix)) {
        val remaining = key.stripPrefix(prefix)
        val dot = remaining.indexOf('.')
        if (dot > 0) Some(remaining.substring(0, dot)) else None
      } else {
        None
      }
    }.filter(id => id != "default").toSet

    if (ids.isEmpty) Seq(DefaultLocalId)
    else ids.toSeq.filter(validId).sorted
  }

  private def createEngine(id: String): Option[SparkOneEngine] = {
    engineType(id) match {
      case "local" if localEnabled(id) =>
        Some(localEngine(id))
      case "kyuubi" if enabled(id, defaultValue = false) =>
        Some(kyuubiEngine(id))
      case _ =>
        None
    }
  }

  private def localEngine(id: String): SparkOneEngine = {
    new LocalSparkEngine(id, label(id, "Local"), SparkOneRuntime.local(), localProperties(id))
  }

  private def kyuubiEngine(id: String): SparkOneEngine = {
    val prefix = s"sparkone.engine.$id.kyuubi"
    val url = property(s"$prefix.url")
      .getOrElse(throw new IllegalArgumentException(s"Kyuubi engine '$id' requires $prefix.url"))
    val driver = property(s"$prefix.driver").getOrElse("org.apache.kyuubi.jdbc.KyuubiHiveDriver")
    val config = KyuubiJdbcConfig(
      url = url,
      user = property(s"$prefix.user"),
      password = property(s"$prefix.password"),
      driver = driver,
      properties = optionProperties(prefix))
    new KyuubiJdbcEngine(id, label(id, "Kyuubi"), config, kyuubiMysqlLoadProfiles(id))
  }

  private def engineType(id: String): String = {
    property(s"sparkone.engine.$id.type").getOrElse(id match {
      case DefaultKyuubiId => "kyuubi"
      case _ => "local"
    }).trim.toLowerCase
  }

  private def localEnabled(id: String): Boolean = {
    enabled(id, defaultValue = true)
  }

  private def enabled(id: String, defaultValue: Boolean): Boolean = {
    property(s"sparkone.engine.$id.enabled")
      .map(value => Set("1", "true", "yes", "on").contains(value.trim.toLowerCase))
      .getOrElse(defaultValue)
  }

  private def label(id: String, defaultValue: String): String = {
    property(s"sparkone.engine.$id.label").getOrElse(defaultValue)
  }

  private def optionProperties(prefix: String): Map[String, String] = {
    val optionPrefix = s"$prefix.option."
    sys.props.toSeq.collect {
      case (key, value) if key.startsWith(optionPrefix) && value.trim.nonEmpty =>
        key.stripPrefix(optionPrefix) -> value.trim
    }.toMap
  }

  private def localProperties(id: String): Map[String, String] = {
    val localPrefix = s"sparkone.engine.$id.local.property."
    sys.props.toSeq.collect {
      case (key, value) if key.startsWith(localPrefix) && value.trim.nonEmpty =>
        key.stripPrefix(localPrefix) -> value.trim
    }.toMap
  }

  private def kyuubiMysqlLoadProfiles(id: String): Map[String, MysqlLoadProfile] = {
    val prefix = s"sparkone.engine.$id.kyuubi.mysqlLoadProfile."
    val names = sys.props.keys.flatMap { key =>
      if (key.startsWith(prefix)) {
        val remaining = key.stripPrefix(prefix)
        val dot = remaining.indexOf('.')
        if (dot > 0) Some(remaining.substring(0, dot)) else None
      } else {
        None
      }
    }.filter(validId).toSet

    names.map { name =>
      val profilePrefix = s"$prefix$name"
      val profile = MysqlLoadProfile(
        name = name,
        strategy = property(s"$profilePrefix.strategy")
          .map(MysqlLoadProfileStrategy.fromString)
          .getOrElse(MysqlLoadProfileStrategy.Catalog),
        catalog = property(s"$profilePrefix.catalog"),
        namespace = property(s"$profilePrefix.namespace").orElse(property(s"$profilePrefix.defaultNamespace")),
        provider = property(s"$profilePrefix.provider").getOrElse("sparkone_mysql"),
        remoteProfileName = property(s"$profilePrefix.remoteProfile"),
        allowedTables = property(s"$profilePrefix.allowedTables")
          .map(_.split("[,\\n]").map(_.trim).filter(_.nonEmpty).toSet)
          .getOrElse(Set.empty),
        maxNumPartitions = property(s"$profilePrefix.maxNumPartitions").map(_.toInt),
        defaultFetchSize = property(s"$profilePrefix.defaultFetchSize"))
        .validate()
      name -> profile
    }.toMap
  }

  private def property(key: String): Option[String] = {
    sys.props.get(key).map(_.trim).filter(_.nonEmpty)
  }

  private def validId(id: String): Boolean = {
    SimpleId.findFirstIn(id).contains(id)
  }
}
