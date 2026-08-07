package ai.queryone.provider.mysql

import ai.queryone.kyuubi.odep.OdepDatasourceResolver
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.slf4j.LoggerFactory

import java.sql.{Connection, Driver, DriverManager}
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.Properties
import scala.collection.JavaConverters._

final class QueryOneMysqlDataSource private[mysql] (
    configuredDatasourceResolver: OdepDatasourceResolver) extends RelationProvider with DataSourceRegister {
  def this() = this(null)

  private val logger = LoggerFactory.getLogger(getClass)
  private val odepRoutingCatalogClass = "ai.queryone.kyuubi.odep.catalog.OdepRoutingCatalog"
  private lazy val datasourceResolver =
    Option(configuredDatasourceResolver).getOrElse(OdepDatasourceResolver.getDefault())

  override def shortName(): String = "queryone_mysql"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val normalized = parameters.map { case (key, value) => key.toLowerCase -> value.trim }
    val catalog = required(normalized, "catalog")
    val dbtable = required(normalized, "dbtable")
    val spark = sqlContext.sparkSession
    val catalogPrefix = s"spark.sql.catalog.$catalog."
    val allConf = spark.sparkContext.getConf.getAll.toMap ++ spark.conf.getAll
    val catalogOptions = allConf.collect {
      case (key, value) if key.startsWith(catalogPrefix) && value.trim.nonEmpty =>
        key.stripPrefix(catalogPrefix) -> value.trim
    }

    val catalogClass = allConf.getOrElse(s"spark.sql.catalog.$catalog", "")
    val (baseJdbcOptions, resolvedDbtable, authzMode, authzNamespace, authzTable) =
      normalized.get("alias").filter(_.nonEmpty) match {
      case Some(alias) =>
        val (options, table) = resolveOdepJdbcRoute(catalog, alias, dbtable, catalogClass)
        (options, table, "odep", alias, dbtable)
      case None =>
        if (!catalog.toLowerCase.endsWith("_static")) {
          throw new IllegalArgumentException(
            s"queryone_mysql static catalog name must end in _static: $catalog")
        }
        if (!catalogClass.toLowerCase.contains("jdbc")) {
          throw new IllegalArgumentException(
            s"queryone_mysql requires JDBC catalog '$catalog', but spark.sql.catalog.$catalog is '$catalogClass'")
        }
        val options = requireCatalogOptions(catalog, catalogOptions, Seq("url"))
        if (!isMysql(options)) {
          throw new IllegalArgumentException(
            s"queryone_mysql static partition reads only support MySQL JDBC catalogs: $catalog")
        }
        val (database, table) = staticTable(dbtable)
        (options, s"${quoteMysqlIdentifier(database)}.${quoteMysqlIdentifier(table)}", "static", database, table)
    }
    val dbtableWithFilter = normalized.get("whereclausebase64") match {
      case Some(encoded) =>
        val where = decodeBase64(encoded).trim
        if (where.isEmpty) resolvedDbtable else s"(select * from $resolvedDbtable where $where) queryone_mysql_load"
      case None => resolvedDbtable
    }
    val loadOptions = copyOptions(normalized, Seq(
      "partitioncolumn" -> "partitionColumn",
      "lowerbound" -> "lowerBound",
      "upperbound" -> "upperBound",
      "numpartitions" -> "numPartitions",
      "fetchsize" -> "fetchsize"))

    val jdbcOptions = enrichLoadOptions(baseJdbcOptions ++ loadOptions + ("dbtable" -> dbtableWithFilter))
    logEffectiveOptions(jdbcOptions)
    val relation = new JdbcRelationProvider().createRelation(sqlContext, jdbcOptions)
    new QueryOneMysqlRelation(
      relation,
      authzMode,
      catalog,
      authzNamespace,
      authzTable)
  }

  private def resolveOdepJdbcRoute(
      catalog: String,
      alias: String,
      table: String,
      catalogClass: String): (Map[String, String], String) = {
    if (catalogClass != odepRoutingCatalogClass) {
      throw new IllegalArgumentException(
        s"queryone_mysql ODEP alias requires routing catalog '$catalog', but spark.sql.catalog.$catalog is '$catalogClass'")
    }
    validateIdentifier(alias, "ODEP alias")
    validateIdentifier(table, "ODEP table")

    val datasource = datasourceResolver.resolve("jdbc", alias)
    val physicalNamespace = datasource.getPhysicalNamespace
    val jdbcOptions = datasource.getOptions.asScala.toMap
    requireCatalogOptions(s"$catalog/$alias", jdbcOptions, Seq("url"))
    if (!isMysql(jdbcOptions)) {
      throw new IllegalArgumentException(
        s"queryone_mysql ODEP partition reads only support MySQL JDBC routes: catalog=$catalog, alias=$alias")
    }

    val qualifiedTable =
      s"${quoteMysqlIdentifier(physicalNamespace)}.${quoteMysqlIdentifier(table)}"
    jdbcOptions -> qualifiedTable
  }

  private def required(options: Map[String, String], key: String): String = {
    options.get(key).filter(_.nonEmpty).getOrElse {
      throw new IllegalArgumentException(s"queryone_mysql requires option '$key'")
    }
  }

  private def staticTable(value: String): (String, String) = {
    value.split("\\.", -1) match {
      case Array(database, table) =>
        validateIdentifier(database, "static database")
        validateIdentifier(table, "static table")
        database -> table
      case _ =>
        throw new IllegalArgumentException(
          s"queryone_mysql static dbtable must be database.table: $value")
    }
  }

  private def requireCatalogOptions(
      catalog: String,
      options: Map[String, String],
      requiredKeys: Seq[String]): Map[String, String] = {
    requiredKeys.foreach { key =>
      if (!options.contains(key)) {
        throw new IllegalArgumentException(
          s"queryone_mysql catalog '$catalog' requires spark.sql.catalog.$catalog.$key")
      }
    }
    options
  }

  private def copyOptions(
      options: Map[String, String],
      keys: Seq[(String, String)]): Map[String, String] = {
    keys.flatMap { case (sourceKey, targetKey) =>
      options.get(sourceKey).filter(_.nonEmpty).map(targetKey -> _)
    }.toMap
  }

  private def enrichLoadOptions(options: Map[String, String]): Map[String, String] = {
    val withFetchSize =
      if (containsIgnoreCase(options, "fetchsize")) options else options + ("fetchsize" -> "10000")

    optionIgnoreCase(withFetchSize, "partitionColumn") match {
      case None if hasPartitionHints(withFetchSize) =>
        throw new IllegalArgumentException("queryone_mysql requires partitionColumn when lowerBound, upperBound, or numPartitions is specified")
      case None =>
        withFetchSize
      case Some(partitionColumn) =>
        validatePartitionColumn(partitionColumn)
        val lowerBound = optionIgnoreCase(withFetchSize, "lowerBound")
        val upperBound = optionIgnoreCase(withFetchSize, "upperBound")
        if (lowerBound.isDefined != upperBound.isDefined) {
          throw new IllegalArgumentException("queryone_mysql requires lowerBound and upperBound together")
        }

        val withPartition =
          putCanonical(
            putCanonical(withFetchSize, "partitionColumn", partitionColumn),
            "numPartitions",
            optionIgnoreCase(withFetchSize, "numPartitions").getOrElse("10"))

        if (lowerBound.isDefined) {
          putCanonical(
            putCanonical(withPartition, "lowerBound", lowerBound.get),
            "upperBound",
            upperBound.get)
        } else {
          fetchBounds(withPartition, partitionColumn) match {
            case Some((lower, upper)) =>
              putCanonical(
                putCanonical(withPartition, "lowerBound", lower),
                "upperBound",
                upper)
            case None =>
              removeIgnoreCase(withPartition, Set("partitionColumn", "lowerBound", "upperBound", "numPartitions"))
          }
        }
    }
  }

  private def fetchBounds(options: Map[String, String], partitionColumn: String): Option[(String, String)] = {
    val url = required(caseInsensitive(options), "url")
    val driverClass = optionIgnoreCase(options, "driver").filter(_.nonEmpty)

    val properties = new Properties()
    optionIgnoreCase(options, "user").foreach(properties.setProperty("user", _))
    optionIgnoreCase(options, "password").foreach(properties.setProperty("password", _))

    val dbtable = required(caseInsensitive(options), "dbtable")
    val sql = s"SELECT MIN(${quoteMysqlIdentifier(partitionColumn)}), MAX(${quoteMysqlIdentifier(partitionColumn)}) FROM $dbtable"
    logger.warn(s"queryone_mysql diagnostic: bounds query sql=$sql")
    val connection = openConnection(url, properties, driverClass)
    try {
      val statement = connection.createStatement()
      try {
        val resultSet = statement.executeQuery(sql)
        try {
          if (!resultSet.next()) None
          else {
            val lower = Option(resultSet.getObject(1)).map(_.toString)
            val upper = Option(resultSet.getObject(2)).map(_.toString)
            lower.zip(upper).headOption
          }
        } finally {
          resultSet.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  private def openConnection(
      url: String,
      properties: Properties,
      driverClass: Option[String]): Connection = {
    driverClass match {
      case Some(className) =>
        val loader = Thread.currentThread().getContextClassLoader
        val driver = Class.forName(className, true, loader).getDeclaredConstructor().newInstance().asInstanceOf[Driver]
        Option(driver.connect(url, properties)).getOrElse {
          throw new IllegalArgumentException(s"queryone_mysql JDBC driver '$className' does not accept url: $url")
        }
      case None =>
        DriverManager.getConnection(url, properties)
    }
  }

  private def validatePartitionColumn(value: String): Unit = {
    validateIdentifier(value, "partitionColumn")
  }

  private def validateIdentifier(value: String, label: String): Unit = {
    if (!value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      throw new IllegalArgumentException(
        s"queryone_mysql $label must be a simple identifier: $value")
    }
  }

  private def isMysql(options: Map[String, String]): Boolean = {
    val url = optionIgnoreCase(options, "url").getOrElse("").toLowerCase
    val driver = optionIgnoreCase(options, "driver").getOrElse("").toLowerCase
    url.startsWith("jdbc:mysql:") || driver.contains("mysql")
  }

  private def quoteMysqlIdentifier(value: String): String = {
    s"`${value.replace("`", "``")}`"
  }

  private def optionIgnoreCase(options: Map[String, String], key: String): Option[String] = {
    options.collectFirst {
      case (candidate, value) if candidate.equalsIgnoreCase(key) && value.trim.nonEmpty => value.trim
    }
  }

  private def containsIgnoreCase(options: Map[String, String], key: String): Boolean = {
    optionIgnoreCase(options, key).nonEmpty
  }

  private def hasPartitionHints(options: Map[String, String]): Boolean = {
    Seq("lowerBound", "upperBound", "numPartitions").exists(key => containsIgnoreCase(options, key))
  }

  private def putCanonical(options: Map[String, String], key: String, value: String): Map[String, String] = {
    removeIgnoreCase(options, Set(key)) + (key -> value)
  }

  private def removeIgnoreCase(options: Map[String, String], keys: Set[String]): Map[String, String] = {
    options.filterNot { case (key, _) => keys.exists(_.equalsIgnoreCase(key)) }
  }

  private def caseInsensitive(options: Map[String, String]): Map[String, String] = {
    options.map { case (key, value) => key.toLowerCase -> value }
  }

  private def logEffectiveOptions(options: Map[String, String]): Unit = {
    val dbtable = optionIgnoreCase(options, "dbtable").getOrElse("")
    val partitionColumn = optionIgnoreCase(options, "partitionColumn").getOrElse("")
    val lowerBound = optionIgnoreCase(options, "lowerBound").getOrElse("")
    val upperBound = optionIgnoreCase(options, "upperBound").getOrElse("")
    val numPartitions = optionIgnoreCase(options, "numPartitions").getOrElse("")
    val fetchsize = optionIgnoreCase(options, "fetchsize").getOrElse("")
    logger.warn(
      s"queryone_mysql diagnostic: effective jdbc options, dbtable=$dbtable, partitionColumn=$partitionColumn, " +
        s"lowerBound=$lowerBound, upperBound=$upperBound, numPartitions=$numPartitions, fetchsize=$fetchsize")
  }

  private def decodeBase64(value: String): String = {
    new String(Base64.getDecoder.decode(value), StandardCharsets.UTF_8)
  }
}
