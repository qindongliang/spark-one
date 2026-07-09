package ai.sparkone.provider.mysql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

import java.nio.charset.StandardCharsets
import java.util.Base64

final class SparkOneMysqlDataSource extends RelationProvider with DataSourceRegister {
  override def shortName(): String = "sparkone_mysql"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val normalized = parameters.map { case (key, value) => key.toLowerCase -> value.trim }
    val catalog = requiredAny(normalized, Seq("catalog", "profile"))
    val dbtable = required(normalized, "dbtable")
    val spark = sqlContext.sparkSession
    val catalogPrefix = s"spark.sql.catalog.$catalog."
    val allConf = spark.sparkContext.getConf.getAll.toMap ++ spark.conf.getAll
    val catalogOptions = allConf.collect {
      case (key, value) if key.startsWith(catalogPrefix) && value.trim.nonEmpty =>
        key.stripPrefix(catalogPrefix) -> value.trim
    }

    val catalogClass = allConf.getOrElse(s"spark.sql.catalog.$catalog", "")
    if (!catalogClass.toLowerCase.contains("jdbc")) {
      throw new IllegalArgumentException(
        s"sparkone_mysql requires JDBC catalog '$catalog', but spark.sql.catalog.$catalog is '$catalogClass'")
    }

    val baseJdbcOptions = requireCatalogOptions(catalog, catalogOptions, Seq("url"))
    val dbtableWithFilter = normalized.get("whereclausebase64") match {
      case Some(encoded) =>
        val where = decodeBase64(encoded).trim
        if (where.isEmpty) dbtable else s"(select * from $dbtable where $where) sparkone_mysql_load"
      case None => dbtable
    }
    val loadOptions = copyOptions(normalized, Seq(
      "partitioncolumn" -> "partitionColumn",
      "lowerbound" -> "lowerBound",
      "upperbound" -> "upperBound",
      "numpartitions" -> "numPartitions",
      "fetchsize" -> "fetchsize"))

    val jdbcOptions = baseJdbcOptions ++ loadOptions + ("dbtable" -> dbtableWithFilter)
    new JdbcRelationProvider().createRelation(sqlContext, jdbcOptions)
  }

  private def required(options: Map[String, String], key: String): String = {
    options.get(key).filter(_.nonEmpty).getOrElse {
      throw new IllegalArgumentException(s"sparkone_mysql requires option '$key'")
    }
  }

  private def requiredAny(options: Map[String, String], keys: Seq[String]): String = {
    keys.flatMap(key => options.get(key).filter(_.nonEmpty)).headOption.getOrElse {
      throw new IllegalArgumentException(s"sparkone_mysql requires one of options: ${keys.mkString(", ")}")
    }
  }

  private def requireCatalogOptions(
      catalog: String,
      options: Map[String, String],
      requiredKeys: Seq[String]): Map[String, String] = {
    requiredKeys.foreach { key =>
      if (!options.contains(key)) {
        throw new IllegalArgumentException(
          s"sparkone_mysql catalog '$catalog' requires spark.sql.catalog.$catalog.$key")
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

  private def decodeBase64(value: String): String = {
    new String(Base64.getDecoder.decode(value), StandardCharsets.UTF_8)
  }
}
