package ai.sparkone.runtime

import ai.sparkone.sql.CompileException
import org.slf4j.LoggerFactory

import java.sql.{Connection, Driver, DriverManager}
import java.util.Properties

object MysqlJdbcLoadOptions {
  val DefaultNumPartitions = "10"
  val DefaultFetchSize = "10000"
  private val logger = LoggerFactory.getLogger(getClass)

  def enrich(options: Map[String, String]): Map[String, String] = {
    val enriched = enrich(options, fetchJdbcBounds)
    logEffectiveOptions(enriched)
    enriched
  }

  private[runtime] def enrich(
      options: Map[String, String],
      fetchBounds: (Map[String, String], String, String) => Option[(String, String)]): Map[String, String] = {
    val withFetchSize =
      if (containsIgnoreCase(options, "fetchsize")) options
      else options + ("fetchsize" -> DefaultFetchSize)

    optionIgnoreCase(withFetchSize, "partitionColumn") match {
      case None if hasPartitionHints(withFetchSize) =>
        throw new CompileException("MySQL partition load requires partitionColumn when lowerBound, upperBound, or numPartitions is specified.")
      case None =>
        withFetchSize
      case Some(partitionColumn) =>
        validatePartitionColumn(partitionColumn)
        val lowerBound = optionIgnoreCase(withFetchSize, "lowerBound")
        val upperBound = optionIgnoreCase(withFetchSize, "upperBound")
        if (lowerBound.isDefined != upperBound.isDefined) {
          throw new CompileException("MySQL partition load requires lowerBound and upperBound together.")
        }

        val withPartition =
          putCanonical(
            putCanonical(withFetchSize, "partitionColumn", partitionColumn),
            "numPartitions",
            optionIgnoreCase(withFetchSize, "numPartitions").getOrElse(DefaultNumPartitions))

        if (lowerBound.isDefined) {
          putCanonical(
            putCanonical(withPartition, "lowerBound", lowerBound.get),
            "upperBound",
            upperBound.get)
        } else {
          val dbtable = optionIgnoreCase(withPartition, "dbtable").getOrElse {
            throw new CompileException("MySQL partition load requires dbtable option.")
          }
          fetchBounds(withPartition, dbtable, partitionColumn) match {
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

  private def fetchJdbcBounds(
      options: Map[String, String],
      dbtable: String,
      partitionColumn: String): Option[(String, String)] = {
    val url = optionIgnoreCase(options, "url").getOrElse {
      throw new CompileException("MySQL partition load requires url option.")
    }
    val driverClass = optionIgnoreCase(options, "driver").filter(_.nonEmpty)

    val properties = new Properties()
    optionIgnoreCase(options, "user").foreach(properties.setProperty("user", _))
    optionIgnoreCase(options, "password").foreach(properties.setProperty("password", _))

    val sql = s"SELECT MIN(${quoteMysqlIdentifier(partitionColumn)}), MAX(${quoteMysqlIdentifier(partitionColumn)}) FROM $dbtable"
    logger.info(s"MySQL Load: bounds query sql=$sql")
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
          throw new CompileException(s"MySQL JDBC driver '$className' does not accept url: $url")
        }
      case None =>
        DriverManager.getConnection(url, properties)
    }
  }

  private def validatePartitionColumn(value: String): Unit = {
    if (!value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      throw new CompileException(s"MySQL partitionColumn must be a simple column identifier: $value")
    }
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

  private def logEffectiveOptions(options: Map[String, String]): Unit = {
    val dbtable = optionIgnoreCase(options, "dbtable").getOrElse("")
    val partitionColumn = optionIgnoreCase(options, "partitionColumn").getOrElse("")
    val lowerBound = optionIgnoreCase(options, "lowerBound").getOrElse("")
    val upperBound = optionIgnoreCase(options, "upperBound").getOrElse("")
    val numPartitions = optionIgnoreCase(options, "numPartitions").getOrElse("")
    val fetchsize = optionIgnoreCase(options, "fetchsize").getOrElse("")
    logger.info(
      s"MySQL Load: effective jdbc options, dbtable=$dbtable, partitionColumn=$partitionColumn, " +
        s"lowerBound=$lowerBound, upperBound=$upperBound, numPartitions=$numPartitions, fetchsize=$fetchsize")
  }
}
