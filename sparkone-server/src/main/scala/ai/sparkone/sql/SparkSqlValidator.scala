package ai.sparkone.sql

import ai.sparkone.extension.overwrite.ManagedHdfsOverwriteProtocol
import org.apache.spark.sql.execution.SparkSqlParser

final class SparkSqlValidator extends SqlValidator {
  private val parser = new SparkSqlParser

  override def validate(sql: String): Unit = {
    if (ManagedHdfsOverwriteProtocol.isCommand(sql)) {
      return
    }
    try {
      parser.parsePlan(sql)
    } catch {
      case e: Exception =>
        throw new CompileException(s"Spark SQL parser rejected statement: ${e.getMessage}", e)
    }
  }
}
