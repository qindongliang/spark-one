package ai.sparkone.sql

import org.apache.spark.sql.catalyst.plans.logical.{Command, ParsedStatement}
import org.apache.spark.sql.execution.SparkSqlParser
import org.slf4j.LoggerFactory

final class StatementPolicy {
  import StatementIntent._

  private val logger = LoggerFactory.getLogger(getClass)
  private val parser = new SparkSqlParser

  def validate(statement: CompiledStatement): Unit = statement.intent match {
    case NativeSql => validateNativeReadOnly(statement.sql)
    case Load => requireMetadata(statement.load.nonEmpty, "LOAD")
    case View =>
    case SetVariable =>
      requireMetadata(statement.set.nonEmpty, "SET")
      statement.set.foreach { metadata =>
        if (metadata.valueType == SetValueType.Sql) {
          validateNativeReadOnly(metadata.value)
        }
      }
    case Save => requireMetadata(statement.writePlan.nonEmpty, "SAVE")
  }

  private def validateNativeReadOnly(sql: String): Unit = {
    val plan = try {
      parser.parsePlan(sql)
    } catch {
      case e: Exception =>
        throw new CompileException(s"Spark SQL parser rejected statement: ${e.getMessage}", e)
    }
    plan.collectFirst {
      case statement: ParsedStatement => statement.nodeName
      case command: Command if !isReadOnlyCommand(command.nodeName) => command.nodeName
      case node if StatementPolicy.NonCommandWritePlanNames.contains(node.nodeName) => node.nodeName
    }.foreach { planName =>
      logger.warn(s"Native SQL command blocked, plan=$planName, sql=${summarizeSql(sql)}")
      throw new CompileException(
        s"Native Spark SQL command '$planName' is disabled; " +
          "SparkOne only allows native read-only SQL and controlled LOAD, VIEW, SET, and SAVE statements")
    }
  }

  private def isReadOnlyCommand(planName: String): Boolean = {
    planName.startsWith("Show") ||
      planName.startsWith("Describe") ||
      planName == "ExplainCommand" ||
      planName == "SetCatalogAndNamespace"
  }

  private def requireMetadata(condition: Boolean, intent: String): Unit = {
    if (!condition) {
      throw new CompileException(s"Invalid internal $intent statement metadata")
    }
  }

  private def summarizeSql(sql: String): String = {
    val normalized = sql.replaceAll("\\s+", " ").trim
    if (normalized.length <= 240) normalized else normalized.take(237) + "..."
  }
}

private object StatementPolicy {
  private val NonCommandWritePlanNames = Set("InsertIntoDir")
}
