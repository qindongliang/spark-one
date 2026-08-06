package ai.sparkone.sql

import ai.sparkone.extension.overwrite.ManagedHdfsWorkspacePolicy
import org.apache.spark.sql.catalyst.plans.logical.{Command, ParsedStatement}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.execution.SparkSqlParser
import org.slf4j.LoggerFactory

import java.util.Locale

final class StatementPolicy {
  import StatementIntent._

  private val logger = LoggerFactory.getLogger(getClass)
  private val parser = new SparkSqlParser

  def validate(statement: CompiledStatement): Unit = statement.intent match {
    case NativeSql => validateNativeReadOnly(statement.sql)
    case Load => requireMetadata(statement.load.nonEmpty, "LOAD")
    case View => validateNoNativeProviderPaths(statement.sql)
    case SetVariable =>
      requireMetadata(statement.set.nonEmpty, "SET")
      statement.set.foreach { metadata =>
        if (metadata.valueType == SetValueType.Sql) {
          validateNativeReadOnly(metadata.value)
        }
      }
    case Save => requireMetadata(statement.writePlan.nonEmpty, "SAVE")
    case Assert =>
      requireMetadata(statement.assertion.nonEmpty, "ASSERT")
      validateNativeReadOnly(statement.sql)
  }

  private def validateNativeReadOnly(sql: String): Unit = {
    val plan = parsePlan(sql)
    validateNativeProviderPaths(plan)
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

  private def validateNoNativeProviderPaths(sql: String): Unit = {
    validateNativeProviderPaths(parsePlan(sql))
  }

  private def validateNativeProviderPaths(plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Unit = {
    val blockedProviders = ManagedHdfsWorkspacePolicy.ReadFormats ++
      Set("jdbc", "avro", "delta", "iceberg", "hudi", "xml")
    plan.collectFirst {
      case relation: UnresolvedRelation
          if relation.multipartIdentifier.size >= 2 && {
            val parts = relation.multipartIdentifier
            val provider = parts.head.toLowerCase(Locale.ROOT)
            val path = parts.tail.mkString(".")
            val isProviderPath =
              (parts.size == 2 && blockedProviders.contains(provider)) ||
                parts.tail.exists(looksLikePath)
            isProviderPath && !isAllowedNativeHdfsRelation(provider, path)
          } =>
        relation.multipartIdentifier.head -> relation.multipartIdentifier.tail.mkString(".")
    }.foreach { case (provider, path) =>
      throw new CompileException(
        s"Native provider path '$provider.`$path`' is disabled; " +
          "only supported file providers with an absolute HDFS path are allowed")
    }
  }

  private def isAllowedNativeHdfsRelation(provider: String, path: String): Boolean = {
    ManagedHdfsWorkspacePolicy.ReadFormats.contains(provider.toLowerCase(Locale.ROOT)) &&
      ManagedHdfsWorkspacePolicy.normalizeNativeHdfsReadPath(path).nonEmpty
  }

  private def looksLikePath(value: String): Boolean = {
    value.startsWith("/") || value.startsWith(".") || value.contains("/") || value.contains("\\") ||
      value.matches("^[A-Za-z][A-Za-z0-9+.-]*:.*")
  }

  private def parsePlan(sql: String): org.apache.spark.sql.catalyst.plans.logical.LogicalPlan = {
    try {
      parser.parsePlan(sql)
    } catch {
      case e: Exception =>
        throw new CompileException(s"Spark SQL parser rejected statement: ${e.getMessage}", e)
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
