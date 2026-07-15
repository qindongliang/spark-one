package ai.sparkone.sql

import ai.sparkone.identity.TenantContext

import java.net.URI
import java.util.Locale
import scala.util.Try

final case class WritePlan(
    tenant: TenantContext,
    mode: WriteMode,
    sourceTable: String,
    target: WriteTarget,
    format: String,
    providerOptions: Map[String, String] = Map.empty,
    partitionColumns: Seq[String] = Nil,
    executionType: WriteExecutionType)

final case class WriteTarget(
    kind: WriteTargetKind,
    identifier: String,
    provider: Option[String] = None,
    connectionOptions: Map[String, String] = Map.empty)

sealed trait WriteMode {
  def name: String
}

object WriteMode {
  case object Append extends WriteMode { override val name: String = "append" }
  case object Overwrite extends WriteMode { override val name: String = "overwrite" }

  def parse(value: String): WriteMode = value.trim.toLowerCase match {
    case Append.name => Append
    case Overwrite.name => Overwrite
    case other => throw new CompileException(s"SAVE mode '$other' is not supported; use append or overwrite")
  }
}

sealed trait WriteTargetKind {
  def name: String
}

object WriteTargetKind {
  case object HiveCatalog extends WriteTargetKind { override val name: String = "hive-catalog" }
  case object DorisCatalog extends WriteTargetKind { override val name: String = "doris-catalog" }
  case object Mysql extends WriteTargetKind { override val name: String = "mysql" }
  case object ManagedHdfs extends WriteTargetKind { override val name: String = "managed-hdfs" }
  case object ExternalPath extends WriteTargetKind { override val name: String = "external-path" }
  case object UnknownProvider extends WriteTargetKind { override val name: String = "unknown-provider" }
}

sealed trait WriteExecutionType

object WriteExecutionType {
  case object CatalogSql extends WriteExecutionType
  case object MysqlAdapter extends WriteExecutionType
  case object FileProvider extends WriteExecutionType
}

object WriteCapabilityMatrix {
  import WriteMode._
  import WriteTargetKind._

  private val Capabilities: Map[WriteTargetKind, Set[WriteMode]] = Map(
    HiveCatalog -> Set(Append),
    DorisCatalog -> Set(Append),
    Mysql -> Set(Append),
    ManagedHdfs -> Set(Append, Overwrite),
    ExternalPath -> Set(Append),
    UnknownProvider -> Set.empty)

  def supports(kind: WriteTargetKind, mode: WriteMode): Boolean = {
    Capabilities.getOrElse(kind, Set.empty).contains(mode)
  }

  def validate(plan: WritePlan): Unit = {
    if (!supports(plan.target.kind, plan.mode)) {
      throw new CompileException(
        s"SAVE ${plan.mode.name} is permanently denied for target type ${plan.target.kind.name}: ${plan.target.identifier}")
    }
  }
}

object WriteSchemaPolicy {
  def validateColumnNames(
      sourceColumns: Seq[String],
      targetColumns: Seq[String],
      targetIdentifier: String): Unit = {
    val normalizedSource = sourceColumns.map(_.toLowerCase(Locale.ROOT))
    val normalizedTarget = targetColumns.map(_.toLowerCase(Locale.ROOT))
    val hasDuplicateColumns =
      normalizedSource.distinct.size != normalizedSource.size ||
        normalizedTarget.distinct.size != normalizedTarget.size
    val hasEmptyColumn = sourceColumns.exists(_.isEmpty) || targetColumns.exists(_.isEmpty)

    if (sourceColumns.isEmpty || targetColumns.isEmpty ||
        hasEmptyColumn || hasDuplicateColumns || normalizedSource.toSet != normalizedTarget.toSet) {
      throw new CompileException(
        s"SAVE source columns must match target columns by name: $targetIdentifier. " +
          s"source=[${sourceColumns.mkString(", ")}], target=[${targetColumns.mkString(", ")}]")
    }
  }

  def sourceColumnsInTargetOrder(
      sourceColumns: Seq[String],
      targetColumns: Seq[String],
      targetIdentifier: String): Seq[String] = {
    validateColumnNames(sourceColumns, targetColumns, targetIdentifier)
    val sourceByName = sourceColumns.map(column => column.toLowerCase(Locale.ROOT) -> column).toMap
    targetColumns.map(column => sourceByName(column.toLowerCase(Locale.ROOT)))
  }
}

private[sparkone] object CatalogWriteSqlRenderer {
  def render(
      plan: WritePlan,
      sourceColumns: Seq[String],
      targetColumns: Seq[String]): String = {
    if (plan.executionType != WriteExecutionType.CatalogSql || plan.mode != WriteMode.Append) {
      throw new CompileException("Catalog SQL renderer only supports SAVE append plans")
    }
    val orderedSourceColumns = WriteSchemaPolicy.sourceColumnsInTargetOrder(
      sourceColumns,
      targetColumns,
      plan.target.identifier)
    SparkOneSqlRender.renderInsertTable(
      plan.mode.name,
      plan.target.identifier,
      plan.sourceTable,
      plan.partitionColumns,
      targetColumns,
      orderedSourceColumns)
  }
}

final class WritePlanner {
  import WriteExecutionType._
  import WriteTargetKind._

  def plan(
      tenant: TenantContext,
      mode: String,
      sourceTable: String,
      format: String,
      path: String,
      providerOptions: Seq[(String, String)],
      partitionColumns: Seq[String],
      resolvedSource: ResolvedSaveSource): WritePlan = {
    val writeMode = WriteMode.parse(mode)
    val plan = resolvedSource match {
      case ProviderSaveSource(provider) =>
        if (partitionColumns.nonEmpty) {
          throw new CompileException("SAVE partitionBy is only supported for catalog targets")
        }
        WritePlan(
          tenant = tenant,
          mode = writeMode,
          sourceTable = sourceTable,
          target = WriteTarget(classifyProviderTarget(provider, path), path, provider = Some(provider)),
          format = format,
          providerOptions = providerOptions.toMap,
          executionType = FileProvider)
      case CatalogSaveSource(identifier, targetType, supportsPartitionBy) =>
        if (providerOptions.nonEmpty) {
          throw new CompileException(s"SAVE to catalog source '$format' does not support provider OPTIONS")
        }
        if (partitionColumns.nonEmpty && !supportsPartitionBy) {
          throw new CompileException(s"SAVE partitionBy is not supported for $format source")
        }
        val kind = targetType match {
          case SaveTargetType.Catalog => HiveCatalog
          case SaveTargetType.DorisCatalog => DorisCatalog
          case other => throw new CompileException(s"Unsupported catalog write target type: $other")
        }
        WritePlan(
          tenant = tenant,
          mode = writeMode,
          sourceTable = sourceTable,
          target = WriteTarget(kind, identifier),
          format = format,
          partitionColumns = partitionColumns,
          executionType = CatalogSql)
      case MysqlSaveSource(dbtable, connectionOptions) =>
        if (partitionColumns.nonEmpty) {
          throw new CompileException("SAVE partitionBy is not supported for mysql source")
        }
        WritePlan(
          tenant = tenant,
          mode = writeMode,
          sourceTable = sourceTable,
          target = WriteTarget(Mysql, dbtable, connectionOptions = connectionOptions.toMap),
          format = format,
          providerOptions = providerOptions.toMap,
          executionType = MysqlAdapter)
    }
    WriteCapabilityMatrix.validate(plan)
    plan
  }

  private def classifyProviderTarget(provider: String, path: String): WriteTargetKind = {
    if (!WritePlanner.KnownFileProviders.contains(provider.toLowerCase)) {
      UnknownProvider
    } else if (isManagedRelativePath(path)) {
      ManagedHdfs
    } else {
      ExternalPath
    }
  }

  private def isManagedRelativePath(path: String): Boolean = {
    val trimmed = path.trim
    if (trimmed.isEmpty || trimmed.startsWith("/") || trimmed.contains("\\")) {
      false
    } else {
      val uri = Try(new URI(trimmed)).toOption
      val segments = trimmed.split("/", -1)
      uri.exists { value =>
        value.getScheme == null &&
          value.getAuthority == null &&
          value.getQuery == null &&
          value.getFragment == null &&
          value.getPath == trimmed &&
          segments.forall(segment => segment.nonEmpty && segment != "." && segment != "..")
      }
    }
  }
}

private object WritePlanner {
  private val KnownFileProviders = Set("parquet", "csv", "json", "orc", "text", "excel")
}

private[sql] object WriteSqlRenderer {
  import WriteExecutionType._
  import WriteMode._

  def render(plan: WritePlan): String = plan.executionType match {
    case CatalogSql =>
      SparkOneSqlRender.renderSparkOneAction(
        "SAVE CATALOG",
        s"${plan.sourceTable} TO ${plan.target.identifier}")
    case MysqlAdapter =>
      SparkOneSqlRender.renderSparkOneAction(
        "SAVE MYSQL",
        s"${plan.sourceTable} TO ${plan.target.identifier}")
    case FileProvider =>
      plan.mode match {
        case Append =>
          throw new CompileException(
            s"SAVE append for ${plan.target.kind.name} is allowed by the capability matrix, but the file append executor is not implemented yet")
        case Overwrite =>
          throw new CompileException(
            "SAVE overwrite for managed-hdfs is disabled until the staging overwrite executor is available")
      }
  }
}
