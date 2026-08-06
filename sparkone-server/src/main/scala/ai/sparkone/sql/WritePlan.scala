package ai.sparkone.sql

import ai.sparkone.identity.TenantContext
import ai.sparkone.extension.overwrite.{ManagedHdfsOverwriteProtocol, ManagedHdfsOverwriteRequest, ManagedHdfsWorkspacePolicy}

import java.util.Locale

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
    provider: Option[String] = None)

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
  case object JdbcCatalog extends WriteTargetKind { override val name: String = "jdbc-catalog" }
  case object ManagedHdfs extends WriteTargetKind { override val name: String = "managed-hdfs" }
  case object ExternalPath extends WriteTargetKind { override val name: String = "external-path" }
  case object UnknownProvider extends WriteTargetKind { override val name: String = "unknown-provider" }
}

sealed trait WriteExecutionType

object WriteExecutionType {
  case object CatalogSql extends WriteExecutionType
  case object FileProvider extends WriteExecutionType
}

object WriteCapabilityMatrix {
  import WriteMode._
  import WriteTargetKind._

  private val Capabilities: Map[WriteTargetKind, Set[WriteMode]] = Map(
    HiveCatalog -> Set(Append),
    DorisCatalog -> Set(Append),
    JdbcCatalog -> Set(Append),
    ManagedHdfs -> Set(Overwrite),
    ExternalPath -> Set.empty,
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
        val targetKind = classifyProviderTarget(provider, path)
        if (targetKind == ManagedHdfs) {
          validateManagedHdfsOptions(providerOptions)
        }
        WritePlan(
          tenant = tenant,
          mode = writeMode,
          sourceTable = sourceTable,
          target = WriteTarget(targetKind, path, provider = Some(provider)),
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
          case SaveTargetType.JdbcCatalog => JdbcCatalog
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
    }
    WriteCapabilityMatrix.validate(plan)
    plan
  }

  private def classifyProviderTarget(provider: String, path: String): WriteTargetKind = {
    if (!WritePlanner.KnownFileProviders.contains(provider.toLowerCase(Locale.ROOT))) {
      UnknownProvider
    } else if (ManagedHdfsWorkspacePolicy.isManagedRelativePath(path)) {
      ManagedHdfs
    } else {
      ExternalPath
    }
  }

  private def validateManagedHdfsOptions(options: Seq[(String, String)]): Unit = {
    options.foreach { case (key, _) =>
      if (!ManagedHdfsWorkspacePolicy.isAllowedOption(key)) {
        throw new CompileException(s"SAVE managed HDFS option is not allowed: $key")
      }
    }
  }
}

private object WritePlanner {
  private val KnownFileProviders = Set("parquet", "csv", "json", "orc", "text", "excel")
}

private[sql] object WriteSqlRenderer {
  import WriteExecutionType._

  def render(plan: WritePlan): String = plan.executionType match {
    case CatalogSql =>
      SparkOneSqlRender.renderSparkOneAction(
        "SAVE CATALOG",
        s"${plan.sourceTable} TO ${plan.target.identifier}")
    case FileProvider =>
      ManagedHdfsOverwriteProtocol.render(ManagedHdfsOverwriteRequest(
        tenant = plan.tenant.username,
        sourceTable = plan.sourceTable,
        format = plan.target.provider.getOrElse(plan.format),
        relativePath = plan.target.identifier,
        options = plan.providerOptions))
  }
}
