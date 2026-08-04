package ai.sparkone.kyuubi.odep.authz

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{ResolvedDBObjectName, ResolvedTable}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, InsertIntoStatement, LogicalPlan, ReplaceTableAsSelect, V2WriteCommand}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, FileTable}
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable

import scala.collection.mutable

private[authz] final class LogicalPlanResourceExtractor(spark: SparkSession) {
  import OdepAuthzResource.{Read, Write}

  private val ManagedHdfsWorkspaceRootKey = "spark.sparkone.overwrite.workspaceRoot"
  private val DefaultManagedHdfsWorkspaceRoot = "/public/sparkone/user"

  def extract(plan: LogicalPlan): Seq[OdepAuthzResource] = {
    val resources = mutable.LinkedHashSet.empty[OdepAuthzResource]

    def add(resource: OdepAuthzResource): Unit = resources += resource

    def visit(current: LogicalPlan): Unit = {
      managedHdfsCommandResource(current) match {
        case Some(resource) => add(resource)
        case None => current match {
          case command: V2WriteCommand =>
            addNamedRelation(command.table, Write).foreach(add)
            visit(command.query)

          case command: CreateTableAsSelect =>
            addWriteTarget(command.name).foreach(add)
            visit(command.query)

          case command: ReplaceTableAsSelect =>
            addWriteTarget(command.name).foreach(add)
            visit(command.query)

          case command: InsertIntoStatement =>
            addWriteTarget(command.table).foreach(add)
            visit(command.query)

          case command: InsertIntoHadoopFsRelationCommand =>
            command.catalogTable match {
              case Some(table) => add(catalogTableResource(table, Write))
              case None => add(OdepAuthzResource.hdfs(command.outputPath.toString, Write))
            }
            visit(command.query)

          case command: InsertIntoHiveTable =>
            add(catalogTableResource(command.table, Write))
            visit(command.query)

          case command: CreateDataSourceTableAsSelectCommand =>
            add(catalogTableResource(command.table, Write))
            visit(command.query)

          case command: DataWritingCommand =>
            throw new OdepAuthorizationException(
              s"Unsupported Spark write plan for authorization: ${command.nodeName}")

          case relation: DataSourceV2Relation =>
            dataSourceV2Resource(relation, Read).foreach(add)

          case relation: LogicalRelation =>
            logicalRelationResources(relation, Read).foreach(add)

          case relation: HiveTableRelation =>
            add(catalogTableResource(relation.tableMeta, Read))

          case table: ResolvedTable =>
            add(catalogIdentifierResource(table.catalog, table.identifier, Read))

          case other =>
            other.children.foreach(visit)
        }
      }
    }

    visit(plan)
    resources.toSeq
  }

  private def managedHdfsCommandResource(plan: LogicalPlan): Option[OdepAuthzResource] = {
    val action = plan.getClass.getName match {
      case "ai.sparkone.extension.overwrite.SparkOneManagedHdfsLoadCommand" => Some(Read)
      case "ai.sparkone.extension.overwrite.SparkOneManagedHdfsOverwriteCommand" => Some(Write)
      case _ => None
    }
    action.map { value =>
      def invoke(name: String): String =
        plan.getClass.getMethod(name).invoke(plan).asInstanceOf[String]
      val workspaceRoot = spark.conf.getOption(ManagedHdfsWorkspaceRootKey)
        .getOrElse(DefaultManagedHdfsWorkspaceRoot)
        .stripSuffix("/")
      val path = s"$workspaceRoot/${invoke("tenant")}/${invoke("relativePath")}"
      OdepAuthzResource.hdfs(path, value)
    }
  }

  private def addWriteTarget(plan: LogicalPlan): Option[OdepAuthzResource] = plan match {
    case relation: DataSourceV2Relation => dataSourceV2Resource(relation, Write)
    case relation: LogicalRelation => logicalRelationResources(relation, Write).headOption
    case relation: HiveTableRelation => Some(catalogTableResource(relation.tableMeta, Write))
    case table: ResolvedTable => Some(catalogIdentifierResource(table.catalog, table.identifier, Write))
    case identifier: ResolvedDBObjectName =>
      val parts = identifier.nameParts
      if (parts.length < 2) {
        throw new OdepAuthorizationException(
          s"Authorization requires a database and table: ${parts.mkString(".")}")
      }
      Some(catalogIdentifierResource(
        identifier.catalog,
        Identifier.of(parts.dropRight(1).toArray, parts.last),
        Write))
    case other =>
      throw new OdepAuthorizationException(
        s"Unsupported Spark write target for authorization: ${other.nodeName}")
  }

  private def addNamedRelation(
      relation: org.apache.spark.sql.catalyst.analysis.NamedRelation,
      action: String): Option[OdepAuthzResource] = relation match {
    case dataSource: DataSourceV2Relation => dataSourceV2Resource(dataSource, action)
    case other =>
      throw new OdepAuthorizationException(
        s"Unsupported Spark V2 write target for authorization: ${other.nodeName}")
  }

  private def dataSourceV2Resource(
      relation: DataSourceV2Relation,
      action: String): Option[OdepAuthzResource] = {
    (relation.catalog, relation.identifier) match {
      case (Some(catalog), Some(identifier)) =>
        Some(catalogIdentifierResource(catalog, identifier, action))
      case (None, None) => relation.table match {
        case fileTable: FileTable =>
          val paths = fileTable.fileIndex.rootPaths
          if (paths.size != 1) {
            throw new OdepAuthorizationException(
              "HDFS authorization requires exactly one root path per relation")
          }
          Some(OdepAuthzResource.hdfs(paths.head.toString, action))
        case _ =>
          throw new OdepAuthorizationException(
            s"Anonymous data source is not supported by ODEP authorization: ${relation.table.name}")
      }
      case _ =>
        throw new OdepAuthorizationException(
          s"Incomplete catalog identifier in Spark plan: ${relation.name}")
    }
  }

  private def logicalRelationResources(
      relation: LogicalRelation,
      action: String): Seq[OdepAuthzResource] = {
    relation.catalogTable match {
      case Some(table) => Seq(catalogTableResource(table, action))
      case None => relation.relation match {
        case hdfs: HadoopFsRelation =>
          hdfs.location.rootPaths.map(path => OdepAuthzResource.hdfs(path.toString, action))
        case baseRelation =>
          sparkOneMysqlResource(baseRelation, action).toSeq match {
            case resources if resources.nonEmpty => resources
            case _ =>
              throw new OdepAuthorizationException(
                s"Anonymous V1 data source is not supported by ODEP authorization: $baseRelation")
          }
      }
    }
  }

  private def sparkOneMysqlResource(
      relation: org.apache.spark.sql.sources.BaseRelation,
      action: String): Option[OdepAuthzResource] = {
    if (relation.getClass.getName != "ai.sparkone.provider.mysql.SparkOneMysqlRelation") {
      None
    } else {
      def invoke(name: String): String =
        relation.getClass.getMethod(name).invoke(relation).asInstanceOf[String]
      Some(OdepAuthzResource.table(
        "jdbc",
        invoke("sparkOneAuthzDatabase"),
        invoke("sparkOneAuthzTable"),
        action))
    }
  }

  private def catalogTableResource(table: CatalogTable, action: String): OdepAuthzResource = {
    OdepAuthzResource.table(
      "hive",
      table.identifier.database.getOrElse(spark.catalog.currentDatabase),
      table.identifier.table,
      action)
  }

  private def catalogIdentifierResource(
      catalog: CatalogPlugin,
      identifier: Identifier,
      action: String): OdepAuthzResource = {
    val namespace = identifier.namespace()
    if (namespace.length != 1) {
      throw new OdepAuthorizationException(
        s"Authorization requires a single database or alias: ${relationName(catalog, identifier)}")
    }
    val resourceType = catalog.name.toLowerCase match {
      case "jdbc" => "jdbc"
      case "doris" => "doris"
      case name if isHiveCatalog(name) => "hive"
      case name =>
        throw new OdepAuthorizationException(s"Unsupported catalog for authorization: $name")
    }
    OdepAuthzResource.table(resourceType, namespace.head, identifier.name(), action)
  }

  private def relationName(catalog: CatalogPlugin, identifier: Identifier): String =
    (catalog.name +: identifier.namespace() :+ identifier.name()).mkString(".")

  private def isHiveCatalog(name: String): Boolean =
    name.equalsIgnoreCase("spark_catalog") || name.equalsIgnoreCase("session_catalog")
}
