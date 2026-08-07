package queryone.extension.hdfs

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, SparkSessionExtensionsProvider}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.{DataType, StructType}

final class QueryOneHdfsWorkspaceExtensions extends SparkSessionExtensionsProvider {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (_, delegate) =>
      new QueryOneManagedHdfsParser(delegate)
    }
  }
}

private final class QueryOneManagedHdfsParser(delegate: ParserInterface) extends ParserInterface {
  override def parsePlan(sqlText: String): LogicalPlan = {
    ManagedHdfsLoadProtocol.parse(sqlText) match {
      case Some(request) =>
        QueryOneManagedHdfsLoadCommand(
          request.workspaceOwner,
          request.targetTable,
          request.format,
          request.relativePath,
          request.options)
      case None => ManagedHdfsOverwriteProtocol.parse(sqlText).map { request =>
        QueryOneManagedHdfsOverwriteCommand(
          request.tenant,
          request.sourceTable,
          request.format,
          request.relativePath,
          request.options)
      }.getOrElse(delegate.parsePlan(sqlText))
    }
  }

  override def parseQuery(sqlText: String): LogicalPlan = delegate.parseQuery(sqlText)
  override def parseExpression(sqlText: String): Expression = delegate.parseExpression(sqlText)
  override def parseTableIdentifier(sqlText: String): TableIdentifier = delegate.parseTableIdentifier(sqlText)
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = delegate.parseFunctionIdentifier(sqlText)
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = delegate.parseMultipartIdentifier(sqlText)
  override def parseTableSchema(sqlText: String): StructType = delegate.parseTableSchema(sqlText)
  override def parseDataType(sqlText: String): DataType = delegate.parseDataType(sqlText)
}

private final case class QueryOneManagedHdfsLoadCommand(
    workspaceOwner: String,
    targetTable: String,
    format: String,
    relativePath: String,
    options: Map[String, String]) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[org.apache.spark.sql.Row] = {
    ManagedHdfsLoadExecutor.execute(
      sparkSession,
      ManagedHdfsLoadRequest(workspaceOwner, targetTable, format, relativePath, options))
    Seq.empty
  }
}

private final case class QueryOneManagedHdfsOverwriteCommand(
    tenant: String,
    sourceTable: String,
    format: String,
    relativePath: String,
    options: Map[String, String]) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[org.apache.spark.sql.Row] = {
    ManagedHdfsOverwriteExecutor.execute(
      sparkSession,
      ManagedHdfsOverwriteRequest(tenant, sourceTable, format, relativePath, options))
    Seq.empty
  }
}
