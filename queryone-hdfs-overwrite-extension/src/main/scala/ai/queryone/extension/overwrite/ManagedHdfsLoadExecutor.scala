package ai.queryone.extension.overwrite

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.util.Locale

private[overwrite] object ManagedHdfsLoadExecutor {
  private val logger = LoggerFactory.getLogger(getClass)

  def execute(spark: SparkSession, request: ManagedHdfsLoadRequest): Unit = {
    ManagedHdfsWorkspacePolicy.validateRequest(
      request.workspaceOwner,
      request.targetTable,
      request.format,
      request.relativePath,
      request.options,
      ManagedHdfsWorkspacePolicy.ReadFormats,
      operation = "load")
    val target = ManagedHdfsWorkspacePolicy.resolveTarget(
      spark,
      request.workspaceOwner,
      request.relativePath)
    if (!target.fs.exists(target.finalPath)) {
      throw new IllegalArgumentException(
        s"Managed HDFS load target does not exist: ${request.relativePath}")
    }

    logger.info(
      s"Managed HDFS load started, workspaceOwner=${request.workspaceOwner}, table=${request.targetTable}, " +
        s"format=${request.format}, target=${target.finalPath}")
    val loaded = ManagedHdfsWorkspacePolicy.withManagedLoadRead(
      spark.sparkContext,
      request.workspaceOwner,
      target.finalPath) {
      spark.read
        .format(request.format.toLowerCase(Locale.ROOT))
        .options(request.options)
        .load(target.finalPath.toString)
    }
    ManagedHdfsWorkspacePolicy.markManagedLoadRelations(
      loaded.queryExecution.logical,
      request.workspaceOwner)
    loaded.createOrReplaceTempView(request.targetTable)
  }
}
