package ai.sparkone.extension.overwrite

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.util.Locale

private[overwrite] object ManagedHdfsLoadExecutor {
  private val logger = LoggerFactory.getLogger(getClass)

  def execute(spark: SparkSession, request: ManagedHdfsLoadRequest): Unit = {
    ManagedHdfsWorkspacePolicy.validateRequest(
      request.tenant,
      request.targetTable,
      request.format,
      request.relativePath,
      request.options,
      ManagedHdfsWorkspacePolicy.ReadFormats,
      operation = "load")
    val target = ManagedHdfsWorkspacePolicy.resolveTarget(
      spark,
      request.tenant,
      request.relativePath)
    if (!target.fs.exists(target.finalPath)) {
      throw new IllegalArgumentException(
        s"Managed HDFS load target does not exist: ${request.relativePath}")
    }

    logger.info(
      s"Managed HDFS load started, tenant=${request.tenant}, table=${request.targetTable}, " +
        s"format=${request.format}, target=${target.finalPath}")
    spark.read
      .format(request.format.toLowerCase(Locale.ROOT))
      .options(request.options)
      .load(target.finalPath.toString)
      .createOrReplaceTempView(request.targetTable)
  }
}
