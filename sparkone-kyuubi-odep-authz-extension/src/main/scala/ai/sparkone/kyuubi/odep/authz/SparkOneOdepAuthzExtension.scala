package ai.sparkone.kyuubi.odep.authz

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, SparkSessionExtensionsProvider}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.slf4j.LoggerFactory

final class SparkOneOdepAuthzExtension extends SparkSessionExtensionsProvider {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectCheckRule { spark =>
      lazy val client = OdepAuthzClient.fromEnvironment()
      new OdepAuthorizationCheck(spark, client.check)
    }
  }
}

private[authz] final class OdepAuthorizationCheck(
    spark: SparkSession,
    authorize: (String, Seq[OdepAuthzResource]) => OdepAuthzResult)
  extends (LogicalPlan => Unit) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val extractor = new LogicalPlanResourceExtractor(spark)

  override def apply(plan: LogicalPlan): Unit = {
    val managedAccesses = extractor.managedHdfsAccesses(plan)
    val unmanagedResources = extractor.extractUnmanaged(plan)
    if (managedAccesses.isEmpty && unmanagedResources.isEmpty) {
      return
    }

    val subject = KyuubiSessionSubject.resolve(spark.sparkContext)
    managedAccesses.find(access =>
      access.action == OdepAuthzResource.Write && access.workspaceOwner != subject).foreach { access =>
      logger.warn(
        s"Managed HDFS overwrite denied, subject=$subject, workspaceOwner=${access.workspaceOwner}")
      throw new OdepAuthorizationException(
        "Managed HDFS overwrite is only allowed in the current user's workspace")
    }
    val ownManagedAccesses = managedAccesses.filter(_.workspaceOwner == subject)
    ownManagedAccesses.foreach { access =>
      logger.info(
        s"Managed HDFS authorization allowed by workspace ownership, subject=$subject, " +
          s"action=${access.action}")
    }
    val resources = (unmanagedResources ++ managedAccesses
      .filterNot(_.workspaceOwner == subject)
      .map(_.resource)).distinct
    if (resources.isEmpty) {
      return
    }
    val result = try {
      authorize(subject, resources)
    } catch {
      case error: Exception =>
        logger.error(
          s"ODEP authorization failed, subject=$subject, resourceCount=${resources.size}",
          error)
        throw new OdepAuthorizationException("Resource authorization service failed", error)
    }
    if (!result.allowed) {
      val deniedForLog = result.denied.map { case (resource, reason) =>
        s"${resource.resourceType}:${resource.displayName}:${resource.action}:$reason"
      }.mkString(", ")
      val deniedForUser = result.denied.map { case (resource, _) =>
        s"${resource.resourceType}:${resource.displayName}:${resource.action}"
      }.mkString(", ")
      logger.warn(
        s"ODEP authorization denied, subject=$subject, resources=$deniedForLog")
      throw new OdepAuthorizationException(s"Resource access denied: $deniedForUser")
    }
    logger.info(
      s"ODEP authorization allowed, subject=$subject, resourceCount=${resources.size}")
  }
}
