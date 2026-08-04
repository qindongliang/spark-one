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
    val resources = extractor.extract(plan)
    if (resources.isEmpty) {
      return
    }

    val subject = KyuubiSessionSubject.resolve(spark.sparkContext)
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
