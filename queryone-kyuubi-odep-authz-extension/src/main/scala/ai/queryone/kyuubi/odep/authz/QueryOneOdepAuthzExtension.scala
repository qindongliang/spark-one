package ai.queryone.kyuubi.odep.authz

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, SparkSessionExtensionsProvider}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.slf4j.LoggerFactory

final class QueryOneOdepAuthzExtension extends SparkSessionExtensionsProvider {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    OdepAuthorizationExtension.install(
      extensions,
      KyuubiSessionSubject.resolve)
  }
}

final class QueryOneLocalOdepAuthzExtension extends SparkSessionExtensionsProvider {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    OdepAuthorizationExtension.install(
      extensions,
      LocalExecutionSubject.resolve)
  }
}

private object OdepAuthorizationExtension {
  def install(
      extensions: SparkSessionExtensions,
      resolveSubject: SparkContext => String): Unit = {
    lazy val client = OdepAuthzClient.fromRuntimeConfiguration()
    val authorize = (subject: String, resources: Seq[OdepAuthzResource]) =>
      client.check(subject, resources)
    extensions.injectParser { (spark, delegate) =>
      new OdepPreAnalysisAuthorizationParser(
        spark,
        delegate,
        authorize,
        resolveSubject)
    }
    extensions.injectCheckRule { spark =>
      new OdepAuthorizationCheck(
        spark,
        authorize,
        resolveSubject)
    }
  }
}

private[authz] final class OdepAuthorizationCheck(
    spark: SparkSession,
    authorize: (String, Seq[OdepAuthzResource]) => OdepAuthzResult,
    resolveSubject: SparkContext => String = KyuubiSessionSubject.resolve)
  extends (LogicalPlan => Unit) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val extractor = new LogicalPlanResourceExtractor(spark)
  private val enforcer = new OdepAuthorizationEnforcer(authorize)

  override def apply(plan: LogicalPlan): Unit = {
    val managedAccesses = extractor.managedHdfsAccesses(plan)
    val unmanagedResources = extractor.extractUnmanaged(plan)
    if (managedAccesses.isEmpty && unmanagedResources.isEmpty) {
      return
    }

    val subject = resolveSubject(spark.sparkContext)
    val unresolvedNativeHdfsResources = withoutPreAuthorizedNativeHdfs(
      plan,
      subject,
      unmanagedResources)
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
    val resources = (unresolvedNativeHdfsResources ++ managedAccesses
      .filterNot(_.workspaceOwner == subject)
      .map(_.resource)).distinct
    if (resources.isEmpty) {
      return
    }
    enforcer.enforce(subject, resources, "analysis")
  }

  private def withoutPreAuthorizedNativeHdfs(
      plan: LogicalPlan,
      subject: String,
      resources: Seq[OdepAuthzResource]): Seq[OdepAuthzResource] = {
    val tagged = OdepPreAnalysisAuthorizationContext.tagged(plan)
    val locallyAuthorized = if (tagged.nonEmpty) {
      try {
        tagged.map { case (node, proof) =>
          val resolved = extractor.extractUnmanaged(node).distinct
          if (resolved.size != 1 || !proof.coversExact(subject, resolved.head)) {
            throw new OdepAuthorizationException(
              "Native HDFS relation resolved to a path that was not authorized before analysis")
          }
          resolved.head
        }.toSet
      } finally {
        OdepPreAnalysisAuthorizationContext.clear()
      }
    } else {
      val activeProofs = OdepPreAnalysisAuthorizationContext.current
      if (activeProofs.isEmpty) {
        Set.empty[OdepAuthzResource]
      } else {
        val nativeHdfsReads = resources.filter(isNativeHdfsRead).toSet
        if (!nativeHdfsReads.forall(resource =>
            activeProofs.exists(_.coversDescendant(subject, resource)))) {
          OdepPreAnalysisAuthorizationContext.clear()
          throw new OdepAuthorizationException(
            "Native HDFS relation resolved to a path that was not authorized before analysis")
        }
        nativeHdfsReads
      }
    }
    resources.filterNot(locallyAuthorized.contains)
  }

  private def isNativeHdfsRead(resource: OdepAuthzResource): Boolean = {
    resource.resourceType == "hdfs" && resource.action == OdepAuthzResource.Read
  }
}

private[authz] final class OdepAuthorizationEnforcer(
    authorize: (String, Seq[OdepAuthzResource]) => OdepAuthzResult) {

  private val logger = LoggerFactory.getLogger(getClass)

  def enforce(subject: String, resources: Seq[OdepAuthzResource], phase: String): Unit = {
    if (resources.isEmpty) {
      return
    }
    val result = try {
      authorize(subject, resources)
    } catch {
      case error: Exception =>
        logger.error(
          s"ODEP authorization failed, phase=$phase, subject=$subject, resourceCount=${resources.size}",
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
        s"ODEP authorization denied, phase=$phase, subject=$subject, resources=$deniedForLog")
      throw new OdepAuthorizationException(s"Resource access denied: $deniedForUser")
    }
    logger.info(
      s"ODEP authorization allowed, phase=$phase, subject=$subject, resourceCount=${resources.size}")
  }
}
