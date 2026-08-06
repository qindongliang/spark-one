package ai.sparkone.kyuubi.odep.authz

import ai.sparkone.extension.overwrite.ManagedHdfsWorkspacePolicy
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.types.{DataType, StructType}

import java.util.Locale

private[authz] final class OdepPreAnalysisAuthorizationParser(
    spark: SparkSession,
    delegate: ParserInterface,
    authorize: (String, Seq[OdepAuthzResource]) => OdepAuthzResult,
    resolveSubject: SparkContext => String) extends ParserInterface {

  private val enforcer = new OdepAuthorizationEnforcer(authorize)

  override def parsePlan(sqlText: String): LogicalPlan = {
    OdepPreAnalysisAuthorizationContext.clear()
    authorizePlan(delegate.parsePlan(sqlText))
  }

  override def parseQuery(sqlText: String): LogicalPlan = {
    OdepPreAnalysisAuthorizationContext.clear()
    authorizePlan(delegate.parseQuery(sqlText))
  }

  override def parseExpression(sqlText: String): Expression = delegate.parseExpression(sqlText)
  override def parseTableIdentifier(sqlText: String): TableIdentifier = delegate.parseTableIdentifier(sqlText)
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = delegate.parseFunctionIdentifier(sqlText)
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = delegate.parseMultipartIdentifier(sqlText)
  override def parseTableSchema(sqlText: String): StructType = delegate.parseTableSchema(sqlText)
  override def parseDataType(sqlText: String): DataType = delegate.parseDataType(sqlText)

  private def authorizePlan(plan: LogicalPlan): LogicalPlan = {
    val relations = plan.collect {
      case relation: UnresolvedRelation
          if relation.multipartIdentifier.size == 2 &&
            ManagedHdfsWorkspacePolicy.ReadFormats.contains(
              relation.multipartIdentifier.head.toLowerCase(Locale.ROOT)) =>
        val rawPath = relation.multipartIdentifier(1)
        val normalizedPath = ManagedHdfsWorkspacePolicy.normalizeNativeHdfsReadPath(rawPath)
          .getOrElse {
            throw new OdepAuthorizationException(
              "Native file relation requires a validated absolute HDFS path without glob patterns")
          }
        relation -> normalizedPath
    }
    val resources = relations.map { case (_, path) =>
      OdepAuthzResource.hdfs(path, OdepAuthzResource.Read)
    }.distinct

    if (resources.nonEmpty) {
      val subject = resolveSubject(spark.sparkContext)
      enforcer.enforce(subject, resources, "pre-analysis")
      val proofs = relations.map { case (relation, path) =>
        val proof = OdepPreAuthorizedHdfsRead(subject, path)
        relation.setTagValue(OdepPreAnalysisAuthorizationContext.ProofTag, proof)
        proof
      }.toSet
      OdepPreAnalysisAuthorizationContext.activate(proofs)
    }
    plan
  }
}

private[authz] final case class OdepPreAuthorizedHdfsRead(subject: String, path: String) {
  def coversExact(currentSubject: String, resource: OdepAuthzResource): Boolean = {
    matchesSubjectAndRead(currentSubject, resource) && resource.path.contains(path)
  }

  def coversDescendant(currentSubject: String, resource: OdepAuthzResource): Boolean = {
    matchesSubjectAndRead(currentSubject, resource) && resource.path.exists { candidate =>
      candidate == path ||
        (path == "/" && candidate.startsWith("/")) ||
        candidate.startsWith(path + "/")
    }
  }

  private def matchesSubjectAndRead(
      currentSubject: String,
      resource: OdepAuthzResource): Boolean = {
    subject == currentSubject &&
      resource.resourceType == "hdfs" &&
      resource.action == OdepAuthzResource.Read
  }
}

private[authz] object OdepPreAnalysisAuthorizationContext {
  val ProofTag: TreeNodeTag[OdepPreAuthorizedHdfsRead] =
    TreeNodeTag[OdepPreAuthorizedHdfsRead]("sparkone.odep.preAuthorizedHdfsRead")

  private val activeProofs = new ThreadLocal[Set[OdepPreAuthorizedHdfsRead]]

  def activate(proofs: Set[OdepPreAuthorizedHdfsRead]): Unit = {
    if (proofs.isEmpty) clear() else activeProofs.set(proofs)
  }

  def current: Set[OdepPreAuthorizedHdfsRead] =
    Option(activeProofs.get()).getOrElse(Set.empty)

  def tagged(plan: LogicalPlan): Seq[(LogicalPlan, OdepPreAuthorizedHdfsRead)] = {
    plan.collect {
      case node if node.getTagValue(ProofTag).nonEmpty =>
        node -> node.getTagValue(ProofTag).get
    }
  }

  def clear(): Unit = activeProofs.remove()
}
