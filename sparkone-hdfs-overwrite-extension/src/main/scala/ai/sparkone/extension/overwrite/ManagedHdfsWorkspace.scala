package ai.sparkone.extension.overwrite

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

import java.net.URI
import java.util.Locale
import scala.util.Try

object ManagedHdfsWorkspacePolicy {
  val WorkspaceRootKey = "spark.sparkone.overwrite.workspaceRoot"
  val ReadFormats: Set[String] = Set(
    "parquet", "csv", "json", "orc", "text", "libsvm", "binaryfile", "excel")
  val WriteFormats: Set[String] = Set("parquet", "csv", "json", "orc", "text", "excel")

  private val DefaultWorkspaceRoot = "/public/odep/user"
  private val InternalWorkPrefix = ".sparkone-overwrite-"
  private val ManagedLoadWorkspaceOwnerTag =
    TreeNodeTag[String]("sparkone.managedHdfsLoad.workspaceOwner")
  private val ManagedLoadReadWorkspaceOwnerLocalProperty =
    "sparkone.managedHdfsLoad.internalRead.workspaceOwner"
  private val ManagedLoadReadExpectedPathLocalProperty =
    "sparkone.managedHdfsLoad.internalRead.expectedPath"
  private val ManagedOverwriteWriteLocalProperty =
    "sparkone.managedHdfsOverwrite.internalWrite"
  private val UsernamePattern = "^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$".r
  private val IdentifierPattern = "^[A-Za-z_][A-Za-z0-9_]*$".r
  private val SensitiveOptionNames = Set(
    "path", "url", "uri", "user", "username", "owner", "password", "token",
    "accesskey", "accesskeyid", "secretkey", "secretaccesskey", "credential", "credentials")

  def isManagedRelativePath(path: String): Boolean = {
    val trimmed = path.trim
    if (trimmed.isEmpty || trimmed != path || trimmed.startsWith("/") || trimmed.contains("\\")) {
      false
    } else {
      val uri = Try(new URI(trimmed)).toOption
      val segments = trimmed.split("/", -1)
      uri.exists { value =>
        value.getScheme == null && value.getAuthority == null && value.getQuery == null &&
          value.getFragment == null && value.getPath == trimmed &&
          segments.forall(segment =>
            segment.nonEmpty && segment != "." && segment != ".." &&
              !segment.toLowerCase(Locale.ROOT).startsWith(InternalWorkPrefix))
      }
    }
  }

  def isAllowedOption(key: String): Boolean = {
    val normalized = key.replaceAll("[^A-Za-z0-9]", "").toLowerCase(Locale.ROOT)
    !SensitiveOptionNames.contains(normalized) && !key.toLowerCase(Locale.ROOT).startsWith("fs.")
  }

  def isValidWorkspaceOwner(owner: String): Boolean = {
    owner != null && UsernamePattern.pattern.matcher(owner).matches()
  }

  def markManagedLoadRelations(plan: LogicalPlan, workspaceOwner: String): Unit = {
    plan.foreach { node =>
      if (node.children.isEmpty) {
        node.setTagValue(ManagedLoadWorkspaceOwnerTag, workspaceOwner)
      }
    }
  }

  def managedLoadWorkspaceOwner(plan: LogicalPlan): Option[String] =
    plan.getTagValue(ManagedLoadWorkspaceOwnerTag)

  def withManagedLoadRead[T](
      sparkContext: SparkContext,
      workspaceOwner: String,
      expectedPath: Path)(body: => T): T = {
    val previousOwner = sparkContext.getLocalProperty(ManagedLoadReadWorkspaceOwnerLocalProperty)
    val previousPath = sparkContext.getLocalProperty(ManagedLoadReadExpectedPathLocalProperty)
    sparkContext.setLocalProperty(ManagedLoadReadWorkspaceOwnerLocalProperty, workspaceOwner)
    sparkContext.setLocalProperty(ManagedLoadReadExpectedPathLocalProperty, expectedPath.toString)
    try {
      body
    } finally {
      sparkContext.setLocalProperty(ManagedLoadReadWorkspaceOwnerLocalProperty, previousOwner)
      sparkContext.setLocalProperty(ManagedLoadReadExpectedPathLocalProperty, previousPath)
    }
  }

  def managedLoadReadContext(sparkContext: SparkContext): Option[ManagedHdfsLoadReadContext] = {
    for {
      owner <- Option(sparkContext.getLocalProperty(ManagedLoadReadWorkspaceOwnerLocalProperty))
      path <- Option(sparkContext.getLocalProperty(ManagedLoadReadExpectedPathLocalProperty))
    } yield ManagedHdfsLoadReadContext(owner, new Path(path))
  }

  def matchesManagedLoadReadPaths(spark: SparkSession, actualPaths: Seq[Path]): Boolean = {
    managedLoadReadContext(spark.sparkContext).exists { context =>
      val expected = qualifiedUri(spark, context.expectedPath)
      actualPaths.nonEmpty && actualPaths.forall { path =>
        isSameOrDescendant(expected, qualifiedUri(spark, path))
      }
    }
  }

  def withManagedOverwriteWrite[T](sparkContext: SparkContext)(body: => T): T = {
    val previous = sparkContext.getLocalProperty(ManagedOverwriteWriteLocalProperty)
    sparkContext.setLocalProperty(ManagedOverwriteWriteLocalProperty, "true")
    try {
      body
    } finally {
      sparkContext.setLocalProperty(ManagedOverwriteWriteLocalProperty, previous)
    }
  }

  def isManagedOverwriteWrite(sparkContext: SparkContext): Boolean =
    sparkContext.getLocalProperty(ManagedOverwriteWriteLocalProperty) == "true"

  def resolveWorkspacePath(
      spark: SparkSession,
      workspaceOwner: String,
      relativePath: String): Path = {
    val workspaceRoot = spark.conf.getOption(WorkspaceRootKey).getOrElse(DefaultWorkspaceRoot)
    val userRoot = new Path(new Path(workspaceRoot), workspaceOwner)
    val candidate = new Path(userRoot, relativePath)
    requireDescendant(userRoot, candidate)
    new Path(candidate.toUri.normalize())
  }

  private[overwrite] def validateRequest(
      workspaceOwner: String,
      table: String,
      format: String,
      relativePath: String,
      options: Map[String, String],
      supportedFormats: Set[String],
      operation: String): Unit = {
    if (!isValidWorkspaceOwner(workspaceOwner)) {
      throw new IllegalArgumentException(s"Invalid managed HDFS $operation workspace owner")
    }
    if (!IdentifierPattern.pattern.matcher(table).matches()) {
      throw new IllegalArgumentException(
        s"Managed HDFS $operation table must be a simple temporary view name")
    }
    if (!supportedFormats.contains(format.toLowerCase(Locale.ROOT))) {
      throw new IllegalArgumentException(s"Managed HDFS $operation format is not supported: $format")
    }
    if (!isManagedRelativePath(relativePath)) {
      throw new IllegalArgumentException(s"Managed HDFS $operation requires a validated relative path")
    }
    options.keys.foreach { key =>
      if (!isAllowedOption(key)) {
        throw new IllegalArgumentException(s"Managed HDFS $operation option is not allowed: $key")
      }
    }
  }

  private[overwrite] def resolveTarget(
      spark: SparkSession,
      tenant: String,
      relativePath: String): ManagedHdfsTarget = {
    val candidate = resolveWorkspacePath(spark, tenant, relativePath)
    val workspaceRoot = spark.conf.getOption(WorkspaceRootKey).getOrElse(DefaultWorkspaceRoot)
    val userRoot = new Path(new Path(workspaceRoot), tenant)
    val fs = candidate.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val qualifiedUserRoot = fs.makeQualified(userRoot)
    val finalPath = fs.makeQualified(candidate)
    requireDescendant(qualifiedUserRoot, finalPath)
    ManagedHdfsTarget(fs, finalPath)
  }

  private def requireDescendant(root: Path, child: Path): Unit = {
    val rootUri = root.toUri.normalize()
    val childUri = child.toUri.normalize()
    val rootPath = rootUri.getPath.stripSuffix("/")
    if (rootUri.getScheme != childUri.getScheme || rootUri.getAuthority != childUri.getAuthority ||
        !childUri.getPath.startsWith(rootPath + "/")) {
      throw new IllegalArgumentException("Managed HDFS target escapes the tenant workspace")
    }
  }

  private def qualifiedUri(spark: SparkSession, path: Path): URI = {
    path.getFileSystem(spark.sparkContext.hadoopConfiguration)
      .makeQualified(path)
      .toUri
      .normalize()
  }

  private def isSameOrDescendant(expected: URI, actual: URI): Boolean = {
    val expectedPath = expected.getPath.stripSuffix("/")
    expected.getScheme == actual.getScheme && expected.getAuthority == actual.getAuthority &&
      (actual.getPath == expectedPath || actual.getPath.startsWith(expectedPath + "/"))
  }
}

private[overwrite] final case class ManagedHdfsTarget(fs: FileSystem, finalPath: Path)

final case class ManagedHdfsLoadReadContext(workspaceOwner: String, expectedPath: Path)
