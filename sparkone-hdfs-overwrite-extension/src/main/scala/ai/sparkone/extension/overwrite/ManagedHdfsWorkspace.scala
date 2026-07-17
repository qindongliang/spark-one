package ai.sparkone.extension.overwrite

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.net.URI
import java.util.Locale
import scala.util.Try

object ManagedHdfsWorkspacePolicy {
  val WorkspaceRootKey = "spark.sparkone.overwrite.workspaceRoot"
  val ReadFormats: Set[String] = Set(
    "parquet", "csv", "json", "orc", "text", "libsvm", "binaryfile", "excel")
  val WriteFormats: Set[String] = Set("parquet", "csv", "json", "orc", "text", "excel")

  private val DefaultWorkspaceRoot = "/public/sparkone/user"
  private val InternalWorkPrefix = ".sparkone-overwrite-"
  private val UsernamePattern = "^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$".r
  private val IdentifierPattern = "^[A-Za-z_][A-Za-z0-9_]*$".r
  private val SensitiveOptionNames = Set(
    "path", "url", "uri", "user", "username", "password", "token",
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

  private[overwrite] def validateRequest(
      tenant: String,
      table: String,
      format: String,
      relativePath: String,
      options: Map[String, String],
      supportedFormats: Set[String],
      operation: String): Unit = {
    if (!UsernamePattern.pattern.matcher(tenant).matches()) {
      throw new IllegalArgumentException(s"Invalid managed HDFS $operation tenant")
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
    val workspaceRoot = spark.conf.getOption(WorkspaceRootKey).getOrElse(DefaultWorkspaceRoot)
    val userRoot = new Path(new Path(workspaceRoot), tenant)
    val candidate = new Path(userRoot, relativePath)
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
}

private[overwrite] final case class ManagedHdfsTarget(fs: FileSystem, finalPath: Path)
