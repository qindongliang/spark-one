package ai.queryone.kyuubi.odep.authz

import java.net.URI
import java.util.Locale

private[authz] final case class OdepAuthzResource(
    resourceType: String,
    database: Option[String],
    table: Option[String],
    path: Option[String],
    action: String) {

  def displayName: String = path.getOrElse(s"${database.get}.${table.get}")
}

private[authz] object OdepAuthzResource {
  val Read = "read"
  val Write = "write"

  def table(resourceType: String, database: String, table: String, action: String): OdepAuthzResource = {
    val normalizedType = requireNonBlank(resourceType, "resource type").toLowerCase(Locale.ROOT)
    if (!Set("jdbc", "doris", "hive").contains(normalizedType)) {
      throw new OdepAuthorizationException(s"Unsupported authorization resource type: $resourceType")
    }
    OdepAuthzResource(
      normalizedType,
      Some(requireNonBlank(database, "database")),
      Some(requireNonBlank(table, "table")),
      None,
      requireAction(action))
  }

  def hdfs(rawPath: String, action: String): OdepAuthzResource = {
    val value = requireNonBlank(rawPath, "HDFS path")
    val uri = try {
      new URI(value)
    } catch {
      case error: Exception =>
        throw new OdepAuthorizationException(s"Invalid HDFS path: $value", error)
    }
    Option(uri.getScheme).map(_.toLowerCase(Locale.ROOT)).foreach { scheme =>
      if (scheme != "hdfs" && scheme != "viewfs") {
        throw new OdepAuthorizationException(s"Unsupported filesystem scheme: $scheme")
      }
    }
    val path = normalizePath(uri.getPath)
    OdepAuthzResource("hdfs", None, None, Some(path), requireAction(action))
  }

  private def normalizePath(rawPath: String): String = {
    var path = requireNonBlank(rawPath, "HDFS path")
    if (!path.startsWith("/")) {
      throw new OdepAuthorizationException("HDFS authorization requires an absolute path")
    }
    while (path.length > 1 && path.endsWith("/")) {
      path = path.substring(0, path.length - 1)
    }
    if (path.split("/", -1).exists(segment => segment == "." || segment == "..")) {
      throw new OdepAuthorizationException("HDFS path cannot contain . or .. segments")
    }
    path
  }

  private def requireAction(action: String): String = {
    val normalized = requireNonBlank(action, "action").toLowerCase(Locale.ROOT)
    if (normalized != Read && normalized != Write) {
      throw new OdepAuthorizationException(s"Unsupported authorization action: $action")
    }
    normalized
  }

  private def requireNonBlank(value: String, label: String): String = {
    if (value == null || value.trim.isEmpty) {
      throw new OdepAuthorizationException(s"$label must not be blank")
    }
    value.trim
  }
}

private[authz] final case class OdepAuthzResult(
    allowed: Boolean,
    denied: Seq[(OdepAuthzResource, String)])

final class OdepAuthorizationException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)
