package ai.sparkone.extension.overwrite

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryOneTime
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.zookeeper.{CreateMode, KeeperException}
import org.slf4j.LoggerFactory

import java.net.URI
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.{Locale, UUID}
import java.util.concurrent.TimeUnit
import scala.util.Try
import scala.util.control.NonFatal

private[overwrite] object ManagedHdfsOverwriteExecutor {
  val WorkspaceRootKey = "spark.sparkone.overwrite.workspaceRoot"
  val ZooKeeperConnectKey = "spark.sparkone.overwrite.zk.connect"
  val ZooKeeperRootKey = "spark.sparkone.overwrite.zk.root"
  val ZooKeeperSessionTimeoutMsKey = "spark.sparkone.overwrite.zk.sessionTimeoutMs"
  val ZooKeeperConnectionTimeoutMsKey = "spark.sparkone.overwrite.zk.connectionTimeoutMs"

  private val DefaultWorkspaceRoot = "/public/sparkone/user"
  private val DefaultZooKeeperRoot = "/sparkone/overwrite"
  private val DefaultSessionTimeoutMs = 60000
  private val DefaultConnectionTimeoutMs = 15000
  private val MaxReadableLockPathLength = 96
  private val UsernamePattern = "^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$".r
  private val IdentifierPattern = "^[A-Za-z_][A-Za-z0-9_]*$".r
  private val SupportedFormats = Set("parquet", "csv", "json", "orc", "text", "excel")
  private val SensitiveOptionNames = Set(
    "path", "url", "uri", "user", "username", "password", "token",
    "accesskey", "accesskeyid", "secretkey", "secretaccesskey", "credential", "credentials")
  private val logger = LoggerFactory.getLogger(getClass)

  def execute(spark: SparkSession, request: ManagedHdfsOverwriteRequest): Unit = {
    validate(request)
    val paths = resolvePaths(spark, request)
    val operationId = UUID.randomUUID().toString
    val lock = ZooKeeperTargetLock(
      connect = requiredConf(spark, ZooKeeperConnectKey),
      root = spark.conf.getOption(ZooKeeperRootKey).getOrElse(DefaultZooKeeperRoot),
      key = targetLockKey(request.tenant, request.relativePath, paths.finalPath.toString),
      data = lockData(operationId, paths.finalPath.toString),
      sessionTimeoutMs = intConf(spark, ZooKeeperSessionTimeoutMsKey, DefaultSessionTimeoutMs),
      connectionTimeoutMs = intConf(spark, ZooKeeperConnectionTimeoutMsKey, DefaultConnectionTimeoutMs))

    lock.acquire()
    try {
      logger.info(
        s"Managed HDFS overwrite started, operationId=$operationId, tenant=${request.tenant}, " +
          s"source=${request.sourceTable}, target=${paths.finalPath}, staging=${paths.stagingPath}")
      recoverPreviousAttempt(paths)
      writeStaging(spark, request, paths.stagingPath)
      publish(paths)
      logger.info(
        s"Managed HDFS overwrite succeeded, operationId=$operationId, tenant=${request.tenant}, " +
          s"target=${paths.finalPath}")
    } catch {
      case NonFatal(e) =>
        logger.error(
          s"Managed HDFS overwrite failed, operationId=$operationId, tenant=${request.tenant}, " +
            s"source=${request.sourceTable}, target=${paths.finalPath}, staging=${paths.stagingPath}",
          e)
        cleanupKnownFailure(paths)
        throw e
    } finally {
      lock.close()
    }
  }

  private def validate(request: ManagedHdfsOverwriteRequest): Unit = {
    if (!UsernamePattern.pattern.matcher(request.tenant).matches()) {
      throw new IllegalArgumentException("Invalid managed HDFS overwrite tenant")
    }
    if (!IdentifierPattern.pattern.matcher(request.sourceTable).matches()) {
      throw new IllegalArgumentException("Managed HDFS overwrite source must be a simple temporary view name")
    }
    val normalizedFormat = request.format.toLowerCase(Locale.ROOT)
    if (!SupportedFormats.contains(normalizedFormat)) {
      throw new IllegalArgumentException(s"Managed HDFS overwrite format is not supported: ${request.format}")
    }
    if (!isManagedRelativePath(request.relativePath)) {
      throw new IllegalArgumentException("Managed HDFS overwrite requires a validated relative path")
    }
    request.options.keys.foreach { key =>
      val normalized = key.replaceAll("[^A-Za-z0-9]", "").toLowerCase(Locale.ROOT)
      if (SensitiveOptionNames.contains(normalized) || key.toLowerCase(Locale.ROOT).startsWith("fs.")) {
        throw new IllegalArgumentException(s"Managed HDFS overwrite option is not allowed: $key")
      }
    }
  }

  private def isManagedRelativePath(path: String): Boolean = {
    val trimmed = path.trim
    if (trimmed.isEmpty || trimmed != path || trimmed.startsWith("/") || trimmed.contains("\\")) {
      false
    } else {
      val uri = Try(new URI(trimmed)).toOption
      val segments = trimmed.split("/", -1)
      uri.exists { value =>
        value.getScheme == null && value.getAuthority == null && value.getQuery == null &&
          value.getFragment == null && value.getPath == trimmed &&
          segments.forall(segment => segment.nonEmpty && segment != "." && segment != "..")
      }
    }
  }

  private def resolvePaths(
      spark: SparkSession,
      request: ManagedHdfsOverwriteRequest): OverwritePaths = {
    val workspaceRoot = spark.conf.getOption(WorkspaceRootKey).getOrElse(DefaultWorkspaceRoot)
    val userRoot = new Path(new Path(workspaceRoot), request.tenant)
    val candidate = new Path(userRoot, request.relativePath)
    val fs = candidate.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val qualifiedUserRoot = fs.makeQualified(userRoot)
    val finalPath = fs.makeQualified(candidate)
    requireDescendant(qualifiedUserRoot, finalPath)
    val parent = finalPath.getParent
    if (parent == null) {
      throw new IllegalArgumentException("Managed HDFS overwrite target must have a parent directory")
    }
    val workPath = new Path(parent, s".sparkone-overwrite-${sha256(finalPath.toString).take(24)}")
    OverwritePaths(
      fs,
      finalPath,
      workPath,
      new Path(workPath, "staging"),
      new Path(workPath, "backup"))
  }

  private def requireDescendant(root: Path, child: Path): Unit = {
    val rootUri = root.toUri.normalize()
    val childUri = child.toUri.normalize()
    val rootPath = rootUri.getPath.stripSuffix("/")
    if (rootUri.getScheme != childUri.getScheme || rootUri.getAuthority != childUri.getAuthority ||
        !childUri.getPath.startsWith(rootPath + "/")) {
      throw new IllegalArgumentException("Managed HDFS overwrite target escapes the tenant workspace")
    }
  }

  private def writeStaging(
      spark: SparkSession,
      request: ManagedHdfsOverwriteRequest,
      stagingPath: Path): Unit = {
    spark.table(request.sourceTable)
      .write
      .format(request.format.toLowerCase(Locale.ROOT))
      .options(request.options)
      .mode(SaveMode.Overwrite)
      .save(stagingPath.toString)
  }

  private def recoverPreviousAttempt(paths: OverwritePaths): Unit = {
    if (paths.fs.exists(paths.backupPath)) {
      if (paths.fs.exists(paths.finalPath)) {
        deleteRequired(paths.fs, paths.backupPath)
      } else {
        renameRequired(paths.fs, paths.backupPath, paths.finalPath, "restore previous backup")
      }
    }
    if (paths.fs.exists(paths.workPath)) {
      deleteRequired(paths.fs, paths.workPath)
    }
  }

  private def publish(paths: OverwritePaths): Unit = {
    var originalMoved = false
    try {
      if (paths.fs.exists(paths.finalPath)) {
        renameRequired(paths.fs, paths.finalPath, paths.backupPath, "move current target to backup")
        originalMoved = true
      }
      renameRequired(paths.fs, paths.stagingPath, paths.finalPath, "publish staging target")
    } catch {
      case NonFatal(e) =>
        if (originalMoved && !paths.fs.exists(paths.finalPath) && paths.fs.exists(paths.backupPath)) {
          renameRequired(paths.fs, paths.backupPath, paths.finalPath, "restore target after publish failure")
        }
        throw e
    }

    deleteBestEffort(paths.fs, paths.backupPath)
    deleteBestEffort(paths.fs, paths.workPath)
  }

  private def cleanupKnownFailure(paths: OverwritePaths): Unit = {
    try {
      if (!paths.fs.exists(paths.finalPath) && paths.fs.exists(paths.backupPath)) {
        renameRequired(paths.fs, paths.backupPath, paths.finalPath, "restore target after overwrite failure")
      }
      deleteBestEffort(paths.fs, paths.workPath)
    } catch {
      case NonFatal(cleanupError) =>
        logger.error(
          s"Managed HDFS overwrite cleanup failed, target=${paths.finalPath}, workPath=${paths.workPath}",
          cleanupError)
    }
  }

  private def renameRequired(fs: FileSystem, source: Path, target: Path, action: String): Unit = {
    if (!fs.rename(source, target)) {
      throw new IllegalStateException(s"Managed HDFS overwrite could not $action: $source -> $target")
    }
  }

  private def deleteRequired(fs: FileSystem, path: Path): Unit = {
    if (fs.exists(path) && !fs.delete(path, true)) {
      throw new IllegalStateException(s"Managed HDFS overwrite could not delete stale path: $path")
    }
  }

  private def deleteBestEffort(fs: FileSystem, path: Path): Unit = {
    try {
      if (fs.exists(path) && !fs.delete(path, true)) {
        logger.warn(s"Managed HDFS overwrite left cleanup path: $path")
      }
    } catch {
      case NonFatal(e) => logger.warn(s"Managed HDFS overwrite could not clean path: $path", e)
    }
  }

  private def requiredConf(spark: SparkSession, key: String): String = {
    spark.conf.getOption(key).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new IllegalArgumentException(s"Managed HDFS overwrite requires Spark config: $key")
    }
  }

  private def intConf(spark: SparkSession, key: String, defaultValue: Int): Int = {
    spark.conf.getOption(key).map(_.trim.toInt).filter(_ > 0).getOrElse(defaultValue)
  }

  private[overwrite] def targetLockKey(
      tenant: String,
      relativePath: String,
      qualifiedTarget: String): String = {
    val readablePath = relativePath.iterator.map {
      case '/' => '~'
      case char if (char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z') ||
          (char >= '0' && char <= '9') || "._-~".contains(char) => char
      case _ => '_'
    }.mkString.take(MaxReadableLockPathLength)
    s"$tenant/$readablePath--${sha256(qualifiedTarget)}"
  }

  private[overwrite] def lockData(operationId: String, qualifiedTarget: String): String =
    s"operationId=$operationId\ntarget=$qualifiedTarget"

  private def sha256(value: String): String = {
    MessageDigest.getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
  }
}

private final case class OverwritePaths(
    fs: FileSystem,
    finalPath: Path,
    workPath: Path,
    stagingPath: Path,
    backupPath: Path)

private[overwrite] final case class ZooKeeperTargetLock(
    connect: String,
    root: String,
    key: String,
    data: String,
    sessionTimeoutMs: Int,
    connectionTimeoutMs: Int) extends AutoCloseable {

  private val normalizedRoot = "/" + root.split("/").filter(_.nonEmpty).mkString("/")
  private val path = s"$normalizedRoot/$key"
  private var client: CuratorFramework = _
  private var acquired = false

  def acquire(): Unit = {
    client = CuratorFrameworkFactory.newClient(
      connect,
      sessionTimeoutMs,
      connectionTimeoutMs,
      new RetryOneTime(1000))
    client.start()
    if (!client.blockUntilConnected(connectionTimeoutMs, TimeUnit.MILLISECONDS)) {
      close()
      throw new IllegalStateException("Managed HDFS overwrite could not connect to ZooKeeper")
    }
    try {
      client.create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL)
        .forPath(path, data.getBytes(StandardCharsets.UTF_8))
      acquired = true
    } catch {
      case _: KeeperException.NodeExistsException =>
        val owner = currentOwnerSummary()
        close()
        throw new IllegalStateException(
          s"Managed HDFS overwrite is already running: lockPath=$path, owner=$owner")
      case NonFatal(e) =>
        close()
        throw e
    }
  }

  private def currentOwnerSummary(): String = {
    try {
      Option(client.getData.forPath(path))
        .map(bytes => new String(bytes, StandardCharsets.UTF_8))
        .map(_.split("\\r?\\n").iterator.map(_.trim).filter(_.nonEmpty).mkString(", "))
        .filter(_.nonEmpty)
        .getOrElse("unavailable")
    } catch {
      case _: KeeperException.NoNodeException => "released-before-owner-read"
      case NonFatal(_) => "unavailable"
    }
  }

  override def close(): Unit = {
    if (client != null) {
      try {
        if (acquired) {
          client.delete().forPath(path)
        }
      } catch {
        case NonFatal(_) =>
      } finally {
        client.close()
        client = null
        acquired = false
      }
    }
  }
}
