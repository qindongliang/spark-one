package ai.sparkone.runtime

import ai.sparkone.sql.{CompileException, SaveControlOptions, SaveStatementMetadata, SaveTargetType}
import org.apache.hadoop.fs.{FileSystem, Path, Trash}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import scala.util.control.NonFatal

final class SaveOverwriteGuard(spark: SparkSession) {
  import SaveOverwriteGuard._

  private val logger = LoggerFactory.getLogger(getClass)
  private val OverwriteProtectedPathsKey = "sparkone.save.overwrite.protected.paths"
  private val AllowMysqlOverwriteKey = "sparkone.save.mysql.overwrite.enabled"

  def prepare(save: Option[SaveStatementMetadata]): Option[OverwritePreparation] = {
    save.filter(_.mode.equalsIgnoreCase("overwrite")).flatMap { metadata =>
      validateMysqlOverwriteAllowed(metadata)
      val policy = effectivePolicy(metadata)
      policy.value match {
        case OverwritePolicy.Deny =>
          logger.warn(s"Safe Save: overwrite denied, path=${metadata.path}, policySource=${policy.source}")
          throw new CompileException(s"SAVE overwrite is denied by SparkOne policy for path: ${metadata.path}")
        case OverwritePolicy.RequireExplicit =>
          logger.warn(
            s"Safe Save: overwrite requires explicit confirmation, path=${metadata.path}, " +
              s"""add ${SaveControlOptions.Overwrite}="allow" to this save statement""")
          throw new CompileException(
            s"SAVE overwrite requires explicit confirmation for path: ${metadata.path}. " +
              s"""Add option ${SaveControlOptions.Overwrite}="allow", or set [save] overwritePolicy = "allow" in TOML.""")
        case OverwritePolicy.Allow =>
          metadata.targetType match {
            case SaveTargetType.File => prepareBackup(metadata, policy)
            case SaveTargetType.Catalog => prepareCatalogOverwrite(metadata, policy)
            case SaveTargetType.Mysql => prepareCatalogOverwrite(metadata, policy)
          }
      }
    }
  }

  private def validateMysqlOverwriteAllowed(metadata: SaveStatementMetadata): Unit = {
    if (metadata.targetType == SaveTargetType.Mysql && !enabledGlobal(AllowMysqlOverwriteKey)) {
      logger.warn(
        s"Safe Save: MySQL overwrite blocked, table=${metadata.table}, target=${metadata.path}, " +
          s"allowMysqlOverwrite=false")
      throw new CompileException(
        s"SAVE overwrite for MySQL is disabled by SparkOne policy for table: ${metadata.path}. " +
          "Set [save] allowMysqlOverwrite = true in TOML, then use option sparkoneOverwrite=\"allow\" for this statement.")
    }
  }

  private def effectivePolicy(metadata: SaveStatementMetadata): ResolvedSetting[OverwritePolicy] = {
    controlOption(metadata, SaveControlOptions.Overwrite) match {
      case Some(value) => ResolvedSetting(OverwritePolicy.parse(value), "statement", value)
      case None =>
        val config = configValue("sparkone.save.overwrite.policy", "requireExplicit")
        ResolvedSetting(OverwritePolicy.parse(config.value), config.source, config.raw)
    }
  }

  private def effectiveBackup(metadata: SaveStatementMetadata): ResolvedSetting[OverwriteBackup] = {
    controlOption(metadata, SaveControlOptions.OverwriteBackup) match {
      case Some(value) => ResolvedSetting(OverwriteBackup.parse(value), "statement", value)
      case None =>
        val config = configValue("sparkone.save.overwrite.backup", "rename")
        ResolvedSetting(OverwriteBackup.parse(config.value), config.source, config.raw)
    }
  }

  private def effectiveBackupPath(metadata: SaveStatementMetadata): ResolvedSetting[String] = {
    controlOption(metadata, SaveControlOptions.OverwriteBackupPath) match {
      case Some(value) => ResolvedSetting(value, "statement", value)
      case None => configValue("sparkone.save.overwrite.backup.path", "/tmp/sparkone_back")
    }
  }

  private def controlOption(metadata: SaveStatementMetadata, key: String): Option[String] = {
    metadata.options.collectFirst {
      case (name, value) if name.equalsIgnoreCase(key) => value
    }
  }

  private def configValue(key: String, defaultValue: String): ResolvedSetting[String] = {
    spark.conf.getOption(key) match {
      case Some(value) => ResolvedSetting(value, "session", value)
      case None =>
        sys.props.get(key) match {
          case Some(value) => ResolvedSetting(value, "global", value)
          case None => ResolvedSetting(defaultValue, "default", defaultValue)
        }
    }
  }

  private def enabledGlobal(key: String): Boolean = {
    sys.props.get(key)
      .exists(value => Set("1", "true", "yes", "on").contains(value.trim.toLowerCase))
  }

  private def prepareBackup(
      metadata: SaveStatementMetadata,
      policy: ResolvedSetting[OverwritePolicy]): Option[OverwritePreparation] = {
    val target = new Path(metadata.path)
    validateTarget(target, metadata.path)

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = target.getFileSystem(conf)
    val qualifiedTarget = target.makeQualified(fs.getUri, fs.getWorkingDirectory)
    validateProtectedPaths(conf, fs, qualifiedTarget, metadata.path)

    val backup = effectiveBackup(metadata)
    val backupRoot =
      if (backup.value == OverwriteBackup.Rename) Some(effectiveBackupPath(metadata))
      else None
    val existsStarted = System.nanoTime()
    val targetExists = fs.exists(target)
    val existsCostMs = elapsedMs(existsStarted)
    logger.info(
      s"Safe Save: plan overwrite, table=${metadata.table}, format=${metadata.format}, " +
        s"rawPath=${metadata.path}, writeTarget=$qualifiedTarget, fs=${fs.getUri}, " +
        s"targetExists=$targetExists, existsCostMs=$existsCostMs, " +
        s"policy=${formatSetting(policy)}, backup=${formatSetting(backup)}" +
        backupRoot.map(root => s", backupRoot=${formatSetting(root)}").getOrElse(""))
    if (!targetExists) {
      return Some(new LogOnlyPreparation(
        writeTarget = qualifiedTarget,
        backup = None,
        trashDir = None,
        commitLevel = CommitLogLevel.Info))
    }

    backup.value match {
      case OverwriteBackup.None =>
        logger.warn(
          s"Safe Save: backup success, action=none, writeTarget=$qualifiedTarget, backup=<none>, costMs=0")
        Some(new LogOnlyPreparation(
          writeTarget = qualifiedTarget,
          backup = None,
          trashDir = None,
          commitLevel = CommitLogLevel.Warn))
      case OverwriteBackup.Trash =>
        if (isLocalFileSystem(fs)) {
          logger.warn(
            s"Safe Save: Hadoop Trash backup is not supported for local file overwrite, " +
              s"rawPath=${metadata.path}, writeTarget=$qualifiedTarget, fs=${fs.getUri}")
          throw new CompileException(
            s"""Safe Save does not support sparkoneOverwriteBackup="trash" for local file path: ${metadata.path}. """ +
              "Use sparkoneOverwriteBackup=\"rename\" for local backup, or use an HDFS path if you need Hadoop Trash.")
        }
        val trash = new Trash(fs, fs.getConf)
        val trashDir = trash.getCurrentTrashDir(target)
        val backupStarted = System.nanoTime()
        val moved =
          try {
            trash.moveToTrash(target)
          } catch {
            case NonFatal(e) =>
              logger.error(
                s"Safe Save: failed to move existing target to Hadoop Trash, " +
                  s"rawPath=${metadata.path}, writeTarget=$qualifiedTarget, trashDir=$trashDir",
                e)
              throw new CompileException(
                s"Safe Save failed to move overwrite target to Hadoop Trash before SAVE. " +
                  s"target=$qualifiedTarget, trashDir=$trashDir, reason=${errorMessage(e)}",
                e)
          }
        val backupCostMs = elapsedMs(backupStarted)
        if (!moved) {
          logger.error(
            s"Safe Save: Hadoop Trash rejected overwrite target, " +
              s"rawPath=${metadata.path}, writeTarget=$qualifiedTarget, trashDir=$trashDir, " +
              s"reason=moveToTrash returned false")
          throw new CompileException(
            s"Safe Save failed to move overwrite target to Hadoop Trash before SAVE. " +
              s"target=$qualifiedTarget, trashDir=$trashDir")
        }
        logger.warn(
          s"Safe Save: backup success, action=trash, writeTarget=$qualifiedTarget, " +
            s"trashDir=$trashDir, costMs=$backupCostMs")
        Some(new LogOnlyPreparation(
          writeTarget = qualifiedTarget,
          backup = None,
          trashDir = Some(trashDir),
          commitLevel = CommitLogLevel.Warn))
      case OverwriteBackup.Rename =>
        val resolvedBackupRoot = backupRoot.get
        val backup = nextBackupPath(conf, fs, target, resolvedBackupRoot.value)
        val backupStarted = System.nanoTime()
        try {
          if (!fs.mkdirs(backup.getParent)) {
            logger.error(
              s"Safe Save: failed to create overwrite backup directory, " +
                s"rawPath=${metadata.path}, writeTarget=$qualifiedTarget, " +
                s"backupRoot=${resolvedBackupRoot.value}, backupParent=${backup.getParent}, " +
                s"reason=mkdirs returned false")
            throw new CompileException(s"Failed to create overwrite backup directory: ${backup.getParent}")
          }
          if (!fs.rename(target, backup)) {
            logger.error(
              s"Safe Save: failed to rename overwrite target to backup, " +
                s"rawPath=${metadata.path}, writeTarget=$qualifiedTarget, " +
                s"backupRoot=${resolvedBackupRoot.value}, backup=$backup, reason=rename returned false")
            throw new CompileException(s"Failed to backup overwrite target from ${metadata.path} to $backup")
          }
        } catch {
          case e: CompileException => throw e
          case NonFatal(e) =>
            logger.error(
              s"Safe Save: failed to rename overwrite target to backup, " +
                s"rawPath=${metadata.path}, writeTarget=$qualifiedTarget, " +
                s"backupRoot=${resolvedBackupRoot.value}, backup=$backup, reason=${errorMessage(e)}",
              e)
            throw new CompileException(
              s"Safe Save failed to rename overwrite target before SAVE. " +
                s"target=$qualifiedTarget, backup=$backup, reason=${errorMessage(e)}",
              e)
        }
        val backupCostMs = elapsedMs(backupStarted)
        logger.warn(
          s"Safe Save: backup success, action=rename, writeTarget=$qualifiedTarget, " +
            s"backup=$backup, costMs=$backupCostMs")
        Some(new RenamePreparation(fs, target, qualifiedTarget, backup))
    }
  }

  private def prepareCatalogOverwrite(
      metadata: SaveStatementMetadata,
      policy: ResolvedSetting[OverwritePolicy]): Option[OverwritePreparation] = {
    val backup = effectiveBackup(metadata)
    logger.warn(
      s"Safe Save: catalog overwrite allowed, table=${metadata.table}, format=${metadata.format}, " +
        s"target=${metadata.path}, policy=${formatSetting(policy)}, " +
        s"backup=${formatSetting(backup)} ignored for catalog target")
    Some(new CatalogPreparation(metadata.path))
  }

  private def validateProtectedPaths(
      conf: org.apache.hadoop.conf.Configuration,
      targetFs: FileSystem,
      qualifiedTarget: Path,
      rawPath: String): Unit = {
    overwriteProtectedPaths.foreach { rawProtectedPath =>
      val configured = new Path(rawProtectedPath)
      val protectedFs =
        if (configured.toUri.getScheme == null) targetFs
        else configured.getFileSystem(conf)
      val qualifiedProtected = configured.makeQualified(protectedFs.getUri, protectedFs.getWorkingDirectory)

      if (sameFileSystem(targetFs, protectedFs) && wouldOverwriteProtectedPath(qualifiedTarget, qualifiedProtected)) {
        logger.error(
          s"Safe Save: overwrite target is blocked by global protected path, " +
            s"rawPath=$rawPath, writeTarget=$qualifiedTarget, protectedPath=$qualifiedProtected")
        throw new CompileException(
          s"Safe Save blocked overwrite target by global protected path. " +
            s"target=$qualifiedTarget, protectedPath=$qualifiedProtected")
      }
    }
  }

  private def overwriteProtectedPaths: Seq[String] = {
    sys.props.get(OverwriteProtectedPathsKey)
      .toSeq
      .flatMap(splitPathList)
  }

  private def splitPathList(value: String): Seq[String] = {
    value.split("[,\\r\\n]+").toSeq.map(_.trim).filter(_.nonEmpty)
  }

  private def wouldOverwriteProtectedPath(target: Path, protectedPath: Path): Boolean = {
    val targetSegments = pathSegments(target)
    val protectedSegments = pathSegments(protectedPath)
    targetSegments.length <= protectedSegments.length &&
      targetSegments.zip(protectedSegments).forall { case (targetSegment, protectedSegment) =>
        protectedSegment == "*" || targetSegment == protectedSegment
      }
  }

  private def normalizedPath(path: Path): String = {
    val raw = Option(path.toUri.getPath).getOrElse("")
    if (raw == "/" || raw.isEmpty) "/" else raw.stripSuffix("/")
  }

  private def pathSegments(path: Path): Seq[String] = {
    normalizedPath(path).split("/").toSeq.filter(_.nonEmpty)
  }

  private def elapsedMs(started: Long): Long = {
    (System.nanoTime() - started) / 1000000L
  }

  private def validateTarget(target: Path, rawPath: String): Unit = {
    val uriPath = Option(target.toUri.getPath).getOrElse("")
    if (uriPath.trim.isEmpty || uriPath == "/") {
      throw new CompileException(s"Refusing to overwrite root path: $rawPath")
    }
  }

  private def nextBackupPath(
      conf: org.apache.hadoop.conf.Configuration,
      fs: FileSystem,
      target: Path,
      backupRoot: String): Path = {
    val name = target.getName
    val timestamp = LocalDateTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val backupDir = resolveBackupRoot(conf, fs, backupRoot)
    val base = new Path(backupDir, s"${name}_$timestamp")

    Iterator.from(0)
      .map {
        case 0 => base
        case index => new Path(backupDir, s"${name}_${timestamp}_$index")
      }
      .find(path => !fs.exists(path))
      .get
  }

  private def resolveBackupRoot(
      conf: org.apache.hadoop.conf.Configuration,
      targetFs: FileSystem,
      rawPath: String): Path = {
    val configured = new Path(rawPath)
    val backupFs =
      if (configured.toUri.getScheme == null) targetFs
      else configured.getFileSystem(conf)

    if (!sameFileSystem(targetFs, backupFs)) {
      throw new CompileException(
        s"Safe Save rename backup path must be on the same filesystem as the overwrite target. " +
          s"targetFs=${targetFs.getUri}, backupFs=${backupFs.getUri}, backupRoot=$rawPath")
    }

    configured.makeQualified(targetFs.getUri, targetFs.getWorkingDirectory)
  }

  private def sameFileSystem(left: FileSystem, right: FileSystem): Boolean = {
    Option(left.getUri.getScheme).map(_.toLowerCase) == Option(right.getUri.getScheme).map(_.toLowerCase) &&
      Option(left.getUri.getAuthority).map(_.toLowerCase) == Option(right.getUri.getAuthority).map(_.toLowerCase)
  }

  private def isLocalFileSystem(fs: FileSystem): Boolean = {
    fs.getUri.getScheme == "file"
  }

  private def errorMessage(error: Throwable): String = {
    Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getName)
  }

  private def formatSetting(setting: ResolvedSetting[_]): String = {
    s"${setting.value}(${setting.source})"
  }
}

object SaveOverwriteGuard {
  private final case class ResolvedSetting[T](value: T, source: String, raw: String)

  private sealed trait CommitLogLevel

  private object CommitLogLevel {
    case object Info extends CommitLogLevel
    case object Warn extends CommitLogLevel
  }

  sealed trait OverwritePreparation {
    def commit(): Unit
    def rollback(): Unit
  }

  private final class LogOnlyPreparation(
      writeTarget: Path,
      backup: Option[Path],
      trashDir: Option[Path],
      commitLevel: CommitLogLevel) extends OverwritePreparation {
    private val logger = LoggerFactory.getLogger(getClass)

    override def commit(): Unit = {
      val message = s"Safe Save: commit success, writeTarget=$writeTarget${backupFields(backup, trashDir)}"
      commitLevel match {
        case CommitLogLevel.Info => logger.info(message)
        case CommitLogLevel.Warn => logger.warn(message)
      }
    }

    override def rollback(): Unit = {
      logger.warn(
        s"Safe Save: rollback requested, no managed backup to restore, " +
          s"writeTarget=$writeTarget${backupFields(backup, trashDir)}")
    }
  }

  private final class RenamePreparation(
      fs: FileSystem,
      target: Path,
      writeTarget: Path,
      backup: Path) extends OverwritePreparation {
    private val logger = LoggerFactory.getLogger(getClass)

    override def commit(): Unit = {
      logger.warn(s"Safe Save: commit success, writeTarget=$writeTarget, backup=$backup")
    }

    override def rollback(): Unit = {
      if (!fs.exists(backup)) {
        logger.warn(s"Safe Save: rollback skipped because backup no longer exists, backup=$backup")
        return
      }
      if (fs.exists(target)) {
        logger.warn(s"Safe Save: rollback removing failed target before restore, writeTarget=$writeTarget")
        val trashed = new Trash(fs, fs.getConf).moveToTrash(target)
        if (!trashed && !fs.delete(target, true)) {
          logger.warn(s"Safe Save: rollback could not remove failed target path, writeTarget=$writeTarget")
          return
        }
      }
      if (fs.rename(backup, target)) {
        logger.warn(s"Safe Save: rollback restored backup, backup=$backup, writeTarget=$writeTarget")
      } else {
        logger.warn(s"Safe Save: rollback failed to restore backup, backup=$backup, writeTarget=$writeTarget")
      }
    }
  }

  private final class CatalogPreparation(targetTable: String) extends OverwritePreparation {
    private val logger = LoggerFactory.getLogger(getClass)

    override def commit(): Unit = {
      logger.warn(s"Safe Save: catalog overwrite commit success, target=$targetTable")
    }

    override def rollback(): Unit = {
      logger.warn(s"Safe Save: catalog overwrite rollback is not available, target=$targetTable")
    }
  }

  private def backupFields(backup: Option[Path], trashDir: Option[Path]): String = {
    backup.map(path => s", backup=$path")
      .orElse(trashDir.map(path => s", trashDir=$path"))
      .getOrElse(", backup=<none>")
  }

  sealed trait OverwritePolicy

  object OverwritePolicy {
    case object Allow extends OverwritePolicy
    case object RequireExplicit extends OverwritePolicy
    case object Deny extends OverwritePolicy

    def parse(value: String): OverwritePolicy = {
      normalize(value) match {
        case "allow" | "true" | "on" | "yes" => Allow
        case "requireexplicit" | "require" | "confirm" | "explicit" => RequireExplicit
        case "deny" | "false" | "off" | "no" => Deny
        case other => throw new CompileException(
          s"Invalid SparkOne overwrite policy '$value'. Supported values: allow, requireExplicit, deny.")
      }
    }
  }

  sealed trait OverwriteBackup

  object OverwriteBackup {
    case object Rename extends OverwriteBackup
    case object Trash extends OverwriteBackup
    case object None extends OverwriteBackup

    def parse(value: String): OverwriteBackup = {
      normalize(value) match {
        case "rename" | "backup" => Rename
        case "trash" => Trash
        case "none" | "off" | "false" | "no" => None
        case other => throw new CompileException(
          s"Invalid SparkOne overwrite backup '$value'. Supported values: rename, trash, none.")
      }
    }
  }

  private def normalize(value: String): String = {
    value.trim.toLowerCase.replace("-", "").replace("_", "")
  }
}
