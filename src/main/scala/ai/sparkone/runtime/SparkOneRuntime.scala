package ai.sparkone.runtime

import ai.sparkone.sql.{CompileException, CompiledStatement, LoadStatementMetadata, LoadTargetType, SaveStatementMetadata, SaveTargetType, SparkOneCompiler, SparkSqlValidator}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.io.File
import java.net.URLClassLoader

final class SparkOneRuntime(
    spark: SparkSession,
    compiler: SparkOneCompiler = new SparkOneCompiler(new SparkSqlValidator),
    driverClassLoader: Option[ClassLoader] = None)
  extends AutoCloseable {

  private val logger = LoggerFactory.getLogger(getClass)
  private val runLock = new AnyRef
  private val saveOverwriteGuard = new SaveOverwriteGuard(spark)
  private val nativeSqlSafetyGuard = new NativeSqlSafetyGuard

  def compile(script: String): Seq[String] = {
    compiler.compile(script).map(_.sql)
  }

  def run(script: String, limit: Int = 200): RunResult = runLock.synchronized {
    withDriverClassLoader {
      val compiled = compiler.compile(script)
      val results = compiled.zipWithIndex.map { case (statement, offset) =>
        val started = System.nanoTime()
        var preparation: Option[SaveOverwriteGuard.OverwritePreparation] = None
        try {
          nativeSqlSafetyGuard.validate(statement)
          preparation = saveOverwriteGuard.prepare(statement.save)
          val dataFrame = execute(statement)
          preparation.foreach(_.commit())
          val schema = dataFrame.schema.fields.map { field =>
            FieldInfo(field.name, field.dataType.simpleString, field.nullable)
          }
          val collected = dataFrame.limit(limit + 1).collect().toSeq
          val visibleRows = collected.take(limit).map(rowToStrings)
          StatementResult(
            index = offset + 1,
            source = statement.source,
            sql = statement.sql,
            success = true,
            schema = schema,
            rows = visibleRows,
            rowCount = visibleRows.size,
            truncated = collected.size > limit,
            durationMs = elapsedMs(started),
            error = None)
        } catch {
          case e: Exception =>
            preparation.foreach(_.rollback())
            logger.error(
              s"Statement ${offset + 1} failed, sql=${summarizeSql(statement.sql)}, reason=${errorMessage(e)}",
              e)
            StatementResult(
              index = offset + 1,
              source = statement.source,
              sql = statement.sql,
              success = false,
              schema = Nil,
              rows = Nil,
              rowCount = 0,
              truncated = false,
              durationMs = elapsedMs(started),
              error = Some(errorMessage(e)))
        }
      }

      RunResult(results.forall(_.success), results)
    }
  }

  override def close(): Unit = {
    spark.stop()
  }

  private def execute(statement: CompiledStatement): DataFrame = {
    statement.load match {
      case Some(metadata) if metadata.targetType == LoadTargetType.Mysql =>
        executeMysqlLoad(metadata)
      case _ =>
        statement.save match {
          case Some(metadata) if metadata.targetType == SaveTargetType.Mysql =>
            executeMysqlSave(metadata)
          case _ =>
            spark.sql(statement.sql)
        }
    }
  }

  private def executeMysqlLoad(metadata: LoadStatementMetadata): DataFrame = {
    spark.read
      .format("jdbc")
      .options(metadata.options)
      .load()
      .createOrReplaceTempView(metadata.table)
    actionResult("LOAD MYSQL", metadata.path, metadata.table)
  }

  private def executeMysqlSave(metadata: SaveStatementMetadata): DataFrame = {
    val mode = metadata.mode.toLowerCase match {
      case "append" => SaveMode.Append
      case "overwrite" => SaveMode.Overwrite
      case other => throw new CompileException(s"SAVE mode '$other' is not supported for mysql source")
    }
    val started = System.nanoTime()
    logger.info(
      s"MySQL Save: start, mode=${metadata.mode}, source=${metadata.table}, target=${metadata.path}")
    spark.table(metadata.table)
      .write
      .format("jdbc")
      .options(metadata.targetOptions)
      .mode(mode)
      .save()
    logger.info(
      s"MySQL Save: success, mode=${metadata.mode}, source=${metadata.table}, target=${metadata.path}, costMs=${elapsedMs(started)}")
    actionResult("SAVE MYSQL", metadata.path, metadata.table)
  }

  private def actionResult(action: String, target: String, table: String): DataFrame = {
    import spark.implicits._
    Seq((action, target, table)).toDF("action", "target", "table")
  }

  private def withDriverClassLoader[T](body: => T): T = {
    driverClassLoader match {
      case Some(classLoader) =>
        val thread = Thread.currentThread()
        val previous = thread.getContextClassLoader
        thread.setContextClassLoader(classLoader)
        try {
          body
        } finally {
          thread.setContextClassLoader(previous)
        }
      case None =>
        body
    }
  }

  private def rowToStrings(row: Row): Seq[String] = {
    row.toSeq.map {
      case null => null
      case value => value.toString
    }
  }

  private def elapsedMs(started: Long): Long = {
    (System.nanoTime() - started) / 1000000L
  }

  private def errorMessage(error: Throwable): String = {
    Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getName)
  }

  private def summarizeSql(sql: String): String = {
    val normalized = sql.replaceAll("\\s+", " ").trim
    if (normalized.length <= 240) normalized else normalized.take(237) + "..."
  }
}

private final class NativeSqlSafetyGuard {
  import NativeSqlSafetyGuard._

  private val logger = LoggerFactory.getLogger(getClass)

  def validate(statement: ai.sparkone.sql.CompiledStatement): Unit = {
    if (statement.save.isEmpty && containsInsertOverwrite(statement.sql) && !isNativeInsertOverwriteEnabled) {
      logger.warn(
        s"Safe Save: native INSERT OVERWRITE blocked, " +
          s"allowNativeInsertOverwrite=false, sql=${summarizeSql(statement.sql)}")
      throw new CompileException(
        "Native Spark SQL INSERT OVERWRITE is disabled by SparkOne Safe Save policy. " +
          "Use SparkOne DSL `save overwrite ...` so overwrite protection can run, " +
          "or set save.allowNativeInsertOverwrite = true in HOCON for compatibility.")
    }
    if (statement.save.isEmpty && containsDropTable(statement.sql) && !isNativeDropTableEnabled) {
      logger.warn(
        s"Safe Save: native DROP TABLE blocked, " +
          s"allowNativeDropTable=false, sql=${summarizeSql(statement.sql)}")
      throw new CompileException(
        "Native Spark SQL DROP TABLE is disabled by SparkOne DDL safety policy. " +
          "Set save.allowNativeDropTable = true in HOCON only when the deployment explicitly allows table drops.")
    }
  }

  private def isNativeInsertOverwriteEnabled: Boolean = {
    sys.props.get(AllowNativeInsertOverwriteKey)
      .exists(value => Set("1", "true", "yes", "on").contains(value.trim.toLowerCase))
  }

  private def isNativeDropTableEnabled: Boolean = {
    sys.props.get(AllowNativeDropTableKey)
      .exists(value => Set("1", "true", "yes", "on").contains(value.trim.toLowerCase))
  }

  private def summarizeSql(sql: String): String = {
    val normalized = sql.replaceAll("\\s+", " ").trim
    if (normalized.length <= 240) normalized else normalized.take(237) + "..."
  }
}

private object NativeSqlSafetyGuard {
  private val AllowNativeInsertOverwriteKey = "sparkone.save.native.insertOverwrite.enabled"
  private val AllowNativeDropTableKey = "sparkone.save.native.dropTable.enabled"

  private[runtime] def containsInsertOverwrite(sql: String): Boolean = {
    val tokens = sqlKeywordTokens(sql).map(_.toLowerCase)
    tokens.sliding(2).exists {
      case Seq("insert", "overwrite") => true
      case _ => false
    }
  }

  private[runtime] def containsDropTable(sql: String): Boolean = {
    val tokens = sqlKeywordTokens(sql).map(_.toLowerCase)
    tokens.sliding(2).exists {
      case Seq("drop", "table") => true
      case _ => false
    }
  }

  private def sqlKeywordTokens(sql: String): Seq[String] = {
    val tokens = scala.collection.mutable.ArrayBuffer.empty[String]
    var index = 0

    while (index < sql.length) {
      val current = sql.charAt(index)
      if (current == '-' && hasNext(sql, index, '-')) {
        index = skipLineComment(sql, index + 2)
      } else if (current == '/' && hasNext(sql, index, '*')) {
        index = skipBlockComment(sql, index + 2)
      } else if (current == '\'' || current == '"') {
        index = skipQuotedString(sql, index, current)
      } else if (current == '`') {
        index = skipBackquotedIdentifier(sql, index)
      } else if (current.isLetter || current == '_') {
        val start = index
        index += 1
        while (index < sql.length && (sql.charAt(index).isLetterOrDigit || sql.charAt(index) == '_')) {
          index += 1
        }
        tokens += sql.substring(start, index)
      } else {
        index += 1
      }
    }

    tokens.toSeq
  }

  private def hasNext(sql: String, index: Int, expected: Char): Boolean = {
    index + 1 < sql.length && sql.charAt(index + 1) == expected
  }

  private def skipLineComment(sql: String, index: Int): Int = {
    val end = sql.indexWhere(ch => ch == '\n' || ch == '\r', index)
    if (end < 0) sql.length else end + 1
  }

  private def skipBlockComment(sql: String, index: Int): Int = {
    val end = sql.indexOf("*/", index)
    if (end < 0) sql.length else end + 2
  }

  private def skipQuotedString(sql: String, index: Int, quote: Char): Int = {
    var cursor = index + 1
    while (cursor < sql.length) {
      val current = sql.charAt(cursor)
      if (current == '\\') {
        cursor += 2
      } else if (current == quote) {
        return cursor + 1
      } else {
        cursor += 1
      }
    }
    sql.length
  }

  private def skipBackquotedIdentifier(sql: String, index: Int): Int = {
    var cursor = index + 1
    while (cursor < sql.length) {
      if (sql.charAt(cursor) == '`') {
        if (hasNext(sql, cursor, '`')) {
          cursor += 2
        } else {
          return cursor + 1
        }
      } else {
        cursor += 1
      }
    }
    sql.length
  }
}

object SparkOneRuntime {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val HadoopStaticGroupOverrides = "hadoop.user.group.static.mapping.overrides"

  def local(): SparkOneRuntime = {
    val master = sys.props.getOrElse("spark.master", "local[*]")

    val builder = SparkSession.builder()
      .appName("SparkOne SQL")
      .master(master)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")

    configureDriverNetwork(builder, master)
    configureHadoopAndHive(builder)
    configureSparkProperty(builder, "spark.jars.packages")
    configureSparkProperty(builder, "spark.jars")
    configureSparkProperty(builder, "spark.files")
    configureSparkProperty(builder, "spark.jars.repositories")
    configureSparkProperty(builder, "spark.kerberos.principal")
    configureSparkProperty(builder, "spark.kerberos.keytab")
    val driverClassLoader = configureDriverClasspathFromSparkJars()

    if (enabled("sparkone.hive.enabled", "SPARKONE_HIVE_ENABLED")) {
      builder.enableHiveSupport()
    }

    val spark = builder.getOrCreate()
    refreshUserGroupInformation(spark.sparkContext.hadoopConfiguration)

    new SparkOneRuntime(spark, driverClassLoader = driverClassLoader)
  }

  private def configureSparkProperty(builder: SparkSession.Builder, propertyName: String): Unit = {
    sys.props.get(propertyName)
      .map(_.trim)
      .filter(_.nonEmpty)
      .foreach(value => builder.config(propertyName, value))
  }

  private def configureDriverNetwork(builder: SparkSession.Builder, master: String): Unit = {
    val driverBindAddress = sparkProperty("spark.driver.bindAddress")
      .orElse(if (isLocalMaster(master)) Some("127.0.0.1") else None)
    val driverHost = sparkProperty("spark.driver.host")
      .orElse(if (isLocalMaster(master)) driverBindAddress else None)

    driverBindAddress.foreach(builder.config("spark.driver.bindAddress", _))
    driverHost.foreach(builder.config("spark.driver.host", _))
  }

  private def isLocalMaster(master: String): Boolean = {
    master.toLowerCase.startsWith("local")
  }

  private def configureDriverClasspathFromSparkJars(): Option[ClassLoader] = {
    val jars = optionalValue("spark.jars", "SPARK_JARS")
      .toSeq
      .flatMap(splitCommaSeparated)
      .flatMap(toLocalJar)

    if (jars.nonEmpty) {
      val parent = Thread.currentThread().getContextClassLoader
      val classLoader = new URLClassLoader(jars.map(_.toURI.toURL).toArray, parent)
      Thread.currentThread().setContextClassLoader(classLoader)
      logger.info(s"Added local Spark jars to driver classloader: ${jars.map(_.getAbsolutePath).mkString(", ")}")
      Some(classLoader)
    } else {
      None
    }
  }

  private def toLocalJar(path: String): Option[File] = {
    val file =
      if (path.startsWith("file:")) new File(java.net.URI.create(path))
      else new File(path)

    if (file.isFile) {
      Some(file)
    } else {
      logger.warn(s"Ignoring spark.jars entry because it is not a local file: $path")
      None
    }
  }

  private def configureHadoopAndHive(builder: SparkSession.Builder): Unit = {
    configureKrb5Conf()
    val files = hadoopConfFiles() ++ hiveConfFiles()
    if (files.nonEmpty) {
      val conf = new Configuration(false)
      files.distinct.foreach(file => conf.addResource(new Path(file.toURI)))
      configureStaticGroupMapping(conf)
      conf.iterator().forEachRemaining { entry =>
        builder.config(s"spark.hadoop.${entry.getKey}", entry.getValue)
      }
      UserGroupInformation.setConfiguration(conf)
      loginFromKeytabIfConfigured(conf)
      logHadoopSecurity(conf, files.distinct)
    }
  }

  private def refreshUserGroupInformation(conf: Configuration): Unit = {
    configureStaticGroupMapping(conf)
    UserGroupInformation.setConfiguration(conf)
    loginFromKeytabIfConfigured(conf)
    logger.info("Refreshed Hadoop UserGroupInformation from SparkContext HadoopConf")
    logger.info(s"SparkContext Hadoop security authentication: ${Option(conf.get("hadoop.security.authentication")).getOrElse("<unset>")}")
    logger.info(s"UGI security enabled after SparkContext start: ${UserGroupInformation.isSecurityEnabled}")
    logger.info(s"UGI login user after SparkContext start: ${safeUser(UserGroupInformation.getLoginUser)}")
    logger.info(s"UGI current user after SparkContext start: ${safeUser(UserGroupInformation.getCurrentUser)}")
  }

  private def configureStaticGroupMapping(conf: Configuration): Unit = {
    optionalValue("sparkone.hadoop.group.static.mapping.overrides", "SPARKONE_HADOOP_GROUP_STATIC_MAPPING_OVERRIDES") match {
      case Some(value) =>
        conf.set(HadoopStaticGroupOverrides, value)
      case None if Option(conf.getTrimmed(HadoopStaticGroupOverrides)).forall(_.isEmpty) =>
        kerberosShortName().foreach(user => conf.set(HadoopStaticGroupOverrides, s"$user=$user"))
      case None =>
    }
  }

  private def hadoopConfFiles(): Seq[File] = {
    val fromDir = optionalValue("sparkone.hadoop.conf.dir", "HADOOP_CONF_DIR")
      .toSeq
      .flatMap(confDirFiles(_, Seq("core-site.xml", "hdfs-site.xml", "yarn-site.xml", "mapred-site.xml")))

    val explicit = optionalValue("sparkone.hadoop.conf.files", "SPARKONE_HADOOP_CONF_FILES")
      .toSeq
      .flatMap(splitPaths)
      .map(requiredFile)

    fromDir ++ explicit
  }

  private def hiveConfFiles(): Seq[File] = {
    val explicit = optionalValue("sparkone.hive.conf.file", "SPARKONE_HIVE_CONF_FILE")
      .toSeq
      .map(requiredFile)

    val fromDir = optionalValue("sparkone.hive.conf.dir", "HIVE_CONF_DIR")
      .toSeq
      .flatMap(confDirFiles(_, Seq("hive-site.xml")))

    explicit ++ fromDir
  }

  private def confDirFiles(path: String, names: Seq[String]): Seq[File] = {
    val dir = requiredDirectory(path)
    names.map(name => new File(dir, name)).filter(_.isFile)
  }

  private def loginFromKeytabIfConfigured(conf: Configuration): Unit = {
    val principal = sparkProperty("spark.kerberos.principal")
    val keytab = sparkProperty("spark.kerberos.keytab")
    (principal, keytab) match {
      case (Some(user), Some(file)) =>
        val keytab = requiredFile(file).getAbsolutePath
        UserGroupInformation.loginUserFromKeytab(user, keytab)
        logger.info(s"Logged in Hadoop user from keytab, principal=$user, keytab=$keytab")
      case _ =>
    }
  }

  private def configureKrb5Conf(): Unit = {
    sys.props.get("java.security.krb5.conf")
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(requiredFile)
      .foreach { file =>
        sys.props.put("java.security.krb5.conf", file.getAbsolutePath)
        logger.info(s"Using Kerberos krb5.conf: ${file.getAbsolutePath}")
      }
  }

  private def logHadoopSecurity(conf: Configuration, files: Seq[File]): Unit = {
    logger.info(s"Loaded Hadoop/Hive config files: ${files.map(_.getAbsolutePath).mkString(", ")}")
    logger.info(s"Hadoop security authentication: ${Option(conf.get("hadoop.security.authentication")).getOrElse("<unset>")}")
    logger.info(s"Hadoop static group overrides: ${Option(conf.get(HadoopStaticGroupOverrides)).getOrElse("<unset>")}")
    logger.info(s"UGI security enabled: ${UserGroupInformation.isSecurityEnabled}")
    logger.info(s"UGI login user: ${safeUser(UserGroupInformation.getLoginUser)}")
    logger.info(s"UGI current user: ${safeUser(UserGroupInformation.getCurrentUser)}")
  }

  private def safeUser(user: UserGroupInformation): String = {
    Option(user).map(_.toString).getOrElse("<unset>")
  }

  private def enabled(propertyName: String, envName: String): Boolean = {
    optionalValue(propertyName, envName).exists(value =>
      Set("1", "true", "yes", "on").contains(value.toLowerCase))
  }

  private def optionalValue(propertyName: String, envName: String): Option[String] = {
    sys.props.get(propertyName)
      .orElse(sys.env.get(envName))
      .map(_.trim)
      .filter(_.nonEmpty)
  }

  private def kerberosShortName(): Option[String] = {
    sparkProperty("spark.kerberos.principal")
      .map(_.takeWhile(_ != '@').takeWhile(_ != '/').trim)
      .filter(_.nonEmpty)
  }

  private def sparkProperty(propertyName: String): Option[String] = {
    sys.props.get(propertyName).map(_.trim).filter(_.nonEmpty)
  }

  private def splitPaths(value: String): Seq[String] = {
    value.split(File.pathSeparator).toSeq.flatMap(_.split(",")).map(_.trim).filter(_.nonEmpty)
  }

  private def splitCommaSeparated(value: String): Seq[String] = {
    value.split(",").toSeq.map(_.trim).filter(_.nonEmpty)
  }

  private def requiredDirectory(path: String): File = {
    val dir = new File(path)
    if (!dir.isDirectory) {
      throw new IllegalArgumentException(s"Configuration directory does not exist: $path")
    }
    dir
  }

  private def requiredFile(path: String): File = {
    val file = new File(path)
    if (!file.isFile) {
      throw new IllegalArgumentException(s"Configuration file does not exist: $path")
    }
    file
  }
}
