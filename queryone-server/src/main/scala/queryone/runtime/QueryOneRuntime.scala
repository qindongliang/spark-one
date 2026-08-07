package queryone.runtime

import queryone.extension.hdfs.QueryOneHdfsWorkspaceExtensions
import queryone.identity.TenantContext
import queryone.kyuubi.odep.authz.{LocalExecutionSubject, QueryOneLocalOdepAuthzExtension}
import queryone.sql.{CatalogWriteSqlRenderer, CompileException, CompiledStatement, LoadStatementMetadata, SetStatementMetadata, SetValueType, QueryOneCompiler, SparkSqlValidator, WriteTargetKind}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import java.io.File
import java.net.URLClassLoader
import scala.collection.mutable
import scala.util.control.NonFatal

final class QueryOneRuntime(
    spark: SparkSession,
    compiler: QueryOneCompiler = new QueryOneCompiler(new SparkSqlValidator),
    driverClassLoader: Option[ClassLoader] = None)
  extends AutoCloseable {

  private val logger = LoggerFactory.getLogger(getClass)
  private val runLock = new AnyRef

  def compile(script: String): Seq[String] = {
    compile(QueryOneRuntime.RuntimeTenant, script)
  }

  def compile(tenant: TenantContext, script: String): Seq[String] = {
    compiler.compile(tenant, script).map(_.sql)
  }

  def run(
      script: String,
      limit: Int = PreviewConfig.current.maxRows): RunResult = {
    run(QueryOneRuntime.RuntimeTenant, script, limit)
  }

  def run(tenant: TenantContext, script: String, limit: Int): RunResult = runLock.synchronized {
    LocalExecutionSubject.withSubject(spark.sparkContext, tenant.username) {
      withDriverClassLoader {
      val previewLimit = PreviewConfig.current.clampRows(Some(limit))
      val sources = compiler.splitStatements(script)
      val variables = mutable.LinkedHashMap[String, String]()
      val results = mutable.ArrayBuffer.empty[StatementResult]
      val remaining = sources.iterator.zipWithIndex
      var executionDecision: ExecutionDecision = ExecutionDecision.Continue
      while (executionDecision == ExecutionDecision.Continue && remaining.hasNext) {
        val (source, offset) = remaining.next()
        val started = System.nanoTime()
        var statement: Option[CompiledStatement] = None
        val result = try {
          val compiledStatement = compiler.compileStatementWithVariables(tenant, source, variables.toMap)
          statement = Some(compiledStatement)
          val executableStatement = prepareWriteStatement(compiledStatement)
          statement = Some(executableStatement)
          val dataFrame = execute(executableStatement, variables)
          val shouldCollectRows = executableStatement.load.isEmpty && executableStatement.set.isEmpty
          val collected =
            if (shouldCollectRows) dataFrame.limit(previewLimit + 1).collect().toSeq
            else Seq.empty[Row]
          val visibleRows = collected.take(previewLimit).map(rowToStrings)
          val assertion = executableStatement.assertion.map { plan =>
            val status =
              if (collected.isEmpty) AssertionStatus.Passed
              else AssertionStatus.Failed
            AssertionResult(
              plan.table,
              plan.predicate,
              status,
              plan.message,
              plan.failureAction.name)
          }
          val assertionPassed = assertion.forall(_.status == AssertionStatus.Passed)
          if (!assertionPassed) {
            executableStatement.assertion.foreach { plan =>
              logger.warn(
                s"Assertion failed, statement=${offset + 1}, table=${plan.table}, " +
                  s"failureAction=${plan.failureAction.name}, " +
                  s"predicate=${summarizeSql(plan.predicate)}, message=${summarizeSql(plan.message)}")
            }
          }
          StatementResult(
            index = offset + 1,
            source = executableStatement.source,
            sql = executableStatement.sql,
            success = assertionPassed,
            schema = schemaInfo(dataFrame),
            rows = visibleRows,
            rowCount = visibleRows.size,
            truncated = shouldCollectRows && collected.size > previewLimit,
            previewTable = executableStatement.load.map(_.table),
            durationMs = elapsedMs(started),
            error = executableStatement.assertion.filter(_ => !assertionPassed).map(_.message),
            assertion = assertion)
        } catch {
          case e: Exception =>
            val sourceSummary = statement.map(_.source).getOrElse(source)
            val sqlSummary = statement.map(_.sql).getOrElse(source)
            logger.error(
              s"Statement ${offset + 1} failed, source=${summarizeSql(sourceSummary)}, " +
                s"sql=${summarizeSql(sqlSummary)}, reason=${errorMessage(e)}",
              e)
            StatementResult(
              index = offset + 1,
              source = sourceSummary,
              sql = sqlSummary,
              success = false,
              schema = Nil,
              rows = Nil,
              rowCount = 0,
              truncated = false,
              previewTable = None,
              durationMs = elapsedMs(started),
              error = Some(errorMessage(e)),
              assertion = statement.flatMap(_.assertion).map { plan =>
                AssertionResult(
                  plan.table,
                  plan.predicate,
                  AssertionStatus.Error,
                  plan.message,
                  plan.failureAction.name)
              })
        }
        results += result
        executionDecision = ExecutionDecision.from(result)
      }

      val success = executionDecision != ExecutionDecision.StopAsFailure
      val outcome = RunOutcome.from(executionDecision, results.lastOption)
      val stoppedEarly = executionDecision != ExecutionDecision.Continue && remaining.hasNext
      val visibleResults =
        if (success && results.nonEmpty && results.forall(_.previewTable.nonEmpty)) results.takeRight(1)
        else results.toSeq
      RunResult(success, visibleResults, outcome, stoppedEarly)
      }
    }
  }

  def previewTable(
      table: String,
      limit: Int = PreviewConfig.current.maxRows): StatementResult = {
    previewTable(QueryOneRuntime.RuntimeTenant, table, limit)
  }

  def previewTable(
      tenant: TenantContext,
      table: String,
      limit: Int): StatementResult = runLock.synchronized {
    LocalExecutionSubject.withSubject(spark.sparkContext, tenant.username) {
      withDriverClassLoader {
      val started = System.nanoTime()
      val previewLimit = PreviewConfig.current.clampRows(Some(limit))
      val dataFrame = spark.table(table)
      val collected = dataFrame.limit(previewLimit + 1).collect().toSeq
      val visibleRows = collected.take(previewLimit).map(rowToStrings)
      StatementResult(
        index = 1,
        source = table,
        sql = s"TABLE $table",
        success = true,
        schema = schemaInfo(dataFrame),
        rows = visibleRows,
        rowCount = visibleRows.size,
        truncated = collected.size > previewLimit,
        previewTable = Some(table),
        durationMs = elapsedMs(started),
        error = None)
      }
    }
  }

  override def close(): Unit = {
    spark.stop()
  }

  private def execute(
      statement: CompiledStatement,
      variables: mutable.Map[String, String]): DataFrame = {
    statement.set match {
      case Some(metadata) =>
        executeSet(metadata, variables)
      case None => statement.load match {
      case Some(metadata) =>
        executeLoad(statement, metadata)
      case _ =>
        spark.sql(statement.sql)
      }
    }
  }

  private def executeSet(
      metadata: SetStatementMetadata,
      variables: mutable.Map[String, String]): DataFrame = {
    val value = metadata.valueType match {
      case SetValueType.Literal =>
        metadata.value
      case SetValueType.Sql =>
        val resultHead = spark.sql(metadata.value).limit(1).collect().headOption
        resultHead.flatMap(row => Option(row.get(0))).map(_.toString).getOrElse("")
    }
    variables(metadata.key) = value
    spark.emptyDataFrame
  }

  private def executeLoad(statement: CompiledStatement, metadata: LoadStatementMetadata): DataFrame = {
    spark.sql(statement.sql)
    spark.table(metadata.table)
  }

  private def prepareWriteStatement(statement: CompiledStatement): CompiledStatement = {
    statement.writePlan match {
      case Some(plan) =>
        plan.target.kind match {
          case WriteTargetKind.HiveCatalog | WriteTargetKind.DorisCatalog | WriteTargetKind.JdbcCatalog =>
            val target = try {
              spark.table(plan.target.identifier)
            } catch {
              case NonFatal(e) =>
                throw new CompileException(
                  s"SAVE target table does not exist or cannot be resolved: ${plan.target.identifier}. " +
                    s"Create the target table explicitly before SAVE ${plan.mode.name}.",
                  e)
            }
            val sourceColumns = spark.table(plan.sourceTable).schema.fieldNames.toSeq
            val targetColumns = target.schema.fieldNames.toSeq
            val sql = CatalogWriteSqlRenderer.render(plan, sourceColumns, targetColumns)
            spark.sql(s"EXPLAIN $sql").collect()
            statement.copy(sql = sql)
          case _ =>
            statement
        }
      case None => statement
    }
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

  private def schemaInfo(dataFrame: DataFrame): Seq[FieldInfo] = {
    dataFrame.schema.fields.map { field =>
      FieldInfo(field.name, field.dataType.simpleString, field.nullable)
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

object QueryOneRuntime {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val HadoopStaticGroupOverrides = "hadoop.user.group.static.mapping.overrides"
  private val RuntimeTenant = TenantContext.development("local-runtime")
  private val OdepRoutingCatalogClass = "queryone.kyuubi.odep.catalog.OdepRoutingCatalog"

  def local(): QueryOneRuntime = {
    val master = sys.props.getOrElse("spark.master", "local[*]")

    val builder = SparkSession.builder()
      .appName("QueryOne SQL")
      .master(master)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .config("spark.sql.catalog.jdbc", OdepRoutingCatalogClass)
      .config("spark.sql.catalog.doris", OdepRoutingCatalogClass)
      .withExtensions(new QueryOneHdfsWorkspaceExtensions().apply)
      .withExtensions(new QueryOneLocalOdepAuthzExtension().apply)

    configureDriverNetwork(builder, master)
    configureHadoopAndHive(builder)
    configureSparkProperty(builder, "spark.jars.packages")
    configureSparkProperty(builder, "spark.jars")
    configureSparkProperty(builder, "spark.files")
    configureSparkProperty(builder, "spark.jars.repositories")
    configureSparkProperty(builder, "spark.kerberos.principal")
    configureSparkProperty(builder, "spark.kerberos.keytab")
    configureSparkProperty(builder, "spark.sql.defaultCatalog")
    configureSparkPropertiesWithPrefix(builder, "spark.sql.catalog.")
    configureSparkPropertiesWithPrefix(builder, "spark.queryone.overwrite.")
    val driverClassLoader = configureDriverClasspathFromSparkJars()

    if (enabled("queryone.hive.enabled", "QUERYONE_HIVE_ENABLED")) {
      builder.enableHiveSupport()
    }

    val spark = builder.getOrCreate()
    refreshUserGroupInformation(spark.sparkContext.hadoopConfiguration)

    new QueryOneRuntime(spark, driverClassLoader = driverClassLoader)
  }

  private def configureSparkProperty(builder: SparkSession.Builder, propertyName: String): Unit = {
    sys.props.get(propertyName)
      .map(_.trim)
      .filter(_.nonEmpty)
      .foreach(value => builder.config(propertyName, value))
  }

  private def configureSparkPropertiesWithPrefix(builder: SparkSession.Builder, prefix: String): Unit = {
    sys.props.toSeq
      .filter { case (key, value) => key.startsWith(prefix) && value.trim.nonEmpty }
      .sortBy(_._1)
      .foreach { case (key, value) => builder.config(key, value.trim) }
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
    optionalValue("queryone.hadoop.group.static.mapping.overrides", "QUERYONE_HADOOP_GROUP_STATIC_MAPPING_OVERRIDES") match {
      case Some(value) =>
        conf.set(HadoopStaticGroupOverrides, value)
      case None if Option(conf.getTrimmed(HadoopStaticGroupOverrides)).forall(_.isEmpty) =>
        kerberosShortName().foreach(user => conf.set(HadoopStaticGroupOverrides, s"$user=$user"))
      case None =>
    }
  }

  private def hadoopConfFiles(): Seq[File] = {
    val fromDir = optionalValue("queryone.hadoop.conf.dir", "HADOOP_CONF_DIR")
      .toSeq
      .flatMap(confDirFiles(_, Seq("core-site.xml", "hdfs-site.xml", "yarn-site.xml", "mapred-site.xml")))

    val explicit = optionalValue("queryone.hadoop.conf.files", "QUERYONE_HADOOP_CONF_FILES")
      .toSeq
      .flatMap(splitPaths)
      .map(requiredFile)

    fromDir ++ explicit
  }

  private def hiveConfFiles(): Seq[File] = {
    val explicit = optionalValue("queryone.hive.conf.file", "QUERYONE_HIVE_CONF_FILE")
      .toSeq
      .map(requiredFile)

    val fromDir = optionalValue("queryone.hive.conf.dir", "HIVE_CONF_DIR")
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
