package ai.sparkone.runtime

import ai.sparkone.sql.{CompileException, SparkOneCompiler, SparkSqlValidator}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{Row, SparkSession}

import java.io.File

final class SparkOneRuntime(
    spark: SparkSession,
    compiler: SparkOneCompiler = new SparkOneCompiler(new SparkSqlValidator))
  extends AutoCloseable {

  def compile(script: String): Seq[String] = {
    compiler.compile(script).map(_.sql)
  }

  def run(script: String, limit: Int = 200): RunResult = {
    val compiled = compiler.compile(script)
    val results = compiled.zipWithIndex.map { case (statement, offset) =>
      val started = System.nanoTime()
      try {
        val dataFrame = spark.sql(statement.sql)
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

  override def close(): Unit = {
    spark.stop()
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
}

object SparkOneRuntime {
  def local(): SparkOneRuntime = {
    val builder = SparkSession.builder()
      .appName("SparkOne SQL")
      .master(sys.props.getOrElse("sparkone.master", "local[*]"))
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")

    configureHadoopAndHive(builder)
    configureOptional(builder, "sparkone.jars.packages", "SPARKONE_JARS_PACKAGES", "spark.jars.packages")
    configureOptional(builder, "sparkone.jars", "SPARKONE_JARS", "spark.jars")
    configureOptional(builder, "sparkone.jars.repositories", "SPARKONE_JARS_REPOSITORIES", "spark.jars.repositories")
    configureOptional(builder, "sparkone.kerberos.principal", "SPARKONE_KERBEROS_PRINCIPAL", "spark.kerberos.principal")
    configureOptional(builder, "sparkone.kerberos.keytab", "SPARKONE_KERBEROS_KEYTAB", "spark.kerberos.keytab")

    if (enabled("sparkone.hive.enabled", "SPARKONE_HIVE_ENABLED")) {
      builder.enableHiveSupport()
    }

    val spark = builder.getOrCreate()

    new SparkOneRuntime(spark)
  }

  private def configureOptional(
      builder: SparkSession.Builder,
      propertyName: String,
      envName: String,
      sparkConfName: String): Unit = {
    sys.props.get(propertyName)
      .orElse(sys.env.get(envName))
      .map(_.trim)
      .filter(_.nonEmpty)
      .foreach(value => builder.config(sparkConfName, value))
  }

  private def configureHadoopAndHive(builder: SparkSession.Builder): Unit = {
    val files = hadoopConfFiles() ++ hiveConfFiles()
    if (files.nonEmpty) {
      val conf = new Configuration(false)
      files.distinct.foreach(file => conf.addResource(new Path(file.toURI)))
      conf.iterator().forEachRemaining { entry =>
        builder.config(s"spark.hadoop.${entry.getKey}", entry.getValue)
      }
      UserGroupInformation.setConfiguration(conf)
      loginFromKeytabIfConfigured(conf)
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
    val principal = optionalValue("sparkone.kerberos.principal", "SPARKONE_KERBEROS_PRINCIPAL")
    val keytab = optionalValue("sparkone.kerberos.keytab", "SPARKONE_KERBEROS_KEYTAB")
    (principal, keytab) match {
      case (Some(user), Some(file)) =>
        UserGroupInformation.loginUserFromKeytab(user, requiredFile(file).getAbsolutePath)
      case _ =>
    }
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

  private def splitPaths(value: String): Seq[String] = {
    value.split(File.pathSeparator).toSeq.flatMap(_.split(",")).map(_.trim).filter(_.nonEmpty)
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
