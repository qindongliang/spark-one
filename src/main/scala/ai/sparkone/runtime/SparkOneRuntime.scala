package ai.sparkone.runtime

import ai.sparkone.sql.{CompileException, SparkOneCompiler, SparkSqlValidator}
import org.apache.spark.sql.{Row, SparkSession}

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
    val spark = SparkSession.builder()
      .appName("SparkOne SQL")
      .master(sys.props.getOrElse("sparkone.master", "local[*]"))
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()

    new SparkOneRuntime(spark)
  }
}
