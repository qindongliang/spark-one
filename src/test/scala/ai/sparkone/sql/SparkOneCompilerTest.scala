package ai.sparkone.sql

import org.junit.Assert._
import org.junit.Test

final class SparkOneCompilerTest {
  private val compiler = new SparkOneCompiler()

  @Test
  def compilesBasicPipeline(): Unit = {
    val script =
      """load parquet.`/tmp/users` as users;
        |
        |create or replace temporary view city_stats as
        |select city, count(*) as cnt
        |from users
        |group by city;
        |
        |save overwrite city_stats as parquet.`/tmp/city_stats`;
        |""".stripMargin

    val sql = compiler.compile(script).map(_.sql)

    assertEquals(
      Seq(
        "CREATE OR REPLACE TEMPORARY VIEW users USING parquet OPTIONS (path '/tmp/users')",
        "create or replace temporary view city_stats as\nselect city, count(*) as cnt\nfrom users\ngroup by city",
        "INSERT OVERWRITE DIRECTORY '/tmp/city_stats' USING parquet SELECT * FROM city_stats"),
      sql)
  }

  @Test
  def supportsLoadOptionsWithAndAndSpacedEquals(): Unit = {
    val sql = compiler.compile(
      """load jdbc.`mysql1.user`
        |where url = "jdbc:mysql://host/db"
        |and dbtable='user'
        |as users;
        |""".stripMargin).head.sql

    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW users USING jdbc OPTIONS " +
        "(path 'mysql1.user', url 'jdbc:mysql://host/db', dbtable 'user')",
      sql)
  }

  @Test
  def leavesNativeCreateViewSqlUntouched(): Unit = {
    val sql = compiler.compile(
      """create or replace temporary view result_table as
        |select id as user_id, city
        |from users
        |""".stripMargin).head.sql

    assertEquals(
      "create or replace temporary view result_table as\nselect id as user_id, city\nfrom users",
      sql)
  }

  @Test
  def leavesPlainSparkSqlUntouched(): Unit = {
    val sql = compiler.compile("create table t (id int) using parquet;").head.sql
    assertEquals("create table t (id int) using parquet", sql)
  }

  @Test
  def rejectsSaveModesThatDoNotHaveASafeSparkSqlMappingYet(): Unit = {
    try {
      compiler.compile("save append users as parquet.`/tmp/users`;")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("append"))
    }
  }

  @Test
  def doesNotSplitSemicolonsInsideBackticks(): Unit = {
    val sql = compiler.compile("load text.`/tmp/a;b` as t;").head.sql
    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW t USING text OPTIONS (path '/tmp/a;b')",
      sql)
  }

  @Test
  def generatedSqlIsAcceptedBySparkSqlParser(): Unit = {
    val validatingCompiler = new SparkOneCompiler(new SparkSqlValidator)
    val sql = validatingCompiler.compile(
      """load parquet.`/tmp/users` as users;
        |create or replace temporary view city_stats as select city, count(*) as cnt from users group by city;
        |save overwrite city_stats as parquet.`/tmp/city_stats`;
        |""".stripMargin).map(_.sql)

    assertEquals(3, sql.size)
  }

  @Test
  def sparkSqlValidatorRejectsInvalidSparkSql(): Unit = {
    val validatingCompiler = new SparkOneCompiler(new SparkSqlValidator)

    try {
      validatingCompiler.compile("create table;")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("Spark SQL parser rejected statement"))
    }
  }
}
