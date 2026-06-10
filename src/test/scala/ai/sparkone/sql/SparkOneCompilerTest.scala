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
        |view city_stats as
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
        "CREATE OR REPLACE TEMPORARY VIEW city_stats AS select city, count(*) as cnt\nfrom users\ngroup by city",
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
  def compilesHiveLoadAsCatalogTableSelect(): Unit = {
    val sql = compiler.compile("load hive.`default.users` as users;").head.sql

    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW users AS SELECT * FROM default.users",
      sql)
  }

  @Test
  def rejectsHiveLoadOptionsUntilCatalogOptionsHaveASparkSqlMapping(): Unit = {
    try {
      compiler.compile("load hive.`default.users` where storage='delta' as users;")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("hive"))
    }
  }

  @Test
  def compilesExcelLoadWithProviderAlias(): Unit = {
    val sql = compiler.compile(
      """load excel.`/tmp/users.xlsx`
        |where header="true"
        |and dataAddress="'Sheet1'!A1"
        |as users;
        |""".stripMargin).head.sql

    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW users USING excel OPTIONS " +
        "(path '/tmp/users.xlsx', header 'true', dataAddress '''Sheet1''!A1')",
      sql)
  }

  @Test
  def compilesExcelSaveWithProviderAlias(): Unit = {
    val sql = compiler.compile("save overwrite users as excel.`/tmp/users.xlsx` where header=true;").head.sql

    assertEquals(
      "INSERT OVERWRITE DIRECTORY '/tmp/users.xlsx' USING excel OPTIONS " +
        "(header 'true') SELECT * FROM users",
      sql)
  }

  @Test
  def compilesSaveOverwriteAsParquet(): Unit = {
    val sql = compiler.compile("save overwrite city_stats as parquet.`/tmp/city_stats`;").head.sql

    assertEquals(
      "INSERT OVERWRITE DIRECTORY '/tmp/city_stats' USING parquet SELECT * FROM city_stats",
      sql)
  }

  @Test
  def compilesSaveOverwriteWithOptions(): Unit = {
    val sql = compiler.compile(
      """save overwrite users as csv.`/tmp/users_csv`
        |where header="true"
        |and delimiter=","
        |and compression="gzip";
        |""".stripMargin).head.sql

    assertEquals(
      "INSERT OVERWRITE DIRECTORY '/tmp/users_csv' USING csv OPTIONS " +
        "(header 'true', delimiter ',', compression 'gzip') SELECT * FROM users",
      sql)
  }

  @Test
  def compilesSaveOverwriteWithLocalFilePath(): Unit = {
    val sql = compiler.compile("save overwrite users as json.`file:///tmp/users_json`;").head.sql

    assertEquals(
      "INSERT OVERWRITE DIRECTORY 'file:///tmp/users_json' USING json SELECT * FROM users",
      sql)
  }

  @Test
  def compilesViewThenSavePipeline(): Unit = {
    val sql = compiler.compile(
      """view active_users as
        |select *
        |from users
        |where status = 'active';
        |
        |save overwrite active_users as parquet.`/tmp/active_users`;
        |""".stripMargin).map(_.sql)

    assertEquals(
      Seq(
        "CREATE OR REPLACE TEMPORARY VIEW active_users AS select *\nfrom users\nwhere status = 'active'",
        "INSERT OVERWRITE DIRECTORY '/tmp/active_users' USING parquet SELECT * FROM active_users"),
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
  def compilesViewAsSelectSugarToTemporaryView(): Unit = {
    val sql = compiler.compile(
      """view result_table as
        |select id as user_id, city
        |from users
        |where id > 0
        |""".stripMargin).head.sql

    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW result_table AS select id as user_id, city\nfrom users\nwhere id > 0",
      sql)
  }

  @Test
  def compilesViewAsWithSelectSugarToTemporaryView(): Unit = {
    val sql = compiler.compile(
      """view result_table as
        |with city_stats as (
        |  select city, count(*) as cnt
        |  from users
        |  group by city
        |)
        |select * from city_stats
        |where cnt > 0
        |""".stripMargin).head.sql

    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW result_table AS " +
        "with city_stats as (\n  select city, count(*) as cnt\n  from users\n  group by city\n)\nselect * from city_stats\nwhere cnt > 0",
      sql)
  }

  @Test
  def compilesViewAsProjectionToTemporaryView(): Unit = {
    val sql = compiler.compile("view result_table as select 1 as id;").head.sql
    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW result_table AS select 1 as id",
      sql)
  }

  @Test
  def compilesViewAsMultipleColumnsToTemporaryView(): Unit = {
    val sql = compiler.compile("view myview as select current_date() as dt, current_timestamp() as ts;").head.sql
    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW myview AS select current_date() as dt, current_timestamp() as ts",
      sql)
  }

  @Test
  def leavesFormerTailAsTableSugarUntouched(): Unit = {
    val sql = compiler.compile("select 1 as id as result_table;").head.sql
    assertEquals("select 1 as id as result_table", sql)
  }

  @Test
  def validatingCompilerRejectsFormerTailAsTableSugar(): Unit = {
    val validatingCompiler = new SparkOneCompiler(new SparkSqlValidator)

    try {
      validatingCompiler.compile("select 1 as id as result_table;")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("Spark SQL parser rejected statement"))
    }
  }

  @Test
  def leavesNativeSelectColumnAliasUntouched(): Unit = {
    val sql = compiler.compile("select 1 as id;").head.sql
    assertEquals("select 1 as id", sql)
  }

  @Test
  def leavesNativeSelectTableAliasUntouched(): Unit = {
    val sql = compiler.compile("select * from users as u;").head.sql
    assertEquals("select * from users as u", sql)
  }

  @Test
  def compilesViewAsWithNativeJoinAliases(): Unit = {
    val sql = compiler.compile(
      """view joined_orders as
        |select u.id, o.order_id
        |from users as u
        |join orders as o on u.id = o.user_id
        |where o.amount > 0
        |""".stripMargin).head.sql

    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW joined_orders AS " +
        "select u.id, o.order_id\nfrom users as u\njoin orders as o on u.id = o.user_id\nwhere o.amount > 0",
      sql)
  }

  @Test
  def prefersNativeSparkSqlWhenFinalAsIsATableAlias(): Unit = {
    val sql = compiler.compile("select * from users as u;").head.sql
    assertEquals("select * from users as u", sql)
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
  def rejectsSaveToCatalogSources(): Unit = {
    try {
      compiler.compile("save overwrite users as hive.`default.users`;")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("catalog"))
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
        |load hive.`default.source_users` as source_users;
        |load excel.`/tmp/users.xlsx` where header="true" as excel_users;
        |view city_stats as select city, count(*) as cnt from users group by city;
        |save overwrite city_stats as parquet.`/tmp/city_stats`;
        |""".stripMargin).map(_.sql)

    assertEquals(5, sql.size)
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
