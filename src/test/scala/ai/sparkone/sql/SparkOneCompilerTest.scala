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
  def compilesMysqlLoadFromHoconDatasourceWithoutRenderingCredentials(): Unit = {
    withSystemProperties(Map(
      "sparkone.datasource.mysql.analytics.url" -> "jdbc:mysql://host:3306/app",
      "sparkone.datasource.mysql.analytics.user" -> "reader",
      "sparkone.datasource.mysql.analytics.password" -> "secret",
      "sparkone.datasource.mysql.analytics.option.fetchsize" -> "1000")) {
      val statement = compiler.compile(
        """load mysql.`analytics.users`
          |options numPartitions = "4"
          |as users;
          |""".stripMargin).head

      assertEquals("SELECT 'LOAD MYSQL' AS sparkone_action, 'users AS users' AS sparkone_target", statement.sql)
      assertFalse(statement.sql.contains("secret"))
      assertEquals(Some(LoadTargetType.Mysql), statement.load.map(_.targetType))
      assertEquals(Some("users"), statement.load.map(_.path))
      assertEquals(Some("jdbc:mysql://host:3306/app"), statement.load.flatMap(_.options.get("url")))
      assertEquals(Some("secret"), statement.load.flatMap(_.options.get("password")))
      assertEquals(Some("users"), statement.load.flatMap(_.options.get("dbtable")))
      assertEquals(Some("1000"), statement.load.flatMap(_.options.get("fetchsize")))
      assertEquals(Some("4"), statement.load.flatMap(_.options.get("numPartitions")))
    }
  }

  @Test
  def compilesDorisLoadAsCatalogTableSelect(): Unit = {
    val statement = compiler.compile(
      """load doris.`app.users`
        |as doris_users;
        |""".stripMargin).head

    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW doris_users AS SELECT * FROM doris.app.users",
      statement.sql)
    assertEquals(None, statement.load)
  }

  @Test
  def rejectsDorisLoadOptionsBecauseCatalogIsConfiguredOutsideSql(): Unit = {
    try {
      compiler.compile(
        """load doris.`app.users`
          |options password="leaked"
          |as doris_users;
          |""".stripMargin)
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("LOAD doris does not support SQL OPTIONS"))
    }
  }

  @Test
  def compilesDorisLoadWhereAsCatalogTableSelectFilter(): Unit = {
    val statement = compiler.compile(
      """load doris.`app.orders`
        |where "biz_date = '2026-06-10' and status = 'PAID'"
        |as doris_orders_paid;
        |""".stripMargin).head

    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW doris_orders_paid AS " +
        "SELECT * FROM doris.app.orders WHERE biz_date = '2026-06-10' and status = 'PAID'",
      statement.sql)
    assertEquals(None, statement.load)
  }

  @Test
  def compilesMysqlLoadWhereAsDbtableSubqueryForPartitionedRead(): Unit = {
    withSystemProperties(Map(
      "sparkone.datasource.mysql.analytics.url" -> "jdbc:mysql://host:3306/app",
      "sparkone.datasource.mysql.analytics.user" -> "reader",
      "sparkone.datasource.mysql.analytics.password" -> "secret")) {
      val statement = compiler.compile(
        """load mysql.`analytics.big_orders`
          |where "biz_date = '2026-06-10' and status = 'PAID'"
          |options partitionColumn="id"
          |and lowerBound="1"
          |and upperBound="30000000"
          |and numPartitions="24"
          |and fetchsize="10000"
          |as big_orders_paid;
          |""".stripMargin).head

      val expectedDbtable =
        "(select * from big_orders where biz_date = '2026-06-10' and status = 'PAID') as sparkone_mysql_load"
      assertEquals(
        s"SELECT 'LOAD MYSQL' AS sparkone_action, '${expectedDbtable.replace("'", "''")} AS big_orders_paid' AS sparkone_target",
        statement.sql)
      assertEquals(Some(expectedDbtable), statement.load.flatMap(_.options.get("dbtable")))
      assertEquals(Some("id"), statement.load.flatMap(_.options.get("partitionColumn")))
      assertEquals(Some("1"), statement.load.flatMap(_.options.get("lowerBound")))
      assertEquals(Some("30000000"), statement.load.flatMap(_.options.get("upperBound")))
      assertEquals(Some("24"), statement.load.flatMap(_.options.get("numPartitions")))
      assertEquals(Some("10000"), statement.load.flatMap(_.options.get("fetchsize")))
      assertFalse(statement.sql.contains("secret"))
    }
  }

  @Test
  def compilesMysqlLoadWhereWithoutPartitionOptions(): Unit = {
    withSystemProperties(Map(
      "sparkone.datasource.mysql.analytics.url" -> "jdbc:mysql://host:3306/app",
      "sparkone.datasource.mysql.analytics.user" -> "reader",
      "sparkone.datasource.mysql.analytics.password" -> "secret")) {
      val statement = compiler.compile(
        """load mysql.`analytics.sparkone_mysql_orders_demo`
          |where "biz_date = '2026-06-10' and status = 'PAID'"
          |as orders_paid_0610;
          |""".stripMargin).head

      val expectedDbtable =
        "(select * from sparkone_mysql_orders_demo where biz_date = '2026-06-10' and status = 'PAID') as sparkone_mysql_load"
      assertEquals(Some(expectedDbtable), statement.load.flatMap(_.options.get("dbtable")))
      assertFalse(statement.load.exists(_.options.contains("partitionColumn")))
      assertFalse(statement.sql.contains("secret"))
    }
  }

  @Test
  def compilesMysqlLoadPartitionOptionsWithoutWhere(): Unit = {
    withSystemProperties(Map(
      "sparkone.datasource.mysql.analytics.url" -> "jdbc:mysql://host:3306/app",
      "sparkone.datasource.mysql.analytics.user" -> "reader",
      "sparkone.datasource.mysql.analytics.password" -> "secret")) {
      val statement = compiler.compile(
        """load mysql.`analytics.sparkone_mysql_orders_demo`
          |options partitionColumn="id"
          |and lowerBound="1"
          |and upperBound="8"
          |and numPartitions="4"
          |as orders_partitioned;
          |""".stripMargin).head

      assertEquals(Some("sparkone_mysql_orders_demo"), statement.load.flatMap(_.options.get("dbtable")))
      assertEquals(Some("id"), statement.load.flatMap(_.options.get("partitionColumn")))
      assertEquals(Some("1"), statement.load.flatMap(_.options.get("lowerBound")))
      assertEquals(Some("8"), statement.load.flatMap(_.options.get("upperBound")))
      assertEquals(Some("4"), statement.load.flatMap(_.options.get("numPartitions")))
      assertFalse(statement.sql.contains("secret"))
    }
  }

  @Test
  def rejectsLoadWhereForNonMysqlSources(): Unit = {
    try {
      compiler.compile("""load parquet.`/tmp/users` where "id > 0" as users;""")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("WHERE filter"))
    }
  }

  @Test
  def rejectsMysqlLoadQueryOptionBecauseDbtableIsManagedBySparkOne(): Unit = {
    withSystemProperties(Map(
      "sparkone.datasource.mysql.analytics.url" -> "jdbc:mysql://host:3306/app",
      "sparkone.datasource.mysql.analytics.user" -> "reader",
      "sparkone.datasource.mysql.analytics.password" -> "secret")) {
      try {
        compiler.compile(
          """load mysql.`analytics.big_orders`
            |options query="select * from big_orders where status = 'PAID'"
            |as big_orders_paid;
            |""".stripMargin)
        fail("Expected CompileException")
      } catch {
        case e: CompileException =>
          assertTrue(e.getMessage.contains("query"))
      }
    }
  }

  @Test
  def compilesHiveLoadAsCatalogTableSelect(): Unit = {
    val sql = compiler.compile("load hive.`default.users` as users;").head.sql

    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW users AS SELECT * FROM default.users",
      sql)
  }

  @Test
  def compilesHiveLoadWhereAsCatalogTableSelectFilter(): Unit = {
    val sql = compiler.compile(
      """load hive.`default.users`
        |where "dt = date '2026-06-17' and status = 'active'"
        |as active_users;
        |""".stripMargin).head.sql

    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW active_users AS " +
        "SELECT * FROM default.users WHERE dt = date '2026-06-17' and status = 'active'",
      sql)
  }

  @Test
  def rejectsHiveLoadOptionsUntilCatalogOptionsHaveASparkSqlMapping(): Unit = {
    try {
      compiler.compile("load hive.`default.users` options storage='delta' as users;")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("hive"))
    }
  }

  @Test
  def rejectsWhereAsDslOptionClause(): Unit = {
    try {
      compiler.compile("save overwrite users as parquet.`/tmp/users` where sparkoneOverwrite='allow';")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("OPTIONS"))
    }
  }

  @Test
  def rejectsWhereAsLoadOptionClause(): Unit = {
    try {
      compiler.compile("load csv.`/tmp/users.csv` where header='true' as users;")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("OPTIONS"))
    }
  }

  @Test
  def compilesExcelLoadWithProviderAlias(): Unit = {
    val sql = compiler.compile(
      """load excel.`/tmp/users.xlsx`
        |options header="true"
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
    val sql = compiler.compile("save overwrite users as excel.`/tmp/users.xlsx` options header=true;").head.sql

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
        |options header="true"
        |and delimiter=","
        |and compression="gzip";
        |""".stripMargin).head.sql

    assertEquals(
      "INSERT OVERWRITE DIRECTORY '/tmp/users_csv' USING csv OPTIONS " +
        "(header 'true', delimiter ',', compression 'gzip') SELECT * FROM users",
      sql)
  }

  @Test
  def stripsSparkOneSaveControlOptionsFromProviderOptions(): Unit = {
    val statement = compiler.compile(
      """save overwrite users as csv.`/tmp/users_csv`
        |options header="true"
        |and sparkoneoverwrite="allow"
        |and sparkoneoverwritebackup="trash"
        |and sparkoneoverwritebackuppath="/tmp/sparkone_back";
        |""".stripMargin).head

    assertEquals(
      "INSERT OVERWRITE DIRECTORY '/tmp/users_csv' USING csv OPTIONS " +
        "(header 'true') SELECT * FROM users",
      statement.sql)
    assertEquals(Some("allow"), statement.save.flatMap(_.options.get("sparkoneoverwrite")))
    assertEquals(Some("trash"), statement.save.flatMap(_.options.get("sparkoneoverwritebackup")))
    assertEquals(Some("/tmp/sparkone_back"), statement.save.flatMap(_.options.get("sparkoneoverwritebackuppath")))
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
  def compilesSaveAppendToHiveTable(): Unit = {
    val statement = compiler.compile("save append users as hive.`default.users`;").head

    assertEquals("INSERT INTO TABLE default.users SELECT * FROM users", statement.sql)
    assertEquals(Some("append"), statement.save.map(_.mode))
    assertEquals(Some(SaveTargetType.Catalog), statement.save.map(_.targetType))
  }

  @Test
  def compilesMysqlSaveAsRuntimeAdapter(): Unit = {
    withSystemProperties(Map(
      "sparkone.datasource.mysql.analytics.url" -> "jdbc:mysql://host:3306/app",
      "sparkone.datasource.mysql.analytics.user" -> "writer",
      "sparkone.datasource.mysql.analytics.password" -> "secret")) {
      val statement = compiler.compile(
        """save append users as mysql.`analytics.user_stats`
          |options batchsize="500";
          |""".stripMargin).head

      assertEquals("SELECT 'SAVE MYSQL' AS sparkone_action, 'users TO user_stats' AS sparkone_target", statement.sql)
      assertFalse(statement.sql.contains("secret"))
      assertEquals(Some("append"), statement.save.map(_.mode))
      assertEquals(Some(SaveTargetType.Mysql), statement.save.map(_.targetType))
      assertEquals(Some("user_stats"), statement.save.map(_.path))
      assertEquals(Some("jdbc:mysql://host:3306/app"), statement.save.flatMap(_.targetOptions.get("url")))
      assertEquals(Some("secret"), statement.save.flatMap(_.targetOptions.get("password")))
      assertEquals(Some("user_stats"), statement.save.flatMap(_.targetOptions.get("dbtable")))
      assertEquals(Some("500"), statement.save.flatMap(_.targetOptions.get("batchsize")))
    }
  }

  @Test
  def rejectsJdbcDslInFavorOfMysqlDatasource(): Unit = {
    try {
      compiler.compile("load jdbc.`analytics.users` as users;")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("LOAD jdbc"))
    }

    try {
      compiler.compile("save append users as jdbc.`analytics.users`;")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("SAVE jdbc"))
    }
  }

  @Test
  def compilesSaveOverwriteToHiveTableWithControlOptions(): Unit = {
    val statement = compiler.compile(
      """save overwrite users as hive.`default.users`
        |options sparkoneOverwrite="allow";
        |""".stripMargin).head

    assertEquals("INSERT OVERWRITE TABLE default.users SELECT * FROM users", statement.sql)
    assertEquals(Some("allow"), statement.save.flatMap(_.options.get("sparkoneoverwrite")))
    assertEquals(Some(SaveTargetType.Catalog), statement.save.map(_.targetType))
  }

  @Test
  def compilesSaveAppendToHivePartition(): Unit = {
    val sql = compiler.compile("save append users as hive.`default.users` partitionBy dt, region;").head.sql

    assertEquals("INSERT INTO TABLE default.users PARTITION (dt, region) SELECT * FROM users", sql)
  }

  @Test
  def rejectsProviderOptionsForHiveSaveUntilThereIsASparkSqlMapping(): Unit = {
    try {
      compiler.compile("save append users as hive.`default.users` options file_format='parquet';")
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
        |load excel.`/tmp/users.xlsx` options header="true" as excel_users;
        |view city_stats as select city, count(*) as cnt from users group by city;
        |save overwrite city_stats as parquet.`/tmp/city_stats`;
        |save append city_stats as hive.`default.city_stats` partitionBy dt;
        |""".stripMargin).map(_.sql)

    assertEquals(6, sql.size)
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

  private def withSystemProperties(values: Map[String, String])(body: => Unit): Unit = {
    val previous = values.keys.map(key => key -> sys.props.get(key)).toMap
    values.foreach { case (key, value) => sys.props.put(key, value) }
    try {
      body
    } finally {
      previous.foreach {
        case (key, Some(value)) => sys.props.put(key, value)
        case (key, None) => sys.props.remove(key)
      }
    }
  }
}
