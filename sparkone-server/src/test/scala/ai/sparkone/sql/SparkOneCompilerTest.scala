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
        |select * from city_stats;
        |""".stripMargin

    val sql = compiler.compile(script).map(_.sql)

    assertEquals(
      Seq(
        "CREATE OR REPLACE TEMPORARY VIEW users USING parquet OPTIONS (path '/tmp/users')",
        "CREATE OR REPLACE TEMPORARY VIEW city_stats AS select city, count(*) as cnt\nfrom users\ngroup by city",
        "select * from city_stats"),
      sql)
  }

  @Test
  def compilesLiteralSetAndSubstitutesLaterStatements(): Unit = {
    val sql = compiler.compile(
      """set biz_date = "2026-03-14";
        |select '${biz_date}' as dt;
        |""".stripMargin).map(_.sql)

    assertEquals(
      Seq(
        "SELECT 'SET' AS sparkone_action, 'biz_date' AS sparkone_target",
        "select '2026-03-14' as dt"),
      sql)
  }

  @Test
  def compilesSetAsSelectAsRuntimeSqlVariable(): Unit = {
    val statement = compiler.compile(
      """set start_date as select date_sub(current_date(), 1) as dt;
        |""".stripMargin).head

    assertEquals("SELECT 'SET' AS sparkone_action, 'start_date' AS sparkone_target", statement.sql)
    assertEquals(Some("start_date"), statement.set.map(_.key))
    assertEquals(Some("select date_sub(current_date(), 1) as dt"), statement.set.map(_.value))
    assertEquals(Some(SetValueType.Sql), statement.set.map(_.valueType))
  }

  @Test
  def rejectsLegacySetWhereTypeSqlSyntax(): Unit = {
    try {
      compiler.compile("""set start_date = `select current_date() as dt` where type = "sql";""")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("set name as select"))
    }
  }

  @Test
  def rejectsNativeSparkSet(): Unit = {
    val error = tryCompile("set spark.sql.shuffle.partitions=8;")
    assertTrue(error.getMessage.contains("native read-only SQL"))
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
  def givesHelpfulMessageForMysqlLoadMissingTargetAlias(): Unit = {
    val validatingCompiler = new SparkOneCompiler(new SparkSqlValidator)

    try {
      validatingCompiler.compile("load mysql.`Dworks.sparkone_city_result`;")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage, e.getMessage.contains("SparkOne LOAD requires a target temp view"))
        assertTrue(e.getMessage, e.getMessage.contains("load mysql.`Dworks.sparkone_city_result` as sparkone_city_result"))
        assertTrue(e.getMessage, e.getMessage.contains("Add `as sparkone_city_result`"))
        assertFalse(e.getMessage, e.getMessage.contains("select * from mysql"))
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
    assertEquals(Some("doris_users"), statement.load.map(_.table))
    assertEquals(Some("doris"), statement.load.map(_.format))
    assertEquals(Some("doris.app.users"), statement.load.map(_.path))
    assertEquals(Some(LoadTargetType.Provider), statement.load.map(_.targetType))
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
    assertEquals(Some("doris_orders_paid"), statement.load.map(_.table))
    assertEquals(
      Some("doris.app.orders WHERE biz_date = '2026-06-10' and status = 'PAID'"),
      statement.load.map(_.path))
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
  def compilesMysqlLoadPartitionColumnOnlyForRuntimeAutoBounds(): Unit = {
    withSystemProperties(Map(
      "sparkone.datasource.mysql.analytics.url" -> "jdbc:mysql://host:3306/app",
      "sparkone.datasource.mysql.analytics.user" -> "reader",
      "sparkone.datasource.mysql.analytics.password" -> "secret")) {
      val statement = compiler.compile(
        """load mysql.`analytics.big_orders`
          |where "biz_date = '2026-06-10' and status = 'PAID'"
          |options partitionColumn="id"
          |as big_orders_paid;
          |""".stripMargin).head

      val expectedDbtable =
        "(select * from big_orders where biz_date = '2026-06-10' and status = 'PAID') as sparkone_mysql_load"
      assertEquals(Some(expectedDbtable), statement.load.flatMap(_.options.get("dbtable")))
      assertEquals(Some("id"), statement.load.flatMap(_.options.get("partitionColumn")))
      assertFalse(statement.load.exists(_.options.contains("lowerBound")))
      assertFalse(statement.load.exists(_.options.contains("upperBound")))
      assertFalse(statement.load.exists(_.options.contains("numPartitions")))
      assertFalse(statement.load.exists(_.options.contains("fetchsize")))
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
    val statement = compiler.compile("load hive.`default.users` as users;").head

    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW users AS SELECT * FROM default.users",
      statement.sql)
    assertEquals(Some("users"), statement.load.map(_.table))
    assertEquals(Some("hive"), statement.load.map(_.format))
    assertEquals(Some("default.users"), statement.load.map(_.path))
  }

  @Test
  def compilesHiveLoadWhereAsCatalogTableSelectFilter(): Unit = {
    val statement = compiler.compile(
      """load hive.`default.users`
        |where "dt = date '2026-06-17' and status = 'active'"
        |as active_users;
        |""".stripMargin).head

    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW active_users AS " +
        "SELECT * FROM default.users WHERE dt = date '2026-06-17' and status = 'active'",
      statement.sql)
    assertEquals(Some("active_users"), statement.load.map(_.table))
    assertEquals(
      Some("default.users WHERE dt = date '2026-06-17' and status = 'active'"),
      statement.load.map(_.path))
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
      compiler.compile("save overwrite users as parquet.`/tmp/users` where header='true';")
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
    val statement = compiler.compile(
      """load excel.`/tmp/users.xlsx`
        |options header="true"
        |and dataAddress="'Sheet1'!A1"
        |as users;
        |""".stripMargin).head

    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW users USING excel OPTIONS " +
        "(path '/tmp/users.xlsx', header 'true', dataAddress '''Sheet1''!A1')",
      statement.sql)
    assertEquals(Some("users"), statement.load.map(_.table))
    assertEquals(Some("/tmp/users.xlsx"), statement.load.map(_.path))
    assertEquals(Some("true"), statement.load.flatMap(_.options.get("header")))
  }

  @Test
  def rejectsFileOverwriteUntilManagedHdfsStagingExecutorExists(): Unit = {
    val external = tryCompile("save overwrite users as excel.`/tmp/users.xlsx` options header=true;")
    assertTrue(external.getMessage.contains("external-path"))
    assertTrue(external.getMessage.contains("permanently denied"))

    val managed = tryCompile("save overwrite users as parquet.`reports/daily`;")
    assertTrue(managed.getMessage.contains("staging overwrite executor"))
  }

  @Test
  def permanentlyRejectsFileAppendForManagedAndExternalPaths(): Unit = {
    Seq(
      "save append users as parquet.`reports/daily`;" -> "managed-hdfs",
      "save append users as parquet.`s3a://bucket/reports/daily`;" -> "external-path").foreach {
      case (sql, targetKind) =>
        val error = tryCompile(sql)
        assertTrue(error.getMessage.contains(targetKind))
        assertTrue(error.getMessage.contains("permanently denied"))
    }
  }

  @Test
  def rejectsNativeCreateViewSql(): Unit = {
    val error = tryCompile(
      """create or replace temporary view result_table as
        |select id as user_id, city
        |from users
        |""".stripMargin)

    assertTrue(error.getMessage.contains("native read-only SQL"))
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
  def defaultCompilerRejectsFormerTailAsTableSugar(): Unit = {
    val error = tryCompile("select 1 as id as result_table;")
    assertTrue(error.getMessage.contains("Spark SQL parser rejected statement"))
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
  def rejectsNativeCreateTable(): Unit = {
    val error = tryCompile("create table t (id int) using parquet;")
    assertTrue(error.getMessage.contains("native read-only SQL"))
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

    assertEquals(
      "SELECT 'SAVE CATALOG' AS sparkone_action, 'users TO default.users' AS sparkone_target",
      statement.sql)
    assertEquals(Some(WriteMode.Append), statement.writePlan.map(_.mode))
    assertEquals(Some(WriteTargetKind.HiveCatalog), statement.writePlan.map(_.target.kind))
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
      assertEquals(Some(WriteMode.Append), statement.writePlan.map(_.mode))
      assertEquals(Some(WriteTargetKind.Mysql), statement.writePlan.map(_.target.kind))
      assertEquals(Some("user_stats"), statement.writePlan.map(_.target.identifier))
      assertEquals(Some("jdbc:mysql://host:3306/app"), statement.writePlan.flatMap(_.target.connectionOptions.get("url")))
      assertEquals(Some("secret"), statement.writePlan.flatMap(_.target.connectionOptions.get("password")))
      assertEquals(Some("user_stats"), statement.writePlan.flatMap(_.target.connectionOptions.get("dbtable")))
      assertEquals(Some("500"), statement.writePlan.flatMap(_.target.connectionOptions.get("batchsize")))
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
  def rejectsSaveOverwriteToHiveTablePermanently(): Unit = {
    val error = tryCompile("save overwrite users as hive.`default.users`;")
    assertTrue(error.getMessage.contains("hive-catalog"))
    assertTrue(error.getMessage.contains("permanently denied"))
  }

  @Test
  def compilesSaveAppendToDorisCatalogTable(): Unit = {
    val statement = compiler.compile("save append users as doris.`dataagent.user_stats`;").head

    assertEquals(
      "SELECT 'SAVE CATALOG' AS sparkone_action, 'users TO doris.dataagent.user_stats' AS sparkone_target",
      statement.sql)
    assertEquals(Some(WriteMode.Append), statement.writePlan.map(_.mode))
    assertEquals(Some("doris.dataagent.user_stats"), statement.writePlan.map(_.target.identifier))
    assertEquals(Some(WriteTargetKind.DorisCatalog), statement.writePlan.map(_.target.kind))
  }

  @Test
  def rejectsSaveOverwriteToDorisCatalogTablePermanently(): Unit = {
    val error = tryCompile("save overwrite users as doris.`dataagent.user_stats`;")
    assertTrue(error.getMessage.contains("doris-catalog"))
    assertTrue(error.getMessage.contains("permanently denied"))
  }

  @Test
  def rejectsProviderOptionsForDorisSaveBecauseCatalogIsConfiguredOutsideSql(): Unit = {
    try {
      compiler.compile("save append users as doris.`dataagent.user_stats` options fenodes='leaked';")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("SAVE doris does not support SQL OPTIONS"))
    }
  }

  @Test
  def rejectsPartitionByForDorisSave(): Unit = {
    try {
      compiler.compile("save append users as doris.`dataagent.user_stats` partitionBy dt;")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("partitionBy"))
        assertTrue(e.getMessage.contains("doris"))
    }
  }

  @Test
  def compilesSaveAppendToHivePartition(): Unit = {
    val statement = compiler.compile("save append users as hive.`default.users` partitionBy dt, region;").head

    assertEquals(Seq("dt", "region"), statement.writePlan.toSeq.flatMap(_.partitionColumns))
    assertEquals(
      "SELECT 'SAVE CATALOG' AS sparkone_action, 'users TO default.users' AS sparkone_target",
      statement.sql)
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
        |save append city_stats as hive.`default.city_stats` partitionBy dt;
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

  private def tryCompile(script: String): CompileException = {
    try {
      compiler.compile(script)
      fail("Expected CompileException")
      throw new AssertionError("unreachable")
    } catch {
      case e: CompileException => e
    }
  }
}
