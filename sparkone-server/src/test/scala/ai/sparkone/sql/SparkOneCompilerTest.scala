package ai.sparkone.sql

import org.junit.Assert._
import org.junit.Test

final class SparkOneCompilerTest {
  private val compiler = new SparkOneCompiler()

  @Test
  def compilesBasicPipeline(): Unit = {
    val script =
      """load parquet.`datasets/users` as users;
        |
        |view city_stats as
        |select city, count(*) as cnt
        |from users
        |group by city;
        |
        |select * from city_stats;
        |""".stripMargin

    val statements = compiler.compile(script)
    val load = ai.sparkone.extension.overwrite.ManagedHdfsLoadProtocol.parse(statements.head.sql)

    assertTrue(load.isDefined)
    assertEquals("compiler", load.get.workspaceOwner)
    assertEquals("users", load.get.targetTable)
    assertEquals("parquet", load.get.format)
    assertEquals("datasets/users", load.get.relativePath)
    assertEquals(
      Seq(
        "CREATE OR REPLACE TEMPORARY VIEW city_stats AS select city, count(*) as cnt\nfrom users\ngroup by city",
        "select * from city_stats"),
      statements.tail.map(_.sql))
  }

  @Test
  def compilesAssertAsViolationQueryWithMetadata(): Unit = {
    val statement = compiler.compile(
      """assert quality_metrics
        |where "row_count > 0 and null_count = 0"
        |message "quality metrics are invalid";
        |""".stripMargin).head

    assertEquals(
      "SELECT * FROM quality_metrics " +
        "WHERE NOT COALESCE((row_count > 0 and null_count = 0), FALSE)",
      statement.sql)
    assertEquals(StatementIntent.Assert, statement.intent)
    assertEquals(Some("quality_metrics"), statement.assertion.map(_.table))
    assertEquals(
      Some("row_count > 0 and null_count = 0"),
      statement.assertion.map(_.predicate))
    assertEquals(
      Some("quality metrics are invalid"),
      statement.assertion.map(_.message))
    assertEquals(
      Some(AssertionFailureAction.Fail),
      statement.assertion.map(_.failureAction))
  }

  @Test
  def compilesInlineQueryAssertAsViolationQuery(): Unit = {
    val statement = compiler.compile(
      """assert (
        |  select dt, count(*) as row_count
        |  from orders
        |  group by dt
        |)
        |where "row_count > 0"
        |message "partition is empty"
        |on failure stop;
        |""".stripMargin).head

    assertEquals(
      """SELECT * FROM (select dt, count(*) as row_count
        |  from orders
        |  group by dt) sparkone_assert_input WHERE NOT COALESCE((row_count > 0), FALSE)""".stripMargin,
      statement.sql)
    assertEquals(
      Some(AssertionSource.InlineQuery(
        """select dt, count(*) as row_count
          |  from orders
          |  group by dt""".stripMargin)),
      statement.assertion.map(_.source))
    assertEquals(Some("inline query"), statement.assertion.map(_.table))
    assertEquals(
      Some(AssertionFailureAction.Stop),
      statement.assertion.map(_.failureAction))
  }

  @Test
  def compilesExplicitAssertFailureActionFail(): Unit = {
    val statement = compiler.compile(
      """assert quality_metrics
        |where "row_count > 0"
        |message "quality metrics are invalid"
        |on failure fail;
        |""".stripMargin).head

    assertEquals(
      Some(AssertionFailureAction.Fail),
      statement.assertion.map(_.failureAction))
  }

  @Test
  def rejectsUnsupportedAssertFailureAction(): Unit = {
    val error = tryCompile(
      """assert quality_metrics
        |where "row_count > 0"
        |message "quality metrics are invalid"
        |on failure continue;
        |""".stripMargin)

    assertFalse(error.getMessage.trim.isEmpty)
  }

  @Test
  def inlineQueryAssertSupportsNestedSqlAndVariables(): Unit = {
    val statements = compiler.compile(
      """set minimum_rows = "1";
        |assert (
        |  select category, count(*) as row_count
        |  from (
        |    select category
        |    from source_data
        |    where id > 0 and category in ('A', 'B')
        |  ) filtered
        |  group by category
        |)
        |where "coalesce(row_count, 0) >= ${minimum_rows}"
        |message "category is empty";
        |""".stripMargin)

    val assertion = statements.last
    assertTrue(assertion.sql.contains("from (\n    select category"))
    assertTrue(assertion.sql.contains("where id > 0 and category in ('A', 'B')"))
    assertEquals(
      Some("coalesce(row_count, 0) >= 1"),
      assertion.assertion.map(_.predicate))
  }

  @Test
  def rejectsEmptyOrUnbalancedInlineQueryAssert(): Unit = {
    val empty = tryCompile(
      """assert () where "row_count > 0" message "invalid";""")
    val unbalanced = tryCompile(
      """assert (select count(*) as row_count from source
        |where "row_count > 0" message "invalid";
        |""".stripMargin)

    assertTrue(
      empty.getMessage,
      empty.getMessage.contains("Parse error") ||
        empty.getMessage.contains("Spark SQL parser rejected statement"))
    assertTrue(
      unbalanced.getMessage,
      unbalanced.getMessage.contains("Parse error") ||
        unbalanced.getMessage.contains("Spark SQL parser rejected statement"))
  }

  @Test
  def inlineQueryAssertCannotBypassNativeProviderPathPolicy(): Unit = {
    val error = tryCompile(
      """assert (
        |  select * from parquet.`file:///tmp/quality_metrics`
        |)
        |where "row_count > 0"
        |message "invalid";
        |""".stripMargin)

    assertTrue(error.getMessage.contains("only supported file providers with an absolute HDFS path"))
  }

  @Test
  def assertUsesSparkSqlPredicatesAndSubstitutesVariables(): Unit = {
    val statement = compiler.compile(
      """set max_nulls = "3";
        |assert quality_metrics
        |where "coalesce(null_count, 0) <= ${max_nulls} and max_event_time >= current_timestamp() - interval 1 day"
        |message "null_count must be <= ${max_nulls}";
        |""".stripMargin).last

    assertEquals(
      Some("coalesce(null_count, 0) <= 3 and max_event_time >= current_timestamp() - interval 1 day"),
      statement.assertion.map(_.predicate))
    assertEquals(
      Some("null_count must be <= 3"),
      statement.assertion.map(_.message))
  }

  @Test
  def rejectsEmptyOrMultiStatementAssertValues(): Unit = {
    val emptyPredicate = tryCompile(
      """assert quality_metrics where "" message "invalid";""")
    val emptyMessage = tryCompile(
      """assert quality_metrics where "row_count > 0" message "";""")
    val semicolonPredicate = tryCompile(
      """assert quality_metrics where "row_count > 0; select 1" message "invalid";""")

    assertTrue(emptyPredicate.getMessage.contains("predicate must not be empty"))
    assertTrue(emptyMessage.getMessage.contains("message must not be empty"))
    assertTrue(semicolonPredicate.getMessage.contains("must not contain semicolons"))
  }

  @Test
  def generatedAssertSqlIsAcceptedBySparkSqlParser(): Unit = {
    val validatingCompiler = new SparkOneCompiler(new SparkSqlValidator)

    val statement = validatingCompiler.compile(
      """assert quality_metrics
        |where "row_count > 0 and coalesce(null_count, 0) = 0"
        |message "quality metrics are invalid";
        |""".stripMargin).head

    assertEquals(StatementIntent.Assert, statement.intent)
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
  def compilesDorisLoadWithExplicitInstanceCatalog(): Unit = {
    val statement = compiler.compile(
      "load doris.`doris_prod.dataagent.users` as users;").head

    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW users AS " +
        "SELECT * FROM doris_prod.dataagent.users",
      statement.sql)
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
      "CREATE OR REPLACE TEMPORARY VIEW users AS SELECT * FROM spark_catalog.default.users",
      statement.sql)
    assertEquals(Some("users"), statement.load.map(_.table))
    assertEquals(Some("hive"), statement.load.map(_.format))
    assertEquals(Some("spark_catalog.default.users"), statement.load.map(_.path))
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
        "SELECT * FROM spark_catalog.default.users WHERE dt = date '2026-06-17' and status = 'active'",
      statement.sql)
    assertEquals(Some("active_users"), statement.load.map(_.table))
    assertEquals(
      Some("spark_catalog.default.users WHERE dt = date '2026-06-17' and status = 'active'"),
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
  def rejectsHiveDslPathsThatAreNotDatabaseAndTable(): Unit = {
    Seq(
      "load hive.`spark_catalog.default.users` as users;",
      "save append users as hive.`spark_catalog.default.users`;").foreach { sql =>
      val error = tryCompile(sql)
      assertTrue(error.getMessage.contains("hive path must be database.table"))
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
      """load excel.`imports/users.xlsx`
        |options header="true"
        |and dataAddress="'Sheet1'!A1"
        |as users;
        |""".stripMargin).head

    val request = ai.sparkone.extension.overwrite.ManagedHdfsLoadProtocol.parse(statement.sql)
    assertTrue(request.isDefined)
    assertEquals("users", request.get.targetTable)
    assertEquals("excel", request.get.format)
    assertEquals("imports/users.xlsx", request.get.relativePath)
    assertEquals(Map("header" -> "true", "dataAddress" -> "'Sheet1'!A1"), request.get.options)
    assertEquals(Some("users"), statement.load.map(_.table))
    assertEquals(Some("imports/users.xlsx"), statement.load.map(_.path))
    assertEquals(Some(LoadTargetType.ManagedHdfs), statement.load.map(_.targetType))
    assertEquals(Some("true"), statement.load.flatMap(_.options.get("header")))
  }

  @Test
  def compilesManagedHdfsLoadWithWorkspaceOwner(): Unit = {
    val statement = compiler.compile(
      """load parquet.`reports/daily`
        |options owner="bob" and mergeSchema="false"
        |as shared_reports;
        |""".stripMargin).head

    val request = ai.sparkone.extension.overwrite.ManagedHdfsLoadProtocol.parse(statement.sql)
    assertTrue(request.isDefined)
    assertEquals("bob", request.get.workspaceOwner)
    assertEquals("reports/daily", request.get.relativePath)
    assertEquals(Map("mergeSchema" -> "false"), request.get.options)
    assertFalse(statement.load.exists(_.options.contains("owner")))
  }

  @Test
  def rejectsUnsafeManagedHdfsLoadPathsAndOptions(): Unit = {
    Seq(
      "/public/odep/user/alice/result",
      "hdfs:///public/odep/user/alice/result",
      "../alice/result",
      "reports/../result",
      "reports/.sparkone-overwrite-target/staging").foreach { path =>
      val error = tryCompile(s"load parquet.`$path` as result;")
      assertTrue(path, error.getMessage.contains("relative tenant workspace path"))
    }

    Seq("path", "url", "password", "access_key").foreach { option =>
      val error = tryCompile(
        s"load parquet.`reports/daily` options $option='secret' as result;")
      assertTrue(option, error.getMessage.contains("option is not allowed"))
    }

    val unknownProvider = tryCompile("load avro.`reports/daily` as result;")
    assertTrue(unknownProvider.getMessage.contains("provider 'avro' is not supported"))

    Seq("", "../bob", "bob/team").foreach { owner =>
      val error = tryCompile(
        s"load parquet.`reports/daily` options owner='$owner' as result;")
      assertTrue(owner, error.getMessage.contains("option 'owner' is invalid"))
    }

    val duplicateOwner = tryCompile(
      "load parquet.`reports/daily` options owner='bob' and OWNER='alice' as result;")
    assertTrue(duplicateOwner.getMessage.contains("must be specified only once"))
  }

  @Test
  def compilesManagedHdfsOverwriteAndRejectsExternalOverwrite(): Unit = {
    val external = tryCompile("save overwrite users as excel.`/tmp/users.xlsx` options header=true;")
    assertTrue(external.getMessage.contains("external-path"))
    assertTrue(external.getMessage.contains("permanently denied"))

    val managed = compiler.compile("save overwrite users as parquet.`reports/daily`;").head
    val request = ai.sparkone.extension.overwrite.ManagedHdfsOverwriteProtocol.parse(managed.sql)
    assertTrue(request.isDefined)
    assertEquals("compiler", request.get.tenant)
    assertEquals("users", request.get.sourceTable)
    assertEquals("parquet", request.get.format)
    assertEquals("reports/daily", request.get.relativePath)
  }

  @Test
  def rejectsSensitiveOptionsForManagedHdfsOverwrite(): Unit = {
    Seq("path", "url", "owner", "password", "access_key").foreach { option =>
      val error = tryCompile(
        s"save overwrite users as parquet.`reports/daily` options $option='secret';")
      assertTrue(option, error.getMessage.contains("option is not allowed"))
    }
  }

  @Test
  def rejectsManagedHdfsInternalCommandSubmittedAsNativeSql(): Unit = {
    val commands = Seq(
      ai.sparkone.extension.overwrite.ManagedHdfsOverwriteProtocol.render(
        ai.sparkone.extension.overwrite.ManagedHdfsOverwriteRequest(
          "bob", "users", "parquet", "reports/daily", Map.empty)),
      ai.sparkone.extension.overwrite.ManagedHdfsLoadProtocol.render(
        ai.sparkone.extension.overwrite.ManagedHdfsLoadRequest(
          "bob", "users", "parquet", "reports/daily", Map.empty)))

    commands.foreach { command =>
      val error = tryCompile(command)
      assertTrue(error.getMessage.contains("Spark SQL parser rejected statement"))
    }
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
  def rewritesHiveCatalogAliasInNativeReadOnlySql(): Unit = {
    val statements = compiler.compile(
      """show databases in hive;
        |show namespaces in `hive`;
        |show tables in /* hive.fake */ hive.default like 'hive.default';
        |select 'hive.default.users' as source_name, u.id
        |from hive.default.users u
        |join `hive`.`analytics`.`orders` o on u.id = o.user_id;
        |describe table hive.default.users;
        |show partitions hive.default.users;
        |""".stripMargin)

    assertEquals("show databases in spark_catalog", statements.head.sql)
    assertEquals("show namespaces in spark_catalog", statements(1).sql)
    assertEquals(
      "show tables in /* hive.fake */ spark_catalog.default like 'hive.default'",
      statements(2).sql)
    assertEquals(
      "select 'hive.default.users' as source_name, u.id\n" +
        "from spark_catalog.default.users u\n" +
        "join spark_catalog.`analytics`.`orders` o on u.id = o.user_id",
      statements(3).sql)
    assertEquals("describe table spark_catalog.default.users", statements(4).sql)
    assertEquals("show partitions spark_catalog.default.users", statements(5).sql)
  }

  @Test
  def keepsTwoPartHiveDatabaseAndTableAliasesUntouched(): Unit = {
    val statements = compiler.compile(
      """select * from hive.users;
        |select hive.id from default.users hive;
        |show tables in hive;
        |""".stripMargin)

    assertEquals("select * from hive.users", statements.head.sql)
    assertEquals("select hive.id from default.users hive", statements(1).sql)
    assertEquals("show tables in hive", statements(2).sql)
  }

  @Test
  def rewritesHiveCatalogAliasInsideViewAndSqlVariableQueries(): Unit = {
    val statements = compiler.compile(
      """view hive_users as select * from hive.default.users;
        |set user_count as select count(*) from hive.default.users;
        |""".stripMargin)

    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW hive_users AS " +
        "select * from spark_catalog.default.users",
      statements.head.sql)
    assertEquals(
      Some("select count(*) from spark_catalog.default.users"),
      statements(1).set.map(_.value))
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
      "SELECT 'SAVE CATALOG' AS sparkone_action, 'users TO spark_catalog.default.users' AS sparkone_target",
      statement.sql)
    assertEquals(Some(WriteMode.Append), statement.writePlan.map(_.mode))
    assertEquals(Some("spark_catalog.default.users"), statement.writePlan.map(_.target.identifier))
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
  def compilesJdbcLoadAsOdepRoutingCatalogReadButStillRejectsSave(): Unit = {
    val load = compiler.compile(
      """load jdbc.`search_prod.users`
        |where "status = 'ACTIVE'"
        |as users;
        |""".stripMargin).head

    assertEquals(
      "CREATE OR REPLACE TEMPORARY VIEW users AS " +
        "SELECT * FROM jdbc.search_prod.users WHERE status = 'ACTIVE'",
      load.sql)
    assertEquals(Some("jdbc.search_prod.users WHERE status = 'ACTIVE'"), load.load.map(_.path))

    try {
      compiler.compile("save append users as jdbc.`analytics.users`;")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("SAVE jdbc"))
    }
  }

  @Test
  def rejectsJdbcConnectionOptionsAndNonAliasTablePaths(): Unit = {
    val connectionOption = tryCompile(
      "load jdbc.`search_prod.users` options url='jdbc:mysql://leaked' as users;")
    assertTrue(connectionOption.getMessage.contains("not allowed"))

    Seq(
      "load jdbc.`users` as users;",
      "load jdbc.`jdbc.search_prod.users` as users;").foreach { sql =>
      val error = tryCompile(sql)
      assertTrue(error.getMessage.contains("LOAD jdbc"))
    }

    val localPartitionOption = tryCompile(
      "load jdbc.`search_prod.users` options partitionColumn='id' as users;")
    assertTrue(localPartitionOption.getMessage.contains("require a Kyuubi engine"))
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
  def compilesSaveAppendToExplicitDorisInstanceCatalog(): Unit = {
    val statement = compiler.compile(
      "save append users as doris.`doris_ads.dataagent.user_stats`;").head

    assertEquals(
      Some("doris_ads.dataagent.user_stats"),
      statement.writePlan.map(_.target.identifier))
  }

  @Test
  def rejectsDorisDslCatalogWithoutDorisPrefix(): Unit = {
    Seq(
      "load doris.`mysql_crm.dataagent.users` as users;",
      "save append users as doris.`mysql_crm.dataagent.users`;").foreach { sql =>
      val error = tryCompile(sql)
      assertTrue(error.getMessage.contains("doris_<instance>.database.table"))
    }
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
      "SELECT 'SAVE CATALOG' AS sparkone_action, 'users TO spark_catalog.default.users' AS sparkone_target",
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
    val sql = compiler.compile("load text.`data/a;b` as t;").head.sql
    val request = ai.sparkone.extension.overwrite.ManagedHdfsLoadProtocol.parse(sql)
    assertEquals(Some("data/a;b"), request.map(_.relativePath))
  }

  @Test
  def generatedSqlIsAcceptedBySparkSqlParser(): Unit = {
    val validatingCompiler = new SparkOneCompiler(new SparkSqlValidator)
    val sql = validatingCompiler.compile(
      """load parquet.`datasets/users` as users;
        |load hive.`default.source_users` as source_users;
        |load excel.`imports/users.xlsx` options header="true" as excel_users;
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
