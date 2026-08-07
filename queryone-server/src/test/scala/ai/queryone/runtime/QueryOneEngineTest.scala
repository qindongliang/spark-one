package ai.queryone.runtime

import ai.queryone.extension.overwrite.{ManagedHdfsLoadProtocol, ManagedHdfsOverwriteProtocol}
import ai.queryone.identity.TenantContext
import ai.queryone.sql.{CompileException, WriteExecutionType, WriteTargetKind}
import org.junit.Assert._
import org.junit.Test

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.sql.{Connection, ResultSet, ResultSetMetaData, SQLException, Statement}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer

final class QueryOneEngineTest {
  private val tenant = TenantContext.development("test-user")

  @Test
  def sessionModeDefaultsToTenantSharedAndRejectsUnknownValues(): Unit = {
    assertEquals(SessionMode.TenantShared, SessionMode.parse(None))
    assertEquals(SessionMode.TenantShared, SessionMode.parse(Some(" tenant_shared ")))
    assertEquals(SessionMode.RunIsolated, SessionMode.parse(Some("RUN_ISOLATED")))

    try {
      SessionMode.parse(Some("per_request"))
      fail("Expected invalid session mode to fail")
    } catch {
      case e: IllegalArgumentException =>
        assertTrue(e.getMessage.contains("tenant_shared"))
        assertTrue(e.getMessage.contains("run_isolated"))
    }
  }

  @Test
  def engineInfosExposeCapabilities(): Unit = {
    withSystemProperties(Map(
      "queryone.engine.local.type" -> "local",
      "queryone.engine.local.enabled" -> "true",
      "queryone.engine.kyuubi.type" -> "kyuubi",
      "queryone.engine.kyuubi.enabled" -> "true",
      "queryone.engine.kyuubi.kyuubi.url" -> "jdbc:kyuubi://host:10009/default")) {
      val registry = QueryOneEngineRegistry.fromSystemProperties()
      try {
        val infos = registry.infos.map(info => info.id -> info).toMap

        assertTrue(infos("local").capabilities.externalCatalogConfiguredByQueryOne)
        assertFalse(infos("local").capabilities.kyuubiExternalEngineConfig)

        assertFalse(infos("kyuubi").capabilities.externalCatalogConfiguredByQueryOne)
        assertTrue(infos("kyuubi").capabilities.kyuubiExternalEngineConfig)
      } finally {
        registry.close()
      }
    }
  }

  @Test
  def localAndKyuubiCompileRejectNativeCreateTable(): Unit = {
    val local = new LocalSparkEngine(
      "local",
      "Local",
      throw new AssertionError("compile must not initialize the local runtime"),
      Map.empty)
    val kyuubi = kyuubiEngine()

    try {
      Seq[QueryOneEngine](local, kyuubi).foreach { engine =>
        try {
          engine.compile(tenant, "create table default.target (id int) using parquet")
          fail(s"Expected ${engine.id} compile to reject CREATE TABLE")
        } catch {
          case e: CompileException => assertTrue(e.getMessage.contains("native read-only SQL"))
        }
      }
    } finally {
      local.close()
      kyuubi.close()
    }
  }

  @Test
  def kyuubiCompileRejectsRemovedMysqlSyntaxBeforeRun(): Unit = {
    val engine = kyuubiEngine()

    try {
      engine.compile(tenant, "load mysql.`analytics.users` as users;")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("LOAD mysql has been removed"))
        assertTrue(e.getMessage.contains("LOAD jdbc"))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCompileOdepJdbcAliasAsRoutingCatalogSql(): Unit = {
    val engine = kyuubiEngine()

    try {
      val statements = engine.compile(tenant,
        """load jdbc.`search_prod.orders`
          |where "biz_date = '2026-07-07'"
          |as orders;
          |""".stripMargin)

      assertEquals(1, statements.size)
      assertEquals(
        "CREATE OR REPLACE TEMPORARY VIEW orders AS " +
          "SELECT * FROM jdbc.search_prod.orders WHERE biz_date = '2026-07-07'",
        statements.head.sql)
      assertFalse(statements.head.sql.contains("jdbc:mysql"))
      assertFalse(statements.head.sql.toLowerCase.contains("password"))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCompileOdepJdbcAliasWithPartitionColumnAsProviderSql(): Unit = {
    val engine = kyuubiEngine()

    try {
      val statements = engine.compile(tenant,
        """load jdbc.`sync_search.drug_ai_drug_decision`
          |where "menu_id = '1_0'"
          |options partitionColumn="id"
          |as t1;
          |""".stripMargin)

      val sql = statements.head.sql
      assertTrue(sql.startsWith("CREATE OR REPLACE TEMPORARY VIEW t1 USING queryone_mysql OPTIONS"))
      assertTrue(sql.contains("catalog 'jdbc'"))
      assertTrue(sql.contains("alias 'sync_search'"))
      assertTrue(sql.contains("dbtable 'drug_ai_drug_decision'"))
      assertTrue(sql.contains("whereClauseBase64 'bWVudV9pZCA9ICcxXzAn'"))
      assertTrue(sql.contains("partitionColumn 'id'"))
      assertTrue(sql.contains("numPartitions '10'"))
      assertTrue(sql.contains("fetchsize '10000'"))
      assertFalse(sql.contains("jdbc:mysql"))
      assertFalse(sql.toLowerCase.contains("password"))
    } finally {
      engine.close()
    }
  }

  @Test
  def localCompileOdepJdbcAliasWithPartitionColumnAsProviderSql(): Unit = {
    val engine = new LocalSparkEngine(
      "local",
      "Local",
      throw new AssertionError("compile must not initialize the local runtime"),
      Map.empty)

    try {
      val sql = engine.compile(tenant,
        "load jdbc.`sync_search.orders` options partitionColumn='id' as orders;").head.sql

      assertTrue(sql.startsWith(
        "CREATE OR REPLACE TEMPORARY VIEW orders USING queryone_mysql OPTIONS"))
      assertTrue(sql.contains("catalog 'jdbc'"))
      assertTrue(sql.contains("alias 'sync_search'"))
      assertTrue(sql.contains("dbtable 'orders'"))
      assertTrue(sql.contains("partitionColumn 'id'"))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCompileStaticJdbcCatalogPathAsRemoteCatalogSql(): Unit = {
    val engine = kyuubiEngine()

    try {
      val statements = engine.compile(tenant,
        """load jdbc.`mysql_static.Dworks.orders`
          |where "biz_date = '2026-07-07'"
          |as orders;
          |""".stripMargin)

      assertEquals(1, statements.size)
      assertEquals(
        "CREATE OR REPLACE TEMPORARY VIEW orders AS SELECT * FROM mysql_static.Dworks.orders WHERE biz_date = '2026-07-07'",
        statements.head.sql)
      assertFalse(statements.head.sql.contains("jdbc:mysql"))
      assertFalse(statements.head.sql.toLowerCase.contains("password"))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCompileStaticJdbcCatalogPathWithPartitionOptionsAsProviderSql(): Unit = {
    val engine = kyuubiEngine()

    try {
      val statements = engine.compile(tenant,
        """load jdbc.`mysql_static.Dworks.big_orders`
          |where "biz_date = '2026-06-10' and status = 'PAID'"
          |options partitionColumn="id"
          |and lowerBound="1"
          |and upperBound="30000000"
          |and numPartitions="24"
          |and fetchsize="10000"
          |as big_orders_paid;
          |""".stripMargin)

      val sql = statements.head.sql
      assertTrue(sql.startsWith("CREATE OR REPLACE TEMPORARY VIEW big_orders_paid USING queryone_mysql OPTIONS"))
      assertTrue(sql.contains("catalog 'mysql_static'"))
      assertTrue(sql.contains("dbtable 'Dworks.big_orders'"))
      assertTrue(sql.contains("whereClauseBase64 'Yml6X2RhdGUgPSAnMjAyNi0wNi0xMCcgYW5kIHN0YXR1cyA9ICdQQUlEJw=='"))
      assertTrue(sql.contains("partitionColumn 'id'"))
      assertTrue(sql.contains("lowerBound '1'"))
      assertTrue(sql.contains("upperBound '30000000'"))
      assertTrue(sql.contains("numPartitions '24'"))
      assertTrue(sql.contains("fetchsize '10000'"))
      assertFalse(sql.contains("jdbc:mysql"))
      assertFalse(sql.toLowerCase.contains("password"))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCompileStaticJdbcCatalogPathWithPartitionColumnOnlyUsesProviderDefaults(): Unit = {
    val engine = kyuubiEngine()

    try {
      val statements = engine.compile(tenant,
        """load jdbc.`mysql_static.Dworks.big_orders`
          |where "biz_date = '2026-06-10' and status = 'PAID'"
          |options partitionColumn="id"
          |as big_orders_paid;
          |""".stripMargin)

      val sql = statements.head.sql
      assertTrue(sql.startsWith("CREATE OR REPLACE TEMPORARY VIEW big_orders_paid USING queryone_mysql OPTIONS"))
      assertTrue(sql.contains("catalog 'mysql_static'"))
      assertTrue(sql.contains("dbtable 'Dworks.big_orders'"))
      assertTrue(sql.contains("whereClauseBase64 'Yml6X2RhdGUgPSAnMjAyNi0wNi0xMCcgYW5kIHN0YXR1cyA9ICdQQUlEJw=='"))
      assertTrue(sql.contains("partitionColumn 'id'"))
      assertTrue(sql.contains("numPartitions '10'"))
      assertTrue(sql.contains("fetchsize '10000'"))
      assertFalse(sql.contains("lowerBound"))
      assertFalse(sql.contains("upperBound"))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCompileStaticJdbcCatalogPathWithPartitionColumnOnlyWithoutWhereStillUsesProviderDefaults(): Unit = {
    val engine = kyuubiEngine()

    try {
      val statements = engine.compile(tenant,
        """load jdbc.`mysql_static.Dworks.big_orders`
          |options partitionColumn="id"
          |as big_orders_paid;
          |""".stripMargin)

      val sql = statements.head.sql
      assertTrue(sql.contains("catalog 'mysql_static'"))
      assertTrue(sql.contains("dbtable 'Dworks.big_orders'"))
      assertTrue(sql.contains("partitionColumn 'id'"))
      assertTrue(sql.contains("numPartitions '10'"))
      assertTrue(sql.contains("fetchsize '10000'"))
      assertFalse(sql.contains("whereClauseBase64"))
      assertFalse(sql.contains("lowerBound"))
      assertFalse(sql.contains("upperBound"))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCompileStaticJdbcSaveAsRemoteCatalogPlan(): Unit = {
    val engine = kyuubiEngine()

    try {
      val statements = engine.compile(tenant,
        """view users as select 1 as id;
          |save append users as jdbc.`mysql_static.app.target_users`;
          |""".stripMargin)

      val save = statements.last
      assertEquals(
        "SELECT 'SAVE CATALOG' AS queryone_action, " +
          "'users TO mysql_static.app.target_users' AS queryone_target",
        save.sql)
      assertEquals(Some(WriteTargetKind.JdbcCatalog), save.writePlan.map(_.target.kind))
      assertEquals(Some(WriteExecutionType.CatalogSql), save.writePlan.map(_.executionType))
      assertEquals(Some("mysql_static.app.target_users"), save.writePlan.map(_.target.identifier))
      assertFalse(save.sql.contains("jdbc:mysql"))
      assertFalse(save.sql.toLowerCase.contains("password"))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCompileJdbcSaveRequiresStaticCatalogAndRejectsOptions(): Unit = {
    val engine = kyuubiEngine()

    try {
      Seq(
        "save append users as jdbc.`analytics.target_users`;",
        "save append users as jdbc.`mysql_static.app.target_users` options batchsize='500';").foreach { sql =>
        try {
          engine.compile(tenant, sql)
          fail("Expected CompileException")
        } catch {
          case e: CompileException =>
            assertTrue(e.getMessage.contains("ODEP alias") || e.getMessage.contains("does not support SQL OPTIONS"))
        }
      }
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCompileRejectsStaticJdbcOverwriteByFixedMatrix(): Unit = {
    val engine = kyuubiEngine()

    try {
      engine.compile(tenant, "save overwrite users as jdbc.`mysql_static.app.target_users`;")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("jdbc-catalog"))
        assertTrue(e.getMessage.contains("permanently denied"))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiUsesOneJdbcSessionPerLogicalTenantWithRealUsernames(): Unit = {
    val openedConnections = ArrayBuffer.empty[FakeJdbcConnection]
    val openedConfigs = ArrayBuffer.empty[KyuubiJdbcConfig]
    val config = KyuubiJdbcConfig(
      url = "jdbc:kyuubi://host:10009/default",
      user = Some("queryone-service"),
      password = Some("secret"),
      driver = "unused-for-test",
      properties = Map("kyuubiClientPrincipal" -> "queryone@EXAMPLE.COM"))
    val engine = new KyuubiJdbcEngine(
      "kyuubi",
      "Kyuubi",
      config,
      connectionFactory = suppliedConfig => {
        openedConfigs += suppliedConfig
        val fake = new FakeJdbcConnection
        openedConnections += fake
        fake.connection
      })
    val alice = TenantContext.development("alice")
    val bob = TenantContext.development("bob")

    try {
      engine.previewTable(alice, "tenant_view", 10)
      engine.previewTable(alice, "tenant_view", 10)
      engine.previewTable(bob, "tenant_view", 10)

      assertEquals(2, openedConnections.size)
      assertEquals(Seq("alice", "bob"), openedConfigs.flatMap(_.user))
      assertTrue(openedConfigs.forall(_.password.contains("secret")))
      assertTrue(openedConfigs.forall(_.properties == config.properties))
    } finally {
      engine.close()
    }

    assertTrue(openedConnections.forall(_.closed))
  }

  @Test
  def kyuubiTenantSharedRunsReuseOneConnectionAndExecuteConcurrently(): Unit = {
    val started = new CountDownLatch(2)
    val release = new CountDownLatch(1)
    val fake = new BlockingJdbcConnection(started, release)
    val openedConnections = new AtomicInteger(0)
    val engine = kyuubiEngine { _ =>
      openedConnections.incrementAndGet()
      fake.connection
    }

    val execution = runConcurrently(
      engine,
      SessionMode.TenantShared,
      started,
      release)

    try {
      assertTrue("Both shared runs should reach JDBC before either completes", execution.concurrent)
      assertTrue(execution.failures.toString, execution.failures.isEmpty)
      assertEquals(1, openedConnections.get())
      assertFalse(fake.closed)
    } finally {
      engine.close()
    }

    assertTrue(fake.closed)
  }

  @Test
  def kyuubiTenantSharedRunReplacesConnectionWhoseServerSessionExpired(): Unit = {
    val openedConnections = ArrayBuffer.empty[RecordingJdbcConnection]
    val engine = kyuubiEngine { _ =>
      val fake = new RecordingJdbcConnection()
      openedConnections += fake
      fake.connection
    }

    try {
      val first = engine.run(tenant, "select 1 as id", 10, SessionMode.TenantShared)
      assertTrue(first.statements.flatMap(_.error).mkString("\n"), first.success)
      assertEquals(1, openedConnections.size)

      openedConnections.head.valid = false
      val second = engine.run(tenant, "select 2 as id", 10, SessionMode.TenantShared)

      assertTrue(second.statements.flatMap(_.error).mkString("\n"), second.success)
      assertEquals(2, openedConnections.size)
      assertEquals(1, openedConnections.head.validationCount)
      assertTrue(openedConnections.head.closed)
      assertEquals(Seq("select 1 as id"), openedConnections.head.executedSql)
      assertEquals(Seq("select 2 as id"), openedConnections.last.executedSql)
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiReadStatementReconnectsWhenFirstConnectionHasInvalidSessionHandle(): Unit = {
    val openedConnections = ArrayBuffer.empty[RecordingJdbcConnection]
    val engine = kyuubiEngine { _ =>
      val connectionIndex = openedConnections.size
      val fake = new RecordingJdbcConnection(
        executeFailure = _ =>
          if (connectionIndex == 0) Some(new SQLException("Invalid SessionHandle [expired]"))
          else None)
      openedConnections += fake
      fake.connection
    }

    try {
      val result = engine.run(tenant, "view users as select 1 as id", 10, SessionMode.TenantShared)

      assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
      assertEquals(2, openedConnections.size)
      assertTrue(openedConnections.head.closed)
      assertEquals(
        Seq("CREATE OR REPLACE TEMPORARY VIEW users AS select 1 as id"),
        openedConnections.head.executedSql)
      assertEquals(openedConnections.head.executedSql, openedConnections.last.executedSql)
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiRunStopsAfterFirstFailedStatement(): Unit = {
    val fake = new RecordingJdbcConnection(
      executeFailure = sql =>
        if (sql == "select missing from unknown_table") {
          Some(new SQLException("TABLE_OR_VIEW_NOT_FOUND"))
        } else None)
    val engine = kyuubiEngine(_ => fake.connection)

    try {
      val result = engine.run(
        tenant,
        """select missing from unknown_table;
          |select 2 as should_not_run;
          |""".stripMargin,
        10,
        SessionMode.TenantShared)

      assertFalse(result.success)
      assertEquals(1, result.statements.size)
      assertEquals(Seq("select missing from unknown_table"), fake.executedSql)
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiAssertPassesAndContinuesWithLaterStatements(): Unit = {
    val assertionSql =
      "SELECT * FROM quality_metrics WHERE NOT COALESCE((row_count > 0), FALSE)"
    val fake = new RecordingJdbcConnection(
      queryColumns = {
        case `assertionSql` => Seq("row_count")
        case _ => Nil
      })
    val engine = kyuubiEngine(_ => fake.connection)

    try {
      val result = engine.run(
        tenant,
        """view quality_metrics as select 1 as row_count;
          |assert quality_metrics
          |where "row_count > 0"
          |message "row_count must be positive"
          |on failure stop;
          |select 2 as continued;
          |""".stripMargin,
        10)

      assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
      assertEquals(RunOutcome.Succeeded, result.outcome)
      assertFalse(result.stoppedEarly)
      assertEquals(3, result.statements.size)
      assertEquals(Some(AssertionStatus.Passed), result.statements(1).assertion.map(_.status))
      assertEquals(Some("stop"), result.statements(1).assertion.map(_.failureAction))
      assertEquals(assertionSql, result.statements(1).sql)
      assertEquals(
        Seq(
          "CREATE OR REPLACE TEMPORARY VIEW quality_metrics AS select 1 as row_count",
          assertionSql,
          "select 2 as continued"),
        fake.executedSql)
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiAssertFailureReturnsLimitedViolationsAndStopsLaterStatements(): Unit = {
    val assertionSql =
      "SELECT * FROM quality_metrics WHERE NOT COALESCE((row_count > 0), FALSE)"
    val fake = new RecordingJdbcConnection(
      queryColumns = {
        case `assertionSql` => Seq("row_count")
        case _ => Nil
      },
      queryRows = {
        case `assertionSql` => Seq(Seq("0"), Seq("-1"))
        case _ => Nil
      })
    val engine = kyuubiEngine(_ => fake.connection)

    try {
      val result = engine.run(
        tenant,
        """view quality_metrics as select 0 as row_count;
          |assert quality_metrics
          |where "row_count > 0"
          |message "row_count must be positive";
          |select 2 as should_not_run;
          |""".stripMargin,
        1)

      assertFalse(result.success)
      assertEquals(RunOutcome.AssertionFailed, result.outcome)
      assertTrue(result.stoppedEarly)
      assertEquals(2, result.statements.size)
      val assertion = result.statements.last
      assertFalse(assertion.success)
      assertEquals(Some(AssertionStatus.Failed), assertion.assertion.map(_.status))
      assertEquals(Some("fail"), assertion.assertion.map(_.failureAction))
      assertEquals(Some("row_count must be positive"), assertion.error)
      assertEquals(Seq(Seq("0")), assertion.rows)
      assertTrue(assertion.truncated)
      assertEquals(
        Seq(
          "CREATE OR REPLACE TEMPORARY VIEW quality_metrics AS select 0 as row_count",
          assertionSql),
        fake.executedSql)
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiInlineQueryAssertCanStopWithoutFailingTheRun(): Unit = {
    val assertionSql =
      "SELECT * FROM (select * from values (1), (0) as metrics(row_count)) " +
        "queryone_assert_input WHERE NOT COALESCE((row_count > 0), FALSE)"
    val fake = new RecordingJdbcConnection(
      queryColumns = {
        case `assertionSql` => Seq("row_count")
        case _ => Nil
      },
      queryRows = {
        case `assertionSql` => Seq(Seq("0"))
        case _ => Nil
      })
    val engine = kyuubiEngine(_ => fake.connection)

    try {
      val result = engine.run(
        tenant,
        """assert (
          |  select * from values (1), (0) as metrics(row_count)
          |)
          |where "row_count > 0"
          |message "row_count must be positive"
          |on failure stop;
          |select 2 as should_not_run;
          |""".stripMargin,
        10)

      assertTrue(result.success)
      assertEquals(RunOutcome.AssertionStopped, result.outcome)
      assertTrue(result.stoppedEarly)
      assertEquals(1, result.statements.size)
      val assertion = result.statements.head
      assertFalse(assertion.success)
      assertEquals(Some(AssertionStatus.Failed), assertion.assertion.map(_.status))
      assertEquals(Some("inline query"), assertion.assertion.map(_.table))
      assertEquals(Some("stop"), assertion.assertion.map(_.failureAction))
      assertEquals(Seq(Seq("0")), assertion.rows)
      assertEquals(Seq(assertionSql), fake.executedSql)
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiAssertReportsQueryExecutionErrorsSeparatelyFromBusinessFailures(): Unit = {
    val assertionSql =
      "SELECT * FROM missing_metrics WHERE NOT COALESCE((row_count > 0), FALSE)"
    val fake = new RecordingJdbcConnection(
      queryFailure = sql =>
        if (sql == assertionSql) Some(new SQLException("TABLE_OR_VIEW_NOT_FOUND"))
        else None)
    val engine = kyuubiEngine(_ => fake.connection)

    try {
      val result = engine.run(
        tenant,
        """assert missing_metrics
          |where "row_count > 0"
          |message "row_count must be positive"
          |on failure stop;
          |select 2 as should_not_run;
          |""".stripMargin,
        10)

      assertFalse(result.success)
      assertEquals(RunOutcome.ExecutionError, result.outcome)
      assertTrue(result.stoppedEarly)
      assertEquals(1, result.statements.size)
      val assertion = result.statements.head
      assertFalse(assertion.success)
      assertEquals(Some(AssertionStatus.Error), assertion.assertion.map(_.status))
      assertEquals(Some("stop"), assertion.assertion.map(_.failureAction))
      assertEquals(Some("TABLE_OR_VIEW_NOT_FOUND"), assertion.error)
      assertEquals(Seq(assertionSql), fake.executedSql)
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiRunIsolatedRunsUseIndependentConnectionsAndCloseThem(): Unit = {
    val started = new CountDownLatch(2)
    val release = new CountDownLatch(1)
    val openedConnections = new ConcurrentLinkedQueue[BlockingJdbcConnection]()
    val engine = kyuubiEngine { _ =>
      val fake = new BlockingJdbcConnection(started, release)
      openedConnections.add(fake)
      fake.connection
    }

    val execution = runConcurrently(
      engine,
      SessionMode.RunIsolated,
      started,
      release)

    try {
      assertTrue("Both isolated runs should reach JDBC before either completes", execution.concurrent)
      assertTrue(execution.failures.toString, execution.failures.isEmpty)
      assertEquals(2, openedConnections.size())
      assertTrue(openedConnections.toArray.forall(_.asInstanceOf[BlockingJdbcConnection].closed))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCatalogAppendRunsReadOnlyPreflightBeforeWrite(): Unit = {
    val fake = new RecordingJdbcConnection(
      queryColumns = {
        case "SELECT * FROM spark_catalog.default.target LIMIT 0" => Seq("name", "id")
        case "SELECT * FROM source_view LIMIT 0" => Seq("id", "name")
        case _ => Nil
      })
    val engine = kyuubiEngine(_ => fake.connection)

    try {
      val result = engine.run(
        tenant,
        """view source_view as select 1 as id, 'alice' as name;
          |save append source_view as hive.`default.target`;
          |""".stripMargin,
        10)

      assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
      assertEquals(
        Seq(
          "CREATE OR REPLACE TEMPORARY VIEW source_view AS select 1 as id, 'alice' as name",
          "SELECT * FROM spark_catalog.default.target LIMIT 0",
          "SELECT * FROM source_view LIMIT 0",
          "EXPLAIN INSERT INTO TABLE spark_catalog.default.target (`name`, `id`) " +
            "SELECT `name`, `id` FROM source_view",
          "INSERT INTO TABLE spark_catalog.default.target (`name`, `id`) " +
            "SELECT `name`, `id` FROM source_view"),
        fake.executedSql)
      assertEquals(
        "INSERT INTO TABLE spark_catalog.default.target (`name`, `id`) " +
          "SELECT `name`, `id` FROM source_view",
        result.statements.last.sql)
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiJdbcCatalogAppendUsesRemoteCatalogWithoutSecrets(): Unit = {
    val fake = new RecordingJdbcConnection(
      queryColumns = {
        case "SELECT * FROM mysql_static.app.target_users LIMIT 0" => Seq("name", "id")
        case "SELECT * FROM source_view LIMIT 0" => Seq("id", "name")
        case _ => Nil
      })
    val engine = kyuubiEngine(_ => fake.connection)

    try {
      val result = engine.run(
        tenant,
        """view source_view as select 1 as id, 'alice' as name;
          |save append source_view as jdbc.`mysql_static.app.target_users`;
          |""".stripMargin,
        10)

      assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
      assertEquals(
        Seq(
          "CREATE OR REPLACE TEMPORARY VIEW source_view AS select 1 as id, 'alice' as name",
          "SELECT * FROM mysql_static.app.target_users LIMIT 0",
          "SELECT * FROM source_view LIMIT 0",
          "EXPLAIN INSERT INTO TABLE mysql_static.app.target_users (`name`, `id`) " +
            "SELECT `name`, `id` FROM source_view",
          "INSERT INTO TABLE mysql_static.app.target_users (`name`, `id`) " +
            "SELECT `name`, `id` FROM source_view"),
        fake.executedSql)
      assertFalse(fake.executedSql.mkString("\n").contains("jdbc:mysql"))
      assertFalse(fake.executedSql.mkString("\n").toLowerCase.contains("password"))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiManagedHdfsOverwriteSubmitsInternalCommandToRemoteEngine(): Unit = {
    val fake = new RecordingJdbcConnection()
    val engine = kyuubiEngine(_ => fake.connection)

    try {
      val result = engine.run(
        tenant,
        """view source_view as select 1 as id;
          |save overwrite source_view as parquet.`reports/daily`;
          |""".stripMargin,
        10)

      assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
      assertEquals(2, fake.executedSql.size)
      val request = ManagedHdfsOverwriteProtocol.parse(fake.executedSql.last)
      assertTrue(request.isDefined)
      assertEquals("test-user", request.get.tenant)
      assertEquals("source_view", request.get.sourceTable)
      assertEquals("reports/daily", request.get.relativePath)
      assertFalse(fake.executedSql.exists(_.startsWith("EXPLAIN INSERT INTO")))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiManagedHdfsLoadSubmitsInternalCommandToRemoteEngine(): Unit = {
    val fake = new RecordingJdbcConnection()
    val engine = kyuubiEngine(_ => fake.connection)

    try {
      val result = engine.run(
        tenant,
        "load parquet.`extension-test/result` as loaded_result;",
        10)

      assertTrue(result.statements.flatMap(_.error).mkString("\n"), result.success)
      val request = fake.executedSql.iterator
        .flatMap(sql => ManagedHdfsLoadProtocol.parse(sql))
        .toSeq
        .headOption
      assertTrue(request.isDefined)
      assertEquals("test-user", request.get.workspaceOwner)
      assertEquals("loaded_result", request.get.targetTable)
      assertEquals("extension-test/result", request.get.relativePath)
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCatalogAppendStopsWhenTargetPreflightFails(): Unit = {
    val fake = new RecordingJdbcConnection(
      queryFailure = sql =>
        if (sql == "SELECT * FROM spark_catalog.default.missing LIMIT 0") {
          Some(new SQLException("TABLE_OR_VIEW_NOT_FOUND"))
        } else None)
    val engine = kyuubiEngine(_ => fake.connection)

    try {
      val result = engine.run(
        tenant,
        """view source_view as select 1 as id;
          |save append source_view as hive.`default.missing`;
          |""".stripMargin,
        10)

      assertFalse(result.success)
      assertTrue(result.statements.flatMap(_.error).mkString("\n").contains("target table does not exist"))
      assertFalse(fake.executedSql.exists(_.startsWith("INSERT INTO")))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCatalogAppendStopsWhenSchemaPreflightFails(): Unit = {
    val fake = new RecordingJdbcConnection(
      queryColumns = {
        case "SELECT * FROM spark_catalog.default.target LIMIT 0" => Seq("id", "name")
        case "SELECT * FROM source_view LIMIT 0" => Seq("id")
        case _ => Nil
      })
    val engine = kyuubiEngine(_ => fake.connection)

    try {
      val result = engine.run(
        tenant,
        """view source_view as select 1 as id;
          |save append source_view as hive.`default.target`;
          |""".stripMargin,
        10)

      assertFalse(result.success)
      assertTrue(result.statements.flatMap(_.error).mkString("\n").contains("must match target columns by name"))
      assertFalse(fake.executedSql.exists(_.startsWith("EXPLAIN INSERT INTO")))
      assertFalse(fake.executedSql.exists(_.startsWith("INSERT INTO")))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCatalogAppendStopsWhenTypeAnalysisFails(): Unit = {
    val fake = new RecordingJdbcConnection(
      queryFailure = sql =>
        if (sql.startsWith("EXPLAIN INSERT INTO")) Some(new SQLException("INCOMPATIBLE_DATA_FOR_TABLE"))
        else None,
      queryColumns = {
        case "SELECT * FROM spark_catalog.default.target LIMIT 0" => Seq("id")
        case "SELECT * FROM source_view LIMIT 0" => Seq("id")
        case _ => Nil
      })
    val engine = kyuubiEngine(_ => fake.connection)

    try {
      val result = engine.run(
        tenant,
        """view source_view as select 'not-an-int' as id;
          |save append source_view as hive.`default.target`;
          |""".stripMargin,
        10)

      assertFalse(result.success)
      assertTrue(result.statements.flatMap(_.error).mkString("\n").contains("schema is incompatible"))
      assertFalse(fake.executedSql.exists(_.startsWith("INSERT INTO")))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCatalogAppendIsNeverRetriedAfterConnectionFailure(): Unit = {
    val openedConnections = ArrayBuffer.empty[RecordingJdbcConnection]
    val engine = kyuubiEngine { _ =>
      val fake = new RecordingJdbcConnection(
        executeFailure = sql =>
          if (sql.startsWith("INSERT INTO")) Some(new SQLException("connection reset by peer")) else None)
      openedConnections += fake
      fake.connection
    }

    try {
      val result = engine.run(
        tenant,
        """view source_view as select 1 as id;
          |save append source_view as hive.`default.target`;
          |""".stripMargin,
        10)

      assertFalse(result.success)
      assertEquals(1, openedConnections.flatMap(_.executedSql).count(_.startsWith("INSERT INTO")))
      assertTrue(result.statements.flatMap(_.error).mkString("\n").contains("write status is unknown"))
    } finally {
      engine.close()
    }
  }

  @Test
  def localCloseDoesNotInitializeLazyRuntime(): Unit = {
    var initialized = false
    val engine = new LocalSparkEngine(
      "local",
      "Local",
      {
        initialized = true
        throw new IllegalStateException("runtime should not be initialized by close")
      },
      Map.empty)

    engine.close()

    assertFalse(initialized)
  }

  @Test
  def localRejectsRunIsolatedWithoutInitializingRuntime(): Unit = {
    var initialized = false
    val engine = new LocalSparkEngine(
      "local",
      "Local",
      {
        initialized = true
        throw new IllegalStateException("runtime should not be initialized")
      },
      Map.empty)

    try {
      engine.run(tenant, "select 1", 10, SessionMode.RunIsolated)
      fail("Expected local engine to reject run_isolated")
    } catch {
      case e: IllegalArgumentException =>
        assertTrue(e.getMessage.contains("run_isolated"))
    } finally {
      engine.close()
    }

    assertFalse(initialized)
  }

  private def runConcurrently(
      engine: KyuubiJdbcEngine,
      sessionMode: SessionMode,
      started: CountDownLatch,
      release: CountDownLatch): ConcurrentExecution = {
    val failures = new ConcurrentLinkedQueue[Throwable]()
    val threads = (1 to 2).map { index =>
      new Thread(new Runnable {
        override def run(): Unit = {
          try {
            val result = engine.run(tenant, s"select $index as id", 10, sessionMode)
            if (!result.success) {
              failures.add(new AssertionError(result.statements.flatMap(_.error).mkString("\n")))
            }
          } catch {
            case error: Throwable => failures.add(error)
          }
        }
      }, s"queryone-session-mode-test-$index")
    }

    threads.foreach(_.start())
    val concurrent = started.await(3, TimeUnit.SECONDS)
    release.countDown()
    threads.foreach(_.join(5000))
    threads.filter(_.isAlive).foreach { thread =>
      failures.add(new AssertionError(s"Thread ${thread.getName} did not finish"))
      thread.interrupt()
    }
    new ConcurrentExecution(concurrent, failures)
  }

  private def withSystemProperties[T](values: Map[String, String])(body: => T): T = {
    val previous = values.keys.map(key => key -> sys.props.get(key)).toMap
    values.foreach { case (key, value) => sys.props.put(key, value) }
    try {
      body
    } finally {
      previous.foreach {
        case (key, Some(oldValue)) => sys.props.put(key, oldValue)
        case (key, None) => sys.props.remove(key)
      }
    }
  }

  private def kyuubiEngine(): KyuubiJdbcEngine = {
    new KyuubiJdbcEngine(
      "kyuubi",
      "Kyuubi",
      KyuubiJdbcConfig(
        url = "jdbc:kyuubi://host:10009/default",
        user = None,
        password = None,
        driver = "org.apache.kyuubi.jdbc.KyuubiHiveDriver",
        properties = Map.empty))
  }

  private def kyuubiEngine(connectionFactory: KyuubiJdbcConfig => Connection): KyuubiJdbcEngine = {
    new KyuubiJdbcEngine(
      "kyuubi",
      "Kyuubi",
      KyuubiJdbcConfig(
        url = "jdbc:kyuubi://host:10009/default",
        user = None,
        password = None,
        driver = "unused-for-test",
        properties = Map.empty),
      connectionFactory = connectionFactory)
  }

  private final class FakeJdbcConnection {
    @volatile var closed: Boolean = false

    private val metadata = proxy(classOf[ResultSetMetaData]) { method =>
      method.getName match {
        case "getColumnCount" => Int.box(0)
        case _ => defaultValue(method.getReturnType)
      }
    }

    private def resultSet: ResultSet = proxy(classOf[ResultSet]) { method =>
      method.getName match {
        case "getMetaData" => metadata
        case "next" => Boolean.box(false)
        case _ => defaultValue(method.getReturnType)
      }
    }

    private def statement: Statement = proxy(classOf[Statement]) { method =>
      method.getName match {
        case "executeQuery" => resultSet
        case _ => defaultValue(method.getReturnType)
      }
    }

    val connection: Connection = proxy(classOf[Connection]) { method =>
      method.getName match {
        case "createStatement" => statement
        case "isClosed" => Boolean.box(closed)
        case "isValid" => Boolean.box(!closed)
        case "close" =>
          closed = true
          null
        case _ => defaultValue(method.getReturnType)
      }
    }
  }

  private final class BlockingJdbcConnection(started: CountDownLatch, release: CountDownLatch) {
    @volatile var closed: Boolean = false

    private def statement: Statement = proxy(classOf[Statement]) { method =>
      method.getName match {
        case "execute" =>
          started.countDown()
          if (!release.await(5, TimeUnit.SECONDS)) {
            throw new SQLException("Timed out waiting to release test statement")
          }
          Boolean.box(false)
        case _ => defaultValue(method.getReturnType)
      }
    }

    val connection: Connection = proxy(classOf[Connection]) { method =>
      method.getName match {
        case "createStatement" => statement
        case "isClosed" => Boolean.box(closed)
        case "isValid" => Boolean.box(!closed)
        case "close" =>
          closed = true
          null
        case _ => defaultValue(method.getReturnType)
      }
    }
  }

  private final class RecordingJdbcConnection(
      queryFailure: String => Option[SQLException] = _ => None,
      executeFailure: String => Option[SQLException] = _ => None,
      queryColumns: String => Seq[String] = sql =>
        if (sql.startsWith("SELECT * FROM")) Seq("id") else Nil,
      queryRows: String => Seq[Seq[String]] = _ => Nil) {
    val executedSql: ArrayBuffer[String] = ArrayBuffer.empty
    @volatile var closed: Boolean = false
    @volatile var valid: Boolean = true
    @volatile var validationCount: Int = 0

    private def resultSet(columns: Seq[String], rows: Seq[Seq[String]]): ResultSet = {
      var rowIndex = -1
      var lastWasNull = false
      proxyWithArgs(classOf[ResultSet]) { (method, args) =>
        method.getName match {
          case "getMetaData" => resultSetMetadata(columns)
          case "next" =>
            rowIndex += 1
            Boolean.box(rowIndex < rows.size)
          case "getString" | "getObject" =>
            val columnIndex = args(0).asInstanceOf[Integer].intValue() - 1
            val value = rows(rowIndex)(columnIndex)
            lastWasNull = value == null
            value
          case "wasNull" => Boolean.box(lastWasNull)
          case _ => defaultValue(method.getReturnType)
        }
      }
    }

    private def resultSetMetadata(columns: Seq[String]): ResultSetMetaData = {
      proxyWithArgs(classOf[ResultSetMetaData]) { (method, args) =>
        method.getName match {
          case "getColumnCount" => Int.box(columns.size)
          case "getColumnLabel" | "getColumnName" => columns(args(0).asInstanceOf[Integer].intValue() - 1)
          case "getColumnTypeName" => "int"
          case "isNullable" => Int.box(ResultSetMetaData.columnNullable)
          case _ => defaultValue(method.getReturnType)
        }
      }
    }

    private def statement: Statement = proxyWithArgs(classOf[Statement]) { (method, args) =>
      method.getName match {
        case "executeQuery" =>
          val sql = args(0).asInstanceOf[String]
          executedSql += sql
          queryFailure(sql).foreach(error => throw error)
          resultSet(queryColumns(sql), queryRows(sql))
        case "execute" =>
          val sql = args(0).asInstanceOf[String]
          executedSql += sql
          executeFailure(sql).foreach(error => throw error)
          Boolean.box(false)
        case _ => defaultValue(method.getReturnType)
      }
    }

    val connection: Connection = proxy(classOf[Connection]) { method =>
      method.getName match {
        case "createStatement" => statement
        case "isClosed" => Boolean.box(closed)
        case "isValid" =>
          validationCount += 1
          Boolean.box(valid && !closed)
        case "close" =>
          closed = true
          null
        case _ => defaultValue(method.getReturnType)
      }
    }
  }

  private def proxy[T](interfaceClass: Class[T])(body: Method => AnyRef): T = {
    Proxy.newProxyInstance(
      interfaceClass.getClassLoader,
      Array(interfaceClass),
      new InvocationHandler {
        override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = body(method)
      }).asInstanceOf[T]
  }

  private def proxyWithArgs[T](interfaceClass: Class[T])(body: (Method, Array[AnyRef]) => AnyRef): T = {
    Proxy.newProxyInstance(
      interfaceClass.getClassLoader,
      Array(interfaceClass),
      new InvocationHandler {
        override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
          body(method, Option(args).getOrElse(Array.empty[AnyRef]))
        }
      }).asInstanceOf[T]
  }

  private def defaultValue(returnType: Class[_]): AnyRef = {
    if (!returnType.isPrimitive || returnType == java.lang.Void.TYPE) null
    else if (returnType == java.lang.Boolean.TYPE) Boolean.box(false)
    else if (returnType == java.lang.Byte.TYPE) Byte.box(0.toByte)
    else if (returnType == java.lang.Short.TYPE) Short.box(0.toShort)
    else if (returnType == java.lang.Integer.TYPE) Int.box(0)
    else if (returnType == java.lang.Long.TYPE) Long.box(0L)
    else if (returnType == java.lang.Float.TYPE) Float.box(0f)
    else if (returnType == java.lang.Double.TYPE) Double.box(0d)
    else if (returnType == java.lang.Character.TYPE) Char.box(0.toChar)
    else null
  }

  private final class ConcurrentExecution(
      val concurrent: Boolean,
      val failures: ConcurrentLinkedQueue[Throwable])
}
