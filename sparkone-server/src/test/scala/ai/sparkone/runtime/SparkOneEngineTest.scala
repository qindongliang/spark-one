package ai.sparkone.runtime

import ai.sparkone.sql.{CompileException, MysqlLoadProfile, MysqlLoadProfileStrategy}
import org.junit.Assert._
import org.junit.Test

final class SparkOneEngineTest {
  @Test
  def engineInfosExposeCapabilities(): Unit = {
    withSystemProperties(Map(
      "sparkone.engine.local.type" -> "local",
      "sparkone.engine.local.enabled" -> "true",
      "sparkone.engine.kyuubi.type" -> "kyuubi",
      "sparkone.engine.kyuubi.enabled" -> "true",
      "sparkone.engine.kyuubi.kyuubi.url" -> "jdbc:kyuubi://host:10009/default")) {
      val registry = SparkOneEngineRegistry.fromSystemProperties()
      try {
        val infos = registry.infos.map(info => info.id -> info).toMap

        assertTrue(infos("local").capabilities.mysqlAdapter)
        assertTrue(infos("local").capabilities.fileSafeBackup)
        assertTrue(infos("local").capabilities.externalCatalogConfiguredBySparkOne)
        assertFalse(infos("local").capabilities.kyuubiExternalEngineConfig)

        assertFalse(infos("kyuubi").capabilities.mysqlAdapter)
        assertFalse(infos("kyuubi").capabilities.fileSafeBackup)
        assertFalse(infos("kyuubi").capabilities.externalCatalogConfiguredBySparkOne)
        assertTrue(infos("kyuubi").capabilities.kyuubiExternalEngineConfig)
      } finally {
        registry.close()
      }
    }
  }

  @Test
  def kyuubiCompileRejectsUnknownMysqlLoadProfileBeforeRun(): Unit = {
    val engine = kyuubiEngine()

    try {
      engine.compile("load mysql.`analytics.users` as users;")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("Kyuubi MySQL load profile 'analytics' is not configured"))
        assertTrue(e.getMessage.contains("mysql.`catalog.db.table`"))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCompileMysqlCatalogPathAsRemoteCatalogSql(): Unit = {
    val engine = kyuubiEngine()

    try {
      val statements = engine.compile(
        """load mysql.`analytics.Dworks.orders`
          |where "biz_date = '2026-07-07'"
          |as orders;
          |""".stripMargin)

      assertEquals(1, statements.size)
      assertEquals(
        "CREATE OR REPLACE TEMPORARY VIEW orders AS SELECT * FROM analytics.Dworks.orders WHERE biz_date = '2026-07-07'",
        statements.head.sql)
      assertFalse(statements.head.sql.contains("jdbc:mysql"))
      assertFalse(statements.head.sql.toLowerCase.contains("password"))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCompileMysqlCatalogPathWithPartitionOptionsAsProviderSql(): Unit = {
    val engine = kyuubiEngine()

    try {
      val statements = engine.compile(
        """load mysql.`analytics.Dworks.big_orders`
          |where "biz_date = '2026-06-10' and status = 'PAID'"
          |options partitionColumn="id"
          |and lowerBound="1"
          |and upperBound="30000000"
          |and numPartitions="24"
          |and fetchsize="10000"
          |as big_orders_paid;
          |""".stripMargin)

      val sql = statements.head.sql
      assertTrue(sql.startsWith("CREATE OR REPLACE TEMPORARY VIEW big_orders_paid USING sparkone_mysql OPTIONS"))
      assertTrue(sql.contains("catalog 'analytics'"))
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
  def kyuubiCompileRejectsMysqlSaveAdapterBeforeRun(): Unit = {
    val engine = kyuubiEngine()

    try {
      engine.compile(
        """view users as select 1 as id;
          |save append users as mysql.`analytics.target_users`;
          |""".stripMargin)
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("Kyuubi engine does not support SparkOne save mysql adapter"))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCompileMysqlCatalogProfileAsRemoteCatalogSql(): Unit = {
    val engine = kyuubiEngine(Map(
      "sales" -> MysqlLoadProfile(
        name = "sales",
        strategy = MysqlLoadProfileStrategy.Catalog,
        catalog = Some("mysql_A"),
        namespace = Some("Dworks"),
        allowedTables = Set("orders")).validate()))

    try {
      val statements = engine.compile(
        """load mysql.`sales.orders`
          |where "biz_date = '2026-07-07'"
          |as orders;
          |""".stripMargin)

      assertEquals(1, statements.size)
      assertEquals(
        "CREATE OR REPLACE TEMPORARY VIEW orders AS SELECT * FROM mysql_A.Dworks.orders WHERE biz_date = '2026-07-07'",
        statements.head.sql)
      assertFalse(statements.head.sql.contains("jdbc:mysql"))
      assertFalse(statements.head.sql.toLowerCase.contains("password"))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCatalogMysqlProfileRejectsPerLoadPartitionOptions(): Unit = {
    val engine = kyuubiEngine(Map(
      "sales" -> MysqlLoadProfile(
        name = "sales",
        strategy = MysqlLoadProfileStrategy.Catalog,
        catalog = Some("mysql_A"),
        namespace = Some("Dworks")).validate()))

    try {
      engine.compile(
        """load mysql.`sales.orders`
          |options partitionColumn="id"
          |and lowerBound="1"
          |and upperBound="8"
          |and numPartitions="4"
          |as orders;
          |""".stripMargin)
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("catalog strategy"))
        assertTrue(e.getMessage.contains("provider strategy"))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiCompileMysqlProviderProfilePassesOnlyProfileTableWhereAndPartitionOptions(): Unit = {
    val engine = kyuubiEngine(Map(
      "sales" -> MysqlLoadProfile(
        name = "sales",
        strategy = MysqlLoadProfileStrategy.Provider,
        provider = "sparkone_mysql",
        remoteProfileName = Some("mysql_A"),
        namespace = Some("Dworks"),
        maxNumPartitions = Some(8),
        defaultFetchSize = Some("10000")).validate()))

    try {
      val statements = engine.compile(
        """load mysql.`sales.orders`
          |where "biz_date = '2026-07-07'"
          |options partitionColumn="id"
          |and lowerBound="1"
          |and upperBound="8"
          |and numPartitions="4"
          |as orders;
          |""".stripMargin)

      val sql = statements.head.sql
      assertTrue(sql.startsWith("CREATE OR REPLACE TEMPORARY VIEW orders USING sparkone_mysql OPTIONS"))
      assertTrue(sql.contains("profile 'mysql_A'"))
      assertTrue(sql.contains("dbtable 'Dworks.orders'"))
      assertTrue(sql.contains("whereClauseBase64 'Yml6X2RhdGUgPSAnMjAyNi0wNy0wNyc='"))
      assertTrue(sql.contains("partitionColumn 'id'"))
      assertTrue(sql.contains("lowerBound '1'"))
      assertTrue(sql.contains("upperBound '8'"))
      assertTrue(sql.contains("numPartitions '4'"))
      assertTrue(sql.contains("fetchsize '10000'"))
      assertFalse(sql.contains("jdbc:mysql"))
      assertFalse(sql.toLowerCase.contains("password"))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiMysqlProviderProfileLimitsNumPartitions(): Unit = {
    val engine = kyuubiEngine(Map(
      "sales" -> MysqlLoadProfile(
        name = "sales",
        strategy = MysqlLoadProfileStrategy.Provider,
        remoteProfileName = Some("mysql_A"),
        maxNumPartitions = Some(2)).validate()))

    try {
      engine.compile(
        """load mysql.`sales.orders`
          |options partitionColumn="id"
          |and lowerBound="1"
          |and upperBound="8"
          |and numPartitions="4"
          |as orders;
          |""".stripMargin)
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("numPartitions=4 exceeds profile 'sales' maxNumPartitions=2"))
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiThreePartMysqlPathPrefersCatalogSemanticsOverProfileAlias(): Unit = {
    val engine = kyuubiEngine(Map(
      "sales" -> MysqlLoadProfile(
        name = "sales",
        strategy = MysqlLoadProfileStrategy.Catalog,
        catalog = Some("mysql_A"),
        namespace = Some("Dworks")).validate()))

    try {
      val statements = engine.compile("load mysql.`sales.other_db.orders` as orders;")
      assertEquals(
        "CREATE OR REPLACE TEMPORARY VIEW orders AS SELECT * FROM sales.other_db.orders",
        statements.head.sql)
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

  private def kyuubiEngine(mysqlLoadProfiles: Map[String, MysqlLoadProfile] = Map.empty): KyuubiJdbcEngine = {
    new KyuubiJdbcEngine(
      "kyuubi",
      "Kyuubi",
      KyuubiJdbcConfig(
        url = "jdbc:kyuubi://host:10009/default",
        user = None,
        password = None,
        driver = "org.apache.kyuubi.jdbc.KyuubiHiveDriver",
        properties = Map.empty),
      mysqlLoadProfiles = mysqlLoadProfiles)
  }
}
