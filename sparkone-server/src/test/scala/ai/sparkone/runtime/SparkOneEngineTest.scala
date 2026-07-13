package ai.sparkone.runtime

import ai.sparkone.identity.TenantContext
import ai.sparkone.sql.{CompileException, MysqlLoadProfile, MysqlLoadProfileStrategy}
import org.junit.Assert._
import org.junit.Test

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.sql.{Connection, ResultSet, ResultSetMetaData, Statement}
import scala.collection.mutable.ArrayBuffer

final class SparkOneEngineTest {
  private val tenant = TenantContext.development("test-user")

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
        assertTrue(infos("local").capabilities.externalCatalogConfiguredBySparkOne)
        assertFalse(infos("local").capabilities.kyuubiExternalEngineConfig)

        assertFalse(infos("kyuubi").capabilities.mysqlAdapter)
        assertFalse(infos("kyuubi").capabilities.externalCatalogConfiguredBySparkOne)
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
      Seq[SparkOneEngine](local, kyuubi).foreach { engine =>
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
  def kyuubiCompileRejectsUnknownMysqlLoadProfileBeforeRun(): Unit = {
    val engine = kyuubiEngine()

    try {
      engine.compile(tenant, "load mysql.`analytics.users` as users;")
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
      val statements = engine.compile(tenant,
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
      val statements = engine.compile(tenant,
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
  def kyuubiCompileMysqlCatalogPathWithPartitionColumnOnlyUsesProviderDefaults(): Unit = {
    val engine = kyuubiEngine()

    try {
      val statements = engine.compile(tenant,
        """load mysql.`analytics.Dworks.big_orders`
          |where "biz_date = '2026-06-10' and status = 'PAID'"
          |options partitionColumn="id"
          |as big_orders_paid;
          |""".stripMargin)

      val sql = statements.head.sql
      assertTrue(sql.startsWith("CREATE OR REPLACE TEMPORARY VIEW big_orders_paid USING sparkone_mysql OPTIONS"))
      assertTrue(sql.contains("catalog 'analytics'"))
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
  def kyuubiCompileMysqlCatalogPathWithPartitionColumnOnlyWithoutWhereStillUsesProviderDefaults(): Unit = {
    val engine = kyuubiEngine()

    try {
      val statements = engine.compile(tenant,
        """load mysql.`analytics.Dworks.big_orders`
          |options partitionColumn="id"
          |as big_orders_paid;
          |""".stripMargin)

      val sql = statements.head.sql
      assertTrue(sql.contains("catalog 'analytics'"))
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
  def kyuubiCompileRejectsMysqlSaveAdapterBeforeRun(): Unit = {
    val engine = kyuubiEngine()

    try {
      engine.compile(tenant,
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
  def kyuubiCompileRejectsMysqlOverwriteByFixedMatrix(): Unit = {
    val engine = kyuubiEngine()

    try {
      engine.compile(tenant, "save overwrite users as mysql.`analytics.target_users`;")
      fail("Expected CompileException")
    } catch {
      case e: CompileException =>
        assertTrue(e.getMessage.contains("mysql"))
        assertTrue(e.getMessage.contains("permanently denied"))
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
      val statements = engine.compile(tenant,
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
      engine.compile(tenant,
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
      val statements = engine.compile(tenant,
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
      engine.compile(tenant,
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
      val statements = engine.compile(tenant, "load mysql.`sales.other_db.orders` as orders;")
      assertEquals(
        "CREATE OR REPLACE TEMPORARY VIEW orders AS SELECT * FROM sales.other_db.orders",
        statements.head.sql)
    } finally {
      engine.close()
    }
  }

  @Test
  def kyuubiUsesOneJdbcSessionPerLogicalTenantWithFixedServiceCredentials(): Unit = {
    val openedConnections = ArrayBuffer.empty[FakeJdbcConnection]
    val openedConfigs = ArrayBuffer.empty[KyuubiJdbcConfig]
    val config = KyuubiJdbcConfig(
      url = "jdbc:kyuubi://host:10009/default",
      user = Some("sparkone-service"),
      password = Some("secret"),
      driver = "unused-for-test",
      properties = Map("kyuubiClientPrincipal" -> "sparkone@EXAMPLE.COM"))
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
      assertTrue(openedConfigs.forall(_ == config))
      assertTrue(openedConfigs.forall(_.user.contains("sparkone-service")))
    } finally {
      engine.close()
    }

    assertTrue(openedConnections.forall(_.closed))
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
}
