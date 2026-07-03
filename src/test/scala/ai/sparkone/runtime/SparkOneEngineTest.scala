package ai.sparkone.runtime

import ai.sparkone.sql.CompileException
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
  def kyuubiCompileRejectsMysqlLoadAdapterBeforeRun(): Unit = {
    withSystemProperties(Map(
      "sparkone.datasource.mysql.analytics.url" -> "jdbc:mysql://host:3306/app",
      "sparkone.datasource.mysql.analytics.user" -> "reader",
      "sparkone.datasource.mysql.analytics.password" -> "secret")) {
      val engine = new KyuubiJdbcEngine(
        "kyuubi",
        "Kyuubi",
        KyuubiJdbcConfig(
          url = "jdbc:kyuubi://host:10009/default",
          user = None,
          password = None,
          driver = "org.apache.kyuubi.jdbc.KyuubiHiveDriver",
          properties = Map.empty))

      try {
        engine.compile("load mysql.`analytics.users` as users;")
        fail("Expected CompileException")
      } catch {
        case e: CompileException =>
          assertTrue(e.getMessage.contains("Kyuubi engine does not support SparkOne load mysql adapter"))
      } finally {
        engine.close()
      }
    }
  }

  @Test
  def kyuubiCompileRejectsMysqlSaveAdapterBeforeRun(): Unit = {
    withSystemProperties(Map(
      "sparkone.datasource.mysql.analytics.url" -> "jdbc:mysql://host:3306/app",
      "sparkone.datasource.mysql.analytics.user" -> "writer",
      "sparkone.datasource.mysql.analytics.password" -> "secret")) {
      val engine = new KyuubiJdbcEngine(
        "kyuubi",
        "Kyuubi",
        KyuubiJdbcConfig(
          url = "jdbc:kyuubi://host:10009/default",
          user = None,
          password = None,
          driver = "org.apache.kyuubi.jdbc.KyuubiHiveDriver",
          properties = Map.empty))

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
}
