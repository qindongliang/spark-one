package queryone.server

import org.junit.Assert.{assertEquals, assertFalse}
import org.junit.Test
import queryone.runtime.{EngineCapabilities, EngineInfo}

final class QueryOneInternalApiTest {

  @Test
  def exposesOnlyKyuubiEnginesAndSelectsAvailableDefault(): Unit = {
    val local = EngineInfo("local", "Local", "local", EngineCapabilities.Local)
    val yarnClient = EngineInfo("kyuubi_yarn_client", "YARN Client", "kyuubi", EngineCapabilities.Kyuubi)
    val yarnCluster = EngineInfo("kyuubi_yarn_cluster", "YARN Cluster", "kyuubi", EngineCapabilities.Kyuubi)

    val catalog = QueryOneServer.internalEngineCatalog(
      "local",
      Seq(yarnCluster, local, yarnClient))

    assertEquals(Some("kyuubi_yarn_client"), catalog.defaultEngine)
    assertEquals(Seq("kyuubi_yarn_client", "kyuubi_yarn_cluster"), catalog.engines.map(_.id))
    assertFalse(catalog.engines.exists(_.engineType == "local"))
  }

  @Test
  def keepsConfiguredKyuubiDefault(): Unit = {
    val yarnClient = EngineInfo("kyuubi_yarn_client", "YARN Client", "kyuubi", EngineCapabilities.Kyuubi)
    val yarnCluster = EngineInfo("kyuubi_yarn_cluster", "YARN Cluster", "kyuubi", EngineCapabilities.Kyuubi)

    val catalog = QueryOneServer.internalEngineCatalog(
      "kyuubi_yarn_cluster",
      Seq(yarnClient, yarnCluster))

    assertEquals(Some("kyuubi_yarn_cluster"), catalog.defaultEngine)
  }
}
