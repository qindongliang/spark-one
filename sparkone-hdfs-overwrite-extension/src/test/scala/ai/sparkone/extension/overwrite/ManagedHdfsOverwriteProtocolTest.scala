package ai.sparkone.extension.overwrite

import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.Test

final class ManagedHdfsOverwriteProtocolTest {
  @Test
  def roundTripsInternalCommand(): Unit = {
    val request = ManagedHdfsOverwriteRequest(
      tenant = "alice",
      sourceTable = "city_stats",
      format = "csv",
      relativePath = "reports/daily",
      options = Map("header" -> "true", "delimiter" -> "|"))

    val sql = ManagedHdfsOverwriteProtocol.render(request)

    assertTrue(sql.startsWith("SPARKONE MANAGED_HDFS_OVERWRITE "))
    assertEquals(Some(request), ManagedHdfsOverwriteProtocol.parse(sql))
    assertEquals(Some(request), ManagedHdfsOverwriteProtocol.parse(sql + ";"))
    assertFalse(ManagedHdfsOverwriteProtocol.isCommand("select 1"))
  }
}
