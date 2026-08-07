package ai.queryone.extension.overwrite

import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.Test

final class ManagedHdfsLoadProtocolTest {
  @Test
  def roundTripsInternalCommand(): Unit = {
    val request = ManagedHdfsLoadRequest(
      workspaceOwner = "alice",
      targetTable = "daily_result",
      format = "parquet",
      relativePath = "reports/daily",
      options = Map("mergeSchema" -> "false"))

    val sql = ManagedHdfsLoadProtocol.render(request)

    assertTrue(sql.startsWith("QUERYONE MANAGED_HDFS_LOAD "))
    assertEquals(Some(request), ManagedHdfsLoadProtocol.parse(sql))
    assertEquals(Some(request), ManagedHdfsLoadProtocol.parse(sql + ";"))
    assertFalse(ManagedHdfsLoadProtocol.isCommand("select 1"))
  }
}
