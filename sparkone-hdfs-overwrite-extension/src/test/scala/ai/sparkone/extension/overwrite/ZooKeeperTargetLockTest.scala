package ai.sparkone.extension.overwrite

import org.apache.curator.test.TestingServer
import org.junit.Assert.{assertEquals, assertFalse, assertTrue, fail}
import org.junit.Test

final class ZooKeeperTargetLockTest {
  @Test
  def rejectsSecondOwnerAndAllowsNextOwnerAfterRelease(): Unit = {
    val server = new TestingServer(true)
    try {
      val first = lock(
        server,
        "operationId=first-operation\ntarget=hdfs://nameservice1/public/sparkone/user/alice/result")
      val second = lock(server, "operationId=second-operation")
      first.acquire()
      try {
        try {
          second.acquire()
          fail("Expected the second lock owner to be rejected")
        } catch {
          case e: IllegalStateException =>
            assertTrue(e.getMessage.contains("already running"))
            assertTrue(e.getMessage.contains(
              "lockPath=/sparkone/test-overwrite/alice/reports~daily--target-key"))
            assertTrue(e.getMessage.contains("operationId=first-operation"))
            assertTrue(e.getMessage.contains("target=hdfs://nameservice1/public/sparkone/user/alice/result"))
        }
      } finally {
        first.close()
      }

      val next = lock(server, "next")
      try {
        next.acquire()
      } finally {
        next.close()
      }
    } finally {
      server.close()
    }
  }

  @Test
  def buildsReadableTenantLockKeyAndCompactValue(): Unit = {
    val key = ManagedHdfsOverwriteExecutor.targetLockKey(
      tenant = "alice",
      relativePath = "reports/daily result",
      qualifiedTarget = "hdfs://nameservice1/public/sparkone/user/alice/reports/daily result")
    val data = ManagedHdfsOverwriteExecutor.lockData(
      operationId = "operation-1",
      qualifiedTarget = "hdfs://nameservice1/public/sparkone/user/alice/reports/daily result")

    assertTrue(key.matches("alice/reports~daily_result--[0-9a-f]{64}"))
    assertEquals(
      "operationId=operation-1\ntarget=hdfs://nameservice1/public/sparkone/user/alice/reports/daily result",
      data)
    assertFalse(data.contains("tenant="))
  }

  private def lock(server: TestingServer, owner: String): ZooKeeperTargetLock = {
    ZooKeeperTargetLock(
      connect = server.getConnectString,
      root = "/sparkone/test-overwrite",
      key = "alice/reports~daily--target-key",
      data = owner,
      sessionTimeoutMs = 10000,
      connectionTimeoutMs = 10000)
  }
}
