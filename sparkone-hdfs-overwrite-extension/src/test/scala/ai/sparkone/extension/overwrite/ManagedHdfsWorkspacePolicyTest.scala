package ai.sparkone.extension.overwrite

import org.junit.Assert.assertEquals
import org.junit.Test

final class ManagedHdfsWorkspacePolicyTest {
  @Test
  def normalizesNativeHdfsReadPathsWithoutFilesystemAccess(): Unit = {
    assertEquals(
      Some("/public/odep/user"),
      ManagedHdfsWorkspacePolicy.normalizeNativeHdfsReadPath("/public/odep/user/"))
    assertEquals(
      Some("/public/share/events"),
      ManagedHdfsWorkspacePolicy.normalizeNativeHdfsReadPath("hdfs:///public/share/events"))
    assertEquals(
      Some("/public/share/events"),
      ManagedHdfsWorkspacePolicy.normalizeNativeHdfsReadPath("viewfs:///public/share/events"))
    assertEquals(Some("/"), ManagedHdfsWorkspacePolicy.normalizeNativeHdfsReadPath("/"))
  }

  @Test
  def rejectsAmbiguousOrNonHdfsNativeReadPaths(): Unit = {
    Seq(
      null,
      "relative/events",
      "file:///tmp/events",
      "s3a://bucket/events",
      "hdfs://nameservice/public/events",
      "/public/../secret",
      "/public//events",
      "/public/events?limit=1",
      "/public/events#fragment",
      "/public/%65vents",
      "/public/*/events",
      "/public/events?.csv",
      "/public/{a,b}/events").foreach { path =>
      assertEquals(path, None, ManagedHdfsWorkspacePolicy.normalizeNativeHdfsReadPath(path))
    }
  }
}
