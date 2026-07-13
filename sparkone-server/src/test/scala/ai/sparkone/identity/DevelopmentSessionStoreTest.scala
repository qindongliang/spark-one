package ai.sparkone.identity

import org.junit.Assert._
import org.junit.Test

final class DevelopmentSessionStoreTest {
  @Test
  def createsAndResolvesDevelopmentTenantSession(): Unit = {
    val store = new DevelopmentSessionStore(() => "session-1")

    val session = store.create(" alice ")

    assertEquals("session-1", session.token)
    assertEquals("alice", session.tenant.username)
    assertEquals(TenantContext.DevelopmentIdentitySource, session.tenant.identitySource)
    assertEquals(Some(session.tenant), store.resolve(Some(session.token)))
    assertEquals(None, store.resolve(Some("unknown")))
  }

  @Test
  def removesSessionOnLogout(): Unit = {
    val store = new DevelopmentSessionStore(() => "session-1")
    val session = store.create("alice")

    store.remove(Some(session.token))

    assertEquals(None, store.resolve(Some(session.token)))
  }

  @Test
  def invalidReplacementDoesNotRemoveExistingSession(): Unit = {
    val store = new DevelopmentSessionStore(() => "session-1")
    val existing = store.create("alice")

    try {
      store.create("../bob")
      fail("Expected invalid replacement username to be rejected")
    } catch {
      case _: IllegalArgumentException =>
    }

    assertEquals(Some(existing.tenant), store.resolve(Some(existing.token)))
  }

  @Test
  def rejectsUsernamesThatCouldEscapeWorkspace(): Unit = {
    Seq("", ".alice", "../alice", "alice/bob", "alice\\bob", "alice user").foreach { username =>
      try {
        TenantContext.development(username)
        fail(s"Expected username to be rejected: $username")
      } catch {
        case e: IllegalArgumentException =>
          assertTrue(e.getMessage.contains("Username must start"))
      }
    }
  }
}
