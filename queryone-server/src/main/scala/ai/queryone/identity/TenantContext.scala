package ai.queryone.identity

import java.util.UUID
import scala.collection.concurrent.TrieMap

final case class TenantContext(username: String, identitySource: String)

object TenantContext {
  val DevelopmentIdentitySource: String = "development"

  private val UsernamePattern = "^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$".r

  def development(username: String): TenantContext = {
    val normalized = Option(username).map(_.trim).getOrElse("")
    if (!UsernamePattern.pattern.matcher(normalized).matches()) {
      throw new IllegalArgumentException(
        "Username must start with a letter or number and contain only letters, numbers, '.', '_' or '-' (maximum 64 characters)")
    }
    TenantContext(normalized, DevelopmentIdentitySource)
  }
}

final class DevelopmentSessionStore(tokenFactory: () => String = () => UUID.randomUUID().toString) {
  private val sessions = TrieMap.empty[String, TenantContext]

  def create(username: String): DevelopmentSession = {
    val tenant = TenantContext.development(username)
    val token = tokenFactory()
    sessions.put(token, tenant)
    DevelopmentSession(token, tenant)
  }

  def resolve(token: Option[String]): Option[TenantContext] = {
    token.flatMap(sessions.get)
  }

  def remove(token: Option[String]): Unit = {
    token.foreach(sessions.remove)
  }
}

final case class DevelopmentSession(token: String, tenant: TenantContext)
