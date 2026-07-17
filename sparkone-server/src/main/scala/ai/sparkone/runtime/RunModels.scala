package ai.sparkone.runtime

sealed trait SessionMode {
  def name: String
}

object SessionMode {
  case object TenantShared extends SessionMode {
    override val name: String = "tenant_shared"
  }

  case object RunIsolated extends SessionMode {
    override val name: String = "run_isolated"
  }

  val Default: SessionMode = TenantShared

  def parse(value: Option[String]): SessionMode = {
    value.map(_.trim.toLowerCase).filter(_.nonEmpty) match {
      case None | Some(TenantShared.name) => TenantShared
      case Some(RunIsolated.name) => RunIsolated
      case Some(other) =>
        throw new IllegalArgumentException(
          s"Unsupported sessionMode '$other'; expected ${TenantShared.name} or ${RunIsolated.name}")
    }
  }
}

final case class FieldInfo(name: String, dataType: String, nullable: Boolean)

final case class StatementResult(
    index: Int,
    source: String,
    sql: String,
    success: Boolean,
    schema: Seq[FieldInfo],
    rows: Seq[Seq[String]],
    rowCount: Int,
    truncated: Boolean,
    previewTable: Option[String],
    durationMs: Long,
    error: Option[String])

final case class RunResult(
    success: Boolean,
    statements: Seq[StatementResult])
