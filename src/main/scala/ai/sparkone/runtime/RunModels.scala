package ai.sparkone.runtime

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
