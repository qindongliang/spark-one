package ai.sparkone.sql

final case class CompiledStatement(
    source: String,
    sql: String,
    save: Option[SaveStatementMetadata] = None)

final case class SaveStatementMetadata(
    mode: String,
    table: String,
    format: String,
    path: String,
    options: Map[String, String])
