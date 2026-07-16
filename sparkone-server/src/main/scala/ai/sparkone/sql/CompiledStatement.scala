package ai.sparkone.sql

final case class CompiledStatement(
    source: String,
    sql: String,
    load: Option[LoadStatementMetadata] = None,
    writePlan: Option[WritePlan] = None,
    set: Option[SetStatementMetadata] = None,
    intent: StatementIntent = StatementIntent.NativeSql)

sealed trait StatementIntent

object StatementIntent {
  case object NativeSql extends StatementIntent
  case object Load extends StatementIntent
  case object View extends StatementIntent
  case object SetVariable extends StatementIntent
  case object Save extends StatementIntent
}

final case class SetStatementMetadata(
    key: String,
    value: String,
    valueType: SetValueType)

final case class LoadStatementMetadata(
    table: String,
    format: String,
    path: String,
    options: Map[String, String],
    targetType: LoadTargetType = LoadTargetType.Provider)

sealed trait LoadTargetType

object LoadTargetType {
  case object Provider extends LoadTargetType
  case object Mysql extends LoadTargetType
}

sealed trait SetValueType

object SetValueType {
  case object Literal extends SetValueType
  case object Sql extends SetValueType
}

sealed trait SaveTargetType

object SaveTargetType {
  case object Catalog extends SaveTargetType
  case object DorisCatalog extends SaveTargetType
  case object MysqlCatalog extends SaveTargetType
}
