package ai.sparkone.sql

final case class CompiledStatement(
    source: String,
    sql: String,
    load: Option[LoadStatementMetadata] = None,
    save: Option[SaveStatementMetadata] = None,
    set: Option[SetStatementMetadata] = None)

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

final case class SaveStatementMetadata(
    mode: String,
    table: String,
    format: String,
    path: String,
    options: Map[String, String],
    targetType: SaveTargetType = SaveTargetType.File,
    targetOptions: Map[String, String] = Map.empty)

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
  case object File extends SaveTargetType
  case object Catalog extends SaveTargetType
  case object DorisCatalog extends SaveTargetType
  case object Mysql extends SaveTargetType
}
