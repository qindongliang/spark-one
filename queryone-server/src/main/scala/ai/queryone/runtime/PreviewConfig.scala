package ai.queryone.runtime

final case class PreviewConfig(maxRows: Int, defaultTab: String) {
  def clampRows(requested: Option[Int]): Int = {
    requested.map(_.max(1).min(maxRows)).getOrElse(maxRows)
  }
}

object PreviewConfig {
  val MaxRowsKey = "queryone.preview.maxRows"
  val DefaultTabKey = "queryone.preview.defaultTab"
  val DefaultMaxRows = 10
  val DefaultTab = "schema"

  def current: PreviewConfig = {
    PreviewConfig(
      maxRows = intProperty(MaxRowsKey, DefaultMaxRows).max(1),
      defaultTab = tabProperty(DefaultTabKey, DefaultTab))
  }

  private def intProperty(key: String, defaultValue: Int): Int = {
    sys.props.get(key).flatMap(value => parseInt(value)).getOrElse(defaultValue)
  }

  private def parseInt(value: String): Option[Int] = {
    try {
      Some(value.trim.toInt)
    } catch {
      case _: NumberFormatException => None
    }
  }

  private def tabProperty(key: String, defaultValue: String): String = {
    sys.props.get(key)
      .map(_.trim.toLowerCase)
      .filter(value => Set("schema", "preview").contains(value))
      .getOrElse(defaultValue)
  }
}
