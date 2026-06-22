package ai.sparkone.runtime

final case class PreviewConfig(maxRows: Int) {
  def clampRows(requested: Option[Int]): Int = {
    requested.map(_.max(1).min(maxRows)).getOrElse(maxRows)
  }
}

object PreviewConfig {
  val MaxRowsKey = "sparkone.preview.maxRows"
  val DefaultMaxRows = 10

  def current: PreviewConfig = {
    PreviewConfig(
      maxRows = intProperty(MaxRowsKey, DefaultMaxRows).max(1))
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
}
