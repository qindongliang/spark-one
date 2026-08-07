package ai.queryone.sql

private[queryone] object QueryOneVariableSubstitutor {
  private val VariablePattern = "\\$\\{([A-Za-z_][A-Za-z0-9_]*)\\}".r

  def render(
      text: String,
      variables: Map[String, String],
      allowUnresolved: Boolean): String = {
    VariablePattern.replaceAllIn(text, matched => {
      val key = matched.group(1)
      variables.get(key) match {
        case Some(value) => java.util.regex.Matcher.quoteReplacement(value)
        case None if allowUnresolved => matched.matched
        case None => throw new CompileException(s"Undefined QueryOne variable: $key")
      }
    })
  }
}
