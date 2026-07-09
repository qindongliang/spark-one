package ai.sparkone.sql

trait SqlValidator {
  def validate(sql: String): Unit
}

object SqlValidator {
  val Noop: SqlValidator = new SqlValidator {
    override def validate(sql: String): Unit = ()
  }
}
