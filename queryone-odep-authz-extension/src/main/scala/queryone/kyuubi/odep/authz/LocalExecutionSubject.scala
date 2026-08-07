package queryone.kyuubi.odep.authz

import org.apache.spark.SparkContext

object LocalExecutionSubject {
  private val SubjectProperty = "queryone.local.execution.subject"

  def withSubject[T](sparkContext: SparkContext, subject: String)(body: => T): T = {
    val normalized = Option(subject).map(_.trim).getOrElse("")
    if (normalized.isEmpty) {
      throw new OdepAuthorizationException("Local execution subject must not be empty")
    }

    val previous = sparkContext.getLocalProperty(SubjectProperty)
    sparkContext.setLocalProperty(SubjectProperty, normalized)
    try {
      body
    } finally {
      sparkContext.setLocalProperty(SubjectProperty, previous)
    }
  }

  def current(sparkContext: SparkContext): Option[String] = {
    Option(sparkContext.getLocalProperty(SubjectProperty))
      .map(_.trim)
      .filter(_.nonEmpty)
  }

  private[authz] def resolve(sparkContext: SparkContext): String = {
    current(sparkContext)
      .getOrElse {
        throw new OdepAuthorizationException("Missing trusted Local execution subject")
      }
  }
}
