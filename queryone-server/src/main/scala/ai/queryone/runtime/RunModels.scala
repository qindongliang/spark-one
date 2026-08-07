package ai.queryone.runtime

import ai.queryone.sql.AssertionFailureAction

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

final case class AssertionResult(
    table: String,
    predicate: String,
    status: String,
    message: String,
    failureAction: String = AssertionFailureAction.Default.name)

object AssertionStatus {
  val Passed: String = "passed"
  val Failed: String = "failed"
  val Error: String = "error"
}

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
    error: Option[String],
    assertion: Option[AssertionResult] = None)

final case class RunResult(
    success: Boolean,
    statements: Seq[StatementResult],
    outcome: String = RunOutcome.Succeeded,
    stoppedEarly: Boolean = false)

object RunOutcome {
  val Succeeded: String = "succeeded"
  val AssertionFailed: String = "assertion_failed"
  val AssertionStopped: String = "assertion_stopped"
  val ExecutionError: String = "execution_error"

  private[runtime] def from(
      decision: ExecutionDecision,
      lastResult: Option[StatementResult]): String = decision match {
    case ExecutionDecision.Continue => Succeeded
    case ExecutionDecision.StopAsSuccess => AssertionStopped
    case ExecutionDecision.StopAsFailure
        if lastResult.flatMap(_.assertion).exists(_.status == AssertionStatus.Failed) =>
      AssertionFailed
    case ExecutionDecision.StopAsFailure => ExecutionError
  }
}

private[runtime] sealed trait ExecutionDecision

private[runtime] object ExecutionDecision {
  case object Continue extends ExecutionDecision
  case object StopAsSuccess extends ExecutionDecision
  case object StopAsFailure extends ExecutionDecision

  def from(result: StatementResult): ExecutionDecision = {
    if (result.success) {
      Continue
    } else if (result.assertion.exists(assertion =>
        assertion.status == AssertionStatus.Failed &&
          assertion.failureAction == AssertionFailureAction.Stop.name)) {
      StopAsSuccess
    } else {
      StopAsFailure
    }
  }
}
