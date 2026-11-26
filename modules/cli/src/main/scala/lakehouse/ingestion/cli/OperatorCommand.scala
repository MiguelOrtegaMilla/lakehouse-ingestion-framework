package lakehouse.ingestion.cli

import java.io.PrintStream
import java.nio.file.Path

import lakehouse.ingestion.core.config.{ConnectorKind, SecretResolver, SourceId}
import lakehouse.ingestion.core.control.{FailureClass, LeaseTiming}
import lakehouse.ingestion.engine.operator.OperatorActions
import lakehouse.ingestion.engine.operator.OperatorActions.Outcome

/**
 * What the commands of an operator share (ADR-0021): the steps that `run` stands on as well
 * (`SourceCommand`), but for the connector of the source and the landing zone, which an operator
 * does not read, and on a source that is disabled, which an operator acts on; then the action,
 * what is said of how it ended, and its code (discovery §14.1).
 */
private[cli] object OperatorCommand {

  /**
   * @param lease whose lease the action takes, as a line says it of a run that holds it: that
   *   of the source, or of the backfill whose sequence the action is on
   */
  def apply(
      profilePath: Path,
      sourcesPath: Path,
      id: SourceId,
      secrets: SecretResolver,
      out: PrintStream,
      err: PrintStream,
      lease: String = "the source"
  )(act: (OperatorActions, SourceCommand.Ready) => Outcome): Int =
    SourceCommand(
      profilePath,
      sourcesPath,
      id,
      secrets,
      out,
      err,
      reads = false,
      operator = true,
      done = "changed"
    ) { ready =>
      val actions = new OperatorActions(
        ready.database,
        ready.spark,
        ready.profile.lake.uri,
        LeaseTiming(ready.profile.leases.ttl),
        ready.profile.retries,
        // The kinds that the configuration was read with.
        ConnectorKind.BuiltIn
      )
      val outcome = act(actions, ready)
      outcome match {
        case Outcome.Done(_, message, warnings) =>
          out.println(s"${capitalized(message)}.")
          warnings.foreach(warning => err.println(s"warning: $warning."))
        case Outcome.Skipped(run, holder, until) =>
          out.println(
            s"The run $run of ${id.value} was skipped: the run $holder holds the lease of " +
              s"$lease until $until, unless it is renewed. Nothing was changed; ask again once " +
              "it has ended."
          )
        case Outcome.Failed(_, _, message) => err.println(s"error: $message")
      }
      exitOf(outcome)
    }

  /** The code of an operator's action, by how it ended (discovery §14.1). */
  def exitOf(outcome: Outcome): Int = outcome match {
    case _: Outcome.Done => Main.Exit.Ok
    case _: Outcome.Skipped => Main.Exit.SkippedLocked
    case Outcome.Failed(_, failure, _) =>
      failure match {
        case FailureClass.Transient | FailureClass.Conflict => Main.Exit.Transient
        case FailureClass.Permanent => Main.Exit.Permanent
        case FailureClass.Data => Main.Exit.DataThreshold
        case FailureClass.Internal => Main.Exit.Internal
      }
  }

  private def capitalized(text: String): String = text.take(1).toUpperCase + text.drop(1)
}
