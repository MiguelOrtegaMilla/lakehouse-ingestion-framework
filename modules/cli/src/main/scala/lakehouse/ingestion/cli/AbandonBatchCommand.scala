package lakehouse.ingestion.cli

import java.io.PrintStream
import java.nio.file.Path

import lakehouse.ingestion.core.config.{SecretResolver, SourceId}
import lakehouse.ingestion.core.control.{BatchSeq, FailureClass, LeaseTiming, OperatorRequest}
import lakehouse.ingestion.engine.operator.OperatorActions
import lakehouse.ingestion.engine.operator.OperatorActions.Outcome

/**
 * The command `abandon-batch`: an operator gives up the batch of a source that is planned, when
 * the table it was planned for does not have it (ADR-0021 §2). It is a run of its own, under the
 * lease of the source, which says who asked for it and why.
 *
 * It stands on what `run` stands on (`SourceCommand`), but for the connector of the source and
 * the landing zone, which it does not read, and it acts on a source that is disabled. Its codes
 * (discovery §14.1): 0 the batch was given up; 2 a run holds the lease; 3 a database did not
 * answer, the table could not be read, or the action lost its lease; 4 refused, with nothing
 * changed: no batch, another batch, a batch that its table has, or a configuration that is not
 * valid.
 */
object AbandonBatchCommand {

  def run(
      profilePath: Path,
      sourcesPath: Path,
      id: SourceId,
      batch: BatchSeq,
      request: OperatorRequest,
      secrets: SecretResolver,
      out: PrintStream,
      err: PrintStream
  ): Int =
    SourceCommand(
      profilePath,
      sourcesPath,
      id,
      secrets,
      out,
      err,
      ingests = false,
      done = "changed"
    ) { ready =>
      val actions = new OperatorActions(
        ready.database,
        ready.spark,
        ready.profile.lake.uri,
        LeaseTiming(ready.profile.leases.ttl)
      )
      val outcome = actions.abandon(ready.source, ready.config, batch, request)
      outcome match {
        case Outcome.Done(_, message) => out.println(s"${capitalized(message)}.")
        case Outcome.Skipped(run, holder, until) =>
          out.println(
            s"The run $run of ${id.value} was skipped: the run $holder holds the lease of the " +
              s"source until $until, unless it is renewed. Nothing was changed; ask again once " +
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
      }
  }

  private def capitalized(text: String): String = text.take(1).toUpperCase + text.drop(1)
}
