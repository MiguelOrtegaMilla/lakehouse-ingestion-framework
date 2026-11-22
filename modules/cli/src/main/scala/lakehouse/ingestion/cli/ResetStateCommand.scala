package lakehouse.ingestion.cli

import java.io.PrintStream
import java.nio.file.Path

import lakehouse.ingestion.core.config.{SecretResolver, SourceId}
import lakehouse.ingestion.core.control.OperatorRequest
import lakehouse.ingestion.engine.operator.OperatorActions.WatermarkReset

/**
 * The command `reset-state`: an operator resets the state of a source (ADR-0021 §3). Its
 * watermark is set to a moment, forgotten or kept, and the configuration that the source has now
 * is the one that later runs are compared with: how a change that Bronze cannot follow is let
 * through. The sequence of its batches goes on, and the files it took stay taken. It is a run of
 * its own, under the lease of the source, which says who asked for it and why.
 *
 * Its codes (discovery §14.1): 0 the state was reset, with a warning for each risk of a watermark
 * kept across a change; 2 a run holds the lease; 3 a database did not answer, or the action lost
 * its lease; 4 refused, with nothing changed: a batch is planned, the source has no state, a
 * watermark that the source does not keep, a moment that cannot be its watermark, or a
 * configuration that is not valid.
 */
object ResetStateCommand {

  def run(
      profilePath: Path,
      sourcesPath: Path,
      id: SourceId,
      to: WatermarkReset,
      request: OperatorRequest,
      secrets: SecretResolver,
      out: PrintStream,
      err: PrintStream
  ): Int =
    OperatorCommand(profilePath, sourcesPath, id, secrets, out, err) { (actions, ready) =>
      actions.reset(ready.source, ready.config, to, request)
    }
}
