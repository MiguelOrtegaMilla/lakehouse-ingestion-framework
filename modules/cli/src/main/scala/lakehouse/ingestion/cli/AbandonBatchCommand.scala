package lakehouse.ingestion.cli

import java.io.PrintStream
import java.nio.file.Path

import lakehouse.ingestion.core.config.{SecretResolver, SourceId}
import lakehouse.ingestion.core.control.{BatchSeq, OperatorRequest}

/**
 * The command `abandon-batch`: an operator gives up the batch of a source that is planned, when
 * the table it was planned for does not have it (ADR-0021 §2). It is a run of its own, under the
 * lease of the source, which says who asked for it and why.
 *
 * Its codes (discovery §14.1): 0 the batch was given up; 2 a run holds the lease; 3 a database
 * did not answer, the table could not be read, or the action lost its lease; 4 refused, with
 * nothing changed: no batch, another batch, a batch that its table has, or a configuration that
 * is not valid.
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
    OperatorCommand(profilePath, sourcesPath, id, secrets, out, err) { (actions, ready) =>
      actions.abandon(ready.source, ready.config, batch, request)
    }
}
