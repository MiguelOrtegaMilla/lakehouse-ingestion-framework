package lakehouse.ingestion.cli

import java.io.PrintStream
import java.nio.file.Path

import lakehouse.ingestion.core.config.{SecretResolver, SourceId}
import lakehouse.ingestion.core.control.{BatchSeq, LeaseTiming, OperatorRequest}
import lakehouse.ingestion.engine.{FaultHooks, IngestionService}

/**
 * The command `reprocess`: an operator has the files of a committed batch of a source read
 * again, into the table that its configuration names now (ADR-0021 §4). It is a run of its own,
 * under the lease of the source, which says who asked for it and why, and which plans and
 * ingests one batch of the regular sequence, of the kind `reprocess`.
 *
 * It stands on what `run` stands on, the connector of the source and the landing zone included,
 * and acts on a source that is disabled, as an operator's action does (`SourceCommand`). It says
 * what a run says, and ends in its codes (discovery §14.1): 0 the batch was committed; 2 a run
 * holds the lease; 3 a database did not answer, the lease was lost, or a file changed since and
 * the batch was given up; 4 refused, with nothing planned: a batch is planned, the batch named
 * was not committed or read no files, or a configuration that is not valid or that Bronze cannot
 * follow; 5 more records were rejected than the source allows.
 */
object ReprocessCommand {

  def run(
      profilePath: Path,
      sourcesPath: Path,
      id: SourceId,
      batch: BatchSeq,
      request: OperatorRequest,
      secrets: SecretResolver,
      out: PrintStream,
      err: PrintStream,
      hooks: FaultHooks = FaultHooks.Off,
      waits: RunSession.LogStoreWaits = RunSession.NoWaits
  ): Int =
    SourceCommand(
      profilePath,
      sourcesPath,
      id,
      secrets,
      out,
      err,
      reads = true,
      operator = true,
      done = "read",
      waits = waits
    ) { ready =>
      val service = new IngestionService(
        ready.database,
        RunCommand.connectors(ready.profile, secrets),
        ready.spark,
        ready.profile.lake.uri,
        LeaseTiming(ready.profile.leases.ttl),
        hooks = hooks
      )
      val outcome = service.reprocess(ready.source, ready.config, batch, request)
      val (stream, line) = RunCommand.report(ready.source.id, outcome)
      (if (stream == RunCommand.Stream.Out) out else err).println(line)
      RunCommand.exitOf(outcome)
    }
}
