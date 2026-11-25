package lakehouse.ingestion.cli

import java.io.PrintStream
import java.nio.file.Path

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

import lakehouse.ingestion.core.config.{Durations, Load, SecretResolver, Source, SourceId}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.core.watermark.BackfillRange
import lakehouse.ingestion.engine.{FaultHooks, IngestionService}
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.control.Backfills

/**
 * The command `backfill`: an operator has the range `(from, to]` of the watermark column of a
 * source read on a sequence of its own, beside the regular one (ADR-0021 §5). The first
 * invocation under an id creates the backfill, and each after it, with the same arguments,
 * resumes it; that is the engine's, under the lease of the backfill (`IngestionService.backfill`).
 *
 * It stands on what `run` stands on, the connector of the source included, and acts on a source
 * that is disabled, as an operator's action does (`SourceCommand`). Its runs are chained as those
 * of `run` are (`Chain`), a chunk a run, until the backfill is complete, up to `--max-batches`
 * and until a signal asks it to stop. Once a run has created the backfill or resumed it, what it
 * reads is said: the range, with the rows at `from` exactly not read, and what of it the regular
 * sequence reads too, which is in Bronze twice (`BackfillRange.readByRegular`, §5.8).
 *
 * Its codes are those of its last run (discovery §14.1): 0 chunks were committed, or the backfill
 * is complete and nothing was read; 2 a run holds the lease of the backfill; 3 a database did not
 * answer, the lease was lost, or a chunk was given up; 4 refused, with nothing planned: a source
 * whose connector reads no range, no chunk, a range that has not settled, another range or chunk
 * under the id, or a configuration that the backfill cannot follow; 5 more records of a chunk
 * were rejected than the source allows.
 */
object BackfillCommand {

  def run(
      profilePath: Path,
      sourcesPath: Path,
      id: SourceId,
      request: BackfillRequest,
      operator: OperatorRequest,
      secrets: SecretResolver,
      out: PrintStream,
      err: PrintStream,
      maxBatches: Option[Int] = None,
      stop: Stop = Stop.Never,
      hooks: FaultHooks = FaultHooks.Off
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
      done = "read"
    ) { ready =>
      val service = new IngestionService(
        ready.database,
        RunCommand.connectors(ready.profile, secrets),
        ready.spark,
        ready.profile.lake.uri,
        LeaseTiming(ready.profile.leases.ttl),
        hooks = hooks
      )
      val backfill = s"the backfill ${request.id} of ${id.value}"
      // What the control plane has of the backfill, to say what it reads. A record that cannot
      // be read is not said: the next run, which needs the control plane too, says why.
      def found(): Option[BackfillRecord] =
        Try(ready.database.run(Backfills.find(_, id, Layer.Bronze, request.id))).toOption.flatten
      var announced = false

      Chain(
        Chain.Limits(maxBatches, stop),
        Chain.Words(
          invocation = s"The invocation of $backfill",
          batch = "chunk",
          batches = "chunks",
          more = "and the backfill has more to read: the next invocation resumes it.",
          afterAnother = "after a chunk that a run before this invocation left: whether the " +
            "backfill has more is for the next invocation to find."
        ),
        out,
        err
      ) { outcome =>
        if (!announced) found().filter(said(outcome, _)).foreach { record =>
          announced = true
          out.println(announcement(ready.source, outcome.run, record))
          readByRegular(ready.source, record).foreach(warning => err.println(s"warning: $warning"))
        }
        report(ready.source, request, outcome).foreach { case (stream, line) =>
          (if (stream == RunCommand.Stream.Out) out else err).println(line)
        }
      }(_ => service.backfill(ready.source, ready.config, request, operator))
    }

  /**
   * Whether what the backfill reads is said after a run: one that created it, or that resumed it
   * and read a chunk of it, or tried to. A run that was refused, or skipped, says why, and
   * nothing of what the backfill reads; a backfill that is complete says that it is.
   */
  private def said(outcome: Outcome, record: BackfillRecord): Boolean = outcome match {
    case _: Outcome.Ingested | _: Outcome.RolledForward | _: Outcome.GivenUp => true
    case _: Outcome.Failed => record.createdBy == outcome.run
    case _: Outcome.NothingNew | _: Outcome.Skipped => false
  }

  /** The column that a backfill of the source reads, as its configuration names it. */
  private def columnOf(source: Source): String = source.load match {
    case Load.Incremental(watermark, _) => watermark.column.value
    case _ => "its watermark"
  }

  /** What the backfill reads: its range, with what of `from` and `to` is in it, and its chunks. */
  private[cli] def announcement(source: Source, run: RunId, record: BackfillRecord): String = {
    val column = columnOf(source)
    val reads = s"(${record.from}, ${record.to}] of $column, in chunks of ${chunkOf(record.chunk)}"
    val bounds = s"The rows whose $column is ${record.from} exactly are not read; those at " +
      s"${record.to} are."
    if (record.createdBy == run)
      s"The run $run of ${source.id.value} created the backfill ${record.id}: it reads $reads. " +
        bounds
    else
      s"The run $run of ${source.id.value} resumed the backfill ${record.id}, created by the run " +
        s"${record.createdBy} at ${record.createdAt}: it reads $reads. $bounds"
  }

  /** What of the range the regular sequence reads too, said as a warning (ADR-0021 §5.8). */
  private[cli] def readByRegular(source: Source, record: BackfillRecord): Option[String] =
    source.load match {
      case Load.Incremental(watermark, _) =>
        BackfillRange.of(record.from, record.to).toOption.flatMap { range =>
          range.readByRegular(watermark.start).map { twice =>
            val reads = watermark.start.fold(
              s"every row of ${watermark.column.value}, as it has no load.watermark.start"
            )(start =>
              s"the rows whose ${watermark.column.value} is after $start, its " +
                "load.watermark.start"
            )
            val part = if (twice == range) s"all of $range" else s"$twice of $range"
            s"the regular sequence of ${source.id.value} reads $reads: $part is read by both, " +
              "and is in Bronze twice, each copy with its run, unless a reset of the state took " +
              "the watermark of the source past it."
          }
        }
      case _ => None
    }

  /** What is said of a run, and whether as what the command did or as an error. */
  private def report(
      source: Source,
      request: BackfillRequest,
      outcome: Outcome
  ): Seq[(RunCommand.Stream, String)] = {
    val run = s"The run ${outcome.run} of ${source.id.value}"
    val backfill = s"the backfill ${request.id}"
    def chunk(batch: BatchId): String = s"the chunk ${batch.seq.value} of $backfill"
    lazy val complete =
      s"The backfill ${request.id} of ${source.id.value} is complete: it has read ${request.range}."
    outcome match {
      case Outcome.Ingested(_, batch, outputs, counts, next) =>
        def counted(count: Option[Long]): String = count.fold("an unknown number")(_.toString)
        Seq(
          RunCommand.Stream.Out -> (s"$run committed ${chunk(batch)}: " +
            s"${counts.rowsRead.fold("an unknown number of")(_.toString)} rows read, " +
            s"${counted(counts.rowsWritten)} written, " +
            s"${counted(counts.rowsRejected)} rejected${RunCommand.written(outputs)}.")
        ) ++ Option.when(next == Outcome.Next.Ends)(RunCommand.Stream.Out -> complete)
      case Outcome.RolledForward(_, batch, outputs, _) =>
        Seq(
          RunCommand.Stream.Out -> (s"$run committed ${chunk(batch)}, which a run before it had " +
            s"written and not committed: nothing was read${RunCommand.written(outputs)}.")
        )
      case Outcome.NothingNew(_) =>
        Seq(RunCommand.Stream.Out -> s"$complete $run read nothing, and ended well.")
      case Outcome.Skipped(_, holder, until) =>
        Seq(
          RunCommand.Stream.Out -> (s"$run was skipped: the run $holder holds the lease of " +
            s"$backfill, until $until at the latest. Nothing was read or written.")
        )
      case Outcome.GivenUp(_, batch, reason) =>
        Seq(
          RunCommand.Stream.Err -> (s"error: $run gave up ${chunk(batch)}: $reason. The next " +
            "run plans its window anew.")
        )
      case Outcome.Failed(_, failure, message) =>
        Seq(RunCommand.Stream.Err -> s"error: $run failed ($failure): $message")
    }
  }

  private def chunkOf(length: FiniteDuration): String = Durations.render(length)
}
