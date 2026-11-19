package lakehouse.ingestion.engine

import java.time.{Clock, Instant}

import io.circe.Json
import lakehouse.ingestion.core.config.{
  ConfigVersion,
  PathTableLocator,
  RootUri,
  Source,
  TableLocation
}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.bronze.{BronzeBatch, BronzeWriter}
import lakehouse.ingestion.engine.connector.{
  BoundSource,
  ConnectorRegistry,
  PlanContext,
  Progress,
  ReadContext,
  ReadResult,
  SourceChangedException
}
import lakehouse.ingestion.engine.control.{
  Application,
  ControlDatabase,
  LeasedRun,
  LeaseLostException,
  SourceStates
}
import lakehouse.ingestion.engine.delta.{BatchAppend, DeltaTableTransactions, Lake}
import lakehouse.ingestion.engine.quarantine.{QuarantineBatch, QuarantineWriter}
import lakehouse.ingestion.logstore.CommitContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * One run of a source into Bronze: the protocol of ADR-0005 §3 from its lease to its commit.
 * It is the one place that holds the steps to each other, so that what the control plane says
 * of a batch and what its table has are never two stories.
 *
 *  1. The lease of the source, and the run under its epoch; or `SKIPPED_LOCKED`.
 *  1. The plan of the next batch, by the connector of the source, written down before anything
 *     of it is read. A source with nothing new has no batch, and its run ends well.
 *  1. The read of the plan, by the connector.
 *  1. The records that could not be parsed, appended to the quarantine of the source, and then
 *     the rows, appended to its table of Bronze: each under the number of the batch, which
 *     Delta takes once (`QuarantineWriter`, `BronzeWriter`).
 *  1. One transaction in the control plane, on condition of the epoch: the state of the source
 *     moved, the files of the batch recorded as taken, the batch committed with what it wrote,
 *     the run succeeded, the lease given up (`LeasedRun.commit`).
 *
 * '''A batch with no rows writes nothing''': files that are all empty, or that have a header
 * and no record. Its files are recorded as taken by the commit, and no table is made nor given
 * a version: a table is made by the first batch that has rows, with the columns of its source.
 * A table made from an empty batch would have the columns of the engine alone, and the next
 * batch could not be appended to it.
 *
 * '''More rejected records than the source allows, and the batch is not committed'''
 * (`quality.max_rejected_ratio`, ADR-0008). The records are in quarantine by then, where
 * whoever owns the source finds them; nothing is written to Bronze, the run ends as a data
 * failure, and the batch stays planned: the same data gives the same result, until the source
 * allows more or the batch is given up. The quarantine is written first so that the table of
 * the data is the last that a batch writes, and the one that says whether the batch is written
 * (`Recovery.of`).
 *
 * '''A plan that the source no longer has is given up''' (ADR-0018). When the connector says
 * that what the plan names changed (`SourceChangedException`), the table is asked whether the
 * batch is in it, as recovery asks. If it is not, the batch is abandoned with the reason and
 * what changed, the run ends as a transient failure, and the next run plans anew
 * (`LeasedRun.abandon`). If it is, nothing is changed and the run ends as an error that nothing
 * classified: a batch that is written is rolled forward, and never given up.
 *
 * '''How a run ends.'''
 *  - A failure that says its class (`RunFailedException`) ends the run as `FAILED` with it, and
 *    gives the lease up. The batch, if one was planned, stays planned.
 *  - Any other exception is let through, and nothing is said to the control plane: the lease is
 *    no longer renewed, and what is left is what a process that died leaves. The run that takes
 *    the lease over marks this one `ABANDONED`. Telling such errors apart is the classification
 *    of errors (P8).
 *
 * '''What is not here yet.''' A batch that a run before this one left planned is not recovered
 * (P5.10): the run fails, and says so. A lease that is lost is found out where the control
 * plane is asked, and at the commit; stopping the jobs of Spark at once is P5.11.
 *
 * @param lake the root of the lake of the profile: every table of a source is under it
 * @param clock what "now" is for a plan, and when the rows of a batch were read
 * @param hooks what a run does at each fault point: nothing, outside tests
 */
final class IngestionService(
    database: ControlDatabase,
    connectors: ConnectorRegistry,
    spark: SparkSession,
    lake: RootUri,
    timing: LeaseTiming,
    clock: Clock = Clock.systemUTC(),
    hooks: FaultHooks = FaultHooks.Off
) {
  import IngestionService._

  private val locator = new PathTableLocator(lake)
  private val confinement = new Lake(lake, spark.sessionState.newHadoopConf())
  private val tables: TableTransactions = new DeltaTableTransactions(spark)

  /**
   * Runs `source` once.
   *
   * @param config the configuration that the run is recorded with: that of `source`, under the
   *   profile of this process
   * @throws java.lang.IllegalArgumentException if the application has no connector for the
   *   source. Nothing was started
   * @throws java.lang.Exception what nothing classified. The run is left as a process that died
   *   leaves it
   */
  def run(source: Source, config: ConfigVersion): Outcome = {
    val bound = connectors.bind(source)
    val start = RunStart(
      RunId.generate(),
      source.id,
      Layer.Bronze,
      RunKind.Regular,
      Application.instance,
      Application.version,
      config.hash
    )

    LeasedRun.open(database, start, config, timing) match {
      case Left(skipped) => Outcome.Skipped(start.id, skipped.holder, skipped.until)
      case Right(leased) =>
        try new Attempt(leased, bound).run()
        catch {
          case failed: RunFailedException =>
            log.error(s"the run ${start.id} of ${source.id} failed (${failed.failure})", failed)
            ended(leased, RunEnd.Failed(failed.failure, failed.getMessage))
          case lost: LeaseLostException =>
            leased.close()
            fenced(start, lost.getMessage)
          case unclassified: Throwable =>
            // Nothing is said of it: the lease runs out, as that of a process that died.
            leased.close()
            throw unclassified
        }
    }
  }

  /** Says how a run failed, and what the process then reports. */
  private def ended(leased: LeasedRun, end: RunEnd.Failed): Outcome =
    if (leased.finish(end)) Outcome.Failed(leased.start.id, end.failure, end.message)
    else fenced(leased.start, "another run ended it")

  /**
   * A run whose lease is another's. What it did is not committed, and its row says `ABANDONED`,
   * written by the run that took its place: there is nothing for it to say but to stop. It may
   * pass, so it is reported as a transient failure (P5.11).
   */
  private def fenced(start: RunStart, how: String): Outcome = {
    val message = s"the run ${start.id} lost the lease of ${start.leaseKey}: $how. Nothing of " +
      "it was committed, and the run that holds the lease goes on with the source"
    log.error(message)
    Outcome.Failed(start.id, FailureClass.Transient, message)
  }

  /** The steps of one run that holds its lease. */
  final private class Attempt(leased: LeasedRun, bound: BoundSource) {
    private val start = leased.start
    private val source = bound.source
    private val table = locator.locate(source.targets.bronze.table)
    private val quarantineTable = Quarantine.table(source.id, start.layer)
    private val quarantine = locator.locate(quarantineTable)

    // One for the run, kept for all its writes: what they lose to other writers is counted in it.
    private val commits = CommitContext.forRun(start.id.uuid)

    def run(): Outcome = {
      confinement.confine(table)
      confinement.confine(quarantine)
      leased.planned().foreach { left =>
        throw RunFailedException.permanent(
          s"the batch ${left.id} was planned by the run ${left.plannedBy} and not committed. " +
            "A batch that a run left is recovered by the run after it, replayed or rolled " +
            "forward as its table says, and that is not built yet (P5.10)"
        )
      }

      val state = database.run(SourceStates.find(_, source.id, start.layer))
      val context = PlanContext(clock.instant(), state.flatMap(_.watermark), leased)
      bound.plan(context) match {
        case None =>
          log.info(s"the run ${start.id} found nothing new in ${source.id}, and has no batch")
          if (leased.finish(RunEnd.Succeeded(Idle))) Outcome.NothingNew(start.id)
          else fenced(start, "another run ended it")
        case Some(plan) =>
          // The write-ahead step: from here on, the plan is what is read.
          val batch = leased.plan(plan)
          log.info(s"the run ${start.id} planned ${batch.id}")
          hooks.at(FaultPoint.AfterPlan, batch)
          ingest(batch)
      }
    }

    /** Steps 4 to 8, for a batch whose plan is written down. */
    private def ingest(batch: PlannedBatch): Outcome = {
      // Before the read: a plan that says nothing of what its commit records is not read.
      val progress = bound.progress(batch.plan)
      val read =
        try Right(bound.read(batch.plan, ReadContext(spark)))
        catch { case changed: SourceChangedException => Left(changed) }
      read.fold(giveUp(batch, _), finish(batch, progress, _))
    }

    /** Writes what was read of a batch, its quarantine and then its data, and commits it. */
    private def finish(batch: PlannedBatch, progress: Progress, read: ReadResult): Outcome = {
      val readAt = clock.instant()
      hooks.at(FaultPoint.AfterRead, batch)

      // Of what Spark keeps of the read (`ReadResult`): the files are not read again.
      val rows = read.rows.count()
      val unparsed = read.unparsed.fold(0L)(_.count())
      val counted = RunCounts(
        rowsRead = Some(rows + unparsed),
        rowsWritten = Some(0L),
        rowsRejected = Some(unparsed),
        bytesRead = progress.bytes
      )

      // The quarantine before the data (ADR-0005 §3, step 5), and whatever becomes of the batch:
      // a record that was rejected is there to be looked at, the more so when there are too many.
      val apart = read.unparsed.filter(_ => unparsed > 0).map { records =>
        val written = this.written(quarantine, batch) {
          new QuarantineWriter(confinement, tables, commits)
            .write(quarantine, QuarantineBatch(start.id, source.id, batch.id, readAt), records)
        }
        hooks.at(FaultPoint.AfterQuarantineWrite, batch)
        written
      }

      if (source.quality.exceededBy(rejected = unparsed, read = rows + unparsed))
        tooManyRejected(batch, withWrites(counted, apart.toSeq))
      else {
        // A batch with no rows writes nothing to Bronze, and its table is not touched.
        val data = Option.when(rows > 0) {
          val written = this.written(table, batch) {
            new BronzeWriter(confinement, tables, commits)
              .write(table, BronzeBatch(start.id, source.id, batch.id, readAt), read.rows)
          }
          hooks.at(FaultPoint.AfterBronzeCommit, batch)
          written
        }

        val outputs = apart.map(write => BatchOutput(quarantineTable, write.version)).toVector ++
          data.map(write => BatchOutput(source.targets.bronze.table, write.version))
        val counts = withWrites(
          // Another writer of this sequence wrote the batch, from the same plan: its rows are
          // there once, and this run wrote none of them, nor counted them.
          counted.copy(rowsWritten =
            data.fold(Option(0L))(write => Option.when(write.byThisRun)(rows))
          ),
          apart.toSeq ++ data
        )

        hooks.at(FaultPoint.BeforeStateCommit, batch)
        if (leased.commit(batch.id, progress.watermark, outputs, counts, progress.files)) {
          log.info(s"the run ${start.id} committed ${batch.id}")
          Outcome.Ingested(start.id, batch.id, outputs, counts)
        } else fenced(start, "another run holds it")
      }
    }

    /** What a write of the batch to one of its tables came to. */
    private def written(to: TableLocation, batch: PlannedBatch)(
        write: => BatchAppend.Result
    ): Written = write match {
      case BatchAppend.Appended(version, conflicts) =>
        Written(version, byThisRun = true, conflicts)
      case BatchAppend.AlreadyThere(Some(version)) =>
        Written(version, byThisRun = false, conflicts = 0)
      case BatchAppend.AlreadyThere(None) =>
        throw new IllegalStateException(
          s"the table at ${to.uri} has ${batch.id}, and no commit that its log keeps holds " +
            "it: a batch that this run has just planned cannot be that old"
        )
    }

    /**
     * What the writes of the run cost it: how often a table changed under one, and the versions
     * that they lost to other writers. Neither is counted for a run that wrote nothing.
     */
    private def withWrites(counts: RunCounts, writes: Seq[Written]): RunCounts =
      if (writes.isEmpty) counts
      else
        counts.copy(
          writeConflicts = Some(writes.map(_.conflicts).sum),
          commitContention = Some(Math.toIntExact(commits.contention))
        )

    /**
     * More records were rejected than the source allows: the batch is not committed (ADR-0008,
     * exit code 5). Its rejected records are in quarantine, and nothing of it is in Bronze.
     */
    private def tooManyRejected(batch: PlannedBatch, counts: RunCounts): Outcome = {
      val (rejected, read) = (counts.rowsRejected.getOrElse(0L), counts.rowsRead.getOrElse(0L))
      ended(
        leased,
        RunEnd.Failed(
          FailureClass.Data,
          s"$rejected of the $read records of ${batch.id} could not be parsed, which is more " +
            s"than the share that ${source.id} allows, ${source.quality.maxRejectedRatio} " +
            "(quality.max_rejected_ratio). The batch is not committed, and nothing of it was " +
            s"written to Bronze: it stays planned. The records are in quarantine, at " +
            quarantine.uri,
          counts
        )
      )
    }

    /**
     * Gives up a batch whose plan the source no longer has, if its table does not have the
     * batch (ADR-0018).
     */
    private def giveUp(batch: PlannedBatch, changed: SourceChangedException): Outcome =
      Recovery.of(Some(batch), table, tables) match {
        case Recovery.RollForward(_) =>
          throw new IllegalStateException(
            s"the source of ${batch.id} changed after the batch was planned, and the table at " +
              s"${table.uri} has the batch: another writer of this sequence wrote it. Nothing " +
              "was given up, and nothing was changed: the run after this one recovers the " +
              s"batch as the table says. What changed: ${changed.summary}"
          )
        case _ =>
          val message = s"${batch.id} was given up, and the next run plans anew: " +
            changed.summary
          log.warn(message)
          val end = RunEnd.Failed(
            FailureClass.Transient,
            message,
            RunCounts(rowsWritten = Some(0L))
          )
          if (leased.abandon(batch.id, changed.summary, Some(changedDetail(changed)), end))
            Outcome.GivenUp(start.id, batch.id, changed.summary)
          else fenced(start, "another run holds it")
      }
  }
}

object IngestionService {

  private val log = LoggerFactory.getLogger(classOf[IngestionService])

  /** How a run ended, for whoever started it. */
  sealed trait Outcome {
    def run: RunId
  }

  object Outcome {

    /** Another run holds the lease of the source: `SKIPPED_LOCKED`, and not a failure (2). */
    final case class Skipped(run: RunId, holder: RunId, until: Instant) extends Outcome

    /** The source had nothing new. The run succeeded, with no batch (0). */
    final case class NothingNew(run: RunId) extends Outcome

    /**
     * A batch was committed (0).
     *
     * @param outputs the tables that it wrote, each with the version of its rows: none, for a
     *   batch that had no rows
     */
    final case class Ingested(
        run: RunId,
        batch: BatchId,
        outputs: Seq[BatchOutput],
        counts: RunCounts
    ) extends Outcome

    /**
     * The source changed under the plan of the batch, and the batch was given up: the next run
     * plans anew (ADR-0018). The run failed, as a transient failure (3).
     */
    final case class GivenUp(run: RunId, batch: BatchId, reason: String) extends Outcome

    /** The run failed, with the class that says what to do next (3, 4 or 5). */
    final case class Failed(run: RunId, failure: FailureClass, message: String) extends Outcome
  }

  /** What a run that had nothing to do counted: nothing, which it knows. */
  private val Idle = RunCounts(
    rowsRead = Some(0L),
    rowsWritten = Some(0L),
    rowsRejected = Some(0L),
    bytesRead = Some(0L)
  )

  /**
   * A table that has a batch, after a write of it.
   *
   * @param version where the batch is in the table
   * @param byThisRun whether this write put it there; if not, the table had it already
   * @param conflicts how many times the write was made again because the table had changed
   */
  final private case class Written(version: Long, byThisRun: Boolean, conflicts: Int)

  /** How many things that changed are kept with a batch that is given up, of however many. */
  val ChangedKept = 1000

  /**
   * What of a plan changed, as a batch that is given up keeps it (`abandon_detail`): each thing
   * by the name the plan has it under, with how it is not as it was planned, and how many there
   * were. The first `ChangedKept` are kept: the plan of the batch names them all.
   */
  def changedDetail(changed: SourceChangedException): Json = Json.obj(
    "changed" -> Json.fromValues(changed.changed.take(ChangedKept).map { one =>
      Json.obj("item" -> Json.fromString(one.item), "change" -> Json.fromString(one.change))
    }),
    "total" -> Json.fromInt(changed.changed.size)
  )
}
