package lakehouse.ingestion.engine

import java.time.{Clock, Instant}

import scala.util.control.NonFatal

import io.circe.Json
import lakehouse.ingestion.core.config.{
  ConfigVersion,
  Durations,
  Load,
  PathTableLocator,
  Profile,
  RootUri,
  Source,
  TableLocation
}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.core.watermark.BackfillRange
import lakehouse.ingestion.engine.bronze.{BronzeBatch, BronzeWriter}
import lakehouse.ingestion.engine.connector.{
  BoundSource,
  ConnectorRegistry,
  PlanContext,
  Progress,
  RangePlanner,
  ReadContext,
  ReadResult,
  SourceChangedException,
  UnreadableFilesException
}
import lakehouse.ingestion.engine.control.{
  Application,
  Backfills,
  Batches,
  ConfigSnapshots,
  ControlDatabase,
  LeasedRun,
  LeaseLostException,
  ProcessedFiles,
  SourceStates
}
import lakehouse.ingestion.engine.delta.{BatchAppend, DeltaTableTransactions, Lake}
import lakehouse.ingestion.engine.failure.RunFailedException
import lakehouse.ingestion.engine.quarantine.{QuarantineBatch, QuarantineWriter}
import lakehouse.ingestion.logstore.coordination.WriterEpoch
import lakehouse.ingestion.logstore.{CommitContext, EpochNotInForceException}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * One run of a source into Bronze: the protocol of ADR-0005 §3 from its lease to its commit.
 * It is the one place that holds the steps to each other, so that what the control plane says
 * of a batch and what its table has are never two stories.
 *
 *  1. The lease of the source, and the run under its epoch; or `SKIPPED_LOCKED`.
 *  1. The configuration of the run, held to the one that the state of the source was last
 *     advanced with: a change that what was ingested cannot follow ends the run, before
 *     anything else is asked (ADR-0007).
 *  1. The epoch of the lease, announced to the LogStore before any table is asked anything
 *     (ADR-0019). From then on a run that held the lease before commits to no table, so what
 *     the tables say of its batch is what they will say.
 *  1. The batch that a run before left planned, if there is one, finished as its table says:
 *     rolled forward, or run again (`Recovery`).
 *  1. Or the plan of the next batch, by the connector of the source, written down before
 *     anything of it is read, with the table that it is to write. A source with nothing new
 *     has no batch, and its run ends well.
 *  1. The read of the plan, by the connector.
 *  1. The records that could not be parsed, appended to the quarantine of the source, and then
 *     the rows, appended to its table of Bronze: each under the number of the batch, which
 *     Delta takes once (`QuarantineWriter`, `BronzeWriter`).
 *  1. One transaction in the control plane, on condition of the epoch: the state of the source
 *     moved, the files of the batch recorded as taken, the batch committed with what it wrote,
 *     the run succeeded, the lease given up (`LeasedRun.commit`).
 *
 * '''A source whose configuration changed in a way that Bronze cannot follow is not run'''
 * (ADR-0007, `ConfigVersion.breakingChanges`): another table of Bronze or another lake, whose
 * state says what was written to the old one; another watermark column; and, for a source
 * loaded by a watermark, another origin of its rows, whose watermark says how far something
 * else was read. The run fails for good, with each change and what it would have done, and
 * the batch that a run left planned, if there is one, stays as it was.
 *  - ''It is asked before a planned batch is looked for.'' A batch that is rolled forward
 *    moves the state under the configuration of the run that does it: asked after, the next
 *    run would be compared with the new configuration, find nothing changed, and go on from a
 *    watermark of what was read before.
 *  - ''What is compared with is the configuration of the run that last advanced the state'',
 *    and not that of the last run, which may be one that was refused for this very change. A
 *    source that has committed nothing has nothing to break.
 *  - ''Only what Bronze cannot follow.'' A key, or the table of Silver and its partitioning,
 *    change nothing of what was ingested: they are for a run into Silver to refuse (P10).
 *  - ''Nothing lets such a change through yet'': the way on is the configuration that the
 *    source had, which the control plane keeps. A reset of the state of a source is P7's.
 *
 * '''A batch with no rows writes nothing''': files that are all empty, or that have a header
 * and no record. Its files are recorded as taken by the commit, and no table is made nor given
 * a version: a table is made by the first batch that has rows, with the columns of its source.
 * A table made from an empty batch would have the columns of the engine alone, and the next
 * batch could not be appended to it.
 *
 * '''Rows that are read from their source each time are gone over once'''
 * (`ReadResult.OnePass`): a table of a database. They are not counted before they are written,
 * which would be a second statement at the source, and one that may see other rows. The run
 * asks for a first row, to know whether the batch writes anything, and then writes: the rows
 * are read by the write, and what the run read and wrote is what the commit of the batch
 * counted.
 *
 * '''What a run wrote is what the commits of its tables say''' (`rows_written`), for every
 * batch that it writes, and not what it counted before. For rows that the run counted as well,
 * the two counts were made apart: where they differ, the run says so, and goes on. What is to
 * come of a batch whose counts differ is not decided here (discovery §20, P11).
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
 * '''A batch that a run left planned is finished by the next''' (ADR-0005 §3, step 2). The
 * table that the batch was planned to write its data to is asked whether it has the batch,
 * which is the one question of recovery (`Recovery.of`). The table is the one that the plan
 * was written down with: a source whose configuration names another by now is refused, since
 * the other table has no batch of it, and would be given the rows a second time.
 *  - ''It has the batch'': the state is rolled forward, with nothing read and nothing written.
 *    What the commit records is said by the connector from the plan alone, and the versions
 *    that the batch wrote are read from the logs of its tables; one that a log no longer keeps
 *    is recorded as not known.
 *  - ''It does not'': the plan is run again as it was written. What of it a table has already,
 *    as its quarantine, is skipped by Delta. A file of it that changed since gives the batch
 *    up, as for the run that planned it.
 *
 * '''A run that lost its lease commits nothing more''' (ADR-0019). Every write of a run names
 * the epoch of its lease, and the LogStore takes a commit only while that epoch is the one in
 * force. So a run that is taken over while it works is stopped at its next commit, to any
 * table, and what the run that took over was told by the tables stays true.
 *
 * '''A run that loses its lease stops working''' (ADR-0006). That is what keeps a run from
 * reading what nobody will commit; what keeps it from doing harm is the epoch, above.
 *  - The jobs of Spark that the run is in the middle of are stopped the moment the lease is
 *    lost, by the run's own count or by what the control plane answered (`LeasedRun.onLost`,
 *    `RunJobs`). Who started one is told at once, and no thread is interrupted.
 *  - A job that is not running yet cannot be stopped, so the run asks for its lease before each
 *    step that starts one or writes a table: the read, the counts of what was read, the
 *    quarantine, the data (`LeasedRun.verify`, which asks nobody: it is the run's count).
 *  - The steps of the control plane are not asked for so: each asks the database, in its own
 *    transaction and under the epoch, which is the answer that counts. A run whose count ran
 *    out with its data written, and that the control plane still has as the holder of its
 *    lease, commits its batch.
 *
 * '''A failure that may pass is tried again where the run is durable''' (ADR-0022 §4, §5): a
 * failure of the class `transient`, or a conflict that Delta asks to be retried, has the run
 * wait and run its body again, under its lease and with its id, within the budget of the
 * profile. The body recovers what the run planned before it failed, as it recovers what a run
 * before it left (ADR-0005, step 2). Nothing is tried again once the run has lost its lease,
 * nor once it has started the transaction that ends it (`LeasedRun.ending`), whose answer may
 * be lost with the transaction committed: that transaction is settled instead (ADR-0022 §6,
 * `Settling`), and the run reads its own row before it makes it again. Taking the lease is
 * tried again the same way. What was tried again is counted in the run (`retries`,
 * `write_conflicts`).
 *
 * '''How a run ends.'''
 *  - A failure that says its class (`RunFailedException`) ends the run as `FAILED` with it, and
 *    gives the lease up. The batch, if one was planned, stays planned. If the control plane
 *    gives no answer, the run is reported with that class all the same; if it was not told,
 *    the run is left there as a process that died leaves it.
 *  - A run that finds its lease another's, where it asks the control plane, at its commit, or
 *    by a commit to a table that is refused for its epoch, stops there. It is not tried again
 *    and it says nothing to the control plane, where the run that took over has ended it.
 *  - A run whose lease ran out by its own count stops as well, and reports a transient failure.
 *    It tells the control plane so once: if the lease is still its own there, the run is
 *    `FAILED` and the lease is given up, so that the next run does not wait for it to expire.
 *    If the control plane does not answer, which is most often why the lease ran out, the
 *    lease expires and the run that takes it over marks this one `ABANDONED`.
 *  - Whatever a run fails with once its lease is lost, it ends as a run that lost its lease:
 *    the error of a job that was stopped, in whatever Spark wrapped it, and any other. A run
 *    without its lease has nothing else to say of its source that counts, and what it failed
 *    with is logged whole, as the cause, for the mistake that it may be.
 *  - A run whose last transaction gave no answer, and that could not learn within its budget
 *    whether it was committed, says nothing more to the control plane, and reports a transient
 *    failure: its row is what that transaction left, and the next run finds it.
 *  - Any other failure that the classifier knows (`Failures`: the LogStore, the conflicts of
 *    Delta, a database by its SQLState) ends the run as `FAILED` with its class, as one that
 *    said its class does (ADR-0022 §2). So does a failure to take the lease, which leaves no
 *    run to tell.
 *  - What nothing classified ends the run as `FAILED`, with the class `internal`, and gives
 *    the lease up (ADR-0022 §3). What the run records of it is its type and where it was
 *    thrown; the log has it whole. A failure to take the lease that nothing classified is let
 *    through, with no run to tell.
 *  - A fatal error of the JVM is let through, and nothing is said to the control plane: the
 *    lease is no longer renewed, and what is left is what a process that died leaves. The run
 *    that takes the lease over marks this one `ABANDONED`.
 *
 * '''A backfill is a sequence of its own''' (ADR-0021 §5). Its runs hold its lease, announce its
 * epoch and number its chunks, apart from the regular sequence of the source, which they
 * neither wait for nor touch. Its chunks are planned by the connector (`RangePlanner`), and the
 * commit of each moves the progress of the backfill, and not the state of the source. What is
 * between, the read, the writes and recovery, is that of any batch.
 *
 * @param lake the root of the lake of the profile: every table of a source is under it
 * @param retries for how long a run tries again after a failure that may pass, and how far
 *   apart (ADR-0022 §5): those of the profile
 * @param clock what "now" is for a plan, and when the rows of a batch were read
 * @param hooks what a run does at each fault point: nothing, outside tests
 * @param draw how the wait before a retry is drawn, from 0 to the bound given
 */
final class IngestionService(
    database: ControlDatabase,
    connectors: ConnectorRegistry,
    spark: SparkSession,
    lake: RootUri,
    timing: LeaseTiming,
    retries: Profile.Retries,
    clock: Clock = Clock.systemUTC(),
    hooks: FaultHooks = FaultHooks.Off,
    draw: Long => Long = RunRetries.Uniform
) {
  import IngestionService._

  private val locator = new PathTableLocator(lake)
  // The configuration that Delta hands the LogStore: where its coordination store is.
  private val hadoop = spark.sessionState.newHadoopConf()
  private val confinement = new Lake(lake, hadoop)
  private val tables = new DeltaTableTransactions(spark)

  /**
   * Runs `source` once.
   *
   * @param config the configuration that the run is recorded with: that of `source`, under the
   *   profile of this process
   * @throws java.lang.IllegalArgumentException if the application has no connector for the
   *   source. Nothing was started
   * @param horizon how far the invocation reads, as the batch before this run in its chain said
   *   (`Outcome.Next.Within`); none for the first run of an invocation, whose plan fixes it
   * @throws java.lang.Exception what nothing classified as the lease was taken (`Failures`),
   *   with no run to tell; and a fatal error of the JVM, which leaves the run as a process that
   *   died leaves it
   */
  def run(source: Source, config: ConfigVersion, horizon: Option[Json] = None): Outcome = {
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

    held(start, bound, config, horizon)(_.run())
  }

  /**
   * Reads the files of the committed batch `of` again, as a batch of the regular sequence of
   * `source` of the kind `reprocess`, by a run of that kind that `request` asked for (ADR-0021
   * §4). The batch has the next number and the plan of `of`, files, sizes and times, and is
   * written to the table that the configuration names now. From its plan on it is a batch like
   * any other: replayed or rolled forward by the run after one that died, given up if one of its
   * files changed since. Its commit records no file as taken.
   *
   * It is refused, as a permanent failure with nothing planned, while a batch is planned, for a
   * batch that was not committed, and for a source whose batches are no files: a window of a
   * table is read again by a backfill, which moves no watermark back, and a full load reads
   * the whole table at every run.
   *
   * @throws java.lang.IllegalArgumentException if the application has no connector for the
   *   source. Nothing was started
   */
  def reprocess(
      source: Source,
      config: ConfigVersion,
      of: BatchSeq,
      request: OperatorRequest
  ): Outcome = {
    val bound = connectors.bind(source)
    val start = RunStart(
      RunId.generate(),
      source.id,
      Layer.Bronze,
      RunKind.Reprocess,
      Application.instance,
      Application.version,
      config.hash,
      Some(request)
    )
    held(start, bound, config, None)(_.reprocess(of))
  }

  /**
   * One run of the backfill `request.id` of `source` (ADR-0021 §5), on the sequence of the
   * backfill, by a run of the kind `backfill` that `operator` asked for: one chunk of its range,
   * read and committed, as `run` reads one batch.
   *
   * The first run under the id creates the backfill, under its lease: its range ends no later
   * than the clock of the source less `safety_margin`, and its chunk is the one asked for, or
   * `max_batch_range` of the source. Each run after it resumes the backfill, held to the
   * configuration that it was created with, and is refused if it asks for another range or
   * another chunk. A chunk that a run left planned is finished first, as a batch of the regular
   * sequence is. A backfill that is complete has nothing more to read, and the run ends well
   * with no batch.
   *
   * It is refused, as a permanent failure with nothing created or planned, for a source whose
   * connector reads no range of it.
   *
   * @throws java.lang.IllegalArgumentException if the application has no connector for the
   *   source. Nothing was started
   */
  def backfill(
      source: Source,
      config: ConfigVersion,
      request: BackfillRequest,
      operator: OperatorRequest
  ): Outcome = {
    val bound = connectors.bind(source)
    val start = RunStart(
      RunId.generate(),
      source.id,
      Layer.Bronze,
      RunKind.Backfill,
      Application.instance,
      Application.version,
      config.hash,
      Some(operator),
      Some(request.id)
    )
    held(start, bound, config, None)(_.backfill(request))
  }

  /**
   * Takes the lease for `start`, and does `body` with the run that holds it; a run that does not
   * get the lease is skipped. How the run ends, whatever `body` throws, is said here, for every
   * kind of run that reads a source.
   */
  private def held(
      start: RunStart,
      bound: BoundSource,
      config: ConfigVersion,
      horizon: Option[Json]
  )(
      body: Attempt => Outcome
  ): Outcome = {
    val source = bound.source
    val retried = new RunRetries(retries, s"the run ${start.id} of ${source.id}", draw)
    opened(start, config, retried) match {
      case Left(notOpened) => notOpened
      case Right(leased) =>
        val jobs = new RunJobs(spark, start.id)
        // Before the first job of the run: what is running when the lease is lost is stopped.
        leased.onLost { loss =>
          jobs.stop(s"the run ${start.id} lost the lease of ${start.leaseKey}: ${loss.reason}")
        }
        val attempt = new Attempt(leased, bound, config, horizon, retried)
        // Tried again where the run is durable: its body, which recovers what it planned.
        try jobs.during(retried(leased.lost.isEmpty && !leased.ending)(body(attempt)))
        catch {
          // Before anything else: whatever it is wrapped in, it says that the lease is lost.
          case refused: Throwable if outOfTurn(refused).nonEmpty =>
            leased.close()
            fenced(start, outOfTurn(refused).fold("")(_.getMessage))
          case lost: LeaseLostException =>
            stopped(leased, lost.loss, failedWith = None, retried)
          // Whatever it is, it is of a run that is over: a job that was stopped, most often.
          case NonFatal(failure) if leased.lost.nonEmpty =>
            stopped(leased, leased.lost.get, failedWith = Some(failure), retried)
          // The transaction that ends the run gave no answer, and the run could not learn within
          // its budget whether it was committed (ADR-0022 §6).
          case failure @ Failures.Known(classed)
              if leased.ending && RunRetries.mayPass(classed.failure) =>
            unsettled(leased, classed, failure)
          // One that says its class, or one that the classifier knows (ADR-0022 §2).
          case failure @ Failures.Known(classed) =>
            log.error(s"the run ${start.id} of ${source.id} failed (${classed.failure})", failure)
            ended(leased, RunEnd.Failed(classed.failure, classed.getMessage), retried)
          // What nothing classified: a mistake of the application (ADR-0022 §3). The run says
          // so, and gives its lease up; the log has the failure whole.
          case NonFatal(unclassified) =>
            log.error(
              s"the run ${start.id} of ${source.id} failed with what nothing classified, a " +
                "mistake of the application",
              unclassified
            )
            ended(
              leased,
              RunEnd.Failed(FailureClass.Internal, Failures.unclassified(unclassified)),
              retried
            )
          case fatal: Throwable =>
            // A fatal error of the JVM: nothing is said of it, and the lease runs out, as that of
            // a process that died.
            leased.close()
            throw fatal
        }
    }
  }

  /**
   * The lease for `start`, or how the run ended without it: skipped, for a lease that another
   * run holds; failed with its class, for a control plane that could not be asked. A run that
   * failed so has no row, and nothing is told; unless the database took the lease and its answer
   * was lost, and then the lease runs out, and the run that takes it over ends this one.
   *
   * A failure that may pass is tried again, within the budget of the run (`retried`), which
   * counts it: the run records it once it has a row.
   *
   * @throws java.lang.Exception what nothing classified
   */
  private def opened(
      start: RunStart,
      config: ConfigVersion,
      retried: RunRetries
  ): Either[Outcome, LeasedRun] =
    try
      retried(may = true)(LeasedRun.open(database, start, config, timing, retried)).left.map {
        skipped =>
          Outcome.Skipped(start.id, skipped.holder, skipped.until)
      }
    catch {
      case failure @ Failures.Known(classed) =>
        log.error(
          s"the run ${start.id} of ${start.source} could not take the lease of " +
            s"${start.leaseKey} (${classed.failure})",
          failure
        )
        Left(Outcome.Failed(start.id, classed.failure, classed.getMessage))
    }

  /**
   * Ends a run that has lost its lease, as its heartbeat or the control plane told it. It may
   * pass, so it is reported as a transient failure.
   *
   * What the run says to the control plane depends on how the lease was lost, and what comes
   * of it is the database's to decide, as ever (`LeasedRun.finish`):
   *  - ''another run holds it'': nothing. That run has ended this one, as `ABANDONED`;
   *  - ''it ran out by the count of the run'': that it failed, once. A control plane that has
   *    the lease as this run's still takes it, and the lease is given up. One that does not
   *    answer is why the lease ran out, most often: the lease expires then.
   *
   * @param failedWith what the run failed with, if it was not with the loss itself: the error
   *   of a job that was stopped, or anything else. It is logged whole, since a mistake of the
   *   application that happens as a lease is lost is still one to look into
   */
  private def stopped(
      leased: LeasedRun,
      loss: LeaseLoss,
      failedWith: Option[Throwable],
      retried: RunRetries
  ): Outcome = {
    val start = leased.start
    failedWith.foreach { failure =>
      log.error(
        s"the run ${start.id} failed after it lost the lease of ${start.leaseKey} " +
          s"(${loss.reason}), and ends as a run that lost its lease. What it failed with " +
          "follows: the error of a job of Spark that was stopped is expected, and any other " +
          "is to be looked into",
        failure
      )
    }
    loss match {
      case LeaseLoss.TakenOver =>
        leased.close()
        fenced(start, loss.reason)
      case _: LeaseLoss.NotRenewed =>
        val message = s"the run ${start.id} lost the lease of ${start.leaseKey}: " +
          s"${loss.reason}. It stopped working, and committed nothing more: what it left is " +
          "finished by the next run of the source"
        log.error(message)
        val told =
          try
            Right(
              leased.finish(
                RunEnd.Failed(FailureClass.Transient, message, retried.into(RunCounts.Nothing))
              )
            )
          catch { case NonFatal(untold) => Left(untold) }
        told match {
          case Right(true) =>
            log.info(s"the run ${start.id} is recorded as failed, and its lease is given up")
          case Right(false) =>
            log.info(s"the run ${start.id} was ended by the run that took its lease")
          case Left(untold) =>
            log.warn(
              s"the control plane gave no answer, and may not have been told that the run " +
                s"${start.id} stopped. If it was not, the lease of the run expires, and the run " +
                "that takes it over ends this one",
              untold
            )
        }
        Outcome.Failed(start.id, FailureClass.Transient, message)
    }
  }

  /**
   * Says how a run failed, and what the process then reports.
   *
   * What the run failed with is known by then, and it is what the process reports whether or
   * not the control plane was told: a failure to say it is not the failure. One that gives no
   * answer may have been told, with the answer lost, and is not asked (ADR-0022 §6 settles the
   * transactions whose answer changes what the run reports, and this one does not). A run that
   * failed for good, or for its data, and whose word did not reach the control plane would
   * otherwise end as an exception that nothing classified, and whoever started it would not
   * know that running it again mends nothing. Untold, the run is left as a process that died
   * leaves it: its lease is no longer renewed, and the run that takes it over marks this one
   * `ABANDONED`, and finds what this one found.
   */
  private def ended(leased: LeasedRun, end: RunEnd.Failed, retried: RunRetries): Outcome = {
    val start = leased.start
    val told =
      try Right(leased.finish(end.copy(counts = retried.into(end.counts))))
      catch { case NonFatal(untold) => Left(untold) }
    told match {
      case Right(true) => Outcome.Failed(start.id, end.failure, end.message)
      case Right(false) => fenced(start, "another run ended it")
      case Left(untold) =>
        log.warn(
          s"the control plane gave no answer, and may not have been told that the run " +
            s"${start.id} failed (${end.failure}). It is reported as it failed all the same. " +
            "If it was not told, the lease of the run expires, and the run that takes it over " +
            "ends this one",
          untold
        )
        Outcome.Failed(start.id, end.failure, end.message)
    }
  }

  /**
   * A run that could not learn, within its budget, whether the transaction that ends it was
   * committed (ADR-0022 §6). Nothing more is said to the control plane: the row of the run is
   * what that transaction left, `RUNNING` with its lease running out, or ended, and the next run
   * of the source finds it, as it finds what a process that died left. It may pass.
   */
  private def unsettled(
      leased: LeasedRun,
      classed: RunFailedException,
      failure: Throwable
  ): Outcome = {
    val start = leased.start
    leased.close()
    val message = s"the run ${start.id} could not learn whether the transaction that ends it " +
      s"was committed: ${classed.getMessage}. What happened is in the control plane, and the " +
      s"next run of ${start.source} finds it"
    log.error(message, failure)
    Outcome.Failed(start.id, classed.failure, message)
  }

  /**
   * A run whose lease is another's. What it did is not committed, and its row says `ABANDONED`,
   * written by the run that took its place: there is nothing for it to say but to stop. It may
   * pass, so it is reported as a transient failure.
   */
  private def fenced(start: RunStart, how: String): Outcome = {
    val message = s"the run ${start.id} lost the lease of ${start.leaseKey}: $how. Nothing of " +
      "it was committed, and the run that holds the lease goes on with the source"
    log.error(message)
    Outcome.Failed(start.id, FailureClass.Transient, message)
  }

  /** The steps of one run that holds its lease. */
  final private class Attempt(
      leased: LeasedRun,
      bound: BoundSource,
      config: ConfigVersion,
      horizon: Option[Json],
      retried: RunRetries
  ) {
    private val start = leased.start
    private val source = bound.source
    private val table = locator.locate(source.targets.bronze.table)
    private val quarantineTable = Quarantine.table(source.id, start.layer)
    private val quarantine = locator.locate(quarantineTable)

    // The turn of this run at its source: the epoch of its lease, under the key of the lease.
    private val turn = WriterEpoch(start.leaseKey.value, leased.epoch.value)

    // One for the run, kept for all its writes: what they lose to other writers is counted in
    // it, and each is held to the epoch of the run.
    private val commits = CommitContext.forRun(start.id.uuid, turn)

    def run(): Outcome =
      prepared(heldToWhatWasIngested()) { _ =>
        leased.planned() match {
          case Some(left) => recover(left)
          case None => planNext()
        }
      }

    /** Plans a batch that reads the files of the committed batch `of` again, and ingests it. */
    def reprocess(of: BatchSeq): Outcome =
      prepared(heldToWhatWasIngested()) { _ =>
        leased.planned() match {
          // Planned by this run, which failed after and is tried again: it is finished.
          case Some(left) if left.plannedBy == start.id => recover(left)
          case Some(left) =>
            throw RunFailedException.permanent(
              s"the batch ${left.id} is planned: no batch is reprocessed while one is, since " +
                "it is the next run's to finish, or an operator's to give up first " +
                "(abandon-batch). Nothing was planned, read or written"
            )
          case None =>
            val earlier = BatchId(start.txnAppId, of)
            val record = database.run(Batches.find(_, earlier)).getOrElse {
              throw RunFailedException.permanent(
                s"there is no batch $earlier to reprocess. Nothing was planned, read or written"
              )
            }
            if (record.status != BatchStatus.Committed)
              throw RunFailedException.permanent(
                s"the batch $earlier is ${record.status.name}, and only a batch that was " +
                  "committed is reprocessed: its files are the ones that were taken. Nothing " +
                  "was planned, read or written"
              )
            if (bound.progress(record.plan).files.isEmpty)
              throw RunFailedException.permanent(
                s"the batch $earlier read no files, and only files are reprocessed: a window " +
                  "of a table is read again by a backfill, which moves no watermark back, and " +
                  "a full load reads the whole table at every run. Nothing was planned, read " +
                  "or written"
              )
            // The files of the batch, read as the source says to read them now (ADR-0024 §3).
            val batch = leased.plan(bound.again(record.plan), table, Some(of))
            log.info(
              s"the run ${start.id} planned ${batch.id}, which reads the files of $earlier again"
            )
            hooks.at(FaultPoint.AfterPlan, batch)
            ingest(batch)
        }
      }

    /**
     * Reads the next chunk of the backfill of this run's sequence, creating the backfill if it
     * is not there; or finishes the chunk that a run before left planned.
     */
    def backfill(request: BackfillRequest): Outcome =
      prepared(backfillOf(request)) { case (backfill, planner) =>
        if (backfill.completedAt.nonEmpty) complete(backfill)
        else
          leased.planned() match {
            case Some(left) => recover(left)
            case None => nextChunk(backfill, planner)
          }
      }

    /**
     * What every run does before it looks for a batch, and then `next`, unless a later epoch is
     * in force.
     *
     * @param held what holds the run to what its sequence has written: it refuses a
     *   configuration that this cannot follow, and gives what the run goes on with
     */
    private def prepared[A](held: => A)(next: A => Outcome): Outcome = {
      confinement.confine(table)
      confinement.confine(quarantine)
      // Before a batch is looked for: one that is rolled forward moves the state of its
      // sequence under the configuration of this run.
      val holding = held
      // Before a table is asked anything: what it then says of a batch is not changed behind
      // this run by one that held the lease before.
      announce().getOrElse(next(holding))
    }

    /**
     * The backfill of this run's sequence, and the connector's planner of its chunks: found and
     * held to what it was created with, or created now (ADR-0021 §5.3).
     *
     * @throws RunFailedException a permanent failure, for a source whose connector reads no
     *   range of it, for a backfill that is not what this run asks for or that cannot be
     *   created as it asks, and for a configuration that what the backfill has read cannot
     *   follow, unless it is complete and reads nothing more. Nothing was planned, read or
     *   written
     */
    private def backfillOf(request: BackfillRequest): (BackfillRecord, RangePlanner) = {
      val planner = bound.ranges.getOrElse {
        throw RunFailedException.permanent(
          s"${source.id} is not backfilled: its connector reads no range of it. A backfill " +
            "reads a range of the watermark column of a source that is loaded by one " +
            "(load.mode: incremental), by a connector that says that it can (ADR-0021 §5). " +
            "Nothing was created, planned, read or written"
        )
      }
      val backfill = database.run(Backfills.find(_, source.id, start.layer, request.id)) match {
        case Some(there) => resumed(there, request)
        case None => created(request, planner)
      }
      // A backfill that is complete reads nothing more, under whatever configuration.
      if (backfill.completedAt.isEmpty) heldToItsCreation(backfill)
      (backfill, planner)
    }

    /**
     * Creates the backfill that `request` asks for, under the lease of its sequence: its range
     * ends no later than the horizon of the source now, and its chunk is the one asked for or
     * `max_batch_range` of the source (ADR-0021 §5.4, §5.5).
     */
    private def created(request: BackfillRequest, planner: RangePlanner): BackfillRecord = {
      def refused(problem: String): Nothing =
        throw RunFailedException.permanent(
          s"the backfill ${start.txnAppId} is not created: $problem. Nothing was created, " +
            "planned, read or written"
        )
      val widest = source.load match {
        case incremental: Load.Incremental => incremental.maxBatchRange
        case _ => None
      }
      val chunk = request.chunk.orElse(widest).getOrElse {
        refused(
          "it is given no chunk (--chunk), and the source has no max_batch_range. A backfill " +
            "is read in chunks, since a history read in one statement is what a backfill is " +
            "for avoiding"
        )
      }
      request.range.settledBy(planner.horizon()).left.foreach(refused)
      leased.backfill(request.range.from, request.range.to, chunk) match {
        case LeasedRun.Backfill.Created(backfill) =>
          log.info(
            s"the run ${start.id} created the backfill ${start.txnAppId}, which reads " +
              s"${request.range} in chunks of ${Durations.render(backfill.chunk)}: the rows " +
              s"whose watermark is after ${backfill.from} and not after ${backfill.to}"
          )
          backfill
        // Not while this run holds the lease of its sequence: it is resumed, as any.
        case LeasedRun.Backfill.There(backfill) => resumed(backfill, request)
      }
    }

    /**
     * A backfill that a run before this one created, resumed if this run asks for what it was
     * created with (ADR-0021 §5.3). A run that asks for no chunk takes the backfill's.
     */
    private def resumed(there: BackfillRecord, request: BackfillRequest): BackfillRecord = {
      val other = Seq(
        Option.when(there.from != request.range.from || there.to != request.range.to)(
          s"the range ${request.range}, and it reads (${there.from}, ${there.to}]"
        ),
        request.chunk
          .filter(_ != there.chunk)
          .map(chunk =>
            s"chunks of ${Durations.render(chunk)}, and its chunks are of " +
              Durations.render(there.chunk)
          )
      ).flatten
      if (other.nonEmpty)
        throw RunFailedException.permanent(
          s"the backfill ${start.txnAppId} was created by the run ${there.createdBy} at " +
            s"${there.createdAt}, and this run asks for ${other.mkString(", and for ")}. A " +
            "backfill is resumed with what it was created with, and another range is another " +
            "backfill, under another id. Nothing was planned, read or written"
        )
      log.info(
        s"the run ${start.id} resumes the backfill ${start.txnAppId}, which reads " +
          s"(${there.from}, ${there.to}] in chunks of ${Durations.render(there.chunk)}, " +
          there.progress.fold("from its start")(at => s"after its chunk ${at.lastCommitted}")
      )
      there
    }

    /**
     * Refuses a configuration that changed, since the backfill of this run's sequence was
     * created, in a way that what it has read cannot follow (ADR-0021 §5.9, ADR-0007): against
     * the configuration of the run that created it, which a reset of the source does not change.
     *
     * @throws RunFailedException a permanent failure, with each such change. Nothing was
     *   planned, read or written
     */
    private def heldToItsCreation(backfill: BackfillRecord): Unit =
      database.run(ConfigSnapshots.ofBackfill(_, start.txnAppId)).foreach { created =>
        val broken = ConfigVersion.breakingChanges(created.content, config).filter(_.breaksBronze)
        if (broken.nonEmpty)
          throw RunFailedException.permanent(
            s"the configuration of ${source.id} changed since the run ${backfill.createdBy} " +
              s"created the backfill ${start.txnAppId}, in a way that what the backfill has " +
              s"read cannot follow: ${broken.map(_.message).mkString("; ")}. Nothing was " +
              "planned, read or written, and a chunk that a run left planned is as it was. A " +
              "backfill is held to the configuration that it was created with, which is in " +
              s"control.config_snapshot under the hash ${created.hash}, and which a reset of " +
              "the state of the source does not change: give the source that one, and run " +
              "the backfill again; or read what is left of its range as a backfill of its " +
              "own, under another id"
          )
      }

    /** A backfill that is complete: there is nothing more of it to read. */
    private def complete(backfill: BackfillRecord): Outcome = {
      log.info(
        s"the backfill ${start.txnAppId} is complete: it has read (${backfill.from}, " +
          s"${backfill.to}], and the run ${start.id} has nothing to read"
      )
      if (leased.finish(RunEnd.Succeeded(retried.into(Idle)))) Outcome.NothingNew(start.id)
      else fenced(start, "another run ended it")
    }

    /** Plans the next chunk of a backfill, from where its last one left it, and ingests it. */
    private def nextChunk(backfill: BackfillRecord, planner: RangePlanner): Outcome = {
      val range = BackfillRange
        .of(backfill.from, backfill.to)
        .fold(
          problem =>
            throw new IllegalStateException(
              s"the backfill ${start.txnAppId} has a range that no backfill is made with: $problem"
            ),
          identity
        )
      planner.chunk(range, backfill.chunk, backfill.progress.map(_.watermark)) match {
        case None =>
          throw new IllegalStateException(
            s"the backfill ${start.txnAppId} has read its range $range, and is not complete: " +
              "the commit of the chunk that ends its range completes it"
          )
        case Some(plan) =>
          // The write-ahead step, as for any batch.
          val batch = leased.plan(plan, table)
          log.info(s"the run ${start.id} planned ${batch.id}, a chunk of its backfill")
          hooks.at(FaultPoint.AfterPlan, batch)
          ingest(batch)
      }
    }

    /**
     * Refuses a configuration that changed in a way that what Bronze has of the source cannot
     * follow (ADR-0007), against the one that the state of the source was last advanced with.
     * The lease is this run's, so that state is not moved while it is asked.
     *
     * @throws RunFailedException a permanent failure, with each such change and what would go
     *   wrong if the source simply ran on. Nothing was planned, read or written
     */
    private def heldToWhatWasIngested(): Unit =
      database.run(ConfigSnapshots.ofState(_, source.id, start.layer)).foreach { last =>
        val broken = ConfigVersion.breakingChanges(last.content, config).filter(_.breaksBronze)
        if (broken.nonEmpty)
          throw RunFailedException.permanent(
            s"the configuration of ${source.id} changed in a way that what was ingested of " +
              s"it cannot follow: ${broken.map(_.message).mkString("; ")}. Nothing was " +
              "planned, read or written, and a batch that a run left planned is as it was. " +
              "The configuration that the state of the source was last advanced with is in " +
              s"control.config_snapshot, under the hash ${last.hash}: give the source that " +
              "one, and run it again. To go on under the new one, an operator resets the " +
              "state of the source with reset-state and a reason, setting its watermark " +
              "(--watermark), forgetting it (--forget-watermark) or keeping it " +
              "(--keep-watermark); its configuration is then the one that later runs are " +
              "compared with"
          )
      }

    /**
     * Says to the LogStore that the epoch of this run is the one in force for its lease, before
     * a table is asked anything (`EpochAnnouncement`).
     *
     * @return how the run ended, if a later epoch is in force; none, if its own is
     */
    private def announce(): Option[Outcome] =
      EpochAnnouncement.announce(hadoop, start, turn).left.toOption.map { message =>
        ended(leased, RunEnd.Failed(FailureClass.Permanent, message), retried) match {
          case refused @ Outcome.Failed(_, FailureClass.Permanent, _) =>
            log.error(s"the run ${start.id} of ${source.id} failed: $message")
            refused
          // Ended by the run that took the lease over: `fenced` said so.
          case lost => lost
        }
      }

    /** Plans the next batch of the source, and ingests it. */
    private def planNext(): Outcome = {
      val state = database.run(SourceStates.find(_, source.id, start.layer))
      val context = PlanContext(clock.instant(), state.flatMap(_.watermark), leased, horizon)
      bound.plan(context) match {
        case None =>
          log.info(s"the run ${start.id} found nothing new in ${source.id}, and has no batch")
          if (leased.finish(RunEnd.Succeeded(retried.into(Idle)))) Outcome.NothingNew(start.id)
          else fenced(start, "another run ended it")
        case Some(plan) =>
          // The write-ahead step: from here on, the plan is what is read, and the table is
          // where it is written.
          val batch = leased.plan(plan, table)
          log.info(s"the run ${start.id} planned ${batch.id}")
          hooks.at(FaultPoint.AfterPlan, batch)
          ingest(batch)
      }
    }

    /**
     * Finishes the batch that a run before this one left planned, as its table says (ADR-0005
     * §3, step 2).
     */
    private def recover(left: PlannedBatch): Outcome = {
      // The table that is asked is the one that the batch was planned for, and no other.
      left.target.filter(_ != table.uri).foreach { planned =>
        throw RunFailedException.permanent(
          s"the batch ${left.id} was planned by the run ${left.plannedBy} to be written to " +
            s"$planned, and the configuration of ${source.id} has its table at ${table.uri} " +
            "now. A batch is finished in the table that it was planned for, where its rows " +
            "may be already: in another it would be written a second time. Nothing was read " +
            "or written. Give the source the table that it had (targets.bronze.table, and " +
            "the lake of the profile) and run it again"
        )
      }
      Recovery.of(Some(left), table, tables) match {
        case Recovery.RollForward(batch) =>
          log.info(
            s"the run ${start.id} found ${batch.id}, which the run ${batch.plannedBy} planned, " +
              s"written to ${table.uri}: it is committed with nothing read"
          )
          rollForward(batch)
        case _ =>
          log.info(
            s"the run ${start.id} found ${left.id}, which the run ${left.plannedBy} planned, " +
              s"not written to ${table.uri}: its plan is run again as it was written"
          )
          ingest(left)
      }
    }

    /**
     * Commits a batch whose data a run before this one wrote, with nothing read and nothing
     * written: what that run did not get to say is said.
     */
    private def rollForward(batch: PlannedBatch): Outcome = {
      // From the plan alone: what the batch read is not there to be read again, and need not be.
      val progress = bound.progress(batch.plan)
      // The quarantine is written before the data, so a batch whose data is written has its
      // quarantine written, if it has one.
      val apart = Option.when(has(quarantine, batch))(
        BatchOutput(quarantineTable, versionOf(quarantine, batch))
      )
      val outputs = apart.toVector :+
        BatchOutput(source.targets.bronze.table, versionOf(table, batch))
      // This run read nothing, which it knows. What the batch wrote, and what of it was
      // rejected, were counted by the run that did, if it got to say so: not by this one.
      val read = RunCounts(rowsRead = Some(0L), bytesRead = Some(0L))

      hooks.at(FaultPoint.BeforeStateCommit, batch)
      val last = completes(batch)
      // The counts with the retries, for each time the commit is made, and once it is: what the
      // run says is what its row has.
      if (
        leased.commit(
          batch.id,
          progress.watermark,
          outputs,
          retried.into(read),
          progress.files,
          last
        )
      ) {
        log.info(s"the run ${start.id} committed ${batch.id}, rolled forward${completed(last)}")
        Outcome.RolledForward(start.id, batch.id, outputs, retried.into(read))
      } else fenced(start, "another run holds it")
    }

    /**
     * Whether the commit of a batch completes its sequence: the chunk of a backfill whose plan
     * says that there is no more after it, as it ends the range. The regular sequence is never
     * complete.
     */
    private def completes(batch: PlannedBatch): Boolean =
      start.backfill.nonEmpty && bound.more(batch.plan).isEmpty

    private def completed(last: Boolean): String =
      if (last) s", and the backfill ${start.txnAppId} is complete" else ""

    private def has(in: TableLocation, batch: PlannedBatch): Boolean =
      tables.lastVersion(in, batch.id.app).exists(_ >= batch.id.seq.value)

    /** Where a batch is in a table that has it; none, if its log no longer keeps the commit. */
    private def versionOf(in: TableLocation, batch: PlannedBatch): Option[Long] = {
      val version = tables.committedAt(in, batch.id)
      if (version.isEmpty)
        log.warn(
          s"the table at ${in.uri} has ${batch.id}, and no commit that its log still keeps " +
            "holds it: the batch is recorded with a version that is not known"
        )
      version
    }

    /** Steps 4 to 8, for a batch whose plan is written down. */
    private def ingest(batch: PlannedBatch): Outcome = {
      // Before the read: a plan that says nothing of what its commit records is not read.
      val progress = bound.progress(batch.plan)
      // Nor is anything read by a run that has lost its lease.
      leased.verify()
      // A read that starts jobs beyond those of its plan asks the lease before each, and waits
      // within the budget of the run, while the run may go on.
      val context = ReadContext(
        spark,
        verify = () => leased.verify(),
        pause = wait => retried.pause(wait, leased.lost.isEmpty && !leased.ending): Unit
      )
      val read =
        try Right(bound.read(batch.plan, context))
        catch {
          case changed: SourceChangedException => Left(changed)
          case cannot: UnreadableFilesException => Left(cannot)
        }
      read match {
        case Left(failed) =>
          // A run that lost its lease while it read gives nothing up. A batch is given up by a
          // transaction that the epoch of the lease fences, and not its count: one whose count
          // ran out, and that nobody took, is still the run's. And what a read meets once the
          // lease is lost may be what the loss did: a read of a file stopped for the lease fails
          // as one of a file that cannot be read does.
          leased.verify()
          failed match {
            case changed: SourceChangedException => giveUp(batch, changed)
            case cannot: UnreadableFilesException => skip(batch, progress, cannot)
            case other => throw other
          }
        case Right(kept: ReadResult.Kept) => finish(batch, progress, kept)
        case Right(once: ReadResult.OnePass) => finishInOnePass(batch, progress, once)
      }
    }

    /**
     * Writes a batch whose rows are read from their source as they are written, and commits
     * it. The source is asked twice: for a first row, and for the rows.
     */
    private def finishInOnePass(
        batch: PlannedBatch,
        progress: Progress,
        read: ReadResult.OnePass
    ): Outcome = {
      val readAt = clock.instant()
      hooks.at(FaultPoint.AfterRead, batch)

      // The probe is a job of Spark, and a statement at the source: neither is for a run that
      // has lost its lease.
      leased.verify()
      // One row of it is taken. A batch with none writes nothing, and its table is not touched.
      val some = overItsRows(read.probe.head(1).nonEmpty)
      hooks.at(FaultPoint.AfterCounts, batch)

      val data = Option.when(some) {
        leased.verify()
        // The one pass over the rows. Nothing was counted, so the commit is held to nothing.
        val written = this.written(table, batch, counted = None) {
          overItsRows {
            new BronzeWriter(confinement, tables, commits)
              .write(table, BronzeBatch(start.id, source.id, batch.id, readAt), read.rows)
          }
        }
        hooks.at(FaultPoint.AfterBronzeCommit, batch)
        written
      }

      // Rows of such a source are never records to parse: none of them is rejected.
      val known = RunCounts(rowsRejected = Some(0L), bytesRead = progress.bytes)
      val counts = data match {
        case None => known.copy(rowsRead = Some(0L), rowsWritten = Some(0L))
        // Read and written are one pass, and one count: that of the commit.
        case Some(write) if write.byThisRun =>
          known.copy(rowsRead = write.rows, rowsWritten = write.rows)
        // Another writer of this sequence wrote the batch, from the same plan: what it read
        // and wrote was that writer's to count, and this run counted nothing.
        case Some(_) => known
      }
      val outputs =
        data.map(write => BatchOutput(source.targets.bronze.table, write.version)).toVector
      // What the table has of the batch is what this run read of its source: its rows, or
      // none, where the source had no first row. Not so if another writer wrote the batch.
      commit(batch, progress, outputs, withWrites(counts, data.toSeq), data.forall(_.byThisRun))
    }

    /**
     * A step that goes over rows that are read from their source as it does. The source may
     * fail in it, long after the connector answered, and what such a failure is for the run is
     * the connector's to say (`SourceConnector.failure`): a failure that it does not know for
     * its source's is let through as it is.
     *
     * A failure that has its class already is left alone. One that is said here keeps what it
     * was as its cause, so that a commit that the LogStore refused for its epoch, or the loss
     * of the lease, is still found in it by whatever ends the run.
     */
    private def overItsRows[T](step: => T): T =
      try step
      catch {
        case classed: RunFailedException => throw classed
        case NonFatal(failed) => throw bound.failure(failed).getOrElse(failed)
      }

    /**
     * The commit of a batch that is written: step 7.
     *
     * @param readByThisRun whether what the tables have of the batch is what this run read of
     *   its source, and not what another writer of its sequence did
     */
    private def commit(
        batch: PlannedBatch,
        progress: Progress,
        outputs: Vector[BatchOutput],
        written: RunCounts,
        readByThisRun: Boolean
    ): Outcome = {
      hooks.at(FaultPoint.BeforeStateCommit, batch)
      val last = completes(batch)
      // As in `rollForward`: the counts are made for each time the commit is, and once it is.
      if (
        leased.commit(
          batch.id,
          progress.watermark,
          outputs,
          retried.into(written),
          progress.files,
          last
        )
      ) {
        log.info(s"the run ${start.id} committed ${batch.id}${completed(last)}")
        val counts = retried.into(written)
        // What comes after: what the plan says, for a plan of this run's, and for a chunk of a
        // backfill, whose range is the same whoever planned it. For a batch that a run before
        // this one planned, under a horizon that was not this invocation's, a plan made anew;
        // unless the plan is its source whole and this run has just read it, which is what a
        // plan made anew would read.
        val next =
          if (batch.plannedBy == start.id || start.backfill.nonEmpty)
            bound.more(batch.plan).fold[Outcome.Next](Outcome.Next.Ends)(Outcome.Next.Within)
          else if (readByThisRun && bound.wholeWhenRead(batch.plan)) Outcome.Next.Ends
          else Outcome.Next.Afresh
        Outcome.Ingested(start.id, batch.id, outputs, counts, next)
      } else fenced(start, "another run holds it")
    }

    /**
     * Writes what was read of a batch and is held apart from its source, its quarantine and
     * then its data, and commits it.
     */
    private def finish(batch: PlannedBatch, progress: Progress, read: ReadResult.Kept): Outcome = {
      val readAt = clock.instant()
      hooks.at(FaultPoint.AfterRead, batch)

      // A run that lost its lease while it read counts nothing, and writes nothing.
      leased.verify()
      // Of what Spark keeps of the read (`ReadResult`): the files are not read again.
      val rows = read.rows.count()
      val unparsed = read.unparsed.fold(0L)(_.count())
      val counted = RunCounts(
        rowsRead = Some(rows + unparsed),
        rowsWritten = Some(0L),
        rowsRejected = Some(unparsed),
        bytesRead = progress.bytes
      )
      hooks.at(FaultPoint.AfterCounts, batch)

      // The quarantine before the data (ADR-0005 §3, step 5), and whatever becomes of the batch:
      // a record that was rejected is there to be looked at, the more so when there are too many.
      val apart = read.unparsed.filter(_ => unparsed > 0).map { records =>
        // The counts were jobs of Spark: a run that lost its lease in them writes no table.
        leased.verify()
        val written = this.written(quarantine, batch, counted = Some(unparsed)) {
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
          leased.verify()
          val written = this.written(table, batch, counted = Some(rows)) {
            new BronzeWriter(confinement, tables, commits)
              .write(table, BronzeBatch(start.id, source.id, batch.id, readAt), read.rows)
          }
          hooks.at(FaultPoint.AfterBronzeCommit, batch)
          written
        }

        val outputs = apart.map(write => BatchOutput(quarantineTable, write.version)).toVector ++
          data.map(write => BatchOutput(source.targets.bronze.table, write.version))
        val counts = withWrites(
          // What the commit of the table counted, and not what the run counted before it. If
          // another writer of this sequence wrote the batch, from the same plan, its rows are
          // there once, and this run wrote none of them.
          counted.copy(rowsWritten = data.fold(Option(0L))(_.rows)),
          apart.toSeq ++ data
        )
        commit(batch, progress, outputs, counts, data.forall(_.byThisRun))
      }
    }

    /**
     * What a write of the batch to one of its tables came to.
     *
     * @param counted how many rows the run counted before it gave them to the write, if it
     *   did: the commit is held to it, and a difference is said
     */
    private def written(to: TableLocation, batch: PlannedBatch, counted: Option[Long])(
        write: => BatchAppend.Result
    ): Written = write match {
      case BatchAppend.Appended(version, conflicts, rows) =>
        rows match {
          case None =>
            log.warn(
              s"the commit of ${batch.id} at version $version of ${to.uri} does not say how " +
                "many rows it wrote, and what the run wrote there is recorded as not known. " +
                s"A session of the engine has Delta say it (${DeltaTableTransactions.RowsWritten})"
            )
          case Some(said) =>
            counted.filter(_ != said).foreach { before =>
              log.warn(
                s"the run counted $before rows of ${batch.id} before it wrote them to " +
                  s"${to.uri}, and the commit at version $version says that $said were " +
                  "written. The batch goes on, with both counts as they were made"
              )
            }
        }
        Written(Some(version), byThisRun = true, conflicts, rows)
      // A run before this one wrote it: the quarantine of a batch that is run again.
      case BatchAppend.AlreadyThere(version) =>
        if (version.isEmpty)
          log.warn(
            s"the table at ${to.uri} has ${batch.id}, and no commit that its log still keeps " +
              "holds it: the batch is recorded with a version that is not known"
          )
        Written(version, byThisRun = false, conflicts = 0, rows = None)
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
          s"$rejected of the $read records of ${batch.id} were set apart, as records that could " +
            "not be parsed or that are not as their source writes them, which is more " +
            s"than the share that ${source.id} allows, ${source.quality.maxRejectedRatio} " +
            "(quality.max_rejected_ratio). The batch is not committed, and nothing of it was " +
            s"written to Bronze: it stays planned. The records are in quarantine, at " +
            quarantine.uri,
          counts
        ),
        retried
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
          // Made for each time the transaction is, with the retries made by then.
          def end = RunEnd.Failed(
            FailureClass.Transient,
            message,
            retried.into(RunCounts(rowsWritten = Some(0L)))
          )
          if (leased.abandon(batch.id, changed.summary, Some(changedDetail(changed)), end))
            Outcome.GivenUp(start.id, batch.id, changed.summary)
          else fenced(start, "another run holds it")
      }

    /**
     * Gives up a batch with files that cannot be read, under a source that skips them, if its
     * table does not have the batch: the files are recorded as `FAILED`, with why, in the
     * transaction that gives the batch up, and the run succeeds, having done what its source
     * says (ADR-0023 §5). The next plan is made without them.
     *
     * A reprocess skips nothing: its files were taken, by the batch that read them, and an
     * operator named them. One that cannot be read fails it as permanent.
     */
    private def skip(
        batch: PlannedBatch,
        progress: Progress,
        cannot: UnreadableFilesException
    ): Outcome = {
      if (batch.reprocessOf.nonEmpty)
        throw RunFailedException.permanent(
          s"${batch.id} reads again files that were taken, which an operator named, and a " +
            s"reprocess skips none of them: ${cannot.summary}. The batch stays planned, and " +
            "abandon-batch gives it up",
          cannot
        )
      val files = progress.files.getOrElse(
        throw new IllegalStateException(
          s"the read of ${batch.id} says that files of it cannot be read, and its plan names none"
        )
      )
      val byName = files.files.map(file => file.name.value -> file).toMap
      val failed = cannot.unreadable.map { one =>
        ProcessedFiles.FailedFile(
          byName.getOrElse(
            one.item,
            throw new IllegalStateException(
              s"the read of ${batch.id} says that '${one.item}' cannot be read, and its plan " +
                "has no file of that name"
            )
          ),
          one.why
        )
      }
      Recovery.of(Some(batch), table, tables) match {
        case Recovery.RollForward(_) =>
          throw new IllegalStateException(
            s"files of ${batch.id} cannot be read, and the table at ${table.uri} has the " +
              "batch: another writer of this sequence wrote it. Nothing was given up, and " +
              "nothing was changed: the run after this one recovers the batch as the table " +
              s"says. What cannot be read: ${cannot.summary}"
          )
        case _ =>
          log.warn(
            s"${batch.id} was given up, and the next run plans without the files that cannot " +
              s"be read: ${cannot.summary}"
          )
          // Made for each time the transaction is, with the retries made by then.
          def end = RunEnd.Succeeded(retried.into(RunCounts(rowsWritten = Some(0L))))
          if (leased.abandon(batch.id, cannot.summary, Some(unreadableDetail(cannot)), end, failed))
            Outcome.FilesSkipped(start.id, batch.id, cannot.unreadable)
          else fenced(start, "another run holds it")
      }
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
     * A batch was committed (0): one that this run planned, or one that a run before left
     * planned with its data not written, whose plan this run ran again.
     *
     * @param outputs the tables that it wrote, each with the version of its rows: none, for a
     *   batch that had no rows
     */
    final case class Ingested(
        run: RunId,
        batch: BatchId,
        outputs: Seq[BatchOutput],
        counts: RunCounts,
        next: Next = Next.Ends
    ) extends Outcome

    /**
     * What an invocation is to do after a batch is committed (discovery §17, P6.10). A batch
     * that a run rolls forward is followed as `Afresh`, whatever its plan: what it has is as
     * old as the run that wrote it.
     */
    sealed trait Next
    object Next {

      /**
       * The invocation has read as far as it may: the plan of the batch says so, for a plan
       * of this invocation's. Or the batch was planned by a run before it, its plan is its
       * source whole (`SourceConnector.wholeWhenRead`), and this run has just read it.
       */
      case object Ends extends Next

      /**
       * The plan says that there is more, and the next run plans against this horizon. For a
       * chunk of a backfill, it is the end of its range, which the next run of the backfill
       * has already.
       */
      final case class Within(horizon: Json) extends Next

      /**
       * The batch was planned by a run before this one, of another invocation, and stops
       * short of what the source has by now: the next run plans as the first of an invocation
       * does, and fixes the horizon.
       */
      case object Afresh extends Next
    }

    /**
     * A batch that a run before this one wrote, and did not get to commit, was committed (0).
     * This run read nothing and wrote nothing: it said what the other could not.
     *
     * @param outputs the tables that the batch wrote, each with the version of its rows where
     *   the log of the table still says it
     */
    final case class RolledForward(
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

    /**
     * Files of the batch cannot be read, and the source skips them (ADR-0023): the batch was
     * given up, and each file recorded as `FAILED`, with why. The run succeeded, and the next
     * run plans without them (0).
     */
    final case class FilesSkipped(
        run: RunId,
        batch: BatchId,
        skipped: Seq[UnreadableFilesException.Unreadable]
    ) extends Outcome

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
   * @param version where the batch is in the table; none, for a batch that the table had
   *   already, in a commit that its log no longer keeps
   * @param byThisRun whether this write put it there; if not, the table had it already
   * @param conflicts how many times the write was made again because the table had changed
   * @param rows how many rows this write put there, as its commit says; none, for a batch that
   *   the table had already, and for a commit that does not say
   */
  final private case class Written(
      version: Option[Long],
      byThisRun: Boolean,
      conflicts: Int,
      rows: Option[Long]
  )

  /**
   * The commit that the LogStore refused for its epoch, if that is what a failure of a run is,
   * in whatever Spark and Delta wrapped it (ADR-0019). It says that the lease of the run is
   * another's: the run stops, and is not tried again.
   */
  private def outOfTurn(failure: Throwable): Option[EpochNotInForceException] =
    Iterator
      .iterate(failure)(_.getCause)
      .takeWhile(_ != null)
      .take(MaxCauses)
      .collectFirst { case refused: EpochNotInForceException => refused }

  /** How far the causes of a failure are followed: further than any chain that ends. */
  private val MaxCauses = 64

  /** How many things that changed are kept with a batch that is given up, of however many. */
  val ChangedKept = 1000

  /**
   * The files of a plan that cannot be read, as a batch that is given up for them keeps them
   * (`abandon_detail`): each by its name in the plan, with why, and how many there were. The
   * first `ChangedKept` are kept: each is recorded in `processed_file` too.
   */
  def unreadableDetail(cannot: UnreadableFilesException): Json = Json.obj(
    "unreadable" -> Json.fromValues(cannot.unreadable.take(ChangedKept).map { one =>
      Json.obj("item" -> Json.fromString(one.item), "why" -> Json.fromString(one.why))
    }),
    "total" -> Json.fromInt(cannot.unreadable.size)
  )

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
