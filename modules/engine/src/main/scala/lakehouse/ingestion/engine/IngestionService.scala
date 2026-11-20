package lakehouse.ingestion.engine

import java.io.IOException
import java.time.{Clock, Instant}

import scala.util.control.NonFatal

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
import lakehouse.ingestion.logstore.coordination.WriterEpoch
import lakehouse.ingestion.logstore.{CommitContext, EpochNotInForceException, WriterEpochs}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * One run of a source into Bronze: the protocol of ADR-0005 §3 from its lease to its commit.
 * It is the one place that holds the steps to each other, so that what the control plane says
 * of a batch and what its table has are never two stories.
 *
 *  1. The lease of the source, and the run under its epoch; or `SKIPPED_LOCKED`.
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
 * '''How a run ends.'''
 *  - A failure that says its class (`RunFailedException`) ends the run as `FAILED` with it, and
 *    gives the lease up. The batch, if one was planned, stays planned. If the control plane
 *    cannot be told, the run is reported with that class all the same, and is left there as
 *    a process that died leaves it.
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
 *  - Any other exception is let through, and nothing is said to the control plane: the lease is
 *    no longer renewed, and what is left is what a process that died leaves. The run that takes
 *    the lease over marks this one `ABANDONED`. Telling such errors apart is the classification
 *    of errors (P8).
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
  // The configuration that Delta hands the LogStore: where its coordination store is.
  private val hadoop = spark.sessionState.newHadoopConf()
  private val confinement = new Lake(lake, hadoop)
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
        val jobs = new RunJobs(spark, start.id)
        // Before the first job of the run: what is running when the lease is lost is stopped.
        leased.onLost { loss =>
          jobs.stop(s"the run ${start.id} lost the lease of ${start.leaseKey}: ${loss.reason}")
        }
        try jobs.during(new Attempt(leased, bound).run())
        catch {
          // Before anything else: whatever it is wrapped in, it says that the lease is lost.
          case refused: Throwable if outOfTurn(refused).nonEmpty =>
            leased.close()
            fenced(start, outOfTurn(refused).fold("")(_.getMessage))
          case lost: LeaseLostException => stopped(leased, lost.loss, failedWith = None)
          // Whatever it is, it is of a run that is over: a job that was stopped, most often.
          case NonFatal(failure) if leased.lost.nonEmpty =>
            stopped(leased, leased.lost.get, failedWith = Some(failure))
          case failed: RunFailedException =>
            log.error(s"the run ${start.id} of ${source.id} failed (${failed.failure})", failed)
            ended(leased, RunEnd.Failed(failed.failure, failed.getMessage))
          case unclassified: Throwable =>
            // Nothing is said of it: the lease runs out, as that of a process that died.
            leased.close()
            throw unclassified
        }
    }
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
      failedWith: Option[Throwable]
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
          try Right(leased.finish(RunEnd.Failed(FailureClass.Transient, message)))
          catch { case NonFatal(untold) => Left(untold) }
        told match {
          case Right(true) =>
            log.info(s"the run ${start.id} is recorded as failed, and its lease is given up")
          case Right(false) =>
            log.info(s"the run ${start.id} was ended by the run that took its lease")
          case Left(untold) =>
            log.warn(
              s"the control plane could not be told that the run ${start.id} stopped. Its " +
                "lease expires, and the run that takes it over ends this one",
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
   * not the control plane could be told: a failure to say it is not the failure. A run that
   * failed for good, or for its data, and whose word did not reach the control plane would
   * otherwise end as an exception that nothing classified, and whoever started it would not
   * know that running it again mends nothing. Untold, the run is left as a process that died
   * leaves it: its lease is no longer renewed, and the run that takes it over marks this one
   * `ABANDONED`, and finds what this one found.
   */
  private def ended(leased: LeasedRun, end: RunEnd.Failed): Outcome = {
    val start = leased.start
    val told =
      try Right(leased.finish(end))
      catch { case NonFatal(untold) => Left(untold) }
    told match {
      case Right(true) => Outcome.Failed(start.id, end.failure, end.message)
      case Right(false) => fenced(start, "another run ended it")
      case Left(untold) =>
        log.warn(
          s"the control plane could not be told that the run ${start.id} failed " +
            s"(${end.failure}). It is reported as it failed all the same. Its lease expires, " +
            "and the run that takes it over ends this one",
          untold
        )
        Outcome.Failed(start.id, end.failure, end.message)
    }
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
  final private class Attempt(leased: LeasedRun, bound: BoundSource) {
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

    def run(): Outcome = {
      confinement.confine(table)
      confinement.confine(quarantine)
      // Before a table is asked anything: what it then says of a batch is not changed behind
      // this run by one that held the lease before.
      announce()
      leased.planned() match {
        case Some(left) => recover(left)
        case None => planNext()
      }
    }

    /**
     * Says to the LogStore that the epoch of this run is the one in force for its lease
     * (ADR-0019). Once it has, a commit of a run under an earlier epoch is in its table, where
     * this run finds it, or will never be.
     *
     * @throws LeaseLostException if a later epoch is in force: the lease was taken again
     */
    private def announce(): Unit = {
      val inForce =
        try WriterEpochs.announce(hadoop, turn)
        catch {
          case e: IOException =>
            throw RunFailedException.transient(
              s"the epoch of the lease of ${start.leaseKey} could not be announced to the " +
                s"coordination store, and nothing was read or written: ${e.getMessage}",
              e
            )
        }
      if (inForce != turn.epoch) {
        log.error(
          s"the run ${start.id} holds $turn, and the epoch in force is $inForce. Either the " +
            "lease was taken again, or the epochs of this key were announced by another " +
            "control plane: those are forgotten with the maintenance role (ADR-0019)"
        )
        throw new LeaseLostException(start.leaseKey.value, LeaseLoss.TakenOver)
      }
    }

    /** Plans the next batch of the source, and ingests it. */
    private def planNext(): Outcome = {
      val state = database.run(SourceStates.find(_, source.id, start.layer))
      val context = PlanContext(clock.instant(), state.flatMap(_.watermark), leased)
      bound.plan(context) match {
        case None =>
          log.info(s"the run ${start.id} found nothing new in ${source.id}, and has no batch")
          if (leased.finish(RunEnd.Succeeded(Idle))) Outcome.NothingNew(start.id)
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
      val counts = RunCounts(rowsRead = Some(0L), bytesRead = Some(0L))

      hooks.at(FaultPoint.BeforeStateCommit, batch)
      if (leased.commit(batch.id, progress.watermark, outputs, counts, progress.files)) {
        log.info(s"the run ${start.id} committed ${batch.id}, rolled forward")
        Outcome.RolledForward(start.id, batch.id, outputs, counts)
      } else fenced(start, "another run holds it")
    }

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
      val read =
        try Right(bound.read(batch.plan, ReadContext(spark)))
        catch { case changed: SourceChangedException => Left(changed) }
      read.fold(giveUp(batch, _), finish(batch, progress, _))
    }

    /** Writes what was read of a batch, its quarantine and then its data, and commits it. */
    private def finish(batch: PlannedBatch, progress: Progress, read: ReadResult): Outcome = {
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
          leased.verify()
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
        Written(Some(version), byThisRun = true, conflicts)
      // A run before this one wrote it: the quarantine of a batch that is run again.
      case BatchAppend.AlreadyThere(version) =>
        if (version.isEmpty)
          log.warn(
            s"the table at ${to.uri} has ${batch.id}, and no commit that its log still keeps " +
              "holds it: the batch is recorded with a version that is not known"
          )
        Written(version, byThisRun = false, conflicts = 0)
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
        counts: RunCounts
    ) extends Outcome

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
   */
  final private case class Written(version: Option[Long], byThisRun: Boolean, conflicts: Int)

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
