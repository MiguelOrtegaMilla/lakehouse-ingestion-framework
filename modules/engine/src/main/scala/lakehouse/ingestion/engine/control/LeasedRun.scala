package lakehouse.ingestion.engine.control

import java.sql.{Connection, SQLException}
import java.time.Instant

import io.circe.Json
import lakehouse.ingestion.core.config.{ConfigVersion, RelativePath, TableLocation}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.core.files.{FilePlan, LandedFile}
import lakehouse.ingestion.engine.connector.FileRegistry
import org.slf4j.LoggerFactory

/**
 * A run that holds the lease of its source (ADR-0006): what the engine has in its hands from
 * the moment it may work to the moment it says how it ended.
 *
 * It is made by `LeasedRun.open`, which takes the lease and writes the run in one transaction.
 * From then on the lease is renewed behind it (`LeaseHeartbeat`), and the run asks `verify`
 * before each thing it must not do without its lease.
 *
 * @param epoch the epoch of the lease: what every statement that advances the state of the
 *   source names, so that a run whose lease was taken changes nothing
 * @param tookOver the run whose lease this one took after it expired, if there was one: it is
 *   marked `ABANDONED`, and what it left halfway is this run's to recover
 */
final class LeasedRun private (
    database: ControlDatabase,
    val start: RunStart,
    val epoch: Epoch,
    val tookOver: Option[RunId],
    heartbeat: LeaseHeartbeat
) extends AutoCloseable
    with FileRegistry {

  /** Why the lease is lost, if it is. */
  def lost: Option[LeaseLoss] = heartbeat.lost

  /** @throws LeaseLostException if the run has lost its lease, and must stop */
  @throws[LeaseLostException]
  def verify(): Unit = heartbeat.verify()

  /** What to do the moment the lease is lost: stop what the run is in the middle of. */
  def onLost(callback: LeaseLoss => Unit): Unit = heartbeat.onLost(callback)

  /**
   * The batch that a run before this one planned and did not commit, if there is one (ADR-0005
   * §3, step 2). It is this run's to finish: to run again, or only to account for, as the
   * table says (`Recovery`).
   *
   * @throws LeaseLostException if the lease is another run's by now
   */
  @throws[SQLException]
  @throws[LeaseLostException]
  def planned(): Option[PlannedBatch] = holding { connection =>
    val found = Batches.planned(connection, start.txnAppId)
    found.foreach(batch => Runs.took(connection, start.id, batch.id))
    found
  }

  /**
   * Writes the plan of the next batch down, before anything of it is read: the write-ahead
   * step. Whatever happens to this run from here on, the next one finds the plan and reads
   * what it says, no more and no less.
   *
   * @param target the table that the batch is to write its data to: the one that the run that
   *   finishes the batch asks whether it has it
   * @throws LeaseLostException if the lease is another run's by now
   * @throws java.sql.SQLException with state 23505 if a batch is planned already: it is to be
   *   finished first (`planned`)
   */
  @throws[SQLException]
  @throws[LeaseLostException]
  def plan(plan: Json, target: TableLocation): PlannedBatch = holding { connection =>
    val batch = Batches.plan(connection, start, plan, target)
    Runs.took(connection, start.id, batch.id)
    batch
  }

  /**
   * The files of `directory` with these names that batches of the source have taken: what a
   * file source plans its next batch without (`FilePlanner`).
   */
  @throws[SQLException]
  override def processed(directory: RelativePath, names: Seq[String]): Set[LandedFile] =
    database.run(ProcessedFiles.among(_, start.source, directory, names))

  /**
   * Commits a batch whose data is in its tables (ADR-0005 §3, steps 7 and 8): moves the state
   * of the source past it, records the files it took, marks the batch committed with what it
   * wrote, ends the run as succeeded, and gives the lease up. All of it in one transaction, on
   * condition that the lease is still this run's under its epoch, or none of it.
   *
   * @param watermark where the next batch starts from; none, for a source that has none
   * @param files the plan of the batch, for a file source: its files are recorded as taken. It
   *   must be the plan that was written down for the batch
   * @return false if the lease is another run's: this process was a zombie, and nothing was
   *   changed. The batch is the other run's to finish, and the data that this one wrote is
   *   in the table once all the same, by the number of the batch
   * @throws java.lang.IllegalStateException if the lease is this run's and the batch cannot be
   *   committed: it is not the planned one, the state is past it, `files` is not its plan, or a
   *   file of it was taken already. Nothing was changed
   */
  @throws[SQLException]
  def commit(
      batch: BatchId,
      watermark: Option[Json],
      outputs: Seq[BatchOutput],
      counts: RunCounts = RunCounts.Nothing,
      files: Option[FilePlan] = None
  ): Boolean = {
    require(batch.app == start.txnAppId, s"the batch $batch is not of ${start.txnAppId}")
    // Before the lease stops being renewed: a watermark that cannot be stored is a mistake of
    // whoever gave it, and no reason to let the lease go.
    watermark.foreach(Batches.readable("watermark", _))
    heartbeat.close()
    database.transaction { connection =>
      Leases.holds(connection, start.leaseKey, start.id, epoch) && {
        // Each says whether it did. One that did not is a state that is not what this run
        // took it for, and the transaction ends with nothing done.
        def must(done: Boolean, what: String): Unit =
          if (!done) throw new IllegalStateException(s"the batch $batch was not committed: $what")

        must(
          SourceStates
            .advance(connection, start.source, start.layer, watermark, batch.seq, start.id),
          s"the state of ${start.txnAppId} is at this batch already, or past it"
        )
        files.foreach { plan =>
          // The files that are recorded are those that the batch read: the plan that was
          // written down for it, and not what a caller says it was.
          val written = Batches.planned(connection, start.txnAppId).filter(_.id == batch)
          must(written.nonEmpty, "it is not a planned batch")
          must(
            written.exists(planned => FilePlan.fromJson(planned.plan) == Right(plan)),
            "the files given are not those of its plan"
          )
          must(
            ProcessedFiles.record(connection, start.source, batch, plan),
            "a file of its plan was taken by a batch already"
          )
        }
        must(Batches.commit(connection, batch, start.id, outputs), "it is not a planned batch")
        must(
          Runs.end(connection, start.id, RunEnd.Succeeded(counts)),
          s"the run ${start.id} is not running"
        )
        must(
          Leases.release(connection, start.leaseKey, start.id, epoch),
          "its lease could not be given up"
        )
        true
      }
    }
  }

  /**
   * Gives a planned batch up, when the source no longer has what its plan names and nothing of
   * the batch is in its tables (ADR-0018): marks the batch abandoned, with who, when and why,
   * ends the run as failed, and gives the lease up. All of it in one transaction, on condition
   * that the lease is still this run's under its epoch, or none of it.
   *
   * The files of the batch are not recorded as taken, the state of the source does not move,
   * and the number of the batch is not used again. The next run finds nothing planned, and
   * plans anew from what the source has by then.
   *
   * That nothing of the batch is in its tables is the caller's to ask first, of the table
   * (`TableTransactions`): a batch whose data is written is rolled forward, and never given up.
   *
   * @param reason why, for a person: it is kept with the batch
   * @param detail what of the plan could not be run, in a form of the engine's own
   * @param end how the run ends. A run that was to ingest the batch did not do what it was
   *   asked, and does not succeed; an operator's run that was asked to give it up does
   *   (ADR-0021 §2)
   * @return false if the lease is another run's: this process was a zombie, and nothing was
   *   changed. The batch is the other run's, to finish or to give up
   * @throws java.lang.IllegalStateException if the lease is this run's and the batch cannot be
   *   given up: it is not the planned one. Nothing was changed, and the lease is no longer
   *   renewed
   * @throws java.lang.IllegalArgumentException if the reason says nothing, or the detail could
   *   not be stored. Nothing was changed, and the lease is still renewed
   */
  @throws[SQLException]
  def abandon(
      batch: BatchId,
      reason: String,
      detail: Option[Json],
      end: RunEnd
  ): Boolean = {
    require(batch.app == start.txnAppId, s"the batch $batch is not of ${start.txnAppId}")
    // Before the lease stops being renewed, as the watermark of a commit: what cannot be
    // stored is a mistake of whoever gave it, and no reason to let the lease go.
    Batches.abandonable(reason, detail)
    heartbeat.close()
    database.transaction { connection =>
      Leases.holds(connection, start.leaseKey, start.id, epoch) && {
        def must(done: Boolean, what: String): Unit =
          if (!done) throw new IllegalStateException(s"the batch $batch was not given up: $what")

        must(
          Batches.abandon(connection, batch, start.id, reason, detail),
          "it is not a planned batch"
        )
        must(Runs.end(connection, start.id, end), s"the run ${start.id} is not running")
        must(
          Leases.release(connection, start.leaseKey, start.id, epoch),
          "its lease could not be given up"
        )
        true
      }
    }
  }

  /**
   * Runs `body` in a transaction that has the lease locked, and found it to be this run's. What
   * `body` writes is then written by the holder of the lease, whatever happens next.
   */
  private def holding[T](body: Connection => T): T = {
    val done = database.transaction { connection =>
      Option.when(Leases.holds(connection, start.leaseKey, start.id, epoch))(body(connection))
    }
    done.getOrElse {
      heartbeat.taken()
      throw new LeaseLostException(start.leaseKey.value, LeaseLoss.TakenOver)
    }
  }

  /**
   * Ends the run, and gives its lease up, in one transaction. The lease is no longer renewed,
   * whatever the answer.
   *
   * What decides is the database, not what this process believes of its lease: the run is
   * ended only if it is still `RUNNING`, which it is not once another run has taken its lease.
   *
   * @return false if another run ended this one, as `ABANDONED`: this process was a zombie,
   *   and nothing was changed
   * @throws java.sql.SQLException if the control plane could not be told. The run stays
   *   `RUNNING` and its lease expires: the next run of the source takes it over
   * @throws java.lang.IllegalStateException if it is to end as succeeded with a batch of its
   *   sequence planned and not committed: the only way past a batch is `commit`. Nothing was
   *   changed, and the lease is no longer renewed
   */
  @throws[SQLException]
  def finish(end: RunEnd): Boolean = {
    heartbeat.close()
    database.transaction { connection =>
      // The lease first, as in every transaction that touches both it and the run. A run that
      // takes the lease over locks the lease and then ends this run; this one, ending itself
      // first and giving the lease up after, would wait for it while it waits for this.
      val held = Leases.holds(connection, start.leaseKey, start.id, epoch)
      if (held && end.status == RunStatus.Succeeded)
        Batches.planned(connection, start.txnAppId).foreach { batch =>
          throw new IllegalStateException(
            s"the run ${start.id} cannot end as succeeded: the batch ${batch.id} is planned and " +
              "not committed. A run succeeds past a batch by committing it"
          )
        }
      val ended = Runs.end(connection, start.id, end)
      if (held) Leases.release(connection, start.leaseKey, start.id, epoch): Unit
      ended
    }
  }

  /**
   * Stops renewing the lease, and tells the control plane nothing: what is left of a process
   * that died. The run stays `RUNNING`, and its lease expires.
   */
  override def close(): Unit = heartbeat.close()
}

object LeasedRun {

  private val logger = LoggerFactory.getLogger(classOf[LeasedRun])

  /**
   * The lease was taken, and the run was recorded as `SKIPPED_LOCKED`: nothing was read or
   * written, and it is not a failure (exit code 2).
   *
   * @param holder the run that holds the lease
   * @param until when its lease expires, unless it is renewed
   */
  final case class Skipped(start: RunStart, holder: RunId, instance: InstanceId, until: Instant)

  /**
   * Starts a run: stores its configuration, takes the lease of its source and writes the run,
   * `RUNNING` under the epoch of the lease, or `SKIPPED_LOCKED` if another run holds it.
   *
   * The lease and the run are one transaction. So are the lease and the end of the run that
   * held it before and let it expire: no moment has a lease whose holder is not a run that is
   * running.
   *
   * @param config the configuration that `start` names by its hash
   */
  @throws[SQLException]
  def open(
      database: ControlDatabase,
      start: RunStart,
      config: ConfigVersion,
      timing: LeaseTiming
  ): Either[Skipped, LeasedRun] = {
    require(
      start.configHash == config.hash,
      "the run names another configuration than the one given"
    )
    // By itself: it is the same whoever stores it, and a run that then does not start leaves
    // nothing wrong behind. Inside the transaction it would make every run wait on the first.
    database.run(ConfigSnapshots.store(_, start.source, config)): Unit

    // Said before the lease is asked for, and not after: a process that has logged nothing yet
    // sets its logging up here, and not on the time of a lease that it already holds.
    logger.info(s"asking for the lease of ${start.leaseKey}, for the run ${start.id}")

    // Before the lease is asked for: what the run counts from runs out before the lease does.
    val askedAt = System.nanoTime()
    val taken = database.transaction { connection =>
      Leases.acquire(connection, start.leaseKey, start.id, start.instance, timing.ttl) match {
        case acquired: Leases.Acquisition.Acquired =>
          // It held the lease and let it expire: it ended halfway, and said nothing.
          acquired.tookOver.foreach(Runs.abandon(connection, _))
          Runs.open(connection, start, acquired.epoch)
          Right(acquired)
        case Leases.Acquisition.Busy(holder) =>
          Runs.skip(connection, start)
          Left(Skipped(start, holder.run, holder.instance, holder.expiresAt))
      }
    }

    taken.left.foreach { skipped =>
      logger.info(
        s"the lease of ${start.leaseKey} is held by the run ${skipped.holder} (${skipped.instance}) " +
          s"until ${skipped.until}, unless it is renewed: the run ${start.id} is skipped"
      )
    }
    taken.map { acquired =>
      logger.info(
        s"the run ${start.id} holds the lease of ${start.leaseKey} under epoch ${acquired.epoch}" +
          acquired.tookOver.fold("")(previous =>
            s"; it expired for the run $previous, which ended halfway and is marked ABANDONED"
          )
      )
      val heartbeat = new LeaseHeartbeat(
        start.leaseKey.value,
        timing,
        askedAt,
        () => database.run(Leases.renew(_, start.leaseKey, start.id, acquired.epoch, timing.ttl))
      )
      new LeasedRun(database, start, acquired.epoch, acquired.tookOver, heartbeat)
    }
  }
}
