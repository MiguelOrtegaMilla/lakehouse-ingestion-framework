package lakehouse.ingestion.engine.operator

import java.time.Instant

import scala.util.control.NonFatal

import lakehouse.ingestion.core.config.{
  ConfigVersion,
  PathTableLocator,
  RootUri,
  Source,
  TableLocation
}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.control.{
  Application,
  ControlDatabase,
  LeasedRun,
  LeaseLostException
}
import lakehouse.ingestion.engine.delta.BatchLookup.Found
import lakehouse.ingestion.engine.delta.{BatchLookup, DeltaTableTransactions}
import lakehouse.ingestion.engine.{EpochAnnouncement, RunFailedException}
import lakehouse.ingestion.logstore.coordination.WriterEpoch
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * What an operator does to a source (ADR-0021). Each action is a run of a kind of its own, under
 * the lease of the sequence that it acts on: it takes the lease, or is recorded as skipped while
 * a run holds it; it announces its epoch before it asks a table anything, so that a run that held
 * the lease before commits nothing after; and it changes what it changes in one transaction under
 * that epoch, with the end of its run and the release of its lease.
 *
 * @param lake the root of the lake of the profile, where the configuration of a source names its
 *   table
 */
final class OperatorActions(
    database: ControlDatabase,
    spark: SparkSession,
    lake: RootUri,
    timing: LeaseTiming
) {
  import OperatorActions._

  private val locator = new PathTableLocator(lake)
  // The configuration that Delta hands the LogStore: where its coordination store is.
  private val hadoop = spark.sessionState.newHadoopConf()
  private val lookup = new BatchLookup(new DeltaTableTransactions(spark), () => hadoop)

  /**
   * Gives up the batch of the regular sequence of `source` that is planned, if it is `seq` and
   * the table that it was planned for does not have it (ADR-0021 §2).
   *
   * The table is the one that the batch was planned for (`ingestion_batch.target_table`), wherever
   * the configuration has the table of the source by now. A batch that it has is committed by the
   * next run, never given up. One whose table cannot be read is not given up either: whether its
   * rows are there is not known.
   *
   * @param config the configuration that the run is recorded with. It is not compared with the
   *   one that the batch was planned under: giving up a batch planned under another is what the
   *   action is for
   */
  def abandon(
      source: Source,
      config: ConfigVersion,
      seq: BatchSeq,
      request: OperatorRequest
  ): Outcome = {
    val start = starting(source, config, RunKind.Abandon, request)
    held(start, config) { leased =>
      leased.planned() match {
        case None =>
          refuse(
            leased,
            s"no batch of ${start.txnAppId} is planned: there is no batch $seq to give up, " +
              "and nothing was changed"
          )
        case Some(planned) if planned.id.seq != seq =>
          refuse(
            leased,
            s"the batch of ${start.txnAppId} that is planned is ${planned.id.seq}, and not " +
              s"$seq: an operator gives up the batch that was looked at, and nothing was changed"
          )
        case Some(planned) =>
          val table = planned.target.fold(locator.locate(source.targets.bronze.table)) { uri =>
            TableLocation
              .stored(uri)
              .fold(
                reason => throw new IllegalStateException(s"the batch ${planned.id} names $reason"),
                identity
              )
          }
          lookup.of(table, planned.id) match {
            case Found.Written =>
              refuse(
                leased,
                s"the batch ${planned.id} is in ${table.uri}, which it was planned for: its " +
                  "rows are written, and a batch whose rows are written is committed, never " +
                  "given up. Nothing was changed: the next run of the source commits it, with " +
                  "nothing read (ADR-0005)"
              )
            case Found.Unknown(why) =>
              fail(
                leased,
                FailureClass.Transient,
                s"$why. The batch ${planned.id} was not given up, since its rows may be " +
                  "there: once the table can be read, ask again"
              )
            case Found.NotWritten =>
              if (leased.abandon(planned.id, request.reason, None, RunEnd.Succeeded())) {
                val done = s"the batch ${planned.id} was given up by the run ${start.id}, " +
                  s"asked by ${request.requestedBy}: ${request.reason}. ${table.uri} does " +
                  "not have it, and the next run of the source plans the next batch from its " +
                  "state as it is"
                log.info(done)
                Outcome.Done(start.id, done)
              } else lost(start, LeaseLoss.TakenOver)
          }
      }
    }
  }

  private def starting(
      source: Source,
      config: ConfigVersion,
      kind: RunKind,
      request: OperatorRequest
  ): RunStart =
    RunStart(
      RunId.generate(),
      source.id,
      Layer.Bronze,
      kind,
      Application.instance,
      Application.version,
      config.hash,
      Some(request)
    )

  /** Takes the lease of the run, announces its epoch, and does `body` with it. */
  private def held(start: RunStart, config: ConfigVersion)(body: LeasedRun => Outcome): Outcome =
    LeasedRun.open(database, start, config, timing) match {
      case Left(skipped) => Outcome.Skipped(start.id, skipped.holder, skipped.until)
      case Right(leased) =>
        try {
          val turn = WriterEpoch(start.leaseKey.value, leased.epoch.value)
          EpochAnnouncement.announce(hadoop, start, turn) match {
            case Left(refused) => refuse(leased, refused)
            case Right(()) => body(leased)
          }
        } catch {
          case gone: LeaseLostException => stopped(leased, gone.loss)
          case NonFatal(_) if leased.lost.nonEmpty => stopped(leased, leased.lost.get)
          case failed: RunFailedException => fail(leased, failed.failure, failed.getMessage)
          case unclassified: Throwable =>
            // Nothing is said of it: the lease runs out, as that of a process that died.
            leased.close()
            throw unclassified
        }
    }

  /** Refused, with nothing changed: running it again as it is does not mend it. */
  private def refuse(leased: LeasedRun, message: String): Outcome =
    fail(leased, FailureClass.Permanent, message)

  /**
   * Ends the run as failed, with its lease given up, and says so. A control plane that has the
   * run ended already ended it for a run that took the lease over: the action lost its lease.
   */
  private def fail(leased: LeasedRun, failure: FailureClass, message: String): Outcome = {
    val start = leased.start
    val told =
      try Right(leased.finish(RunEnd.Failed(failure, message)))
      catch { case NonFatal(untold) => Left(untold) }
    told match {
      case Right(false) => lost(start, LeaseLoss.TakenOver)
      case Right(true) =>
        log.error(s"the run ${start.id} of ${start.source} failed ($failure): $message")
        Outcome.Failed(start.id, failure, message)
      case Left(untold) =>
        log.warn(
          s"the control plane could not be told that the run ${start.id} failed ($failure). " +
            "It is reported as it failed all the same. Its lease expires, and the run that " +
            "takes it over ends this one",
          untold
        )
        Outcome.Failed(start.id, failure, message)
    }
  }

  /** Ends a run that lost its lease: it changed nothing, and running again later may do. */
  private def stopped(leased: LeasedRun, loss: LeaseLoss): Outcome = {
    loss match {
      case LeaseLoss.TakenOver => leased.close()
      case _: LeaseLoss.NotRenewed =>
        try leased.finish(RunEnd.Failed(FailureClass.Transient, loss.reason)): Unit
        catch { case NonFatal(_) => () }
    }
    lost(leased.start, loss)
  }

  private def lost(start: RunStart, loss: LeaseLoss): Outcome = {
    val message = s"the run ${start.id} lost the lease of ${start.leaseKey}: ${loss.reason}. " +
      "Nothing was changed"
    log.error(message)
    Outcome.Failed(start.id, FailureClass.Transient, message)
  }
}

object OperatorActions {

  private val log = LoggerFactory.getLogger(classOf[OperatorActions])

  /** How an operator's action ended. */
  sealed trait Outcome {
    def run: RunId
  }

  object Outcome {

    /** It did what it was asked. */
    final case class Done(run: RunId, message: String) extends Outcome

    /** A run holds the lease of the sequence: nothing was done, and the action is recorded. */
    final case class Skipped(run: RunId, holder: RunId, until: Instant) extends Outcome

    /** It did nothing, for the reason given, of the class that says what running again does. */
    final case class Failed(run: RunId, failure: FailureClass, message: String) extends Outcome
  }
}
