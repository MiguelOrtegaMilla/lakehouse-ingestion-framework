package lakehouse.ingestion.engine.operator

import java.time.Instant

import scala.util.control.NonFatal

import io.circe.Json
import lakehouse.ingestion.core.config.ConfigVersion.BreakingChange
import lakehouse.ingestion.core.config.{
  ConfigVersion,
  ConnectorKind,
  PathTableLocator,
  RootUri,
  Source,
  TableLocation
}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.control.{
  Application,
  Backfills,
  ConfigSnapshots,
  ControlDatabase,
  LeasedRun,
  LeaseLostException,
  SourceStates
}
import lakehouse.ingestion.engine.delta.BatchLookup.Found
import lakehouse.ingestion.engine.delta.{BatchLookup, DeltaTableTransactions}
import lakehouse.ingestion.engine.{EpochAnnouncement, Failures}
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
 * @param kinds the kinds of source of the application: what makes a moment into the watermark
 *   of a source
 */
final class OperatorActions(
    database: ControlDatabase,
    spark: SparkSession,
    lake: RootUri,
    timing: LeaseTiming,
    kinds: Seq[ConnectorKind]
) {
  import OperatorActions._

  private val locator = new PathTableLocator(lake)
  // The configuration that Delta hands the LogStore: where its coordination store is.
  private val hadoop = spark.sessionState.newHadoopConf()
  private val lookup = new BatchLookup(new DeltaTableTransactions(spark), () => hadoop)

  /**
   * Gives up the batch of a sequence of `source` that is planned, if it is `seq` and the table
   * that it was planned for does not have it (ADR-0021 §2): of the regular sequence, or of that of
   * a backfill, the chunk that is planned, under the lease of the backfill. A backfill that is not
   * there is refused, as having no batch.
   *
   * The table is the one that the batch was planned for (`ingestion_batch.target_table`), wherever
   * the configuration has the table of the source by now. A batch that it has is committed by the
   * next run, never given up. One whose table cannot be read is not given up either: whether its
   * rows are there is not known.
   *
   * @param config the configuration that the run is recorded with. It is not compared with the
   *   one that the batch was planned under: giving up a batch planned under another is what the
   *   action is for
   * @param backfill the backfill whose chunk is given up; none, for the regular sequence
   */
  def abandon(
      source: Source,
      config: ConfigVersion,
      seq: BatchSeq,
      request: OperatorRequest,
      backfill: Option[BackfillId] = None
  ): Outcome = {
    val start = starting(source, config, RunKind.Abandon, request).copy(backfill = backfill)
    // What runs next on the sequence: it commits a batch that its table has, or plans anew.
    val next =
      backfill.fold("the next run of the source")(id => s"the next run of the backfill $id")
    held(start, config) { leased =>
      val missing =
        backfill.filter(id => database.run(Backfills.find(_, source.id, start.layer, id)).isEmpty)
      missing match {
        case Some(id) =>
          refuse(
            leased,
            s"${source.id} has no backfill $id: there is no batch $seq of ${start.txnAppId} to " +
              "give up, and nothing was changed"
          )
        case None => givenUp(leased, source, seq, request, next)
      }
    }
  }

  /** The planned batch of the sequence of the run, given up if it is `seq` and not written. */
  private def givenUp(
      leased: LeasedRun,
      source: Source,
      seq: BatchSeq,
      request: OperatorRequest,
      next: String
  ): Outcome = {
    val start = leased.start
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
                s"given up. Nothing was changed: $next commits it, with nothing read " +
                "(ADR-0005)"
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
                s"not have it, and $next plans the next batch from its " +
                (if (start.backfill.isEmpty) "state as it is"
                 else "progress as it is: the window of this one, again")
              log.info(done)
              Outcome.Done(start.id, done)
            } else lost(start, LeaseLoss.TakenOver)
        }
    }
  }

  /**
   * Resets the state of the regular sequence of `source` (ADR-0021 §3): its watermark is set,
   * forgotten or kept, as `to` says, and the configuration that it is given is the one that
   * later runs are compared with. That is how a change that Bronze cannot follow is let through.
   *
   * The last batch committed does not move, and the next batch has the next number: a sequence
   * that started again would be skipped by every table that it wrote. The files that were taken
   * stay taken. It is refused while a batch is planned, and for a source that has no state.
   *
   * What it lets through is said with the outcome. When the watermark is kept across such a
   * change, so are its risks: a Bronze table that is another has none of the history, and a
   * watermark kept for another column skips rows if that column does not mean what the old one
   * did.
   */
  def reset(
      source: Source,
      config: ConfigVersion,
      to: WatermarkReset,
      request: OperatorRequest
  ): Outcome = {
    val start = starting(source, config, RunKind.Reset, request)
    held(start, config) { leased =>
      leased.planned() match {
        case Some(planned) =>
          refuse(
            leased,
            s"the batch ${planned.id} is planned: the state of a source is not reset while a " +
              "batch is, since that batch is the next run's to finish under the configuration " +
              "it was planned under, or an operator's to give up first (abandon-batch). " +
              "Nothing was changed"
          )
        case None =>
          database.run(SourceStates.find(_, source.id, start.layer)) match {
            case None =>
              refuse(
                leased,
                s"${source.id} has no state in ${start.layer.name}: no batch of it was ever " +
                  "committed, and there is nothing to reset. Where its first batch starts is " +
                  "load.watermark.start. Nothing was changed"
              )
            case Some(before) =>
              watermarkOf(source, before.watermark, to) match {
                case Left(why) => refuse(leased, s"$why. Nothing was changed")
                case Right(watermark) =>
                  val through = database
                    .run(ConfigSnapshots.ofState(_, source.id, start.layer))
                    .toSeq
                    .flatMap(last => ConfigVersion.breakingChanges(last.content, config))
                    .filter(_.breaksBronze)
                  if (leased.reset(before, watermark)) {
                    val done = s"the state of ${start.txnAppId} was reset by the run " +
                      s"${start.id}, asked by ${request.requestedBy}: ${request.reason}. Its " +
                      s"watermark was ${shown(before.watermark)} and is ${shown(watermark)}; " +
                      s"the last batch committed is still ${before.lastCommitted}, and the " +
                      s"next one is ${before.lastCommitted.value + 1}. The configuration of " +
                      "this run is the one that later runs are compared with, and it lets " +
                      (if (through.isEmpty) "no breaking change through"
                       else s"through: ${through.map(_.message).mkString("; ")}")
                    val warnings = if (to == WatermarkReset.Keep) risks(through) else Nil
                    log.info(done)
                    warnings.foreach(warning => log.warn(warning))
                    Outcome.Done(start.id, done, warnings)
                  } else lost(start, LeaseLoss.TakenOver)
              }
          }
      }
    }
  }

  /** The watermark that a reset leaves, or why it cannot. */
  private def watermarkOf(
      source: Source,
      now: Option[Json],
      to: WatermarkReset
  ): Either[String, Option[Json]] = to match {
    case WatermarkReset.Keep => Right(now)
    case WatermarkReset.Forget =>
      if (now.isEmpty)
        Left(
          s"${source.id} has no watermark to forget: what it took is said by the files that " +
            "were taken, or it is loaded whole. --keep-watermark resets what later runs are " +
            "compared with, and nothing else"
        )
      else Right(None)
    case WatermarkReset.At(moment) =>
      kinds.find(_.name == source.connector.kind).flatMap(_.watermarkAt(source.load)) match {
        case None =>
          Left(
            s"${source.id} keeps no watermark: it is of the kind '${source.connector.kind}', " +
              s"loaded ${source.load.mode.name}, and a moment is no watermark of it. " +
              "--keep-watermark resets what later runs are compared with, and nothing else"
          )
        case Some(make) =>
          make(moment).map(Some(_)).left.map { why =>
            s"the watermark of ${source.id} cannot be set to $moment: $why"
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

  /**
   * Takes the lease of the run, announces its epoch, and does `body` with it. A failure that
   * nothing knows the class of is let through (ADR-0022 §2), with the lease left to run out.
   */
  private def held(start: RunStart, config: ConfigVersion)(body: LeasedRun => Outcome): Outcome =
    opened(start, config) match {
      case Left(notOpened) => notOpened
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
          // One that says its class, or one that the classifier knows.
          case Failures.Known(classed) => fail(leased, classed.failure, classed.getMessage)
          case unclassified: Throwable =>
            // Nothing is said of it: the lease runs out, as that of a process that died.
            leased.close()
            throw unclassified
        }
    }

  /**
   * The lease for `start`, or how the action ended without it: skipped, for a lease that a run
   * holds; failed with its class, for a control plane that could not be asked. Such an action
   * has no row, and nothing is told; unless the database took the lease and its answer was lost,
   * and then the lease runs out, and the run that takes it over ends this one.
   */
  private def opened(start: RunStart, config: ConfigVersion): Either[Outcome, LeasedRun] =
    try
      LeasedRun.open(database, start, config, timing).left.map { skipped =>
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

  /** What a reset does to the watermark of a source (ADR-0021 §3). */
  sealed trait WatermarkReset

  object WatermarkReset {

    /** Set to a moment, which the kind of the source makes into its watermark. */
    final case class At(moment: Instant) extends WatermarkReset

    /** Forgotten: the next run plans as a source with no watermark does. */
    case object Forget extends WatermarkReset

    /** Kept: only what later runs are compared with is reset. */
    case object Keep extends WatermarkReset
  }

  private def shown(watermark: Option[Json]): String = watermark.fold("none")(_.noSpaces)

  /**
   * What keeping the watermark across the changes `through` may do, that the operator is to
   * know: none, for changes that keeping it is right for.
   */
  private[operator] def risks(through: Seq[BreakingChange]): Seq[String] =
    through.filter(_.movesBronze).map { change =>
      s"the watermark was kept, and ${change.field} of the ${change.of} was ${change.was} and " +
        s"is ${change.is}: the history of the source stays in the table that it was ingested " +
        "to, and the new one receives only what is ingested from now on. It is read into the " +
        "new one only by a reset to an earlier watermark, a backfill or a reprocess"
    } ++ through.filter(_.ofWatermarkColumn).map { change =>
      s"the watermark was kept, and it was a value of ${change.was}: it is taken now as a " +
        s"value of ${change.is}. If ${change.is} does not mean what ${change.was} did, rows " +
        s"whose ${change.is} is at or before the watermark are skipped without a word. Keeping " +
        "it is right for a column that was renamed at the source; for another, reset with " +
        "--watermark or --forget-watermark"
    }

  /** How an operator's action ended. */
  sealed trait Outcome {
    def run: RunId
  }

  object Outcome {

    /** It did what it was asked, with what the operator is to know of what it did. */
    final case class Done(run: RunId, message: String, warnings: Seq[String] = Nil) extends Outcome

    /** A run holds the lease of the sequence: nothing was done, and the action is recorded. */
    final case class Skipped(run: RunId, holder: RunId, until: Instant) extends Outcome

    /** It did nothing, for the reason given, of the class that says what running again does. */
    final case class Failed(run: RunId, failure: FailureClass, message: String) extends Outcome
  }
}
