package lakehouse.ingestion.it.ingestion

import java.net.URI
import java.nio.file.{Files, Paths}

import scala.util.Try

import io.circe.Json
import lakehouse.ingestion.core.config.{
  EngineColumns,
  PathTableLocator,
  RelativePath,
  RootUri,
  Source
}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.core.watermark.WatermarkPlan
import lakehouse.ingestion.engine.connector.{ConnectorRegistry, Progress}
import lakehouse.ingestion.engine.control._
import lakehouse.ingestion.engine.delta.DeltaTableTransactions
import lakehouse.ingestion.it.control.ControlPlaneSuite
import lakehouse.ingestion.it.ingestion.IngestionLedger.{Facts, TakenFile, Violation}
import lakehouse.ingestion.it.stress.{LogModel, LogReader}
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, SparkSession}
import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers

/**
 * What the ledger of a source reads (P7.2, `IngestionLedger`): the control plane, the tables of
 * the source and `processed_file`, for each of its sequences, once no run of it is in flight.
 *
 * It reads the tables with a session of Spark that the suite gives it, on the coordinated
 * LogStore, so that a read publishes a commit that a writer took and did not publish, as the next
 * reader of a deployment would. `IngestionSuite` gives it its own; the suites of the commands,
 * whose commands open and close a session of their own in the JVM of the test, give it one that
 * is open while it reads (`it.cli.CommandLedger`).
 */
trait LedgerFacts {
  this: ControlPlaneSuite with Suite with Matchers =>

  /** The session that the tables are read with, open while the ledger reads. */
  protected def ledgerSession: SparkSession

  /** The lake that the tables of the sources are in. */
  protected def ledgerLake: RootUri

  /**
   * The connectors that the ledger asks what the plan of a batch commits: the watermark and the
   * files.
   */
  protected def connectors: ConnectorRegistry

  private def ledgerLocator: PathTableLocator = new PathTableLocator(ledgerLake)

  // ---------------------------------------------------------------------------------------------
  // The ledger of a source (P7.2)
  // ---------------------------------------------------------------------------------------------

  /**
   * Every way in which the control plane, the tables and the files taken of a source disagree,
   * in each of its sequences: its regular one, and that of each backfill that a run acted on.
   */
  protected def ledgerViolations(of: Source, earlier: Source*): Seq[Violation] =
    ledgersOf(of, earlier: _*).flatMap { case (app, facts) =>
      val found = IngestionLedger.check(facts)
      app.backfill.fold(found) { id =>
        found.map(violation =>
          violation.copy(message = s"in the backfill $id, ${violation.message}")
        )
      }
    }

  /**
   * Asks the ledger of a source, once no run of it is in flight.
   *
   * @param earlier the configurations that the source had before, that a batch may have been
   *   planned under: a plan is read by its connector as it was written, and a connector refuses
   *   one of another configuration (a window of another watermark column)
   */
  protected def assertLedger(of: Source, earlier: Source*): Unit = {
    val violations = ledgerViolations(of, earlier: _*)
    withClue(violations.mkString(s"the ledger of ${of.id}:\n  ", "\n  ", "\n")) {
      violations shouldBe empty
    }: Unit
  }

  /**
   * The facts of each sequence of a source: its regular one, always, and that of each backfill
   * that a run acted on. The tables are the same for all, and each has what is of its sequence:
   * the commits under its `txnAppId`, and the rows that its runs wrote. A row whose run is of no
   * sequence is the regular one's, which has no such run.
   */
  protected def ledgersOf(of: Source, earlier: Source*): Seq[(TxnAppId, Facts)] = {
    val regular = TxnAppId.of(of.id, Layer.Bronze)
    val runs = asSuperuser { c =>
      query(
        c,
        "SELECT run_id::text, txn_app_id FROM control.ingestion_run " +
          "WHERE source_id = ? AND layer = ?",
        of.id.value,
        Layer.Bronze.name
      )
    }.map(row => TxnAppId.parse(row(1)).value -> RunId.parse(row(0)).value)
    val bySequence = runs.groupMap(_._1)(_._2)
    val sequences = regular +: bySequence.keys.filter(_ != regular).toSeq.sortBy(_.value)
    sequences.map { app =>
      val its = bySequence.getOrElse(app, Nil)
      val others = bySequence.view.filterKeys(_ != app).values.flatten.map(_.toString).toSeq
      val itsRows: String => Column =
        if (app != regular) run => col(run).isin(its.map(_.toString): _*)
        else if (others.isEmpty) _ => lit(true)
        else run => !col(run).isin(others: _*)
      app -> ledgerOf(of, earlier, app, its, itsRows)
    }
  }

  /**
   * What the control plane, the tables and `processed_file` say of one sequence of a source.
   *
   * Delta is asked before the log is read as files: a read publishes a commit that a writer took
   * and did not publish (ADR-0004 §3.3), and what is looked at is the table that the next reader
   * gets.
   *
   * @param runIds the runs that acted on the sequence
   * @param itsRows which rows of a table are of the sequence, by the column of their run
   */
  private def ledgerOf(
      of: Source,
      earlier: Seq[Source],
      app: TxnAppId,
      runIds: Seq[RunId],
      itsRows: String => Column
  ): Facts = {
    val bound = (of +: earlier).map(connectors.bind)
    val configured = ledgerLocator.locate(of.targets.bronze.table)

    /** What the commit of a plan records, by the first configuration that reads it. */
    def progressOf(plan: Json): Progress =
      bound.iterator
        .map(source => Try(source.progress(plan)))
        .find(_.isSuccess)
        .getOrElse(Try(bound.head.progress(plan)))
        .get

    val records = asSuperuser { c =>
      query(
        c,
        "SELECT batch_seq::text FROM control.ingestion_batch WHERE txn_app_id = ? ORDER BY 1",
        app.value
      )
    }.map(row =>
      database.run(Batches.find(_, BatchId(app, BatchSeq.of(row.head.toLong).value))).value
    )

    val batches = records.map { record =>
      val progress = progressOf(record.plan)
      IngestionLedger.Batch(
        seq = record.id.seq.value,
        status = record.status,
        target = record.target.getOrElse(configured.uri),
        committedBy = record.committedBy,
        outputs = record.outputs.toSeq.flatten.map { output =>
          IngestionLedger.Output(ledgerLocator.locate(output.table).uri, output.version)
        },
        watermark = progress.watermark,
        files = progress.files.toSeq.flatMap { plan =>
          plan.files.map { file =>
            TakenFile(
              s"${plan.directory.value}/${file.name.value}",
              file.size,
              file.modifiedAt.toEpochMilli
            )
          }
        }.toSet,
        reprocessOf = record.reprocessOf.map(_.value),
        window = WatermarkPlan
          .fromJson(record.plan)
          .toOption
          .map(window => IngestionLedger.Window(window.lower, window.upper))
      )
    }

    val runs = runIds.map { id =>
      val run = database.run(Runs.find(_, id)).value
      IngestionLedger.Run(
        run.start.id,
        run.batch.map(_.value),
        run.status,
        run.counts.rowsWritten,
        run.start.kind
      )
    }

    // A reset is of the state of the source, which is that of its regular sequence.
    val resets = app.backfill.fold {
      asSuperuser { c =>
        query(
          c,
          "SELECT run_id::text, before::text, after::text FROM control.state_change_log " +
            "WHERE source_id = ? AND layer = ? ORDER BY change_id",
          of.id.value,
          Layer.Bronze.name
        )
      }.map { row =>
        def state(text: String): IngestionLedger.State = {
          val document = StoredJson.parse(text).value.hcursor
          IngestionLedger.State(
            document.downField("watermark").focus.filterNot(_.isNull),
            document.get[Long]("last_committed_batch_seq").value,
            RunId.parse(document.get[String]("updated_by_run").value).value
          )
        }
        IngestionLedger.Reset(RunId.parse(row(0)).value, state(row(1)), state(row(2)))
      }
    }(_ => Nil)

    val taken = asSuperuser { c =>
      query(
        c,
        "SELECT path, size::text, (extract(epoch FROM modified_at) * 1000)::bigint::text, " +
          "batch_seq::text FROM control.processed_file WHERE source_id = ? AND txn_app_id = ?",
        of.id.value,
        app.value
      )
    }.map(row => TakenFile(row(0), row(1).toLong, row(2).toLong) -> row(3).toLong).toSet

    val data = (batches.map(_.target) :+ configured.uri).distinct.map { uri =>
      ledgerTable(
        uri,
        app,
        EngineColumns.RunId,
        EngineColumns.BatchSeq,
        // A table of several sources has the rows of each: these are the source's.
        itsRows(EngineColumns.RunId) && col(EngineColumns.SourceId) === of.id.value
      )
    }
    val apart =
      ledgerTable(
        ledgerLocator.locate(Quarantine.table(of.id, Layer.Bronze)).uri,
        app,
        Quarantine.RunId,
        Quarantine.BatchSeq,
        itsRows(Quarantine.RunId)
      )

    // The state of the sequence: that of the source, or the progress of the backfill.
    val (state, backfill) = app.backfill match {
      case None =>
        val state = database
          .run(SourceStates.find(_, of.id, Layer.Bronze))
          .map(state =>
            IngestionLedger.State(state.watermark, state.lastCommitted.value, state.updatedBy)
          )
        (state, None)
      case Some(id) =>
        database.run(Backfills.find(_, of.id, Layer.Bronze, id)) match {
          case None => (None, None)
          case Some(record) =>
            val progress = record.progress.map { at =>
              IngestionLedger.State(Some(at.watermark), at.lastCommitted.value, at.updatedBy)
            }
            (
              progress,
              Some(IngestionLedger.Backfill(record.from, record.to, record.completedAt.nonEmpty))
            )
        }
    }

    Facts(batches, runs, state, taken, data, apart, resets, backfill)
  }

  private def ledgerTable(
      uri: String,
      app: TxnAppId,
      run: String,
      batch: String,
      itsRows: Column
  ): IngestionLedger.Table = {
    val directory = Paths.get(URI.create(uri))
    if (!Files.isDirectory(directory.resolve("_delta_log"))) IngestionLedger.Table(uri)
    else {
      val location = ledgerLocator.locate(
        RelativePath.stored(uri.stripPrefix(s"${ledgerLake.value}/")).fold(e => fail(e), identity)
      )
      val lastTxn = new DeltaTableTransactions(ledgerSession).lastVersion(location, app)
      val log = LogReader.logView(ledgerSession, directory)
      withClue(s"the log of $uri: ")(log.unreadable shouldBe empty)
      // A first commit that the LogStore refused leaves the directory of a log with no version
      // in it, which Delta asked just now: there is no table, as there is none for Delta.
      if (log.versions.isEmpty) IngestionLedger.Table(uri)
      else tableOf(uri, app, run, batch, itsRows, lastTxn, log)
    }
  }

  /** What the ledger reads of a table that has a version at least. */
  private def tableOf(
      uri: String,
      app: TxnAppId,
      run: String,
      batch: String,
      itsRows: Column,
      lastTxn: Option[Long],
      log: LogModel.LogView
  ): IngestionLedger.Table = {
    val rows = ledgerSession.read
      .format("delta")
      .load(uri)
      .filter(itsRows)
      .groupBy(col(run), col(batch))
      .count()
      .collect()
      .map(row => IngestionLedger.Rows(row.getString(0), row.getLong(1), row.getLong(2)))
      .toSeq
    IngestionLedger.Table(
      uri,
      log.commits.map(commit =>
        IngestionLedger
          .Commit(commit.version, commit.txns.filter(_.appId == app.value).map(_.version))
      ),
      log.versions.minOption,
      lastTxn,
      rows
    )
  }
}
