package lakehouse.ingestion.it.control

import java.sql.Connection
import java.util.UUID

import scala.util.Using

import lakehouse.ingestion.cli.Main
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.control.{Batches, ControlSchema, Runs}
import lakehouse.ingestion.it.support.{BootstrappedPostgres, ControlPlaneSchema}
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import org.scalatest.{BeforeAndAfterAll, EitherValues, OptionValues}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The control plane upgraded under a write-ahead log that is in use: batches written by version
 * 2 of the schema, then migration V3 (who gave a batch up, when and why: ADR-0018) and those
 * after it, up to V7 (backfills: ADR-0021 §5), whose rules on a sequence and the kinds of its
 * runs and batches take the rows of before as they are, and V9 (a file that could not be read:
 * ADR-0023), which takes every file of before for one that was taken.
 * `ControlSchemaSpec` has the schema as the migrations build it on an empty database.
 */
final class ControlSchemaUpgradeSpec
    extends AnyFunSuite
    with Matchers
    with EitherValues
    with OptionValues
    with BeforeAndAfterAll {

  private lazy val pg = BootstrappedPostgres.start()

  override def afterAll(): Unit = pg.stop()

  private val Hash = "0123456789abcdef" * 4

  private def as[T](role: String)(body: Connection => T): T =
    Using.resource(pg.connect(role, Database))(body)

  /** A run and its batch, as the engine of version 2 wrote them. */
  private def batchOfVersion2(c: Connection, source: String, status: String): BatchId = {
    val run = RunId.generate()
    update(
      c,
      """INSERT INTO control.ingestion_run
        |  (run_id, source_id, layer, run_kind, txn_app_id, status, instance_id, app_version,
        |   config_hash, lease_epoch)
        |VALUES (?, ?, 'bronze', 'regular', ?, 'RUNNING', 'host:1:0', '0.1.0', ?, 1)""".stripMargin,
      run.uuid,
      source,
      s"$source/bronze",
      Hash
    ) shouldBe 1
    update(
      c,
      """INSERT INTO control.ingestion_batch
        |  (txn_app_id, batch_seq, source_id, layer, run_kind, plan, planned_by_run)
        |VALUES (?, 1, ?, 'bronze', 'regular', '{"from": 0, "to": 10}'::jsonb, ?)""".stripMargin,
      s"$source/bronze",
      source,
      run.uuid
    ) shouldBe 1
    val ended = status match {
      case "PLANNED" => None
      case "COMMITTED" =>
        Some(
          s"status = 'COMMITTED', committed_at = now(), committed_by_run = '$run', " +
            """outputs = '[{"table": "bronze/t", "version": 4}]'::jsonb"""
        )
      // All that version 2 could say of a batch that was given up: its status.
      case "ABANDONED" => Some("status = 'ABANDONED'")
    }
    ended.foreach { change =>
      update(
        c,
        s"UPDATE control.ingestion_batch SET $change WHERE txn_app_id = ?",
        s"$source/bronze"
      ) shouldBe 1
    }
    BatchId(TxnAppId.parse(s"$source/bronze").value, BatchSeq.First)
  }

  /**
   * What an operator did to the source of `committed` under version 6: a run of each of its
   * kinds, and the batch of the reprocess, which reads the files of the committed batch again.
   *
   * @return each run, with its kind
   */
  private def actionsOfVersion6(c: Connection, committed: BatchId): Seq[(RunId, RunKind)] = {
    val source = committed.app.value.stripSuffix("/bronze")
    val runs = Seq(RunKind.Abandon, RunKind.Reset, RunKind.Reprocess).map(RunId.generate() -> _)
    runs.foreach { case (run, kind) =>
      update(
        c,
        """INSERT INTO control.ingestion_run
          |  (run_id, source_id, layer, run_kind, txn_app_id, status, instance_id, app_version,
          |   config_hash, lease_epoch, requested_by, reason)
          |VALUES (?, ?, 'bronze', ?, ?, 'RUNNING', 'host:1:0', '0.1.0', ?, 1, 'ops', ?)""".stripMargin,
        run.uuid,
        source,
        kind.name,
        committed.app.value,
        Hash,
        s"what an operator did: $kind"
      ) shouldBe 1
    }
    update(
      c,
      """INSERT INTO control.ingestion_batch
        |  (txn_app_id, batch_seq, source_id, layer, run_kind, plan, planned_by_run, reprocess_of)
        |VALUES (?, 2, ?, 'bronze', 'reprocess', '{"from": 0, "to": 10}'::jsonb, ?, 1)""".stripMargin,
      committed.app.value,
      source,
      runs.last._1.uuid
    ) shouldBe 1
    runs
  }

  private def newSource(): String = s"test.s${UUID.randomUUID().toString.take(8)}"

  test("V3 leaves the batches of version 2 as they were, and each can end as V3 has it") {
    ControlPlaneSchema.migrateUpTo(pg, "2") shouldBe 2
    val (planned, committed, abandoned) = as(Engine) { c =>
      update(
        c,
        "INSERT INTO control.config_snapshot (config_hash, source_id, content) " +
          "VALUES (?, 'test.source', '{}'::jsonb)",
        Hash
      ) shouldBe 1
      (
        batchOfVersion2(c, newSource(), "PLANNED"),
        batchOfVersion2(c, newSource(), "COMMITTED"),
        // Nothing in the application did this. Its role could, by hand, and so could an operator.
        batchOfVersion2(c, newSource(), "ABANDONED")
      )
    }
    // A file that the committed batch took, as version 2 recorded it.
    as(Engine) { c =>
      update(
        c,
        "INSERT INTO control.processed_file " +
          "(source_id, path, size, modified_at, txn_app_id, batch_seq) " +
          "VALUES (?, 'in/a.csv', 10, '2026-11-18T10:00:00.123Z', ?, 1)",
        committed.app.value.stripSuffix("/bronze"),
        committed.app.value
      ) shouldBe 1
    }
    val before = as(Engine)(c => query(c, "SELECT now()::text")).head.head

    // V3 to V6, and what an operator did under version 6 (ADR-0021): a batch given up, a reset,
    // and the files of the committed batch read again, as a batch of the regular sequence.
    ControlPlaneSchema.migrateUpTo(pg, "6") shouldBe 4
    val operators = as(Engine)(c => actionsOfVersion6(c, committed))

    val ran = ControlPlaneSchema.migrate(pg)

    withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
    // And V7 to V9 on top of them: the engine works on the schema that it was built for.
    ran.lines.head shouldBe s"control: version 9, 3 migrations applied (${pg.jdbcUrl(Database)})"
    as(Engine) { c =>
      ControlSchema.verify(c) shouldBe Right(())

      // What was planned or committed has nothing to say of being given up, and reads as it did.
      val stillPlanned = Batches.find(c, planned).value
      stillPlanned.status shouldBe BatchStatus.Planned
      stillPlanned.abandoned shouldBe None
      Batches.planned(c, planned.app).value.id shouldBe planned
      // V4: a batch of before says no table of its own, planned or ended, and the run that
      // finishes it asks the one that its source names.
      Seq(planned, committed, abandoned).map(Batches.find(c, _).value.target) shouldBe
        Seq(None, None, None)
      Batches.planned(c, planned.app).value.target shouldBe None
      // V5: a run of before is a regular run, and says neither who asked for it nor why.
      val plannedBy = Batches.find(c, planned).value.plannedBy
      Runs.find(c, plannedBy).value.start.kind shouldBe RunKind.Regular
      Runs.find(c, plannedBy).value.start.request shouldBe None
      // V7: and it acts on the regular sequence of its source, as every run of before does.
      Runs.find(c, plannedBy).value.start.backfill shouldBe None
      Runs.find(c, plannedBy).value.txnAppId shouldBe planned.app
      planned.app.backfill shouldBe None
      // So does what an operator did under version 6, which V7 takes as it was written.
      operators.foreach { case (run, kind) =>
        val found = Runs.find(c, run).value
        found.start.kind shouldBe kind
        found.start.backfill shouldBe None
        found.start.request.value.reason shouldBe s"what an operator did: $kind"
      }
      val reprocess = Batches.find(c, BatchId(committed.app, BatchSeq.First.next)).value
      reprocess.kind shouldBe RunKind.Reprocess
      reprocess.reprocessOf shouldBe Some(BatchSeq.First)
      // V9: the file that it took is a file taken, with nothing to say of an error.
      query(
        c,
        "SELECT status, coalesce(error, 'none') FROM control.processed_file WHERE txn_app_id = ?",
        committed.app.value
      ) shouldBe Seq(Seq("TAKEN", "none"))
      val stillCommitted = Batches.find(c, committed).value
      stillCommitted.status shouldBe BatchStatus.Committed
      stillCommitted.outputs.value.map(_.version) shouldBe Vector(Some(4L))
      stillCommitted.abandoned shouldBe None

      // What was given up by hand says that it was, and that nobody said why.
      val givenUp = Batches.find(c, abandoned).value
      givenUp.status shouldBe BatchStatus.Abandoned
      val said = givenUp.abandoned.value
      said.by shouldBe None
      said.detail shouldBe None
      said.reason shouldBe "abandoned before the control plane kept why (schema version 2); " +
        "the time is that of the migration to version 3"
      query(
        c,
        "SELECT abandoned_at >= ?::timestamptz FROM control.ingestion_batch " +
          "WHERE txn_app_id = ?",
        before,
        abandoned.app.value
      ) shouldBe Seq(Seq("t"))

      // The rule that the migration lifted for one statement is back: what ended does not change.
      intercept[java.sql.SQLException] {
        update(
          c,
          "UPDATE control.ingestion_batch SET abandon_reason = 'said late' WHERE txn_app_id = ?",
          abandoned.app.value
        )
      }.getMessage should include("and what ended does not change")

      // And the batch that was planned under version 2 is given up as version 3 has it.
      val run = stillPlanned.plannedBy
      Batches.abandon(c, planned, run, "a file of it was written again", None) shouldBe true
      val now = Batches.find(c, planned).value
      now.status shouldBe BatchStatus.Abandoned
      now.abandoned.value.by shouldBe Some(run)
      now.plan shouldBe stillPlanned.plan
    }
  }
}
