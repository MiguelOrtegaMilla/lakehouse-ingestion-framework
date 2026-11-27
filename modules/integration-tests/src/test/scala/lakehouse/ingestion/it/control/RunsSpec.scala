package lakehouse.ingestion.it.control

import java.sql.SQLException

import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.control.{Application, Runs}
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The runs of the audit trail, written by the engine's role on a real PostgreSQL: how a run is
 * opened and ended, and what a run that was ended by another is told.
 */
final class RunsSpec extends AnyFunSuite with Matchers with ControlPlaneSuite {

  private def find(run: RunId): Option[RunRecord] = database.run(Runs.find(_, run))

  test("a run that took its lease is opened running, with who runs it and under which epoch") {
    val start = starting(source(newSourceId().value))
    val before = databaseNow()

    database.run(Runs.open(_, start, epoch(3)))

    val record = find(start.id).value
    record shouldBe RunRecord(
      start = start,
      txnAppId = start.txnAppId,
      batch = None,
      status = RunStatus.Running,
      epoch = Some(epoch(3)),
      startedAt = record.startedAt,
      finishedAt = None,
      counts = RunCounts.Nothing,
      failure = None
    )
    // Who, by the application itself; when, by the clock of the database.
    record.start.instance shouldBe Application.instance
    record.start.appVersion shouldBe Application.version
    record.startedAt should (be >= before and be <= databaseNow())
  }

  test("a run that found the lease taken is recorded as skipped: ended, with no epoch") {
    val start = starting(source(newSourceId().value), Layer.Silver)

    database.run(Runs.skip(_, start))

    val record = find(start.id).value
    record.status shouldBe RunStatus.SkippedLocked
    record.epoch shouldBe None
    record.finishedAt shouldBe Some(record.startedAt)
    record.txnAppId.value shouldBe s"${start.source}/silver"
    // It never ran: it is ended by nobody.
    database.run(Runs.end(_, start.id, RunEnd.Succeeded())) shouldBe false
    database.run(Runs.abandon(_, start.id)) shouldBe false
  }

  test("a run that succeeds is ended with what it counted, in one statement") {
    val start = starting(source(newSourceId().value))
    database.run(Runs.open(_, start, epoch(1)))
    val counts = RunCounts(
      rowsRead = Some(1200),
      rowsWritten = Some(1188),
      rowsRejected = Some(12),
      bytesRead = Some(3L * 1024 * 1024 * 1024),
      retries = Some(2),
      writeConflicts = Some(1),
      commitContention = Some(0)
    )

    database.run(Runs.end(_, start.id, RunEnd.Succeeded(counts))) shouldBe true

    val record = find(start.id).value
    record.status shouldBe RunStatus.Succeeded
    record.counts shouldBe counts
    record.failure shouldBe None
    record.finishedAt.value should be >= record.startedAt
    record.epoch shouldBe Some(epoch(1))
  }

  test("a run that fails says the class of its failure, and no more than a message of it") {
    val start = starting(source(newSourceId().value))
    database.run(Runs.open(_, start, epoch(1)))
    val long = "the source did not answer; " * 200

    database.run(
      Runs.end(
        _,
        start.id,
        RunEnd.Failed(FailureClass.Transient, long, RunCounts(retries = Some(5)))
      )
    ) shouldBe true

    val record = find(start.id).value
    record.status shouldBe RunStatus.Failed
    record.failure.value.failure shouldBe FailureClass.Transient
    record.failure.value.message shouldBe long.take(Runs.MaxMessageLength)
    // What it did not get to count is missing, not zero.
    record.counts shouldBe RunCounts(retries = Some(5))
  }

  /** A character outside the first plane: two units of UTF-16, a pair of surrogates. */
  private val Pair = new String(Character.toChars(0x1f600))

  test("a message is cut by its characters, and never inside one") {
    val start = starting(source(newSourceId().value))
    database.run(Runs.open(_, start, epoch(1)))
    // Its 2000th character is outside the first plane, a pair of surrogates, as a name of a
    // file may have one: a cut of 2000 units of UTF-16 kept half of it, which was written `?`.
    val kept = "x" * (Runs.MaxMessageLength - 1) + Pair
    database.run(
      Runs.end(_, start.id, RunEnd.Failed(FailureClass.Permanent, kept + " and more"))
    ) shouldBe true

    find(start.id).value.failure.value.message shouldBe kept
  }

  test("each class of failure is one that the audit trail takes") {
    FailureClass.All.foreach { failure =>
      val start = starting(source(newSourceId().value))
      database.run(Runs.open(_, start, epoch(1)))
      database.run(Runs.end(_, start.id, RunEnd.Failed(failure, "why"))) shouldBe true
      find(start.id).value.failure shouldBe Some(RunFailure(failure, "why"))
    }
  }

  test("a run that was abandoned by the one that took its lease is told so, and changes nothing") {
    val start = starting(source(newSourceId().value))
    database.run(Runs.open(_, start, epoch(1)))

    // The run that takes over the lease, which expired.
    database.run(Runs.abandon(_, start.id)) shouldBe true
    val abandoned = find(start.id).value
    abandoned.status shouldBe RunStatus.Abandoned
    abandoned.finishedAt should not be empty

    // The zombie finishes its work, and reports.
    database.run(
      Runs.end(_, start.id, RunEnd.Succeeded(RunCounts(rowsWritten = Some(10))))
    ) shouldBe false
    database.run(
      Runs.end(_, start.id, RunEnd.Failed(FailureClass.Permanent, "late"))
    ) shouldBe false
    find(start.id).value shouldBe abandoned
  }

  test("a run ends once: the second end, and an abandon after it, change nothing") {
    val start = starting(source(newSourceId().value))
    database.run(Runs.open(_, start, epoch(1)))
    database.run(
      Runs.end(_, start.id, RunEnd.Succeeded(RunCounts(rowsRead = Some(1))))
    ) shouldBe true
    val ended = find(start.id).value

    database.run(Runs.end(_, start.id, RunEnd.Failed(FailureClass.Data, "again"))) shouldBe false
    database.run(Runs.abandon(_, start.id)) shouldBe false
    find(start.id).value shouldBe ended
  }

  test("a run is not opened without its configuration stored, nor twice") {
    val start = starting(source(newSourceId().value))
    val unknown = start.copy(id = RunId.generate(), configHash = "f" * 64)

    intercept[SQLException](
      database.run(Runs.open(_, unknown, epoch(1)))
    ).getSQLState shouldBe "23503"
    find(unknown.id) shouldBe None

    database.run(Runs.open(_, start, epoch(1)))
    intercept[SQLException](
      database.run(Runs.open(_, start, epoch(2)))
    ).getSQLState shouldBe "23505"
    intercept[SQLException](database.run(Runs.skip(_, start))).getSQLState shouldBe "23505"
    find(start.id).value.epoch shouldBe Some(epoch(1))
  }

  test("a run is opened in the transaction of whoever opens it: with it, or not at all") {
    val start = starting(source(newSourceId().value))

    intercept[IllegalStateException] {
      database.transaction { c =>
        Runs.open(c, start, epoch(1))
        // Seen by the transaction that wrote it, and by no other.
        Runs.find(c, start.id) should not be empty
        find(start.id) shouldBe None
        throw new IllegalStateException("the lease was not taken after all")
      }
    }
    find(start.id) shouldBe None

    database.transaction { c =>
      Runs.open(c, start, epoch(1))
      Runs.end(c, start.id, RunEnd.Succeeded()) shouldBe true
    }
    find(start.id).value.status shouldBe RunStatus.Succeeded
  }

  test("a run that does not exist is not found, and is ended by nobody") {
    val nobody = RunId.generate()

    find(nobody) shouldBe None
    database.run(Runs.end(_, nobody, RunEnd.Succeeded())) shouldBe false
    database.run(Runs.abandon(_, nobody)) shouldBe false
  }

  test("a row that the model does not know is said, and not read as something else") {
    // A schema and an application that are not each other's: a status of a later version.
    val start = starting(source(newSourceId().value))
    database.run(Runs.open(_, start, epoch(1)))
    asSuperuser { c =>
      update(c, "ALTER TABLE control.ingestion_run DROP CONSTRAINT ingestion_run_layer_check"): Unit
      // With the name of its sequence in that layer, which V7 holds to its layer.
      update(
        c,
        "UPDATE control.ingestion_run SET layer = 'gold', txn_app_id = source_id || '/gold' " +
          "WHERE run_id = ?",
        start.id.uuid
      ) shouldBe 1
    }

    try
      intercept[IllegalStateException](find(start.id)).getMessage shouldBe
        "control.ingestion_run.layer 'gold' must be one of bronze, silver"
    finally
      asSuperuser { c =>
        update(
          c,
          "UPDATE control.ingestion_run SET layer = 'bronze', txn_app_id = source_id || '/bronze' " +
            "WHERE run_id = ?",
          start.id.uuid
        ): Unit
        update(
          c,
          "ALTER TABLE control.ingestion_run ADD CONSTRAINT ingestion_run_layer_check " +
            "CHECK (layer IN ('bronze', 'silver'))"
        ): Unit
      }
    find(start.id).value.start.layer shouldBe Layer.Bronze
  }
}
