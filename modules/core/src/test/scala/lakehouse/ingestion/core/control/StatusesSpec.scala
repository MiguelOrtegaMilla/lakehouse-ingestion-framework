package lakehouse.ingestion.core.control

import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** How a run and a batch stand, and what each may come to (ADR-0005, ADR-0006). */
final class StatusesSpec extends AnyFunSuite with Matchers with EitherValues {

  import RunStatus._

  test("the statuses of a run have the names of the contract") {
    // They are in a column of the audit trail and in every query on it: pinned by their text.
    RunStatus.All.map(_.name) shouldBe
      Seq("RUNNING", "SUCCEEDED", "FAILED", "SKIPPED_LOCKED", "ABANDONED")
    RunStatus.All.foreach(status => RunStatus.parse(status.name).value shouldBe status)
    RunStatus.parse("Running").left.value shouldBe
      "must be one of RUNNING, SUCCEEDED, FAILED, SKIPPED_LOCKED, ABANDONED"
    RunStatus.parse("FENCED").isLeft shouldBe true
  }

  test("a run is born running, or skipped because the lease is taken") {
    RunStatus.AtBirth shouldBe Set(Running, SkippedLocked)
    Running.ended shouldBe false
    RunStatus.All.filter(_.ended) shouldBe Seq(Succeeded, Failed, SkippedLocked, Abandoned)
  }

  test("a run that is running succeeds, fails or is abandoned, and nothing else changes") {
    val allowed = for {
      from <- RunStatus.All
      to <- RunStatus.All
      if RunStatus.mayBecome(from, to)
    } yield from -> to

    allowed shouldBe Seq(Running -> Succeeded, Running -> Failed, Running -> Abandoned)
  }

  test("what ended stays as it ended: a zombie that reports late changes nothing") {
    RunStatus.All.filter(_.ended).foreach { ended =>
      RunStatus.All
        .foreach(to => withClue(s"$ended -> $to")(RunStatus.mayBecome(ended, to) shouldBe false))
    }
    // The run that was taken over is told so by its row, whatever it says afterwards.
    RunStatus.mayBecome(Abandoned, Failed) shouldBe false
    RunStatus.mayBecome(Abandoned, Succeeded) shouldBe false
    // And a run that was skipped never ran.
    RunStatus.mayBecome(SkippedLocked, Running) shouldBe false
    RunStatus.mayBecome(Running, SkippedLocked) shouldBe false
    RunStatus.mayBecome(Running, Running) shouldBe false
  }

  test("a run is of a kind: a regular run, or what an operator does to a source") {
    RunKind.All.map(_.name) shouldBe Seq("regular", "abandon", "reset", "reprocess", "backfill")
    RunKind.All.foreach(kind => RunKind.parse(kind.name).value shouldBe kind)
    // ADR-0021 §1: every kind but the regular one is asked for by an operator.
    RunKind.All.filter(_.operators) shouldBe
      Seq(RunKind.Abandon, RunKind.Reset, RunKind.Reprocess, RunKind.Backfill)
    // A batch is of a kind that plans one: a regular batch, the files of one read again, or a
    // chunk of a backfill.
    RunKind.OfBatches shouldBe Seq(RunKind.Regular, RunKind.Reprocess, RunKind.Backfill)
    RunKind.parse("replay").left.value shouldBe
      "must be one of regular, abandon, reset, reprocess, backfill"
  }

  test("a kind acts on the regular sequence of its source, on a backfill's, or on either") {
    // ADR-0021 §5: a backfill on its own sequence alone; a batch is given up in the sequence
    // that it is planned in; the state of a source and its files are its regular sequence's.
    RunKind.All.filter(_.ofRegular) shouldBe
      Seq(RunKind.Regular, RunKind.Abandon, RunKind.Reset, RunKind.Reprocess)
    RunKind.All.filter(_.ofBackfills) shouldBe Seq(RunKind.Abandon, RunKind.Backfill)
    // Each acts on one at least.
    RunKind.All.filterNot(kind => kind.ofRegular || kind.ofBackfills) shouldBe empty
  }

  test("a failure has one of the four classes of the requirements") {
    // RF-14: transient, permanent, data, conflict. They are a column of the audit trail.
    FailureClass.All.map(_.name) shouldBe Seq("transient", "conflict", "permanent", "data")
    FailureClass.All.foreach(failure => FailureClass.parse(failure.name).value shouldBe failure)
    FailureClass.parse("TRANSIENT").left.value shouldBe
      "must be one of transient, conflict, permanent, data"
  }

  test("the statuses of a batch have the names of the contract") {
    BatchStatus.All.map(_.name) shouldBe Seq("PLANNED", "COMMITTED", "ABANDONED")
    BatchStatus.All.foreach(status => BatchStatus.parse(status.name).value shouldBe status)
    BatchStatus.parse("RUNNING").left.value shouldBe "must be one of PLANNED, COMMITTED, ABANDONED"
  }

  test("a batch that is planned is committed or abandoned, and stays so") {
    import BatchStatus._
    val allowed = for {
      from <- BatchStatus.All
      to <- BatchStatus.All
      if BatchStatus.mayBecome(from, to)
    } yield from -> to

    allowed shouldBe Seq(Planned -> Committed, Planned -> Abandoned)
    Planned.ended shouldBe false
    // A number belongs to one plan for ever: a committed batch is not planned again.
    BatchStatus.mayBecome(Committed, Planned) shouldBe false
    BatchStatus.mayBecome(Abandoned, Planned) shouldBe false
  }
}
