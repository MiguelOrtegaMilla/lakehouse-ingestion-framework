package lakehouse.ingestion.it.ingestion

import io.circe.Json
import lakehouse.ingestion.core.control.{BatchStatus, RunId, RunKind, RunStatus}
import lakehouse.ingestion.it.ingestion.IngestionLedger._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The rules of the ledger, each on facts made by hand: a source as it should be, and the same
 * source with one thing wrong, which the ledger names and nothing else.
 */
final class IngestionLedgerSpec extends AnyFunSuite with Matchers {

  private val T = "file:/lake/bronze/s"
  private val U = "file:/lake/bronze/other"
  private val Q = "file:/lake/_quarantine/bronze/s"

  private def file(name: String) = TakenFile(s"s/$name", 10, 1000)

  // The runs of the source. r3 wrote batch 3 and died; r4 rolled it forward. r5 gave batch 4
  // up when one of its files changed, after its quarantine was written. Batch 5 had no rows.
  private val r1, r2, r3, r4, r5, r6 = RunId.generate()

  private val clean = Facts(
    batches = Seq(
      Batch(1, BatchStatus.Committed, T, Some(r1), Seq(Output(T, Some(0))), files = Set(file("a"))),
      Batch(
        2,
        BatchStatus.Committed,
        T,
        Some(r2),
        Seq(Output(Q, Some(0)), Output(T, Some(1))),
        files = Set(file("b"))
      ),
      Batch(3, BatchStatus.Committed, T, Some(r4), Seq(Output(T, Some(2))), files = Set(file("c"))),
      Batch(4, BatchStatus.Abandoned, T, files = Set(file("d"))),
      Batch(5, BatchStatus.Committed, T, Some(r6), Nil, files = Set(file("e")))
    ),
    runs = Seq(
      Run(r1, Some(1), RunStatus.Succeeded, Some(2)),
      Run(r2, Some(2), RunStatus.Succeeded, Some(1)),
      Run(r3, Some(3), RunStatus.Abandoned),
      Run(r4, Some(3), RunStatus.Succeeded),
      Run(r5, Some(4), RunStatus.Failed),
      Run(r6, Some(5), RunStatus.Succeeded, Some(0))
    ),
    state = Some(State(None, 5, r6)),
    taken = Set(file("a") -> 1L, file("b") -> 2L, file("c") -> 3L, file("e") -> 5L),
    data = Seq(
      Table(
        T,
        Seq(Commit(0, Seq(1)), Commit(1, Seq(2)), Commit(2, Seq(3))),
        keptFrom = Some(0),
        lastTxn = Some(3),
        rows = Seq(Rows(r1.toString, 1, 2), Rows(r2.toString, 2, 1), Rows(r3.toString, 3, 4))
      )
    ),
    quarantine = Table(
      Q,
      Seq(Commit(0, Seq(2)), Commit(1, Seq(4))),
      keptFrom = Some(0),
      lastTxn = Some(4),
      rows = Seq(Rows(r2.toString, 2, 1), Rows(r5.toString, 4, 3))
    )
  )

  private def rules(facts: Facts): Set[Rule] = IngestionLedger.check(facts).map(_.rule).toSet

  private def batch(facts: Facts, seq: Long)(change: Batch => Batch): Facts =
    facts.copy(batches = facts.batches.map(b => if (b.seq == seq) change(b) else b))

  private def table(facts: Facts)(change: Table => Table): Facts =
    facts.copy(data = facts.data.map(change))

  private def quarantine(facts: Facts)(change: Table => Table): Facts =
    facts.copy(quarantine = change(facts.quarantine))

  private def run(facts: Facts, id: RunId)(change: Run => Run): Facts =
    facts.copy(runs = facts.runs.map(r => if (r.id == id) change(r) else r))

  /** A sixth batch, planned by a run that died; the run that found it ended. */
  private val r7 = RunId.generate()
  private val planned = clean.copy(
    batches = clean.batches :+ Batch(6, BatchStatus.Planned, T, files = Set(file("f"))),
    runs = clean.runs :+ Run(r7, Some(6), RunStatus.Abandoned)
  )

  /** Two resets after batch 5 (ADR-0021 §3): r8 kept the watermark, and r9 set one. */
  private val r8, r9 = RunId.generate()
  private val at = Json.obj("value" -> Json.fromString("2026-11-19T11:00:00Z"))
  private def resetBy(id: RunId) = Run(id, None, RunStatus.Succeeded, kind = RunKind.Reset)
  private val reset = clean.copy(
    runs = clean.runs ++ Seq(resetBy(r8), resetBy(r9)),
    state = Some(State(Some(at), 5, r9)),
    resets = Seq(
      Reset(r8, State(None, 5, r6), State(None, 5, r8)),
      Reset(r9, State(None, 5, r8), State(Some(at), 5, r9))
    )
  )

  // -----------------------------------------------------------------------------------------------
  // What is as it should be
  // -----------------------------------------------------------------------------------------------

  test("a source as it should be breaks no rule, with a batch rolled forward, given up and empty") {
    IngestionLedger.check(clean) shouldBe empty
  }

  test("a planned batch in no table is to be replayed, and breaks no rule") {
    IngestionLedger.check(planned) shouldBe empty
  }

  test("a planned batch that its quarantine has and its data table has not is to be replayed") {
    // What a run that dies at AfterQuarantineWrite leaves: the data table decides, and has not.
    val left = quarantine(planned)(q =>
      q.copy(
        commits = q.commits :+ Commit(2, Seq(6)),
        lastTxn = Some(6),
        rows = q.rows :+ Rows(r7.toString, 6, 2)
      )
    )
    IngestionLedger.check(left) shouldBe empty
  }

  test(
    "a planned batch that its data table has is to be rolled forward, with or without its quarantine"
  ) {
    val written = table(planned)(t =>
      t.copy(
        commits = t.commits :+ Commit(3, Seq(6)),
        lastTxn = Some(6),
        rows = t.rows :+ Rows(r7.toString, 6, 5)
      )
    )
    IngestionLedger.check(written) shouldBe empty
    val withItsQuarantine = quarantine(written)(q =>
      q.copy(commits = q.commits :+ Commit(2, Seq(6)), rows = q.rows :+ Rows(r7.toString, 6, 1))
    )
    IngestionLedger.check(withItsQuarantine) shouldBe empty
  }

  test("a batch whose commit the log no longer keeps is in its table by what Delta says") {
    val cleaned = quarantine(
      table(clean)(t => t.copy(commits = t.commits.drop(2), keptFrom = Some(2)))
    )(q => q.copy(commits = q.commits.drop(1), keptFrom = Some(1)))
    IngestionLedger.check(cleaned) shouldBe empty
    // A version that was not known when the batch was committed (P5.10), the same.
    IngestionLedger.check(batch(cleaned, 1)(_.copy(outputs = Seq(Output(T, None))))) shouldBe empty
    // Delta says that the table has not reached the batch: then it is not there.
    rules(table(cleaned)(_.copy(lastTxn = Some(0)))) shouldBe Set(Rule.RecordedNotInTable)
  }

  test("the state of a source loaded by a watermark is that of the plan of its last batch") {
    val at = Json.obj("value" -> Json.fromString("2026-11-19T11:00:00Z"))
    val loaded = batch(clean, 5)(_.copy(watermark = Some(at)))
    IngestionLedger.check(loaded.copy(state = Some(State(Some(at), 5, r6)))) shouldBe empty
    rules(loaded) shouldBe Set(Rule.StateNotOfLastBatch)
  }

  test("a state reset after the last batch is the reset's, and each reset is of what there was") {
    IngestionLedger.check(reset) shouldBe empty
    // A reset before batch 5, of the state of batch 3; and one after it, of the state of 5.
    val between = clean.copy(
      runs = clean.runs ++ Seq(resetBy(r8), resetBy(r9)),
      state = Some(State(None, 5, r9)),
      resets = Seq(
        Reset(r8, State(None, 3, r4), State(None, 3, r8)),
        Reset(r9, State(None, 5, r6), State(None, 5, r9))
      )
    )
    IngestionLedger.check(between) shouldBe empty
    // And with a batch after both, the state is that batch's again.
    IngestionLedger.check(
      between.copy(state = clean.state, resets = between.resets.take(1))
    ) shouldBe
      empty
  }

  // -----------------------------------------------------------------------------------------------
  // Each rule, broken alone
  // -----------------------------------------------------------------------------------------------

  test("a committed batch that its table has no commit of") {
    rules(table(clean)(t => t.copy(commits = t.commits.tail))) shouldBe Set(Rule.RecordedNotInTable)
  }

  test("a table that has a committed batch whose commit does not record it") {
    rules(batch(clean, 2)(_.copy(outputs = Seq(Output(T, Some(1)))))) shouldBe
      Set(Rule.InTableNotRecorded)
  }

  test("a batch in two commits of one table") {
    rules(table(clean)(t => t.copy(commits = t.commits :+ Commit(3, Seq(1))))) shouldBe
      Set(Rule.InTwoCommits)
    rules(
      quarantine(planned)(q =>
        q.copy(commits = q.commits ++ Seq(Commit(2, Seq(6)), Commit(3, Seq(6))))
      )
    ) shouldBe
      Set(Rule.InTwoCommits)
  }

  test("a batch that records its table at another version than the one that has it") {
    rules(batch(clean, 1)(_.copy(outputs = Seq(Output(T, Some(1)))))) shouldBe
      Set(Rule.AtAnotherVersion)
  }

  test("a batch in a data table other than the one that it was planned for") {
    val other = clean.copy(data = clean.data :+ Table(U, Seq(Commit(0, Seq(3))), Some(0), Some(3)))
    rules(other) shouldBe Set(Rule.InTableNotRecorded, Rule.InAnotherTable)
    val plannedThere =
      planned.copy(data = planned.data :+ Table(U, Seq(Commit(0, Seq(6))), Some(0), Some(6)))
    rules(plannedThere) shouldBe Set(Rule.InAnotherTable)
  }

  test("a batch that was given up, in its data table") {
    rules(table(clean)(t => t.copy(commits = t.commits :+ Commit(3, Seq(4))))) shouldBe
      Set(Rule.AbandonedInTable)
  }

  test("a planned batch that is not the last of its sequence") {
    rules(batch(clean, 4)(_.copy(status = BatchStatus.Planned))) shouldBe Set(Rule.PlannedNotLast)
  }

  test("a number of the sequence in a table that no batch has") {
    rules(table(clean)(t => t.copy(commits = t.commits :+ Commit(3, Seq(9))))) shouldBe
      Set(Rule.TxnOfNoBatch)
    rules(quarantine(clean)(q => q.copy(commits = q.commits :+ Commit(2, Seq(9))))) shouldBe
      Set(Rule.TxnOfNoBatch)
  }

  test("rows under a run that the source does not have") {
    val stranger = RunId.generate().toString
    rules(table(clean)(t => t.copy(rows = t.rows :+ Rows(stranger, 99, 1)))) shouldBe
      Set(Rule.RowsOfNoRun)
  }

  test("rows under a batch that is not the one that their run took") {
    rules(table(clean)(t => t.copy(rows = t.rows :+ Rows(r1.toString, 7, 1)))) shouldBe
      Set(Rule.RowsOfAnotherBatch)
  }

  test("rows of a batch that was given up, or planned for another table, in a data table") {
    rules(table(clean)(t => t.copy(rows = t.rows :+ Rows(r5.toString, 4, 3)))) shouldBe
      Set(Rule.RowsOfNoWrittenBatch)
    val other = clean.copy(data = clean.data :+ Table(U, rows = Seq(Rows(r1.toString, 1, 1))))
    rules(other) shouldBe Set(Rule.RowsOfNoWrittenBatch)
  }

  test("the rows of one batch under two runs") {
    val again = RunId.generate()
    val twice = table(clean.copy(runs = clean.runs :+ Run(again, Some(2), RunStatus.Abandoned)))(
      t => t.copy(rows = t.rows :+ Rows(again.toString, 2, 1))
    )
    rules(twice) shouldBe Set(Rule.BatchByTwoRuns)
    rules(
      quarantine(clean.copy(runs = clean.runs :+ Run(again, Some(2), RunStatus.Abandoned)))(q =>
        q.copy(rows = q.rows :+ Rows(again.toString, 2, 1))
      )
    ) shouldBe Set(Rule.BatchByTwoRuns)
  }

  test("a run that wrote and committed a batch, and says another number of rows") {
    rules(run(clean, r1)(_.copy(rowsWritten = Some(3)))) shouldBe Set(Rule.RowsNotAsCounted)
  }

  test("a state that is not that of the last committed batch") {
    rules(clean.copy(state = Some(State(None, 3, r6)))) shouldBe Set(Rule.StateNotOfLastBatch)
    rules(clean.copy(state = Some(State(None, 5, r4)))) shouldBe Set(Rule.StateNotOfLastBatch)
    rules(clean.copy(state = None)) shouldBe Set(Rule.StateNotOfLastBatch)
    val nothing = clean.copy(
      batches = clean.batches.filter(_.status != BatchStatus.Committed),
      taken = Set.empty,
      data = Seq(Table(T)),
      quarantine = Table(Q),
      runs = clean.runs.filter(_.id == r5)
    )
    rules(nothing) shouldBe Set(Rule.StateNotOfLastBatch)
    IngestionLedger.check(nothing.copy(state = None)) shouldBe empty
  }

  test("a state that is not what the last reset left") {
    rules(reset.copy(state = Some(State(None, 5, r9)))) shouldBe Set(Rule.StateNotOfLastBatch)
    rules(reset.copy(state = clean.state)) shouldBe Set(Rule.StateNotOfLastBatch)
  }

  test("a reset of a state that was not there, that moves the sequence, or says another run") {
    // Of the state of batch 5, which r8 had reset already.
    rules(
      reset.copy(resets =
        reset.resets.updated(1, reset.resets(1).copy(before = State(None, 5, r6)))
      )
    ) shouldBe
      Set(Rule.ResetNotChained)
    // To batch 1: the sequence started again. What r9 reset is then not what r8 left.
    rules(
      reset.copy(resets = reset.resets.updated(0, reset.resets(0).copy(after = State(None, 1, r8))))
    ) shouldBe
      Set(Rule.ResetNotChained)
    // Moved by a run that is not the reset's.
    rules(
      reset.copy(
        state = Some(State(Some(at), 5, r8)),
        resets = reset.resets.updated(1, reset.resets(1).copy(after = State(Some(at), 5, r8)))
      )
    ) shouldBe Set(Rule.ResetNotChained)
    // Of no committed batch.
    rules(
      reset.copy(resets =
        reset.resets.updated(0, reset.resets(0).copy(before = State(None, 4, r5)))
      )
    ) shouldBe
      Set(Rule.ResetNotChained)
  }

  test("a reset by a run that did not succeed, that does not reset, or that is no run") {
    rules(run(reset, r9)(_.copy(status = RunStatus.Failed))) shouldBe Set(Rule.ResetNotSucceeded)
    rules(run(reset, r9)(_.copy(kind = RunKind.Regular))) shouldBe Set(Rule.ResetNotSucceeded)
    rules(reset.copy(runs = reset.runs.filter(_.id != r9))) shouldBe Set(Rule.ResetNotSucceeded)
    IngestionLedger.check(run(reset, r9)(_.copy(status = RunStatus.Failed))).map(_.message) shouldBe
      Seq(s"the state was reset by the run $r9, of the kind reset, which ended FAILED")
  }

  test("files taken that are not those of the committed plans") {
    rules(clean.copy(taken = clean.taken - (file("b") -> 2L))) shouldBe Set(
      Rule.FilesNotAsCommitted
    )
    rules(clean.copy(taken = clean.taken + (file("d") -> 4L))) shouldBe Set(
      Rule.FilesNotAsCommitted
    )
    rules(clean.copy(taken = clean.taken - (file("b") -> 2L) + (file("b") -> 3L))) shouldBe
      Set(Rule.FilesNotAsCommitted)
  }

  test("a run still running, and a batch committed by a run that did not succeed") {
    rules(run(clean, r3)(_.copy(status = RunStatus.Running))) shouldBe Set(Rule.StillRunning)
    rules(run(clean, r1)(_.copy(status = RunStatus.Failed))) shouldBe
      Set(Rule.CommitterNotSucceeded)
  }

  test("each violation says what it found, by batch, table and run") {
    val found = IngestionLedger.check(run(clean, r1)(_.copy(rowsWritten = Some(3))))
    found.map(_.message) shouldBe Seq(
      s"the run $r1 wrote and committed batch 1, and says 3 rows written; $T has 2"
    )
  }
}
