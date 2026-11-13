package lakehouse.ingestion.it.stress

import java.net.URI
import java.nio.file.{Files, Path, Paths}

import io.delta.tables.DeltaTable
import lakehouse.ingestion.it.stress.StressHarness.{HarnessFailure, Settings}
import lakehouse.ingestion.it.support.LocalSparkSuite
import org.scalatest.LoneElement
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The harness plumbing, with a single writer JVM: launch, barrier, commits, journal, exit code.
 * One writer has nobody to race with, so any anomaly here is a harness bug. The concurrent runs
 * are the spike's (P1.4c).
 */
final class StressHarnessSpec
    extends AnyFunSuite
    with Matchers
    with LoneElement
    with LocalSparkSuite {

  private def newRun(label: String): StressRun = StressRun.create(workDir.resolve("runs"), label)

  private def logDir(tableUri: String): Path =
    Paths.get(URI.create(tableUri)).resolve("_delta_log")

  test("one writer JVM commits every txnVersion once, journals each, and exits cleanly") {
    val run = newRun("single-writer")
    StressTable.create(spark, run.tableUri)

    val outcomes = StressHarness.run(run, Settings(writers = 1, commits = 5, rowsPerCommit = 3))

    val outcome = outcomes.loneElement
    withClue(s"see ${outcome.log}: ") {
      outcome.exitCode shouldBe StressWriter.ExitCode.Ok
    }
    outcome.journal shouldBe AckJournal.Contents((1L to 5L).toVector, None)

    // Version 0 is the empty table; then one version per commit.
    val table = DeltaTable.forPath(spark, run.tableUri)
    table.history().count() shouldBe 6
    Files.exists(logDir(run.tableUri).resolve(f"${5}%020d.json")) shouldBe true

    val rowsPerCommit = table.toDF
      .groupBy("writer_id", "txn_version")
      .count()
      .collect()
      .map(r => (r.getString(0), r.getLong(1)) -> r.getLong(2))
      .toMap
    rowsPerCommit shouldBe (1L to 5L).map(k => ("writer-0", k) -> 3L).toMap
  }

  test("a writer that cannot start fails the run before the barrier, with its log") {
    val run = newRun("no-table")
    // No StressTable.create: the writer cannot load the table.

    val failure = intercept[HarnessFailure] {
      StressHarness.run(run, Settings(writers = 1, commits = 1, rowsPerCommit = 1))
    }

    failure.getMessage should include(
      s"writer-0 exited with code ${StressWriter.ExitCode.SetupFailed} before the start barrier"
    )
    failure.getMessage should include("setup failed before the start barrier")
    Files.exists(run.startSignal) shouldBe false
    AckJournal.read(run.journal(0)) shouldBe AckJournal.Contents(Vector.empty, None)
  }
}
