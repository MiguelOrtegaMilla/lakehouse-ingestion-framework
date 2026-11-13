package lakehouse.ingestion.it.stress

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}

import io.delta.tables.DeltaTable
import lakehouse.ingestion.it.stress.LogModel.Reader
import lakehouse.ingestion.it.stress.StressHarness.{HarnessFailure, Settings, WriterOutcome}
import lakehouse.ingestion.it.stress.Verifier.Anomaly.{LostCommit, UnreadableTable, VersionGap}
import lakehouse.ingestion.it.stress.Verifier.Expected
import lakehouse.ingestion.it.support.LocalSparkSuite
import org.scalatest.LoneElement
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The harness plumbing, with a single writer JVM: launch, barrier, commits, journal, exit code,
 * and the verifier on the real table it leaves. One writer has nobody to race with, so any
 * anomaly on that run is a harness or verifier bug. The concurrent runs are the spike's (P1.4c).
 */
final class StressHarnessSpec
    extends AnyFunSuite
    with Matchers
    with LoneElement
    with LocalSparkSuite {

  private def newRun(label: String): StressRun = StressRun.create(workDir.resolve("runs"), label)

  private def copyOf(run: StressRun, label: String): StressRun = {
    val target = workDir.resolve("runs").resolve(label)
    Using.resource(Files.walk(run.root)) {
      _.iterator.asScala.foreach(p => Files.copy(p, target.resolve(run.root.relativize(p))))
    }
    StressRun.at(target)
  }

  private def logDir(tableUri: String): Path =
    Paths.get(URI.create(tableUri)).resolve("_delta_log")

  /**
   * One real single-writer run, shared by the tests that only read it. Held in a `Try`, so that
   * if the run fails every test that uses it reports that failure, not a retry's.
   */
  private lazy val sharedRun: Try[(StressRun, Settings, Seq[WriterOutcome])] = Try {
    val run = newRun("single-writer")
    StressTable.create(spark, run.tableUri)
    val settings = Settings(writers = 1, commits = 5, rowsPerCommit = 3)
    (run, settings, StressHarness.run(run, settings))
  }

  private def singleWriter = sharedRun.get

  test("one writer JVM commits every txnVersion once, journals each, and exits cleanly") {
    val (run, _, outcomes) = singleWriter

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

  test("the verifier finds nothing wrong with a real single-writer run") {
    val (run, settings, outcomes) = singleWriter

    val report = LogReader.verify(spark, run, Expected.of(settings), outcomes)

    withClue(report.anomalies.map(_.detail).mkString("\n")) { report.isClean shouldBe true }
    report.summary.latestVersion shouldBe Some(5)
    report.summary.acknowledged shouldBe 5
    report.summary.writerCommitsInLog shouldBe 5
    report.summary.otherCommits shouldBe Seq(0L -> "CREATE TABLE")
    report.summary.orphanDataFiles shouldBe empty
    report.summary.checksumMismatches shouldBe empty
    report.summary.readers shouldBe Reader.all.map(_ -> None)
  }

  test("the report states the writers' Spark settings with their reasons") {
    val (run, settings, outcomes) = singleWriter
    val report = LogReader.verify(spark, run, Expected.of(settings), outcomes)

    val markdown = StressReport.markdown(
      "Single writer",
      StressReport.environment ++ StressReport.harness(settings),
      StressWriter.SparkSettings,
      report
    )

    markdown should include("| `spark.databricks.delta.snapshotPartitions` | `1` |")
    markdown should include("not the commit protocol")
    markdown should include(s"| Delta Lake | ${io.delta.VERSION} |")
    markdown should include("**No anomalies.**")
  }

  test("on a real run with a deleted commit file the verifier finds the lost commit") {
    val (original, settings, outcomes) = singleWriter
    val run = copyOf(original, "deleted-commit")
    // Version 3 holds txnVersion 3: v0 is the empty table, then one version per commit.
    Files.delete(logDir(run.tableUri).resolve(f"${3}%020d.json"))

    val report = LogReader.verify(spark, run, Expected.of(settings), outcomes)

    report.anomalies should contain allOf (
      VersionGap(Seq(3)),
      LostCommit("writer-0", 3)
    )
    // The data file of the deleted commit stays on disk, referenced by nothing: the physical
    // trace a lost commit leaves.
    report.summary.orphanDataFiles should have size 1
    // Delta notices a gap. The RED spike's lost commits leave none: another writer's commit
    // takes the version, so a Delta reader has nothing to notice.
    report.anomalies.collect { case UnreadableTable(r, e) =>
      r -> e.contains("DELTA_VERSIONS_NOT_CONTIGUOUS")
    } shouldBe Reader.all.map(_ -> true)
  }

  test("a commit file whose .crc sidecar went stale fails the default reader only") {
    val (original, settings, outcomes) = singleWriter
    val run = copyOf(original, "stale-sidecar")
    // Same length, other bytes, nothing the verifier reads: the sidecar no longer matches.
    val commit = logDir(run.tableUri).resolve(f"${3}%020d.json")
    val bytes = new String(Files.readAllBytes(commit), UTF_8)
    // commitInfo's epoch-millis timestamp starts with 1 in this century; 2 keeps it valid JSON.
    val altered = bytes.replaceFirst("\"timestamp\":1", "\"timestamp\":2")
    altered should not equal bytes
    Files.write(commit, altered.getBytes(UTF_8))

    val report = LogReader.verify(spark, run, Expected.of(settings), outcomes)

    report.summary.checksumMismatches shouldBe Seq(3L)
    report.anomalies.loneElement match {
      case UnreadableTable(reader, error) =>
        reader shouldBe Reader.Default
        error should include("ChecksumException")
      case other => fail(s"unexpected anomaly: $other")
    }
    // The commit files themselves are intact, so nothing is lost and nothing else is wrong.
    report.summary.lost shouldBe 0
    report.summary.readers.toMap.apply(Reader.SidecarFree) shouldBe None
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
