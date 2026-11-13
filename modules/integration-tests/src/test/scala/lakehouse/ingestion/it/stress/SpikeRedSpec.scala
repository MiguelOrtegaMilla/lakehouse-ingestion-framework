package lakehouse.ingestion.it.stress

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

import scala.concurrent.duration._

import io.delta.storage.HDFSLogStore
import lakehouse.ingestion.it.stress.LogModel.Reader
import lakehouse.ingestion.it.stress.StressHarness.Settings
import lakehouse.ingestion.it.stress.Verifier.Anomaly.{
  LostCommit,
  RowMismatch,
  SnapshotDivergence,
  UnreadableTable
}
import lakehouse.ingestion.it.stress.Verifier.{Expected, Report}
import lakehouse.ingestion.it.support.{LocalSparkSuite, StressTest}
import org.apache.hadoop.fs.LocalFileSystem
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Spike RED (ADR-0004 §7.3): Delta's default LogStore on `file://`, with several writer JVMs on
 * one table, loses commits that its writers saw succeed.
 *
 * Every run starts with its preconditions, checked on each writer's own probe: the LogStore is
 * `HDFSLogStore`, its `synchronized` block is not entered (the log's file system is not a
 * `RawLocalFileSystem`), and commit files are published through the expected `FileContext`.
 * Then:
 *
 *   - two controls with one writer, without and with the delay: nobody to race with, so any
 *     anomaly would be the harness's or the delay's own;
 *   - the widened window: `DelayingLocalFs` holds each commit-file rename after the last
 *     existence check, so concurrent writers reliably both rename. It must lose at least one
 *     acknowledged commit and leave no version gap: the losing commit file is replaced, not
 *     removed, which is why Delta itself notices nothing. A reader that checks no sidecars
 *     must read the table without error and without the lost commits: the silent loss an
 *     object store would give;
 *   - the natural race, measured only. Whether it loses a commit within the budget is the
 *     measurement, not a pass condition.
 *
 * Stale `.crc` sidecars and a default reader that fails on them are reported, never required:
 * they follow a loss on most rename interleavings, not on all (`LogView.checksumMismatches`).
 *
 * Each run, with its table, journals, logs and probes, stays in `target/stress/<run>/`, next to
 * its `report.md`.
 */
final class SpikeRedSpec extends AnyFunSuite with Matchers with LocalSparkSuite {

  private val RowsPerCommit = 10

  /**
   * Longer than a whole commit cycle of a writer on `local[1]` (about 1 s, P1.4a), so that
   * writers released by the barrier together are all inside the window of the same version.
   */
  private val WidenedDelay = 1500.millis

  private val stamp = DateTimeFormatter
    .ofPattern("yyyyMMdd'T'HHmmss'Z'")
    .withZone(ZoneOffset.UTC)
    .format(Instant.now())

  private def stressRoot: Path =
    Paths.get(sys.props.getOrElse("repo.root", ".")).resolve("target").resolve("stress")

  private val LocalFs = "org.apache.hadoop.fs.local.LocalFs"

  /** Runs one variant, writes its report and checks its preconditions. */
  private def spike(
      variant: String,
      title: String,
      settings: Settings,
      commitFileContext: String
  ): (StressRun, Report) = {
    val run = StressRun.create(stressRoot, s"$stamp-red-$variant")
    StressTable.create(spark, run.tableUri)
    val outcomes = StressHarness.run(run, settings)
    val report = LogReader.verify(spark, run, Expected.of(settings), outcomes)
    val markdown = StressReport.markdown(
      title,
      StressReport.environment ++ StressReport.harness(settings),
      StressWriter.SparkSettings,
      report
    )
    Files.write(run.report, markdown.getBytes(UTF_8))
    info(s"report: ${run.report}")
    info(
      s"lost ${report.summary.lost} of ${report.summary.acknowledged} acknowledged commits; " +
        s"stale sidecars at ${report.summary.checksumMismatches.mkString("[", ", ", "]")}; " +
        s"anomalies: ${report.anomalies.groupMapReduce(_.kind)(_ => 1)(_ + _)}"
    )

    withClue(s"see ${run.report}: ") {
      outcomes.foreach { o =>
        val probe = o.probe.getOrElse(fail(s"${StressTable.writerId(o.writer)} left no probe"))
        probe.logStore shouldBe classOf[HDFSLogStore].getName
        probe.logFileSystem shouldBe classOf[LocalFileSystem].getName
        probe.hdfsLogStoreLockTaken shouldBe false
        probe.commitFileContext shouldBe commitFileContext
      }
      // A contradiction in a journal is a harness bug in any variant, never a measurement.
      report.count("InconsistentJournal") shouldBe 0
    }
    (run, report)
  }

  private def isClean(report: Report): Assertion =
    withClue(report.anomalies.map(_.detail).mkString("\n")) { report.isClean shouldBe true }

  test("control: one writer without the delay leaves a clean table", StressTest) {
    val settings = Settings(writers = 1, commits = 10, rowsPerCommit = RowsPerCommit)
    val (_, report) =
      spike("control-no-delay", "RED control — one writer, no delay", settings, LocalFs)

    isClean(report)
    report.summary.checksumMismatches shouldBe empty
    report.summary.writers.flatMap(_.probe).map(_.delayedRenames) shouldBe Seq(0L)
  }

  test(
    "control: one writer with the delay leaves a clean table, every commit delayed",
    StressTest
  ) {
    val commits = 5
    val settings = Settings(
      writers = 1,
      commits = commits,
      rowsPerCommit = RowsPerCommit,
      extraJvmOptions = DelayingLocalFs.jvmOptions(WidenedDelay.toMillis)
    )
    val (_, report) = spike(
      "control-delay",
      "RED control — one writer, widened window",
      settings,
      classOf[DelayingLocalFs].getName
    )

    isClean(report)
    report.summary.checksumMismatches shouldBe empty
    // One writer never retries: exactly one delayed rename per commit, and nothing else delayed.
    report.summary.writers.flatMap(_.probe).map(_.delayedRenames) shouldBe Seq(commits.toLong)
  }

  test("widened window: concurrent writers lose acknowledged commits, with no gap", StressTest) {
    val settings = Settings(
      writers = 3,
      commits = 10,
      rowsPerCommit = RowsPerCommit,
      extraJvmOptions = DelayingLocalFs.jvmOptions(WidenedDelay.toMillis)
    )
    val (run, report) = spike(
      "widened",
      "Spike RED — default LogStore on file://, widened window",
      settings,
      classOf[DelayingLocalFs].getName
    )

    withClue(report.anomalies.map(_.detail).mkString("\n")) {
      report.count("LostCommit") should be >= 1
      // The lost commit's file was replaced by another writer's, not removed.
      report.count("VersionGap") shouldBe 0

      // A reader without sidecars to check, as on object storage, reads the table without any
      // error and agrees with the commit files: the loss is silent.
      report.summary.readers.toMap.apply(Reader.SidecarFree) shouldBe None
      report.anomalies.filter(readerOf(_).contains(Reader.SidecarFree)) shouldBe empty
      val lost = report.anomalies.collect { case LostCommit(w, k) => (w, k) }
      val seen = LogReader.tableView(spark, run.tableUri, Reader.SidecarFree).map(_.rows.keySet)
      seen.map(keys => lost.filter(keys)) shouldBe Right(Nil)
    }
  }

  private def readerOf(a: Verifier.Anomaly): Option[Reader] = a match {
    case UnreadableTable(r, _) => Some(r)
    case SnapshotDivergence(r, _, _) => Some(r)
    case RowMismatch(r, _, _, _, _) => Some(r)
    case _ => None
  }

  test("natural race: concurrent writers without the delay, measured only", StressTest) {
    val settings = Settings(writers = 3, commits = 200, rowsPerCommit = RowsPerCommit)
    spike(
      "natural",
      "Spike RED — default LogStore on file://, natural race",
      settings,
      LocalFs
    ): Unit
  }
}
