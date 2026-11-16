package lakehouse.ingestion.it.stress

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

import scala.concurrent.duration._

import lakehouse.ingestion.it.stress.StressHarness.Settings
import lakehouse.ingestion.it.stress.Verifier.Report
import lakehouse.ingestion.it.support.StressTest
import lakehouse.ingestion.logstore.{FaultHooks, FaultPoint, PostgresLogStore}
import org.apache.hadoop.fs.LocalFileSystem
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Spike GREEN (ADR-0004 §7.3): the suite of the RED spike, with the same harness and the same
 * verifier, on `PostgresLogStore`. Where Delta's default LogStore lost commits that its writers
 * saw succeed, this one must lose none.
 *
 * '''The criterion''', for every variant: no commit lost and no other anomaly, no commit file
 * with a stale `.crc` sidecar, and both readers reading the table, the one that checks sidecars
 * and the one that does not. "No other anomaly" includes what the verifier holds a coordinated
 * run to: a complete entry for every commit and a commit for every entry, no taint, each
 * writer's commits under its own run, no sidecar at all on a commit file, and no stale one on
 * any other file of the log.
 *
 * '''The preconditions''', checked on each writer's own probe before any result counts: its
 * Delta resolved `PostgresLogStore` for the table, on Hadoop's own local file system, and the
 * writer passed the startup guard before it touched the table.
 *
 * So far the two controls, one writer each: nobody to race with, so any anomaly would be the
 * harness's own, or the fault hooks'. The delay of the RED spike sat on a rename that this
 * LogStore never makes, so the widened window is made with the LogStore's own fault hooks, and
 * the control counts their waits: a hook that was not in the path of every commit would widen
 * nothing.
 *
 * Each run, with its table, journals, logs and probes, stays in `target/stress/<run>/`, next to
 * its `report.md`.
 */
final class SpikeGreenSpec extends AnyFunSuite with Matchers with CoordinatedStressSuite {

  private val RowsPerCommit = 10

  /** The delay of the RED spike's widened window: longer than a writer's whole commit cycle. */
  private val WidenedDelay = 1500.millis

  private val stamp = DateTimeFormatter
    .ofPattern("yyyyMMdd'T'HHmmss'Z'")
    .withZone(ZoneOffset.UTC)
    .format(Instant.now())

  private def stressRoot: Path =
    Paths.get(sys.props.getOrElse("repo.root", ".")).resolve("target").resolve("stress")

  /** The setting that makes every commit of a writer wait at `point`. */
  private def delayAt(point: FaultPoint): String =
    s"-Dspark.hadoop.${FaultHooks.delayKey(point)}=${WidenedDelay.toMillis}"

  /** Writers set up for `PostgresLogStore`, each of which must pass its startup guard. */
  private def coordinated(writers: Int, commits: Int, alsoWith: Seq[String] = Nil): Settings =
    Settings(
      writers = writers,
      commits = commits,
      rowsPerCommit = RowsPerCommit,
      extraJvmOptions = coordinatedJvmOptions ++ alsoWith,
      guard = true
    )

  /** Runs one variant, writes its report and checks its preconditions. */
  private def spike(variant: String, title: String, settings: Settings): (StressRun, Report) = {
    val run = StressRun.create(stressRoot, s"$stamp-green-$variant")
    StressTable.create(spark, run.tableUri)
    val outcomes = StressHarness.run(run, settings)
    val report = verify(run, settings, outcomes)
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
        s"versions lost to other writers: " +
        s"${report.summary.writers.flatMap(_.probe).map(_.contention).sum}; " +
        s"anomalies: ${report.anomalies.groupMapReduce(_.kind)(_ => 1)(_ + _)}"
    )

    withClue(s"see ${run.report}: ") {
      outcomes.foreach { o =>
        val probe = o.probe.getOrElse(fail(s"${StressTable.writerId(o.writer)} left no probe"))
        probe.logStore shouldBe classOf[PostgresLogStore].getName
        probe.logFileSystem shouldBe classOf[LocalFileSystem].getName
        probe.guarded shouldBe true
      }
      // A contradiction in a journal is a harness bug in any variant, never a measurement.
      report.count("InconsistentJournal") shouldBe 0
    }
    (run, report)
  }

  /** The criterion of the spike. */
  private def isGreen(report: Report): Assertion =
    withClue(report.anomalies.map(_.detail).mkString("\n")) {
      report.summary.lost shouldBe 0
      report.isClean shouldBe true
      report.summary.checksumMismatches shouldBe empty
      report.summary.commitSidecars shouldBe empty
      report.summary.staleSidecars shouldBe empty
      report.summary.readers.map { case (reader, error) => reader.label -> error } shouldBe
        LogModel.Reader.all.map(_.label -> None)
      report.summary.registry.flatMap(_.registeredVersion) shouldBe Some(-1L)
    }

  private def faultWaits(report: Report): Seq[Seq[Long]] =
    report.summary.writers.flatMap(_.probe).map(_.faultWaits.map(_._2))

  test("control: one writer without a delay leaves a clean table", StressTest) {
    val (_, report) = spike(
      "control-no-delay",
      "GREEN control — one writer, no delay",
      coordinated(writers = 1, commits = 10)
    )

    isGreen(report)
    faultWaits(report) shouldBe Seq(Seq(0L, 0L, 0L))
  }

  test(
    "control: one writer that waits at the fault hooks leaves a clean table, every commit delayed",
    StressTest
  ) {
    val commits = 5
    val (_, report) = spike(
      "control-delay",
      "GREEN control — one writer, widened windows",
      coordinated(
        writers = 1,
        commits = commits,
        alsoWith = Seq(delayAt(FaultPoint.BeforeInsert), delayAt(FaultPoint.AfterInsert))
      )
    )

    isGreen(report)
    // One writer never loses a version: exactly one wait per commit at each hook that is on,
    // and none at the one that is not. The waits were in the path of every commit.
    faultWaits(report) shouldBe Seq(Seq(commits.toLong, commits.toLong, 0L))
  }
}
