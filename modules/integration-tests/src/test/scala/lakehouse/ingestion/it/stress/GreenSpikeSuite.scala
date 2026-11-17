package lakehouse.ingestion.it.stress

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

import scala.concurrent.duration._

import lakehouse.ingestion.it.stress.StressHarness.{Chaos, Settings, WriterOutcome}
import lakehouse.ingestion.it.stress.Verifier.Report
import lakehouse.ingestion.logstore.{FaultHooks, FaultPoint, PostgresLogStore}
import org.apache.hadoop.fs.LocalFileSystem
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What the suites of the GREEN spike share (ADR-0004 §7.3): how a variant is run on
 * `PostgresLogStore`, what is checked of every writer before a result counts, and the criterion
 * every variant is held to.
 *
 * '''The preconditions''', on each writer's own probe: its Delta resolved `PostgresLogStore` for
 * the table, on Hadoop's own local file system, and the writer passed the startup guard before
 * it touched the table.
 *
 * '''The criterion''': no commit lost and no other anomaly, no commit file with a stale `.crc`
 * sidecar, and both readers reading the table. "No other anomaly" includes what the verifier
 * holds a coordinated run to: a complete entry for every commit and a commit for every entry,
 * no taint, each writer's commits under its own runs, no sidecar at all on a commit file, and
 * no stale one on any other file of the log.
 *
 * Each run, with its table, journals, logs and probes, stays in `target/stress/<run>/`, next to
 * its `report.md`.
 */
trait GreenSpikeSuite extends CoordinatedStressSuite { self: AnyFunSuite with Matchers =>

  import GreenSpikeSuite.Spiked

  protected val RowsPerCommit = 10

  private val stamp = DateTimeFormatter
    .ofPattern("yyyyMMdd'T'HHmmss'Z'")
    .withZone(ZoneOffset.UTC)
    .format(Instant.now())

  private def stressRoot: Path =
    Paths.get(sys.props.getOrElse("repo.root", ".")).resolve("target").resolve("stress")

  /** The setting that makes every commit of a writer wait for `delay` at `point`. */
  protected def delayAt(point: FaultPoint, delay: FiniteDuration): String =
    s"-Dspark.hadoop.${FaultHooks.delayKey(point)}=${delay.toMillis}"

  /** Writers set up for `PostgresLogStore`, each of which must pass its startup guard. */
  protected def coordinated(writers: Int, commits: Int, alsoWith: Seq[String] = Nil): Settings =
    Settings(
      writers = writers,
      commits = commits,
      rowsPerCommit = RowsPerCommit,
      extraJvmOptions = coordinatedJvmOptions ++ alsoWith,
      guard = true
    )

  /**
   * Runs one variant, writes its report and checks its preconditions.
   *
   * @param chaos what is done to the writers while they commit
   * @param notes what the run shows, for its report; they may quote what the run found
   */
  protected def spikeRun(
      variant: String,
      title: String,
      settings: Settings,
      chaos: (StressRun, Chaos) => Unit = (_, _) => ()
  )(notes: (Seq[WriterOutcome], Report) => Seq[String]): Spiked = {
    val run = StressRun.create(stressRoot, s"$stamp-green-$variant")
    StressTable.create(spark, run.tableUri)
    val started = System.nanoTime()
    val outcomes = StressHarness.run(run, settings, chaos(run, _))
    val elapsed = (System.nanoTime() - started).nanos
    val report = verify(run, settings, outcomes)
    val markdown = StressReport.markdown(
      title,
      StressReport.environment ++ StressReport.harness(settings) :+
        StressReport.Setting(
          "time from the first writer's launch to the last one's exit",
          f"${elapsed.toMillis / 1000.0}%.1f s"
        ),
      StressWriter.SparkSettings,
      report,
      notes(outcomes, report)
    )
    Files.write(run.report, markdown.getBytes(UTF_8))
    info(s"report: ${run.report}")
    info(
      s"lost ${report.summary.lost} of ${report.summary.acknowledged} acknowledged commits; " +
        s"stale sidecars at ${report.summary.checksumMismatches.mkString("[", ", ", "]")}; " +
        s"versions lost to other writers: ${contention(report)}, " +
        s"${contentionAtInsert(report)} of them at the INSERT; " +
        s"anomalies: ${report.anomalies.groupMapReduce(_.kind)(_ => 1)(_ + _)}; " +
        f"${elapsed.toMillis / 1000.0}%.1f s"
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
    Spiked(run, outcomes, report)
  }

  /** The criterion of the spike. */
  protected def isGreen(report: Report): Assertion =
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

  protected def probes(report: Report): Seq[WriterProbe] = report.summary.writers.flatMap(_.probe)

  /** The versions that the writers of the run lost to each other, all together. */
  protected def contention(report: Report): Long = probes(report).map(_.contention).sum

  /**
   * Of those, the ones lost at the `INSERT`: the times two writers were committing one version
   * at once, which is when Delta's default LogStore loses a commit.
   */
  protected def contentionAtInsert(report: Report): Long =
    probes(report).map(_.contentionAtInsert).sum
}

object GreenSpikeSuite {

  /** A variant that was run: where it is, how its writers ended, and what the verifier found. */
  final case class Spiked(run: StressRun, outcomes: Seq[WriterOutcome], report: Report)
}
