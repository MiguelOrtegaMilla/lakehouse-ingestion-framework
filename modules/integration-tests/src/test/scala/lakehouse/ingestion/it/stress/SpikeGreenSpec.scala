package lakehouse.ingestion.it.stress

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

import scala.collection.mutable
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
 * '''The variants''', which are those of RED and what RED's own widening can no longer do:
 *
 *   - two controls with one writer, without and with the delay: nobody to race with, so any
 *     anomaly would be the harness's own, or the fault hooks';
 *   - the widened window. The delay of the RED spike sat on a rename that this LogStore never
 *     makes, so the window is widened with the LogStore's own fault hooks, once on each side of
 *     the `INSERT` that takes a version. Each run must also show that it tested something: the
 *     hook was in the path of the commits, and the writers did collide;
 *   - the configuration of RED itself, `DelayingLocalFs` included: the same experiment, in
 *     which the delay is no longer in the path of a commit;
 *   - the natural race, several times. It is the weak evidence: a run that loses nothing can be
 *     luck, as three of the five RED runs were. What it adds is the contention that happens
 *     without help, which is reported for each run and required over all of them: how many
 *     versions the writers lost to each other, and how many of those at the `INSERT`, which are
 *     the times two writers were committing one version at once.
 *
 * The strong evidence is the widened windows, and the writers killed inside them (P2b.4).
 *
 * Each run, with its table, journals, logs and probes, stays in `target/stress/<run>/`, next to
 * its `report.md`.
 */
final class SpikeGreenSpec extends AnyFunSuite with Matchers with CoordinatedStressSuite {

  import SpikeGreenSpec._

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

  /**
   * Runs one variant, writes its report and checks its preconditions.
   *
   * @param notes what the run shows, for its report; they may quote what the run found
   */
  private def spike(variant: String, title: String, settings: Settings)(
      notes: Report => Seq[String]
  ): Report = {
    val run = StressRun.create(stressRoot, s"$stamp-green-$variant")
    StressTable.create(spark, run.tableUri)
    val started = System.nanoTime()
    val outcomes = StressHarness.run(run, settings)
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
      notes(report)
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
    report
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

  private def probes(report: Report): Seq[WriterProbe] = report.summary.writers.flatMap(_.probe)

  /** The waits of each writer at the three fault hooks, in the order of the protocol. */
  private def faultWaits(report: Report): Seq[Seq[Long]] =
    probes(report).map(_.faultWaits.map(_._2))

  private def waitsAt(report: Report, point: FaultPoint): Seq[Long] =
    probes(report).map(_.faultWaits.toMap.apply(point.key))

  /** The versions that the writers of the run lost to each other, all together. */
  private def contention(report: Report): Long = probes(report).map(_.contention).sum

  /**
   * Of those, the ones lost at the `INSERT`: the times two writers were committing one version
   * at once, which is when Delta's default LogStore loses a commit.
   */
  private def contentionAtInsert(report: Report): Long =
    probes(report).map(_.contentionAtInsert).sum

  /**
   * The commits of writers that were complete in less time than their own writer had to wait
   * before publishing them: somebody else published them.
   */
  private def finishedByAnother(report: Report, ownWait: FiniteDuration): Int =
    report.summary.registry.toSeq
      .flatMap(_.entries)
      .count(entry => entry.runId.isDefined && entry.tookMillis.exists(_ < ownWait.toMillis))

  // ---------------------------------------------------------------------------------------------
  // Controls
  // ---------------------------------------------------------------------------------------------

  test("control: one writer without a delay leaves a clean table", StressTest) {
    val report = spike(
      "control-no-delay",
      "GREEN control — one writer, no delay",
      coordinated(writers = 1, commits = 10)
    )(_ => Seq("One writer, nobody to race with: an anomaly here would be the harness's own."))

    isGreen(report)
    faultWaits(report) shouldBe Seq(Seq(0L, 0L, 0L))
  }

  test(
    "control: one writer that waits at the fault hooks leaves a clean table, every commit delayed",
    StressTest
  ) {
    val commits = 5
    val report = spike(
      "control-delay",
      "GREEN control — one writer, widened windows",
      coordinated(
        writers = 1,
        commits = commits,
        alsoWith = Seq(delayAt(FaultPoint.BeforeInsert), delayAt(FaultPoint.AfterInsert))
      )
    )(_ =>
      Seq(
        "One writer with both fault hooks on: the hooks themselves lose nothing, and each of " +
          "them is in the path of every commit, exactly once."
      )
    )

    isGreen(report)
    // One writer never loses a version: exactly one wait per commit at each hook that is on,
    // and none at the one that is not. The waits were in the path of every commit.
    faultWaits(report) shouldBe Seq(Seq(commits.toLong, commits.toLong, 0L))
  }

  // ---------------------------------------------------------------------------------------------
  // The widened window, on each side of the INSERT
  // ---------------------------------------------------------------------------------------------

  test(
    "widened window before the INSERT: writers that claim one version lose no commit",
    StressTest
  ) {
    val commits = 10
    val report = spike(
      "widened-before-insert",
      "Spike GREEN — PostgresLogStore, window widened before the INSERT",
      coordinated(writers = 3, commits = commits, alsoWith = Seq(delayAt(FaultPoint.BeforeInsert)))
    ) { report =>
      Seq(
        s"The LogStore's fault hook `beforeInsert` holds every writer for ${WidenedDelay.toMillis} " +
          "ms between its look at the log and the `INSERT` that takes a version. Writers that " +
          "were released together therefore all find the version free, and all claim it. It is " +
          "the window in which Delta's default LogStore lost a commit in every run of the RED " +
          "spike, moved to where this LogStore has it.",
        "The run is evidence only if the wait was in the path of the commits and the writers " +
          s"did collide. They waited ${waitsAt(report, FaultPoint.BeforeInsert).sum} times at " +
          s"the hook, for ${report.summary.acknowledged} commits, and lost " +
          s"${contention(report)} versions to each other, ${contentionAtInsert(report)} of " +
          "them at the `INSERT`: so many times a writer had found the version free, had waited " +
          "with its commit ready, and found the version taken when it claimed it. Delta " +
          "settled each at the next version."
      )
    }

    isGreen(report)
    // A writer waits once for each commit it makes and once for each version it loses at the
    // INSERT, and for nothing else: the wait is where the versions were lost.
    waitsAt(report, FaultPoint.BeforeInsert) shouldBe
      probes(report).map(_.contentionAtInsert + commits)
    contentionAtInsert(report) should be >= 1L
  }

  test(
    "widened window after the INSERT: commits that other writers publish are not lost",
    StressTest
  ) {
    val commits = 10
    val report = spike(
      "widened-after-insert",
      "Spike GREEN — PostgresLogStore, window widened after the INSERT",
      coordinated(writers = 3, commits = commits, alsoWith = Seq(delayAt(FaultPoint.AfterInsert)))
    ) { report =>
      Seq(
        s"The fault hook `afterInsert` holds every writer for ${WidenedDelay.toMillis} ms " +
          "between the `INSERT` that takes its version and the publication of its commit: the " +
          "commit exists and is not in the log. The other writers find the version taken, read " +
          "the table, and publish the commit themselves before they go on. The writer then " +
          "finds its commit published. This is recovery by another process while the owner is " +
          "alive, on every commit.",
        s"${finishedByAnother(report, WidenedDelay)} of the ${report.summary.writerCommitsInLog} " +
          s"commits were complete in less than the ${WidenedDelay.toMillis} ms that their own " +
          "writer waited before publishing: another process published them. The writers lost " +
          s"${contention(report)} versions to each other, ${contentionAtInsert(report)} of " +
          "them at the `INSERT`: the version was taken and its commit was not in the log yet."
      )
    }

    isGreen(report)
    all(waitsAt(report, FaultPoint.AfterInsert)) shouldBe commits.toLong
    contentionAtInsert(report) should be >= 1L
    finishedByAnother(report, WidenedDelay) should be >= 1
  }

  // ---------------------------------------------------------------------------------------------
  // The experiment of RED, as it was
  // ---------------------------------------------------------------------------------------------

  test(
    "the configuration of the RED spike, its delaying file system included, loses no commit",
    StressTest
  ) {
    val delaying = classOf[DelayingLocalFs].getName
    val report = spike(
      "red-configuration",
      "Spike GREEN — PostgresLogStore, with the configuration of the RED spike's widened window",
      coordinated(
        writers = 3,
        commits = 10,
        alsoWith = DelayingLocalFs.jvmOptions(WidenedDelay.toMillis)
      )
    ) { report =>
      Seq(
        "The writers have the configuration of the RED spike's widened window, and the LogStore: " +
          s"`DelayingLocalFs` is the `FileContext` of `file:`, with its ${WidenedDelay.toMillis} " +
          "ms before the rename that publishes a commit file. It is the same experiment as RED.",
        "It does not test the same thing. `PostgresLogStore` publishes a commit as a hard link, " +
          "and never renames one, so that delay is no longer in the path of a commit: every " +
          s"probe shows `$delaying` resolved for the log, and " +
          s"${probes(report).map(_.delayedRenames).sum} renames delayed. The result says that " +
          "the window RED widened is gone, not that it was survived. The windows this LogStore " +
          "does have are widened in the two runs next to this one.",
        s"With no delay of this LogStore's, the writers lost ${contention(report)} versions to " +
          s"each other, ${contentionAtInsert(report)} of them at the `INSERT`."
      )
    }

    isGreen(report)
    // The delaying file system is what each writer's FileContext for the log resolves to, as
    // in RED, and it delayed nothing: no commit goes through it any more.
    probes(report).map(_.commitFileContext).distinct shouldBe Seq(delaying)
    probes(report).map(_.delayedRenames) shouldBe Seq(0L, 0L, 0L)
  }

  // ---------------------------------------------------------------------------------------------
  // The natural race
  // ---------------------------------------------------------------------------------------------

  /**
   * For each natural run, in order: the versions that its writers lost to each other, and how
   * many of those at the `INSERT`.
   */
  private val naturalContention = mutable.ArrayBuffer.empty[(Long, Long)]

  (1 to NaturalRuns).foreach { number =>
    test(s"natural race, run $number of $NaturalRuns: no delay, no commit lost", StressTest) {
      val report = spike(
        f"natural-$number%02d",
        s"Spike GREEN — PostgresLogStore, natural race (run $number of $NaturalRuns)",
        coordinated(writers = 3, commits = 200)
      ) { report =>
        Seq(
          "No fault injection: three writers, 200 commits each, as in the RED spike.",
          "This is the weak evidence. A run in which no commit is lost can be luck: three of the " +
            "five RED runs lost none either, with a LogStore that loses commits. It is run " +
            s"$NaturalRuns times. The strong evidence is in the runs that widen the window, and " +
            "in the writers killed inside it: there the race is made to happen every time.",
          "What this run adds is the contention that happens without help. Its writers lost " +
            s"${contention(report)} versions to each other, and Delta settled each at the next " +
            "version. Most of those say little: the version's commit was already in the log " +
            "when the writer looked, as it also was, as often, under Delta's default LogStore. " +
            s"${contentionAtInsert(report)} of them were lost at the `INSERT`: the commit was " +
            "not in the log when the writer looked, and the claim found another writer's. That " +
            "is how many times two writers were committing one version at the same time in " +
            "this run, which is when the default LogStore loses a commit."
        )
      }

      naturalContention += ((contention(report), contentionAtInsert(report)))
      isGreen(report)
    }
  }

  if (NaturalRuns > 0)
    test("the natural runs, taken together, did contend", StressTest) {
      val (lost, atInsert) = naturalContention.unzip
      info(s"versions lost to other writers, by run: ${lost.mkString(", ")}")
      info(s"of them at the INSERT, by run: ${atInsert.mkString(", ")} (${atInsert.sum} in all)")
      // Every run got as far as its report. Otherwise this would speak for fewer than it says.
      naturalContention should have size NaturalRuns.toLong
      lost.sum should be >= 1L
    }
}

object SpikeGreenSpec {

  /** The environment variable with the number of natural runs; the default is the spike's. */
  val NaturalRunsVariable = "STRESS_NATURAL_RUNS"

  /**
   * How many times the natural race is run. Ten for the spike: one clean run says little. Fewer
   * while the suite itself is being worked on, and none to run only what is deterministic.
   */
  val NaturalRuns: Int =
    sys.env.get(NaturalRunsVariable).flatMap(_.toIntOption).filter(_ >= 0).getOrElse(10)
}
