package lakehouse.ingestion.it.stress

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.util.UUID

import scala.jdk.CollectionConverters._
import scala.util.Using

import lakehouse.ingestion.it.stress.StressHarness.{Settings, WriterOutcome}
import lakehouse.ingestion.it.stress.Verifier.Expected
import lakehouse.ingestion.logstore.{FaultHooks, FaultPoint, PostgresLogStore}
import org.apache.hadoop.fs.LocalFileSystem
import org.scalatest.LoneElement
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The stress harness on `PostgresLogStore`, in runs small enough for every build: writer JVMs
 * that are set up for the LogStore, pass its startup guard and commit through it; two of them
 * contending for the versions of one table; and one that is set up wrongly, and refuses to run.
 *
 * The runs that measure something are the spikes (`SpikeGreenSpec`, `sbt stress`). This suite
 * shows that the harness itself runs what those claim to measure.
 */
final class CoordinatedHarnessSpec
    extends AnyFunSuite
    with Matchers
    with LoneElement
    with CoordinatedStressSuite {

  private val coordinated = classOf[PostgresLogStore].getName

  private def newRun(label: String): StressRun = {
    val run = StressRun.create(workDir.resolve("runs"), label)
    StressTable.create(spark, run.tableUri)
    run
  }

  private def logDir(run: StressRun): Path =
    Paths.get(URI.create(run.tableUri)).resolve("_delta_log")

  private def logFiles(run: StressRun): Seq[String] =
    Using.resource(Files.list(logDir(run)))(_.iterator.asScala.map(_.getFileName.toString).toList)

  private def logOf(outcome: WriterOutcome): String =
    new String(Files.readAllBytes(outcome.log), UTF_8)

  private def delay(point: FaultPoint, millis: Long): String =
    s"-Dspark.hadoop.${FaultHooks.delayKey(point)}=$millis"

  test("a writer JVM set up for the LogStore passes its guard and commits through it") {
    val run = newRun("one-writer")
    val settings = Settings(
      writers = 1,
      commits = 3,
      rowsPerCommit = 4,
      extraJvmOptions = coordinatedJvmOptions,
      guard = true
    )

    val outcome = StressHarness.run(run, settings).loneElement

    withClue(logOf(outcome)) {
      outcome.exitCode shouldBe StressWriter.ExitCode.Ok
      outcome.journal.acked shouldBe Vector(1L, 2L, 3L)
    }
    // What the writer's own Delta resolved, and what the writer did before and around it.
    val probe = outcome.probe.getOrElse(fail("the writer left no probe"))
    probe.logStore shouldBe coordinated
    probe.logFileSystem shouldBe classOf[LocalFileSystem].getName
    probe.guarded shouldBe true
    probe.contention shouldBe 0L
    probe.faultWaits shouldBe WriterProbe.NoWaits

    // The registry has the orchestrator's creation and the writer's three commits, each one
    // complete, and the writer's attributed to the run its probe names.
    val entries = (0L to 3L).map { version =>
      registry.entry(keyOf(run), version).getOrElse(fail(s"version $version has no entry"))
    }
    entries.map(_.complete) shouldBe Seq.fill(4)(true)
    entries.map(_.runId) shouldBe None +: Seq.fill(3)(Some(UUID.fromString(probe.runId)))
    registry.registration(keyOf(run)).map(_.isTainted) shouldBe Some(false)

    // Nothing the verifier of the spikes objects to, through either reader; and the commits
    // have no sidecar that could go stale.
    val report = LogReader.verify(spark, run, Expected.of(settings), Seq(outcome))
    withClue(report.anomalies.map(_.detail).mkString("\n")) { report.isClean shouldBe true }
    report.summary.readers.map(_._2) shouldBe Seq(None, None)
    report.summary.checksumMismatches shouldBe empty
    logFiles(run).filter(_.endsWith(".json.crc")) shouldBe empty
  }

  test("two writer JVMs that reach for the same versions both finish, and count what they lost") {
    val run = newRun("two-writers")
    val commits = 4
    // Each writer waits between its look at the log and its INSERT, far longer than the two
    // need to get there: released together, they claim the same version again and again.
    val settings = Settings(
      writers = 2,
      commits = commits,
      rowsPerCommit = 4,
      extraJvmOptions = coordinatedJvmOptions :+ delay(FaultPoint.BeforeInsert, millis = 400),
      guard = true
    )

    val outcomes = StressHarness.run(run, settings)

    outcomes.foreach { outcome =>
      withClue(logOf(outcome)) {
        outcome.exitCode shouldBe StressWriter.ExitCode.Ok
        outcome.journal.acked shouldBe (1L to commits.toLong).toVector
      }
    }
    val probes = outcomes.map(_.probe.getOrElse(fail("a writer left no probe")))
    // The wait was in the path of every commit, and of every attempt that lost its version.
    val waited = probes.map(_.faultWaits.toMap.apply(FaultPoint.BeforeInsert.key))
    all(waited) should be >= commits.toLong
    // They did collide, and Delta settled each collision at the next version. A version lost
    // at the INSERT was waited for; one found taken in the log beforehand was not.
    val lost = probes.map(_.contention).sum
    lost should be >= 1L
    waited.sum should be <= 2L * commits + lost

    val report = LogReader.verify(spark, run, Expected.of(settings), outcomes)
    withClue(report.anomalies.map(_.detail).mkString("\n")) { report.isClean shouldBe true }
    report.summary.latestVersion shouldBe Some(2L * commits)
    report.summary.readers.map(_._2) shouldBe Seq(None, None)
    registry.latestEntry(keyOf(run)).map(entry => (entry.version, entry.complete)) shouldBe
      Some((2L * commits, true))
  }

  test("a writer JVM that would commit with Delta's default LogStore refuses to run") {
    val run = newRun("misconfigured")
    val filesBefore = logFiles(run)
    // Set up as in the RED spike: nothing names the LogStore. The writer is asked to check.
    val settings = Settings(writers = 1, commits = 3, rowsPerCommit = 4, guard = true)

    val failure = the[StressHarness.HarnessFailure] thrownBy StressHarness.run(run, settings)

    failure.getMessage should include(s"exited with code ${StressWriter.ExitCode.Refused}")
    failure.getMessage should include("spark.delta.logStore.file.impl is not set")
    // It stopped before it touched the table: no commit, and nothing acknowledged.
    logFiles(run) shouldBe filesBefore
    AckJournal.read(run.journal(0)).acked shouldBe empty
    registry.latestEntry(keyOf(run)).map(_.version) shouldBe Some(0L)
  }
}
