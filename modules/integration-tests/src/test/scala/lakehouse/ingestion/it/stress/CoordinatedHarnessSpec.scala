package lakehouse.ingestion.it.stress

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.util.UUID

import scala.jdk.CollectionConverters._
import scala.util.Using

import io.delta.storage.HDFSLogStore
import lakehouse.ingestion.it.stress.StressHarness.{Settings, WriterOutcome}
import lakehouse.ingestion.it.stress.Verifier.Anomaly.{
  CommitWithoutEntry,
  SidecarOnCommit,
  TaintedTable,
  UnfinishedEntry,
  UnreadableTable
}
import lakehouse.ingestion.logstore.{
  FaultHooks,
  FaultPoint,
  LogStores,
  PostgresLogStore,
  SimulatedCrash
}
import org.apache.hadoop.fs.{LocalFileSystem, Path => HadoopPath}
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

  /** A commit that changes nothing, as Delta writes one. */
  private val EmptyCommit =
    """{"commitInfo":{"timestamp":1763200000000,"operation":"Manual Update","operationParameters":{}}}"""

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
    (probe.contention, probe.contentionAtInsert) shouldBe ((0L, 0L))
    probe.faultWaits shouldBe WriterProbe.NoWaits

    // The registry has the orchestrator's creation and the writer's three commits, each one
    // complete, and the writer's attributed to the run its probe names.
    val entries = (0L to 3L).map { version =>
      registry.entry(keyOf(run), version).getOrElse(fail(s"version $version has no entry"))
    }
    entries.map(_.complete) shouldBe Seq.fill(4)(true)
    entries.map(_.runId) shouldBe None +: Seq.fill(3)(Some(UUID.fromString(probe.runId)))
    registry.registration(keyOf(run)).map(_.isTainted) shouldBe Some(false)

    // Nothing the verifier of the spikes objects to, through either reader, in the log or in
    // the registry; and the commits have no sidecar that could go stale.
    val report = verify(run, settings, Seq(outcome))
    withClue(report.anomalies.map(_.detail).mkString("\n")) { report.isClean shouldBe true }
    report.summary.readers.map(_._2) shouldBe Seq(None, None)
    report.summary.checksumMismatches shouldBe empty
    report.summary.commitSidecars shouldBe empty
    report.summary.registry.map(r => (r.registeredVersion, r.taint, r.entries.size)) shouldBe
      Some((Some(-1L), None, 4))
    // Delta writes its version checksums through Hadoop's checksummed file system: they have
    // sidecars, which match.
    logFiles(run).filter(_.endsWith(".crc.crc")) should not be empty
    report.summary.staleSidecars shouldBe empty
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
    // at the INSERT was waited for; one found taken in the log beforehand was not. So every
    // wait is a commit that was made or a version that was lost at the INSERT, writer by writer.
    probes.map(_.contention).sum should be >= 1L
    all(probes.map(p => p.contention - p.contentionAtInsert)) should be >= 0L
    waited shouldBe probes.map(_.contentionAtInsert + commits)

    val report = verify(run, settings, outcomes)
    withClue(report.anomalies.map(_.detail).mkString("\n")) { report.isClean shouldBe true }
    // Two runs in the registry, one for each writer, and none of them shared.
    report.summary.registry.map(_.entries.flatMap(_.runId).distinct.size) shouldBe Some(2)
    report.summary.latestVersion shouldBe Some(2L * commits)
    report.summary.readers.map(_._2) shouldBe Seq(None, None)
    registry.latestEntry(keyOf(run)).map(entry => (entry.version, entry.complete)) shouldBe
      Some((2L * commits, true))
  }

  /** A finished run of one writer and two commits, for a test to damage. */
  private def finishedRun(label: String): (StressRun, Settings, Seq[WriterOutcome]) = {
    val run = newRun(label)
    val settings = Settings(
      writers = 1,
      commits = 2,
      rowsPerCommit = 4,
      extraJvmOptions = coordinatedJvmOptions,
      guard = true
    )
    val outcomes = StressHarness.run(run, settings)
    outcomes.map(_.exitCode) shouldBe Seq(StressWriter.ExitCode.Ok)
    (run, settings, outcomes)
  }

  private def commitFile(run: StressRun, version: Long): Path =
    logDir(run).resolve(f"$version%020d.json")

  test("the verifier finds what a writer without the LogStore leaves on a real run") {
    val (run, settings, outcomes) = finishedRun("foreign-writer")
    // Another process commits version 3 with Delta's default LogStore.
    val hadoopConf = spark.sessionState.newHadoopConf()
    new HDFSLogStore(hadoopConf).write(
      new HadoopPath(commitFile(run, 3).toUri),
      Iterator(EmptyCommit).asJava,
      false,
      hadoopConf
    )

    val report = verify(run, settings, outcomes)

    // The commit has no entry; the reader that met it tainted the table; and the default
    // LogStore left a sidecar next to it, as it does for every commit.
    report.anomalies.map(_.kind) should contain theSameElementsAs
      Seq("CommitWithoutEntry", "TaintedTable", "SidecarOnCommit")
    report.anomalies should contain allOf (CommitWithoutEntry(3), SidecarOnCommit(3))
    report.anomalies.collect { case TaintedTable(reason) => reason }.loneElement should
      include("version 3")
    // The table itself is read, with the foreign commit in it: nothing is lost.
    report.summary.readers.map(_._2) shouldBe Seq(None, None)
    report.summary.lost shouldBe 0
  }

  test("the verifier finds a commit that was taken and can no longer be published") {
    val (run, settings, outcomes) = finishedRun("unpublishable")
    // A writer takes version 3 and dies, and the temporary file of its commit is lost (F17).
    val dying = LogStores.over(registry, LogStores.crashAt(FaultPoint.AfterInsert))
    a[SimulatedCrash] should be thrownBy dying.write(
      new HadoopPath(commitFile(run, 3).toUri),
      Iterator(EmptyCommit).asJava,
      false,
      spark.sessionState.newHadoopConf()
    )
    val taken = registry.entry(keyOf(run), 3L).getOrElse(fail("version 3 was not taken"))
    Files.delete(logDir(run).resolve(taken.tempPath))

    val report = verify(run, settings, outcomes)

    // No reader gets the table, and the entry is still not complete after they tried.
    report.anomalies.collect { case UnreadableTable(reader, error) =>
      reader -> error.contains("UnpublishableCommitException")
    } shouldBe LogModel.Reader.all.map(_ -> true)
    report.anomalies.filterNot(_.kind == "UnreadableTable") shouldBe Seq(UnfinishedEntry(3))
    // The writer's own commits are all in the log: nothing that was acknowledged is lost.
    report.summary.lost shouldBe 0
  }

  test("a writer killed after it took a version leaves a commit that is in the table once") {
    val run = newRun("killed-after-insert")
    val commits = 4
    val settings = Settings(
      writers = 2,
      commits = commits,
      rowsPerCommit = 4,
      extraJvmOptions = coordinatedJvmOptions,
      guard = true,
      // The first writer is held for three seconds after each INSERT, where it is to die.
      alsoFor = Map(0 -> Seq(delay(FaultPoint.AfterInsert, millis = 3000)))
    )
    var killed = Option.empty[WriterKiller.Killed]

    val outcomes = Using.resource(killerOf(run)) { killer =>
      StressHarness.run(
        run,
        settings,
        chaos => {
          // kill -9, the moment the registry shows a version of its own that is not published.
          killed =
            Some(killer.killAt(chaos, writer = 0, FaultPoint.AfterInsert, afterAcknowledging = 1))
          chaos.restart(0)
        }
      )
    }

    val kill = killed.getOrElse(fail("no writer was killed"))
    // It was seen with a version taken and its commit not in the log, and it died there: well
    // inside the three seconds it was held for.
    val taken = kill.seen match {
      case Some(WriterKiller.InFlight(version, false)) => version
      case other => fail(s"the writer was killed on seeing $other")
    }
    kill.goneAfter.toMillis should be < 1500L
    // One version taken more than batches acknowledged: its second batch was a commit by
    // then, and nobody had been told.
    (kill.acknowledged, kill.entriesOfProcess, kill.batchInFlight) shouldBe ((1, 2, 2L))

    // The process started in its place repeated that batch, and both writers finished.
    outcomes.foreach { outcome =>
      withClue(logOf(outcome)) {
        outcome.exitCode shouldBe StressWriter.ExitCode.Ok
        outcome.journal.acked shouldBe (1L to commits.toLong).toVector
      }
    }
    outcomes.map(_.kills) shouldBe Seq(1, 0)
    outcomes.head.runs should have size 2

    // Every batch is in the table exactly once, the one in flight too, and nothing else is
    // wrong: with the registry, through both readers.
    val report = verify(run, settings, outcomes)
    withClue(report.anomalies.map(_.detail).mkString("\n")) { report.isClean shouldBe true }
    report.summary.readers.map(_._2) shouldBe Seq(None, None)
    // The commit of the batch in flight is the one the killed process made, at the version it
    // had taken: another process published it, and the writer that repeated the batch found
    // it there and wrote nothing.
    commitOfBatch(run, writer = 0, batch = 2) shouldBe
      Some(CoordinatedStressSuite.BatchCommit(taken, outcomes.head.runs.headOption))
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
