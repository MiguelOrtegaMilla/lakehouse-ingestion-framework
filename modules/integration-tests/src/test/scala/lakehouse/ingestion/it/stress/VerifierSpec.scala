package lakehouse.ingestion.it.stress

import java.nio.file.Paths

import lakehouse.ingestion.it.stress.AckJournal.{Contents, Failure}
import lakehouse.ingestion.it.stress.LogModel._
import lakehouse.ingestion.it.stress.StressHarness.WriterOutcome
import lakehouse.ingestion.it.stress.StressWriter.ExitCode
import lakehouse.ingestion.it.stress.Verifier.Anomaly._
import lakehouse.ingestion.it.stress.Verifier.{CommitState, Expected, Report}
import org.scalatest.LoneElement
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The verifier's rules on hand-built runs. Each anomaly class has a case that produces it and
 * nothing else, so a report naming a class can be trusted to mean that class.
 */
final class VerifierSpec extends AnyFunSuite with Matchers with LoneElement {

  private val expected = Expected(writers = 2, commits = 3, rowsPerCommit = 2)
  private val killedBySignal = 137

  private def id(w: Int) = StressTable.writerId(w)
  private def file(w: Int, k: Long) = s"part-$w-$k.snappy.parquet"

  private def writer(w: Int, exit: Int, acked: Seq[Long], failure: Option[Failure] = None) =
    WriterOutcome(w, exit, Contents(acked.toVector, failure), Paths.get(s"${id(w)}.log"))

  private def allOk = Seq(0, 1).map(w => writer(w, ExitCode.Ok, 1L to 3L))

  private val create = Commit(0, Nil, Nil, Nil, Some("WRITE"))

  private def append(version: Long, w: Int, k: Long) =
    Commit(version, Seq(Txn(id(w), k)), Seq(file(w, k)), Nil, Some("WRITE"))

  /** Writers 0 and 1 interleaved, as a correct run would leave them: versions 1..6. */
  private val interleaved: Seq[(Int, Long)] = for (k <- 1L to 3L; w <- Seq(0, 1)) yield (w, k)

  private def logOf(appends: Seq[(Int, Long)]): LogView = {
    val commits = create +: appends.zipWithIndex.map { case ((w, k), i) =>
      append(i + 1L, w, k)
    }
    LogView(commits.map(_.version), commits, Nil)
  }

  /** What a Delta reader would see if the table were consistent with `log`. */
  private def consistentTable(log: LogView): TableView =
    TableView(
      log.activeFiles,
      log.commits
        .flatMap(_.txns)
        .groupBy(t => (t.appId, t.version))
        .view
        .mapValues(ts => RowCount(ts.size * 2L, 2L))
        .toMap
    )

  private def run(
      writers: Seq[WriterOutcome],
      log: LogView,
      table: Option[Either[String, TableView]] = None,
      disk: Option[DiskView] = None
  ): Report =
    Verifier.verify(
      expected,
      writers,
      log,
      Seq(Reader.Default -> table.getOrElse(Right(consistentTable(log)))),
      disk.getOrElse(DiskView(log.everAdded))
    )

  test("a correct interleaved run is clean, and its summary counts what happened") {
    val report = run(allOk, logOf(interleaved))

    report.anomalies shouldBe empty
    report.summary.latestVersion shouldBe Some(6)
    report.summary.writerCommitsInLog shouldBe 6
    report.summary.acknowledged shouldBe 6
    report.summary.lost shouldBe 0
    report.summary.otherCommits shouldBe Seq(0L -> "WRITE")
    report.summary.orphanDataFiles shouldBe empty
  }

  test("an acknowledged commit missing from the log is a LostCommit, and its file an orphan") {
    // What the RED spike expects: writer-1's txnVersion 2 overwritten by another commit.
    val log = logOf(interleaved.filterNot(_ == (1 -> 2L)))
    val report = run(allOk, log, disk = Some(DiskView(log.everAdded + file(1, 2))))

    report.anomalies shouldBe Seq(LostCommit("writer-1", 2))
    report.summary.lost shouldBe 1
    report.summary.orphanDataFiles shouldBe Seq(file(1, 2))
  }

  test("the same txn in two versions is a DuplicateCommit") {
    val report = run(allOk, logOf(interleaved :+ (0 -> 2L)))

    report.anomalies shouldBe Seq(DuplicateCommit("writer-0", 2, Seq(3, 7)))
  }

  test("a commit nobody attempted, a foreign txn or a multi-txn commit is UnexpectedCommit") {
    // writer-1 failed its setup: it never reached the barrier, so it attempted nothing.
    val writers = Seq(writer(0, ExitCode.Ok, 1L to 3L), writer(1, ExitCode.SetupFailed, Nil))
    val foreign = Commit(7, Seq(Txn("writer-9", 1)), Seq("f.parquet"), Nil, Some("WRITE"))
    val multi = Commit(
      8,
      Seq(Txn(id(0), 1), Txn(id(0), 2)),
      Seq("g.parquet"),
      Nil,
      Some("WRITE")
    )
    val base = logOf(interleaved.filter(_._1 == 0) :+ (1 -> 1L))
    val log = base.copy(
      versions = base.versions ++ Seq(7L, 8L),
      commits = base.commits ++ Seq(foreign, multi)
    )
    val report = run(writers, log)

    report.anomalies.collect { case u: UnexpectedCommit => u.detail } shouldBe Seq(
      "version 4: writer-1 txnVersion=1 was never attempted",
      "version 7: txn writer-9/1 belongs to no expected writer",
      "version 8: 2 txn actions"
    )
  }

  test("the attempt in flight when a writer stopped may be absent or present once") {
    val failed = writer(1, ExitCode.CommitFailed, Seq(1, 2), Some(Failure(3, "e: boom")))
    val killed = writer(1, killedBySignal, Seq(1, 2))
    val withK3 = logOf(interleaved)
    val withoutK3 = logOf(interleaved.filterNot(_ == (1 -> 3L)))

    for (w1 <- Seq(failed, killed); log <- Seq(withK3, withoutK3))
      withClue(s"exit ${w1.exitCode}, ${log.commits.size} commits: ") {
        run(Seq(allOk.head, w1), log).anomalies shouldBe empty
      }

    Verifier.commitStates(killed, expected) shouldBe Map(
      1L -> CommitState.Acked,
      2L -> CommitState.Acked,
      3L -> CommitState.Unknown
    )
  }

  test("after the attempt in flight, nothing of a stopped writer may be in the log") {
    val killed = writer(1, killedBySignal, Seq(1))
    // writer-1 acknowledged 1 and was killed: 2 is in flight (Unknown), 3 was never attempted.
    val report = run(Seq(allOk.head, killed), logOf(interleaved))

    report.anomalies shouldBe Seq(
      UnexpectedCommit(6, "writer-1 txnVersion=3 was never attempted")
    )
  }

  test("missing versions are a VersionGap; unreadable commit files are UnparseableCommit") {
    val log = logOf(interleaved)
    val gapped = log.copy(
      versions = log.versions.filterNot(_ == 2) :+ 7L,
      commits = log.commits.filterNot(_.version == 2),
      unreadable = Seq(UnreadableCommit(7, "malformed line: {"))
    )
    val report = run(allOk, gapped)

    report.anomalies should contain allOf (
      VersionGap(Seq(2)),
      UnparseableCommit(7, "malformed line: {")
    )
    // Version 2 held writer-1's txnVersion 1, now missing from the readable log.
    report.anomalies should contain(LostCommit("writer-1", 1))
  }

  test("rows that differ from the log's commits are a RowMismatch") {
    val log = logOf(interleaved)
    val table = consistentTable(log)
    val tampered = table.copy(rows =
      table.rows +
        (("writer-0", 1L) -> RowCount(1, 1)) +
        (("writer-0", 2L) -> RowCount(2, 1)) +
        (("writer-7", 1L) -> RowCount(2, 2))
    )
    val report = run(allOk, log, table = Some(Right(tampered)))

    report.anomalies shouldBe Seq(
      RowMismatch(Reader.Default, "writer-0", 1, 2, RowCount(1, 1)),
      RowMismatch(Reader.Default, "writer-0", 2, 2, RowCount(2, 1)),
      RowMismatch(Reader.Default, "writer-7", 1, 0, RowCount(2, 2))
    )
  }

  test("a Delta reader that sees other files than the commit files is a SnapshotDivergence") {
    val log = logOf(interleaved)
    val table = consistentTable(log)
    val diverged = table.copy(activeFiles = table.activeFiles - file(0, 1) + "stale.parquet")
    val report = run(allOk, log, table = Some(Right(diverged)))

    report.anomalies shouldBe Seq(
      SnapshotDivergence(Reader.Default, Set("stale.parquet"), Set(file(0, 1)))
    )
  }

  test("a table the Delta reader cannot load is UnreadableTable; the log checks still run") {
    val log = logOf(interleaved.filterNot(_ == (0 -> 3L)))
    val report = run(allOk, log, table = Some(Left("DeltaIllegalStateException: gap")))

    report.anomalies shouldBe Seq(
      LostCommit("writer-0", 3),
      UnreadableTable(Reader.Default, "DeltaIllegalStateException: gap")
    )
  }

  test("each reader is checked on its own, and its anomalies name it") {
    val log = logOf(interleaved).copy(checksumMismatches = Seq(2L))
    val report = Verifier.verify(
      expected,
      allOk,
      log,
      Seq(
        Reader.Default -> Left("ChecksumException: Checksum error"),
        Reader.SidecarFree -> Right(consistentTable(log))
      ),
      DiskView(log.everAdded)
    )

    report.anomalies shouldBe Seq(
      UnreadableTable(Reader.Default, "ChecksumException: Checksum error")
    )
    report.anomalies.loneElement.detail should startWith("[default reader] ")
    report.summary.readers shouldBe Seq(
      Reader.Default -> Some("ChecksumException: Checksum error"),
      Reader.SidecarFree -> None
    )
    // A stale sidecar is an observation: it adds no anomaly of its own.
    report.summary.checksumMismatches shouldBe Seq(2L)
  }

  test("a writer that refused to run attempted nothing") {
    val writers = Seq(writer(0, ExitCode.Ok, 1L to 3L), writer(1, ExitCode.Refused, Nil))
    val onlyWriter0 = (1L to 3L).map(0 -> _)

    run(writers, logOf(onlyWriter0)).anomalies shouldBe empty
    // Not even its first commit was in flight: one in the log is nobody's.
    run(writers, logOf(onlyWriter0 :+ (1 -> 1L))).anomalies shouldBe Seq(
      UnexpectedCommit(4, "writer-1 txnVersion=1 was never attempted")
    )
  }

  // ---------------------------------------------------------------------------------------------
  // A run on PostgresLogStore: the registry, and the sidecars of the log
  // ---------------------------------------------------------------------------------------------

  private def runOf(w: Int) = s"00000000-0000-0000-0000-00000000000$w"

  /** A writer that reported, as it exited, the run it attributed its commits to. */
  private def reporting(outcome: WriterOutcome): WriterOutcome =
    outcome.copy(probe =
      Some(WriterProbe("LogStore", "FileSystem", "FileContext", 0, runId = runOf(outcome.writer)))
    )

  /**
   * The registry a correct run leaves: the table created through the LogStore, and an entry
   * for every commit, complete, a writer's under the writer's run.
   */
  private def registryOf(log: LogView): RegistryView =
    RegistryView(
      registeredVersion = Some(-1L),
      taint = None,
      entries = log.commits.sortBy(_.version).map { c =>
        val writerRun = c.txns.headOption.map(txn => runOf(txn.appId.last.asDigit))
        RegistryEntry(c.version, complete = true, writerRun)
      }
    )

  private def coordinated(
      log: LogView,
      registry: RegistryView,
      writers: Seq[WriterOutcome] = allOk.map(reporting)
  ): Report =
    Verifier.verify(
      expected,
      writers,
      log,
      Seq(Reader.Default -> Right(consistentTable(log))),
      DiskView(log.everAdded),
      Some(registry)
    )

  private def changed(registry: RegistryView)(change: RegistryEntry => RegistryEntry) =
    registry.copy(entries = registry.entries.map(change))

  test("a correct run on the LogStore is clean against its registry too") {
    val log = logOf(interleaved)
    val report = coordinated(log, registryOf(log))

    report.anomalies shouldBe empty
    report.summary.registry.map(_.entries.size) shouldBe Some(7)
  }

  test("a table that the registry does not know, or that is tainted, is each its own anomaly") {
    val log = logOf(interleaved)
    val registry = registryOf(log)

    coordinated(log, RegistryView(None, None, Nil)).anomalies shouldBe Seq(TableNotRegistered)
    coordinated(log, registry.copy(taint = Some("version 9 has no entry"))).anomalies shouldBe
      Seq(TaintedTable("version 9 has no entry"))
  }

  test("an entry that is not complete is UnfinishedEntry, with or without its commit file") {
    val log = logOf(interleaved)
    val unfinished = changed(registryOf(log)) { e =>
      if (e.version == 4) e.copy(complete = false) else e
    }
    // Version 7 was taken by a writer that died, and no reader could publish it.
    val alsoTaken =
      unfinished.copy(entries = unfinished.entries :+ RegistryEntry(7, complete = false, None))

    coordinated(log, alsoTaken).anomalies shouldBe Seq(UnfinishedEntry(4), UnfinishedEntry(7))
  }

  test("a complete entry whose commit file is not in the log is EntryWithoutCommit") {
    val log = logOf(interleaved)
    val registry = registryOf(log)
    val recorded =
      registry.copy(entries = registry.entries :+ RegistryEntry(7, complete = true, None))

    coordinated(log, recorded).anomalies shouldBe Seq(EntryWithoutCommit(7))
  }

  test("a commit that nobody took is CommitWithoutEntry, unless the table was adopted above it") {
    val log = logOf(interleaved)
    val registry = registryOf(log)
    val without = registry.copy(entries = registry.entries.filterNot(e => Set(2L, 5L)(e.version)))

    coordinated(log, without).anomalies shouldBe Seq(CommitWithoutEntry(2), CommitWithoutEntry(5))
    // A table adopted at version 2: its commits up to there were made before it was coordinated.
    val adopted = registry.copy(
      registeredVersion = Some(2L),
      entries = registry.entries.filter(_.version > 2)
    )
    coordinated(log, adopted).anomalies shouldBe empty
  }

  test("a writer's commit under no run, or under a run with another writer's, is misattributed") {
    val log = logOf(interleaved)
    val registry = registryOf(log)

    val noRun = changed(registry)(e => if (e.version == 1) e.copy(runId = None) else e)
    coordinated(log, noRun).anomalies shouldBe Seq(
      MisattributedCommit(1, "writer-0", "its entry names no run")
    )

    // Version 2 is a commit of writer-1, recorded under the run of writer-0: neither writer's
    // commits under that run can be told from the other's.
    val shared = changed(registry)(e => if (e.version == 2) e.copy(runId = Some(runOf(0))) else e)
    val confused = coordinated(log, shared).anomalies.collect { case m: MisattributedCommit =>
      m.version -> m.writer
    }
    confused shouldBe Seq(1L -> "writer-0", 2L -> "writer-1", 3L -> "writer-0", 5L -> "writer-0")
  }

  test("a writer's commits under another run than the one it reported are misattributed") {
    val log = logOf(interleaved)
    val elsewhere = changed(registryOf(log)) { e =>
      if (e.runId.contains(runOf(1))) e.copy(runId = Some("a-run-nobody-reported")) else e
    }

    coordinated(log, elsewhere).anomalies shouldBe Seq(2L, 4L, 6L).map { version =>
      MisattributedCommit(
        version,
        "writer-1",
        s"its entry names run a-run-nobody-reported, the writer ran as ${runOf(1)}"
      )
    }
    // A writer that was killed reported nothing: its commits only need a run of their own.
    coordinated(log, elsewhere, writers = allOk).anomalies shouldBe empty
  }

  test("on the LogStore no commit file has a sidecar, and no other file of the log a stale one") {
    val log = logOf(interleaved).copy(
      commitSidecars = Seq(3L),
      staleSidecars = Seq("_last_checkpoint", "00000000000000000004.crc")
    )

    coordinated(log, registryOf(log)).anomalies shouldBe Seq(
      SidecarOnCommit(3),
      StaleSidecar("00000000000000000004.crc"),
      StaleSidecar("_last_checkpoint")
    )

    // A run that is not coordinated is not held to it: Delta's default LogStore leaves a
    // sidecar next to every commit. They are counted, and are no anomaly.
    val uncoordinated = run(allOk, log)
    uncoordinated.anomalies shouldBe empty
    uncoordinated.summary.commitSidecars shouldBe Seq(3L)
    uncoordinated.summary.staleSidecars shouldBe Seq("_last_checkpoint", "00000000000000000004.crc")
    uncoordinated.summary.registry shouldBe None
  }

  test("a journal that contradicts itself or its exit code is InconsistentJournal") {
    val writers = Seq(
      writer(0, ExitCode.Ok, Seq(1, 2)),
      writer(1, ExitCode.CommitFailed, Seq(1, 3))
    )
    val report = run(writers, logOf(interleaved))

    report.anomalies.collect { case j: InconsistentJournal => j.detail } shouldBe Seq(
      "writer-0: exit code 0 contradicts the journal (2 acks, failure None)",
      "writer-1: acknowledgements are not 1..n: 1,3",
      "writer-1: exit code 1 contradicts the journal (2 acks, failure None)"
    )
  }
}
