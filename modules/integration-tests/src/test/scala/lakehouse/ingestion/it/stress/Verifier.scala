package lakehouse.ingestion.it.stress

import lakehouse.ingestion.it.stress.LogModel._
import lakehouse.ingestion.it.stress.StressHarness.WriterOutcome

/**
 * Checks a finished stress run against what its writers believe they committed (ADR-0004 §7.2).
 *
 * The verifier does not trust the component under test. It reads the commit files directly
 * (`LogView`), takes the writers' word from their journals, and only then compares both with
 * what a Delta reader sees (`TableView`). A lost commit is visible only in the first comparison:
 * the Delta reader simply does not see it.
 *
 * Each `(writer, txnVersion)` is in one of three states (`CommitState`), and each state has its
 * rule:
 *
 *   - `Acked`: the writer's write returned. It must be in the log exactly once.
 *   - `Unknown`: the attempt in flight when the writer stopped (it failed, or the writer was
 *     killed). It may be absent, or present exactly once.
 *   - `NotAttempted`: it must be absent.
 */
object Verifier {

  final case class Expected(writers: Int, commits: Int, rowsPerCommit: Int)

  object Expected {
    def of(settings: StressHarness.Settings): Expected =
      Expected(settings.writers, settings.commits, settings.rowsPerCommit)
  }

  sealed trait CommitState
  object CommitState {
    case object Acked extends CommitState
    case object Unknown extends CommitState
    case object NotAttempted extends CommitState
  }

  /** A violated invariant. `kind` groups anomalies of one class in a report. */
  sealed trait Anomaly {
    def kind: String
    def detail: String
  }

  object Anomaly {

    /** Versions missing between 0 and the latest commit file. */
    final case class VersionGap(missing: Seq[Long]) extends Anomaly {
      val kind = "VersionGap"
      def detail = s"missing versions ${missing.mkString(", ")}"
    }

    final case class UnparseableCommit(version: Long, reason: String) extends Anomaly {
      val kind = "UnparseableCommit"
      def detail = s"version $version: $reason"
    }

    /** The writer saw its write succeed, and the log does not contain it. */
    final case class LostCommit(writer: String, txnVersion: Long) extends Anomaly {
      val kind = "LostCommit"
      def detail = s"$writer txnVersion=$txnVersion was acknowledged and is not in the log"
    }

    final case class DuplicateCommit(writer: String, txnVersion: Long, versions: Seq[Long])
        extends Anomaly {
      val kind = "DuplicateCommit"
      def detail = s"$writer txnVersion=$txnVersion is in versions ${versions.mkString(", ")}"
    }

    /** A commit in the log that no writer attempted, or that is not a single-txn append. */
    final case class UnexpectedCommit(version: Long, reason: String) extends Anomaly {
      val kind = "UnexpectedCommit"
      def detail = s"version $version: $reason"
    }

    /** The table's rows for one commit do not match what the log says was committed. */
    final case class RowMismatch(
        writer: String,
        txnVersion: Long,
        expectedRows: Long,
        actual: RowCount
    ) extends Anomaly {
      val kind = "RowMismatch"
      def detail =
        s"$writer txnVersion=$txnVersion: expected $expectedRows rows, found ${actual.rows} " +
          s"(${actual.distinctRowSeqs} distinct row_seq)"
    }

    /** The Delta reader's latest snapshot and the replayed commit files disagree on the files. */
    final case class SnapshotDivergence(onlyInReader: Set[String], onlyInLog: Set[String])
        extends Anomaly {
      val kind = "SnapshotDivergence"
      def detail =
        s"${onlyInReader.size} files only in the Delta reader " +
          s"(${onlyInReader.toSeq.sorted.mkString(", ")}), ${onlyInLog.size} only in the " +
          s"commit files (${onlyInLog.toSeq.sorted.mkString(", ")})"
    }

    /** A Delta reader could not load the table at all. */
    final case class UnreadableTable(error: String) extends Anomaly {
      val kind = "UnreadableTable"
      def detail = error
    }

    /** The journal contradicts the writer's exit code or itself: a harness bug. */
    final case class InconsistentJournal(writer: String, reason: String) extends Anomaly {
      val kind = "InconsistentJournal"
      def detail = s"$writer: $reason"
    }
  }

  import Anomaly._

  final case class Summary(
      latestVersion: Option[Long],
      writerCommitsInLog: Int,
      acknowledged: Int,
      lost: Int,
      otherCommits: Seq[(Long, String)],
      orphanDataFiles: Seq[String],
      writers: Seq[WriterOutcome]
  )

  final case class Report(anomalies: Seq[Anomaly], summary: Summary) {
    def isClean: Boolean = anomalies.isEmpty
    def count(kind: String): Int = anomalies.count(_.kind == kind)
  }

  def verify(
      expected: Expected,
      writers: Seq[WriterOutcome],
      log: LogView,
      table: Either[String, TableView],
      disk: DiskView
  ): Report = {
    val states = writers.map(w => StressTable.writerId(w.writer) -> commitStates(w, expected))
    val occurrences = txnOccurrences(log)

    val anomalies =
      writers.flatMap(journalAnomalies(_, expected)) ++
        logShapeAnomalies(log) ++
        commitAnomalies(expected, states.toMap, occurrences, log) ++
        (table match {
          case Left(error) => Seq(UnreadableTable(error))
          case Right(view) => tableAnomalies(expected, occurrences, log, view)
        })

    val acked = states.map(_._2.values.count(_ == CommitState.Acked)).sum
    Report(
      anomalies,
      Summary(
        latestVersion = log.versions.maxOption,
        writerCommitsInLog = occurrences.values.map(_.size).sum,
        acknowledged = acked,
        lost = anomalies.count(_.kind == "LostCommit"),
        otherCommits = log.commits
          .filter(_.txns.isEmpty)
          .sortBy(_.version)
          .map(c => c.version -> c.operation.getOrElse("(no commitInfo)")),
        orphanDataFiles = (disk.dataFiles -- log.everAdded).toSeq.sorted,
        writers = writers
      )
    )
  }

  /**
   * Classifies each `txnVersion` of one writer. Writers commit sequentially and stop at their
   * first failure, so at most one attempt is in flight when a writer stops: the failed one, or,
   * if the writer stopped without journaling a failure (killed, orphaned), the one after its
   * last acknowledgement. A writer that failed its setup never reached the barrier.
   */
  def commitStates(outcome: WriterOutcome, expected: Expected): Map[Long, CommitState] = {
    val acked = outcome.journal.acked.toSet
    val inFlight = outcome.journal.failure.map(_.txnVersion).orElse {
      outcome.exitCode match {
        // A clean exit leaves nothing in flight; a setup failure never started a commit.
        case StressWriter.ExitCode.Ok | StressWriter.ExitCode.SetupFailed => None
        case _ => Some(outcome.journal.acked.maxOption.getOrElse(0L) + 1)
      }
    }
    (1L to expected.commits.toLong).map { k =>
      k -> {
        if (acked(k)) CommitState.Acked
        else if (inFlight.contains(k)) CommitState.Unknown
        else CommitState.NotAttempted
      }
    }.toMap
  }

  private def journalAnomalies(w: WriterOutcome, expected: Expected): Seq[Anomaly] = {
    val id = StressTable.writerId(w.writer)
    val acked = w.journal.acked
    val contiguous = acked == (1L to acked.size.toLong).toVector
    val failureFollows = w.journal.failure.forall(_.txnVersion == acked.size + 1L)
    val matchesExit = w.exitCode match {
      case StressWriter.ExitCode.Ok =>
        acked.size == expected.commits && w.journal.failure.isEmpty
      case StressWriter.ExitCode.CommitFailed => w.journal.failure.nonEmpty
      case StressWriter.ExitCode.SetupFailed => acked.isEmpty && w.journal.failure.isEmpty
      case _ => w.journal.failure.isEmpty
    }
    Seq(
      contiguous -> s"acknowledgements are not 1..n: ${acked.mkString(",")}",
      failureFollows -> s"failure ${w.journal.failure} does not follow the last acknowledgement",
      matchesExit -> (s"exit code ${w.exitCode} contradicts the journal (${acked.size} acks, " +
        s"failure ${w.journal.failure})")
    ).collect { case (false, reason) => InconsistentJournal(id, reason) }
  }

  private def logShapeAnomalies(log: LogView): Seq[Anomaly] = {
    val present = log.versions.toSet
    val missing = log.versions.maxOption.toSeq.flatMap(max => (0L to max).filterNot(present))
    (if (missing.nonEmpty) Seq(VersionGap(missing)) else Nil) ++
      log.unreadable.sortBy(_.version).map(u => UnparseableCommit(u.version, u.detail))
  }

  /** For each `(appId, txnVersion)` in the log, the versions whose commit carries it. */
  private def txnOccurrences(log: LogView): Map[(String, Long), Seq[Long]] =
    log.commits
      .flatMap(c => c.txns.map(t => (t.appId, t.version) -> c.version))
      .groupMap(_._1)(_._2)
      .view
      .mapValues(_.sorted)
      .toMap

  private def commitAnomalies(
      expected: Expected,
      states: Map[String, Map[Long, CommitState]],
      occurrences: Map[(String, Long), Seq[Long]],
      log: LogView
  ): Seq[Anomaly] = {
    val perState = for {
      (writer, byVersion) <- states.toSeq.sortBy(_._1)
      (k, state) <- byVersion.toSeq.sortBy(_._1)
      versions = occurrences.getOrElse((writer, k), Nil)
      anomaly <- (state, versions.size) match {
        case (CommitState.Acked, 0) => Some(LostCommit(writer, k))
        case (CommitState.NotAttempted, n) if n > 0 =>
          Some(UnexpectedCommit(versions.head, s"$writer txnVersion=$k was never attempted"))
        case (_, n) if n > 1 => Some(DuplicateCommit(writer, k, versions))
        case _ => None
      }
    } yield anomaly

    val foreign = occurrences.toSeq.sortBy(_._2.head).collect {
      case ((appId, k), versions) if !states.get(appId).exists(_.contains(k)) =>
        UnexpectedCommit(versions.head, s"txn $appId/$k belongs to no expected writer")
    }

    val multiTxn = log.commits.sortBy(_.version).collect {
      case c if c.txns.size > 1 => UnexpectedCommit(c.version, s"${c.txns.size} txn actions")
    }

    perState ++ foreign ++ multiTxn
  }

  private def tableAnomalies(
      expected: Expected,
      occurrences: Map[(String, Long), Seq[Long]],
      log: LogView,
      table: TableView
  ): Seq[Anomaly] = {
    val logFiles = log.activeFiles
    val divergence =
      if (logFiles == table.activeFiles) Nil
      else Seq(SnapshotDivergence(table.activeFiles -- logFiles, logFiles -- table.activeFiles))

    val keys = (occurrences.keySet ++ table.rows.keySet).toSeq.sorted
    val rows = keys.flatMap { case key @ (writer, k) =>
      val commits = occurrences.getOrElse(key, Nil).size
      val expectedRows = commits.toLong * expected.rowsPerCommit
      val expectedDistinct = if (commits > 0) expected.rowsPerCommit.toLong else 0L
      val actual = table.rows.getOrElse(key, RowCount(0, 0))
      if (actual == RowCount(expectedRows, expectedDistinct)) None
      else Some(RowMismatch(writer, k, expectedRows, actual))
    }

    divergence ++ rows
  }
}
