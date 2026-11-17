package lakehouse.ingestion.it.stress

import lakehouse.ingestion.it.stress.LogModel._
import lakehouse.ingestion.it.stress.StressHarness.WriterOutcome

/**
 * Checks a finished stress run against what its writers believe they committed (ADR-0004 §7.2).
 *
 * The verifier does not trust the component under test. It reads the commit files directly
 * (`LogView`), takes the writers' word from their journals, and only then compares both with
 * what Delta readers see (`TableView`, once per `Reader`). A lost commit is visible only in the
 * first comparison: a Delta reader simply does not see it. Each reader's anomalies name it, so
 * a report can show one reader failing on a table that another reads without complaint.
 *
 * Each `(writer, txnVersion)` is in one of three states (`CommitState`), and each state has its
 * rule:
 *
 *   - `Acked`: the writer's write returned. It must be in the log exactly once.
 *   - `Unknown`: the attempt in flight when the writer stopped (it failed, or the writer was
 *     killed). It may be absent, or present exactly once.
 *   - `NotAttempted`: it must be absent.
 *
 * A run on `PostgresLogStore` is also checked against its registry, read after the readers have
 * read the table (`RegistryView`). What the LogStore promises on top of Delta is then a rule
 * here: every commit has a complete entry and every entry its commit, the table is not tainted,
 * each writer's commits are attributed to its own run, no commit file has a `.crc` sidecar, and
 * no other file of the log has one that does not match it. A run without a registry, on
 * Delta's default LogStore, is held to none of these: there a sidecar is ordinary, and a stale
 * one is reported as an observation.
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
        reader: Reader,
        writer: String,
        txnVersion: Long,
        expectedRows: Long,
        actual: RowCount
    ) extends Anomaly {
      val kind = "RowMismatch"
      def detail =
        s"[${reader.label}] $writer txnVersion=$txnVersion: expected $expectedRows rows, " +
          s"found ${actual.rows} " +
          s"(${actual.distinctRowSeqs} distinct row_seq)"
    }

    /** The Delta reader's latest snapshot and the replayed commit files disagree on the files. */
    final case class SnapshotDivergence(
        reader: Reader,
        onlyInReader: Set[String],
        onlyInLog: Set[String]
    ) extends Anomaly {
      val kind = "SnapshotDivergence"
      def detail =
        s"[${reader.label}] ${onlyInReader.size} files only in the Delta reader " +
          s"(${onlyInReader.toSeq.sorted.mkString(", ")}), ${onlyInLog.size} only in the " +
          s"commit files (${onlyInLog.toSeq.sorted.mkString(", ")})"
    }

    /** A Delta reader could not load the table at all. */
    final case class UnreadableTable(reader: Reader, error: String) extends Anomaly {
      val kind = "UnreadableTable"
      def detail = s"[${reader.label}] $error"
    }

    /** The journal contradicts the writer's exit code or itself: a harness bug. */
    final case class InconsistentJournal(writer: String, reason: String) extends Anomaly {
      val kind = "InconsistentJournal"
      def detail = s"$writer: $reason"
    }

    /** The run was to be coordinated, and the registry does not know the table. */
    case object TableNotRegistered extends Anomaly {
      val kind = "TableNotRegistered"
      def detail = "the coordination registry has no registration for the table"
    }

    /** A commit made outside the LogStore was detected on the table: it refuses writes. */
    final case class TaintedTable(reason: String) extends Anomaly {
      val kind = "TaintedTable"
      def detail = s"the table is tainted: $reason"
    }

    /**
     * An entry that is still not complete after the table was read. A read publishes and
     * acknowledges a commit that its writer left taken, so this one could not be finished.
     */
    final case class UnfinishedEntry(version: Long) extends Anomaly {
      val kind = "UnfinishedEntry"
      def detail = s"the entry of version $version is not complete"
    }

    /** A commit file, above the version the table is registered at, that nobody took. */
    final case class CommitWithoutEntry(version: Long) extends Anomaly {
      val kind = "CommitWithoutEntry"
      def detail = s"version $version is in the log and has no coordination entry"
    }

    /** An entry recorded as published whose commit file is not in the log. */
    final case class EntryWithoutCommit(version: Long) extends Anomaly {
      val kind = "EntryWithoutCommit"
      def detail = s"version $version has a complete entry and no commit file"
    }

    /** A writer's commit whose entry is not attributed to that writer's own run. */
    final case class MisattributedCommit(version: Long, writer: String, reason: String)
        extends Anomaly {
      val kind = "MisattributedCommit"
      def detail = s"version $version, a commit of $writer: $reason"
    }

    /** A commit file with a `.crc` sidecar, on a table whose commits are published without. */
    final case class SidecarOnCommit(version: Long) extends Anomaly {
      val kind = "SidecarOnCommit"
      def detail = s"the commit file of version $version has a .crc sidecar"
    }

    /** A file of the log, other than a commit, whose `.crc` sidecar does not match it. */
    final case class StaleSidecar(file: String) extends Anomaly {
      val kind = "StaleSidecar"
      def detail = s"the .crc sidecar of _delta_log/$file does not match the file"
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
      checksumMismatches: Seq[Long],
      readers: Seq[(Reader, Option[String])],
      writers: Seq[WriterOutcome],
      commitSidecars: Seq[Long] = Nil,
      staleSidecars: Seq[String] = Nil,
      registry: Option[RegistryView] = None
  )

  final case class Report(anomalies: Seq[Anomaly], summary: Summary) {
    def isClean: Boolean = anomalies.isEmpty
    def count(kind: String): Int = anomalies.count(_.kind == kind)
  }

  /**
   * @param registry the registry of the table, for a run on `PostgresLogStore`; `None` for a
   *   run that was not coordinated, which is then held to none of the registry's rules
   */
  def verify(
      expected: Expected,
      writers: Seq[WriterOutcome],
      log: LogView,
      tables: Seq[(Reader, Either[String, TableView])],
      disk: DiskView,
      registry: Option[RegistryView] = None
  ): Report = {
    val states = writers.map(w => StressTable.writerId(w.writer) -> commitStates(w, expected))
    val occurrences = txnOccurrences(log)

    val anomalies =
      writers.flatMap(journalAnomalies(_, expected)) ++
        logShapeAnomalies(log) ++
        commitAnomalies(expected, states.toMap, occurrences, log) ++
        tables.flatMap {
          case (reader, Left(error)) => Seq(UnreadableTable(reader, error))
          case (reader, Right(view)) => tableAnomalies(reader, expected, occurrences, log, view)
        } ++
        registry.toSeq.flatMap(coordinationAnomalies(_, log, writers))

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
        checksumMismatches = log.checksumMismatches,
        readers = tables.map { case (reader, view) => reader -> view.left.toOption },
        writers = writers,
        commitSidecars = log.commitSidecars,
        staleSidecars = log.staleSidecars,
        registry = registry
      )
    )
  }

  /**
   * What a run on `PostgresLogStore` must leave, beyond what Delta itself promises: the
   * registry and the log say the same, and the log files are as the LogStore writes them.
   */
  private def coordinationAnomalies(
      registry: RegistryView,
      log: LogView,
      writers: Seq[WriterOutcome]
  ): Seq[Anomaly] = {
    val entries = registry.entries.map(e => e.version -> e).toMap
    val inLog = log.versions.toSet

    val registration = registry.registeredVersion match {
      case None => Seq(TableNotRegistered)
      case Some(registeredAt) =>
        // Commits up to the version a table was adopted at were made before it was coordinated.
        log.versions.sorted.collect {
          case version if version > registeredAt && !entries.contains(version) =>
            CommitWithoutEntry(version)
        }
    }
    val unfinished = registry.entries.collect {
      case e if !e.complete => UnfinishedEntry(e.version)
    }
    val withoutCommit = registry.entries.collect {
      case e if e.complete && !inLog(e.version) => EntryWithoutCommit(e.version)
    }

    registration ++
      registry.taint.map(TaintedTable(_)) ++
      unfinished ++
      withoutCommit ++
      attributionAnomalies(entries, log, writers) ++
      log.commitSidecars.sorted.map(SidecarOnCommit(_)) ++
      log.staleSidecars.sorted.map(StaleSidecar(_))
  }

  /**
   * Every commit of a writer is attributed to a run, no run has commits of two writers, and a
   * writer that reported its runs has all of its commits under one of them. Two processes that
   * commit to one table at the same time must never be confused in the registry.
   *
   * A writer has one run for each of its processes: one, unless it was killed and started
   * again. Each process writes its run down before it commits (`WriterOutcome.runs`), and the
   * one that exits reports its own once more with its probe.
   */
  private def attributionAnomalies(
      entries: Map[Long, RegistryEntry],
      log: LogView,
      writers: Seq[WriterOutcome]
  ): Seq[Anomaly] = {
    val attributed = for {
      commit <- log.commits.sortBy(_.version)
      txn <- commit.txns.headOption.toSeq
      entry <- entries.get(commit.version).toSeq
    } yield (commit.version, txn.appId, entry.runId)

    val writersOfRun = attributed
      .collect { case (_, writer, Some(run)) => run -> writer }
      .groupMap(_._1)(_._2)
      .view
      .mapValues(_.toSet)
      .toMap
    val reported = writers.flatMap { w =>
      val runs = (w.runs ++ w.probe.map(_.runId)).filter(_.nonEmpty).toSet
      Option.when(runs.nonEmpty)(StressTable.writerId(w.writer) -> runs)
    }.toMap

    attributed.flatMap {
      case (version, writer, None) =>
        Some(MisattributedCommit(version, writer, "its entry names no run"))
      case (version, writer, Some(run)) =>
        val others = writersOfRun(run) - writer
        if (others.nonEmpty)
          Some(
            MisattributedCommit(
              version,
              writer,
              s"run $run also has commits of ${others.toSeq.sorted.mkString(", ")}"
            )
          )
        else
          reported.get(writer).filterNot(_.contains(run)).map { own =>
            MisattributedCommit(
              version,
              writer,
              s"its entry names run $run, the writer ran as ${own.toSeq.sorted.mkString(", ")}"
            )
          }
    }
  }

  /**
   * Classifies each `txnVersion` of one writer. Writers commit sequentially and stop at the
   * first failure they do not repeat, so at most one batch is in flight when a writer stops: the
   * failed one, or,
   * if the writer stopped without journaling a failure (killed, orphaned), the one after its
   * last acknowledgement. A writer that failed its setup never reached the barrier.
   */
  def commitStates(outcome: WriterOutcome, expected: Expected): Map[Long, CommitState] = {
    val acked = outcome.journal.acked.toSet
    val inFlight = outcome.journal.failure.map(_.txnVersion).orElse {
      outcome.exitCode match {
        // A clean exit leaves nothing in flight; a writer that failed its setup, or that
        // refused to run, never started a commit.
        case StressWriter.ExitCode.Ok | StressWriter.ExitCode.SetupFailed |
            StressWriter.ExitCode.Refused =>
          None
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
    // A batch is repeated before it is acknowledged, or it is the one after the last.
    val beyond = w.journal.repeated.map(_.txnVersion).filter(_ > acked.size + 1L).distinct
    val matchesExit = w.exitCode match {
      case StressWriter.ExitCode.Ok =>
        acked.size == expected.commits && w.journal.failure.isEmpty
      case StressWriter.ExitCode.CommitFailed => w.journal.failure.nonEmpty
      case StressWriter.ExitCode.SetupFailed | StressWriter.ExitCode.Refused =>
        acked.isEmpty && w.journal.failure.isEmpty
      case _ => w.journal.failure.isEmpty
    }
    Seq(
      contiguous -> s"acknowledgements are not 1..n: ${acked.mkString(",")}",
      failureFollows -> s"failure ${w.journal.failure} does not follow the last acknowledgement",
      beyond.isEmpty -> (s"repeated attempts at ${beyond.mkString(",")}, beyond the batch after " +
        "the last acknowledgement"),
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
      reader: Reader,
      expected: Expected,
      occurrences: Map[(String, Long), Seq[Long]],
      log: LogView,
      table: TableView
  ): Seq[Anomaly] = {
    val logFiles = log.activeFiles
    val divergence =
      if (logFiles == table.activeFiles) Nil
      else
        Seq(
          SnapshotDivergence(reader, table.activeFiles -- logFiles, logFiles -- table.activeFiles)
        )

    val keys = (occurrences.keySet ++ table.rows.keySet).toSeq.sorted
    val rows = keys.flatMap { case key @ (writer, k) =>
      val commits = occurrences.getOrElse(key, Nil).size
      val expectedRows = commits.toLong * expected.rowsPerCommit
      val expectedDistinct = if (commits > 0) expected.rowsPerCommit.toLong else 0L
      val actual = table.rows.getOrElse(key, RowCount(0, 0))
      if (actual == RowCount(expectedRows, expectedDistinct)) None
      else Some(RowMismatch(reader, writer, k, expectedRows, actual))
    }

    divergence ++ rows
  }
}
