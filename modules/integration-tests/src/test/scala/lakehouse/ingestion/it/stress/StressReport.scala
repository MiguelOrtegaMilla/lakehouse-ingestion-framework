package lakehouse.ingestion.it.stress

import lakehouse.ingestion.it.stress.Verifier.Report

/**
 * A stress run as Markdown: what ran and with which settings, then what the verifier found.
 *
 * The settings come first because a result is only reproducible together with them. Every
 * setting that departs from a default says why (`Setting.why`), including those that only make
 * the run faster, so that a reader can judge whether they could have changed the outcome.
 */
object StressReport {

  final case class Setting(key: String, value: String, why: String = "")

  /** Details listed per anomaly class before the rest is only counted. */
  private val MaxDetailsPerKind = 20

  /** Versions of what produced the result, read from the running JVM. */
  def environment: Seq[Setting] = Seq(
    Setting("Java", sys.props("java.runtime.version")),
    Setting("Apache Spark", org.apache.spark.SPARK_VERSION),
    Setting("Delta Lake", io.delta.VERSION),
    Setting("OS", s"${sys.props("os.name")} ${sys.props("os.version")}")
  )

  def harness(settings: StressHarness.Settings): Seq[Setting] = Seq(
    Setting("writers (JVMs)", settings.writers.toString),
    Setting("commits per writer", settings.commits.toString),
    Setting("rows per commit", settings.rowsPerCommit.toString),
    Setting("writer heap", settings.writerHeap),
    Setting(
      "extra writer JVM options",
      if (settings.extraJvmOptions.isEmpty) "(none)"
      else settings.extraJvmOptions.mkString(" ")
    )
  )

  /**
   * @param run what was run and on what: `environment` and `harness`
   * @param spark the Spark settings that depart from Spark's and Delta's defaults, with reasons
   */
  def markdown(title: String, run: Seq[Setting], spark: Seq[Setting], report: Report): String = {
    val s = report.summary
    val verdict =
      if (report.isClean) "**No anomalies.**"
      else s"**${report.anomalies.size} anomalies.**"

    val runTable = table(Seq("", ""), run.map(x => Seq(x.key, x.value)))
    val sparkTable = table(
      Seq("Spark setting", "Value", "Why"),
      spark.map(x => Seq(code(x.key), code(x.value), x.why))
    )

    val summaryTable = table(
      Seq("Measure", "Value"),
      Seq(
        Seq("Latest version", s.latestVersion.fold("(no commit files)")(_.toString)),
        Seq("Writer commits in the log", s.writerCommitsInLog.toString),
        Seq("Commits acknowledged by writers", s.acknowledged.toString),
        Seq("Lost commits (acknowledged, not in the log)", s.lost.toString),
        Seq("Orphan data files (on disk, never added)", s.orphanDataFiles.size.toString),
        Seq(
          "Commit files with a stale `.crc` sidecar",
          if (s.checksumMismatches.isEmpty) "0"
          else s"${s.checksumMismatches.size} (versions ${s.checksumMismatches.mkString(", ")})"
        )
      )
    )

    val readerTable = table(
      Seq("Reader", "How it reads", "Result"),
      s.readers.map { case (reader, error) =>
        Seq(reader.label, reader.description, error.fold("read the table")(e => s"failed: $e"))
      }
    )

    val probeTable = table(
      Seq(
        "Writer",
        "LogStore",
        "Log FileSystem",
        "HDFSLogStore lock",
        "Commit FileContext",
        "Delayed renames"
      ),
      s.writers.map { w =>
        StressTable.writerId(w.writer) +: w.probe.fold(Seq.fill(5)("(no probe)")) { p =>
          Seq(
            code(p.logStore),
            code(p.logFileSystem),
            if (p.hdfsLogStoreLockTaken) "taken (per instance)" else "not taken",
            code(p.commitFileContext),
            p.delayedRenames.toString
          )
        }
      }
    )

    val byKind = report.anomalies.groupBy(_.kind).toSeq.sortBy(_._1)
    val anomalySection =
      if (byKind.isEmpty) "None."
      else
        table(Seq("Class", "Count"), byKind.map { case (k, as) => Seq(k, as.size.toString) }) +
          "\n" +
          byKind
            .map { case (kind, as) =>
              val shown = as.take(MaxDetailsPerKind).map(a => s"- ${a.detail}")
              val more =
                if (as.size > MaxDetailsPerKind) Seq(s"- … and ${as.size - MaxDetailsPerKind} more")
                else Nil
              (s"### $kind" +: (shown ++ more)).mkString("\n")
            }
            .mkString("\n\n")

    val writerTable = table(
      Seq("Writer", "Exit code", "Acknowledged", "Failure"),
      s.writers.map { w =>
        Seq(
          StressTable.writerId(w.writer),
          w.exitCode.toString,
          w.journal.acked.size.toString,
          w.journal.failure.fold("")(f => s"txnVersion=${f.txnVersion}: ${f.error}")
        )
      }
    )

    val otherCommits =
      if (s.otherCommits.isEmpty) "None."
      else s.otherCommits.map { case (v, op) => s"- version $v: $op" }.mkString("\n")

    // Sections are joined, not written as one stripMargin literal: the tables' lines start
    // with '|', which stripMargin would strip.
    Seq(
      s"# $title",
      "## Run",
      runTable,
      "## Spark settings",
      "Writers run with these settings. Any other setting keeps its value from " +
        "`LocalSparkSuite.baseConf` (no UI, UTC, loopback, Delta session extension), or else " +
        "its Spark or Delta default. `-Dspark.*` options in the run table override both.",
      sparkTable,
      "## Writer probes",
      "What each writer JVM resolved for the table, recorded by the writer itself: the code " +
        "path the result was measured on. `HDFSLogStore` takes its `synchronized (this)` lock " +
        "only on a `RawLocalFileSystem`, and even then the lock is per instance, never across " +
        "JVMs.",
      probeTable,
      "## Result",
      verdict,
      summaryTable,
      "## Readers",
      "The verifier reads the commit files as bytes, without Delta or Hadoop. Delta readers " +
        "then load the table each in its own way, and their anomalies are tagged with the " +
        "reader. A lost commit does not show in either: the reader just does not see it.",
      readerTable,
      "## Anomalies",
      anomalySection,
      "## Writers",
      writerTable,
      "## Commits without a writer txn",
      otherCommits
    ).map(_.stripSuffix("\n")).mkString("", "\n\n", "\n")
  }

  private def code(s: String): String = s"`$s`"

  private def table(header: Seq[String], rows: Seq[Seq[String]]): String = {
    def line(cells: Seq[String]) = cells.map(_.replace("|", "\\|")).mkString("| ", " | ", " |")
    (Seq(line(header), header.map(_ => "---").mkString("|", "|", "|")) ++ rows.map(line))
      .mkString("", "\n", "\n")
  }
}
