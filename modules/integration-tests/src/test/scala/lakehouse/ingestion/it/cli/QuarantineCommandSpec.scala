package lakehouse.ingestion.it.cli

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._
import scala.util.Using

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import lakehouse.ingestion.cli.{Main, QuarantineCommand}
import lakehouse.ingestion.core.config.Source
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.{CapturedLogs, CommandProcess, DeploymentFiles}
import org.apache.logging.log4j.Level
import org.apache.spark.sql.SparkSession
import org.scalatest.LoneElement
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The command `quarantine` from end to end, as an operator runs it (P8.10b): on the quarantines
 * that the command `run` writes, with the configuration of a deployment, the real control plane
 * and LogStore, and its exit codes (discovery §14.1). `QuarantineReaderSpec` has what is counted
 * and shown, and `QuarantineLinesSpec` how it is written.
 *
 * Three things are the command's own, and are held here:
 *
 *  - '''a record goes to the standard output and nowhere else''': not to the error output, and
 *    not to a log, at any level, in this JVM and in a process of its own;
 *  - '''it reads, and writes nothing''': every row of the control plane and of the coordination
 *    schema, and every file of the lake, is as it was;
 *  - '''what it writes of a record is one line that a terminal acts on no character of'''.
 */
final class QuarantineCommandSpec
    extends AnyFunSuite
    with Matchers
    with LoneElement
    with RunCommandQueries {

  private lazy val files = new DeploymentFiles(pg, Files.createTempDirectory("quarantine-command-"))
  import files.{arguments, drop, validProfile}

  override def afterAll(): Unit =
    try files.close()
    finally super.afterAll()

  private def run(of: Source): Ran = command(Map.empty)(arguments(of): _*)

  private def asking(of: String, more: Seq[String]): Seq[String] =
    Seq(
      "quarantine",
      "--profile",
      validProfile.toString,
      "--sources",
      files.sources.toString,
      "--source",
      of
    ) ++ more

  private def quarantine(of: Source, more: String*): Ran = {
    val ran = command(Map.empty)(asking(of.id.value, more): _*)
    // The command leaves the JVM as it found it.
    SparkSession.getDefaultSession shouldBe empty
    ran
  }

  /** `source_file` of a file of a source, as its records have it. */
  private def fileOf(of: Source, name: String): String =
    s"${files.directoryOf(of.id).getFileName}/$name"

  private def runIds(of: Source): Seq[String] = asSuperuser { c =>
    query(
      c,
      "SELECT run_id::text FROM control.ingestion_run WHERE source_id = ? ORDER BY started_at",
      of.id.value
    ).map(_.head)
  }

  /** The runs of a source that has had two, in the order they started. */
  private def twoRuns(of: Source): (String, String) = {
    val ids = runIds(of)
    ids should have size 2
    (ids(0), ids(1))
  }

  // What the records hold, which is to be on the standard output alone. Each has a name of
  // its own, and no other text of a test or of the application has it.
  private val Alpha = "PERSON-ALPHA"
  private val Beta = "PERSON-BETA"
  private val Gamma = "PERSON-GAMMA"
  private val Held = Seq(Alpha, Beta, Gamma)

  // A record with what a terminal acts on: ESC [ 31 m, the same started by U+009B, U+007F, and
  // a separator of lines that is no end of a line of its file.
  private val Acted = s"$Alpha Pe\u00f1a \u001b[31mred\u009b0m \u007f one\u2028two"
  private val Trailing = s"""{"id":2} $Beta"""
  private val Quoted = s"""$Gamma "quoted" C:\\temp"""

  /** A source of JSON with three records set apart, in two batches. */
  private def rejecting(): Source = {
    val returns = files.fileSource(newSourceId(), rejected = Some("0.9"), format = "json")
    drop(returns, "a.json", Seq("""{"id":1}""", Acted, Trailing).mkString("\n") + "\n")
    withClue(run(returns).err)(batchesOf(returns) shouldBe Seq("1" -> "COMMITTED"))
    drop(returns, "b.json", Seq("""{"id":3}""", Quoted).mkString("\n") + "\n")
    withClue(run(returns).err)(batchesOf(returns).map(_._2) shouldBe Seq("COMMITTED", "COMMITTED"))
    returns
  }

  private def read(line: String): JsonNode = new ObjectMapper().readTree(line)

  /**
   * A line of a log in the pattern of Spark's default (`%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m`),
   * as its level and the last part of the name of its logger.
   */
  private val Logged = """\d\d/\d\d/\d\d \d\d:\d\d:\d\d ([A-Z]+) ([^ :]+): .*""".r

  /** Whether a terminal, or a reader of lines, acts on a character of a text. */
  private def actedOn(text: String): Boolean =
    text.codePoints.toArray.exists { codePoint =>
      codePoint != '\n' && Seq[Int](
        Character.CONTROL,
        Character.FORMAT,
        Character.LINE_SEPARATOR,
        Character.PARAGRAPH_SEPARATOR
      ).contains(Character.getType(codePoint))
    }

  // ---------------------------------------------------------------------------------------------
  // The counts
  // ---------------------------------------------------------------------------------------------

  test("the records of a source are counted in a table, with no record in it, and it ends in 0") {
    val returns = rejecting()
    val (first, second) = twoRuns(returns)

    val ran = quarantine(returns)

    withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
    said(ran) shouldBe empty
    ran.lines.head shouldBe
      s"The quarantine of ${returns.id.value} has 3 records of what was asked, in 3 groups by " +
      "run, file and type:"
    ran.lines(2).split("\\s+").toSeq shouldBe
      Seq("RUN", "BATCH", "STATUS", "FILE", "TYPE", "RECORDS", "LATEST")
    // The latest first: the batch 2, and then the groups of the batch 1 by their type.
    ran.lines.slice(3, 6).map(_.split("\\s+").toSeq.take(6)) shouldBe Seq(
      Seq(second, "2", "COMMITTED", fileOf(returns, "b.json"), "unparsed", "1"),
      Seq(first, "1", "COMMITTED", fileOf(returns, "a.json"), "trailing_text", "1"),
      Seq(first, "1", "COMMITTED", fileOf(returns, "a.json"), "unparsed", "1")
    )
    ran.lines.last shouldBe "By type: trailing_text 1, unparsed 2."
    // The counts have no record: nothing of what one holds.
    Held.foreach(held => (ran.out + ran.err) should not include held)
  }

  test("what is asked for is of a run, of a batch, of a file or of a type") {
    val returns = rejecting()
    val (first, second) = twoRuns(returns)
    def counted(more: String*): (String, Seq[Seq[String]]) = {
      val ran = quarantine(returns, more: _*)
      withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
      ran.lines.head -> ran.lines.drop(3).takeWhile(_.nonEmpty).map(_.split("\\s+").toSeq.take(6))
    }
    val ofSecond = Seq(second, "2", "COMMITTED", fileOf(returns, "b.json"), "unparsed", "1")
    val trailing = Seq(first, "1", "COMMITTED", fileOf(returns, "a.json"), "trailing_text", "1")
    val unparsed = Seq(first, "1", "COMMITTED", fileOf(returns, "a.json"), "unparsed", "1")

    counted("--run", second)._2 shouldBe Seq(ofSecond)
    counted("--batch", "1")._2 shouldBe Seq(trailing, unparsed)
    counted("--file", fileOf(returns, "a.json"))._2 shouldBe Seq(trailing, unparsed)
    counted("--type", "unparsed")._2 shouldBe Seq(ofSecond, unparsed)
    counted("--batch", "1", "--type", "trailing_text", "--run", first)._2 shouldBe Seq(trailing)
    // Nothing of what was asked is no failure.
    counted("--type", "duplicate_field")._1 shouldBe
      s"The quarantine of ${returns.id.value} has no record of what was asked."
    counted("--run", first, "--batch", "2")._1 should include("has no record of what was asked")
  }

  test("a source that never had a record set apart has no quarantine, which is said, in 0") {
    val clean = files.fileSource(newSourceId())
    drop(clean, "a.csv", "id\n1\n")
    run(clean).exit shouldBe Main.Exit.Ok
    val none =
      s"The source ${clean.id.value} has no quarantine: no record of it was ever set apart."

    val counts = quarantine(clean)
    counts.exit shouldBe Main.Exit.Ok
    counts.lines shouldBe Seq(none)

    // With the records asked for, the standard output is theirs alone: it has nothing.
    val records = quarantine(clean, "--records", "5")
    records.exit shouldBe Main.Exit.Ok
    records.out shouldBe empty
    said(records) shouldBe Seq(none)
  }

  test("a batch stopped by its threshold is shown as planned, and one given up is left out") {
    val strict = files.fileSource(newSourceId())
    drop(strict, "a.csv", "id,amount\n1,2\n3\n")
    run(strict).exit shouldBe Main.Exit.DataThreshold

    val stopped = quarantine(strict)
    stopped.exit shouldBe Main.Exit.Ok
    stopped.lines(3).split("\\s+").toSeq.slice(1, 6) shouldBe
      Seq("1", "PLANNED", fileOf(strict, "a.csv"), "unparsed", "1")

    // Its file is written again, and the run after gives the batch up: its record is left out.
    drop(strict, "a.csv", "id,amount\n1,2\n3\n4\n")
    run(strict).exit shouldBe Main.Exit.Transient
    batchesOf(strict) shouldBe Seq("1" -> "ABANDONED")
    val givenUp = quarantine(strict)
    givenUp.exit shouldBe Main.Exit.Ok
    givenUp.lines shouldBe Seq(
      s"The quarantine of ${strict.id.value} has no record of what was asked.",
      "Left out: 1 record of 1 run that took a batch that was given up. They count for " +
        "nothing: the batch after sets the same records apart again."
    )
    val shown = quarantine(strict, "--records", "10")
    shown.exit shouldBe Main.Exit.Ok
    shown.out shouldBe empty
  }

  // ---------------------------------------------------------------------------------------------
  // What is asked that is not there
  // ---------------------------------------------------------------------------------------------

  test("a run or a batch that the source does not have, or a source that is not, ends in 4") {
    val returns = rejecting()
    val other = files.fileSource(newSourceId())
    drop(other, "a.csv", "id\n1\n")
    run(other).exit shouldBe Main.Exit.Ok
    val ofAnother = runIds(other).loneElement
    def refused(more: String*): String = {
      val ran = quarantine(returns, more: _*)
      ran.exit shouldBe Main.Exit.Permanent
      ran.out shouldBe empty
      said(ran).loneElement
    }

    refused("--batch", "9") shouldBe
      s"error: there is no batch ${returns.id.value}/bronze#9. Nothing was read."
    refused("--batch", "1", "--backfill", "history") should
      (include("error: there is no batch") and include("history") and include("#1"))
    refused("--run", ofAnother, "--records", "5") shouldBe
      s"error: the run $ofAnother is of ${other.id.value}/bronze, and not of " +
      s"${returns.id.value}/bronze. Nothing was read."
    refused("--run", "0193a5f0-7c2e-7b1a-9f3e-2d4c6a8b0e1f") shouldBe
      "error: there is no run 0193a5f0-7c2e-7b1a-9f3e-2d4c6a8b0e1f. Nothing was read."

    val unknown = command(Map.empty)(asking("no.such", Nil): _*)
    unknown.exit shouldBe Main.Exit.Permanent
    unknown.out shouldBe empty
    unknown.err should include("there is no source no.such")
    unknown.err should include("nothing was read")
  }

  // ---------------------------------------------------------------------------------------------
  // The records, as they were received
  // ---------------------------------------------------------------------------------------------

  test("--records writes records as lines of JSON to the standard output, and nothing else") {
    val returns = rejecting()
    val (first, second) = twoRuns(returns)

    val ran = quarantine(returns, "--records", "10")

    withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
    // Each line of the standard output is a record, and reads back as it was received: the
    // latest first, and those of a batch by their type and their text.
    val records = ran.lines.map(read)
    records.map(_.get("raw_record").asText) shouldBe Seq(Quoted, Trailing, Acted)
    records.map(_.get("run_id").asText) shouldBe Seq(second, first, first)
    records.map(_.get("batch_seq").asLong) shouldBe Seq(2L, 1L, 1L)
    records.map(_.get("error_type").asText) shouldBe Seq("unparsed", "trailing_text", "unparsed")
    records.map(_.get("source_file").asText) shouldBe
      Seq("b.json", "a.json", "a.json").map(fileOf(returns, _))
    records.foreach { record =>
      record.get("batch_status").asText shouldBe "COMMITTED"
      record.get("backfill").isNull shouldBe true
      record.get("truncated").asBoolean shouldBe false
    }
    records.last.get("raw_record_chars").asLong shouldBe Acted.codePointCount(0, Acted.length)
    records(1).get("error_detail").asText should not be empty

    // No character of a line is one that a terminal acts on: each is written as its escape.
    actedOn(ran.out) shouldBe false
    val acted = ran.lines.last
    acted should include("\\u001b[31mred\\u009b0m \\u007f one\\u2028two")
    acted should include(s"$Alpha Pe\u00f1a")

    // The warning, and what is said of what was written, are on the error output, which has
    // no record.
    said(ran) shouldBe Seq(
      s"3 records of the quarantine of ${returns.id.value} were written, the latest first."
    )
    ran.errors should contain(QuarantineCommand.PersonalData)
    Held.foreach(held => ran.err should not include held)

    // No more than are asked for, of what is asked for.
    val one = quarantine(returns, "--records", "1", "--batch", "1")
    one.lines.map(read(_).get("raw_record").asText) shouldBe Seq(Trailing)
    said(one).loneElement should (
      startWith("1 record of the quarantine") and include("It may have more")
    )
  }

  test("no record reaches a log, at any level, nor the error output: the standard output alone") {
    val returns = rejecting()

    // Every logger of the JVM, the application's and Spark's, at their lowest level.
    val (ran, heard) = CapturedLogs.everything(Level.ALL)(line => Held.exists(line.contains)) {
      quarantine(returns, "--records", "10")
    }

    withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
    Held.foreach(held => ran.out should include(held))
    Held.foreach(held => ran.err should not include held)
    // Of a line that has a record, what is around it: a line of a log may be very long.
    def around(message: String): String = {
      val at = Held.map(message.indexOf).filter(_ >= 0).min
      message.slice(at - 400, at + 80)
    }
    withClue(heard.kept.map(line => s"${line.level} …${around(line.message)}…").mkString("\n")) {
      heard.kept shouldBe empty
    }
    // The loggers were heard, as low as they log: those of Spark that read the table, among
    // thousands of lines.
    heard.events should be > 1000L
    heard.loggers.exists(_.startsWith("org.apache.spark.sql.execution")) shouldBe true
    heard.loggers.exists(_.startsWith("org.apache.spark.sql.delta")) shouldBe true
  }

  test("in a process of its own, the records are on its standard output, and no log has one") {
    val returns = rejecting()
    // As a deployment that logs everything that there is to log: every logger at its lowest
    // level, to the error output, which is where a process logs unless it is told otherwise.
    val logging = Files.write(
      Files.createTempFile("quarantine-logging-", ".properties"),
      """rootLogger.level = debug
        |rootLogger.appenderRef.console.ref = console
        |appender.console.type = Console
        |appender.console.name = console
        |appender.console.target = SYSTEM_ERR
        |appender.console.layout.type = PatternLayout
        |appender.console.layout.pattern = %d{HH:mm:ss.SSS} %-5level %c - %msg%n%throwable
        |""".stripMargin.getBytes(UTF_8)
    )
    val (out, err) = (files.newLog(), files.newLog())

    val ended = CommandProcess
      .start(
        asking(returns.id.value, Seq("--records", "10")),
        out,
        jvm = Seq(s"-Dlog4j2.configurationFile=${logging.toUri}"),
        errors = Some(err)
      )
      .await()

    withClue(Files.readAllLines(err, UTF_8).asScala.takeRight(30).mkString("\n")) {
      ended.exit shouldBe Main.Exit.Ok
    }
    // Its standard output is the records, a line each, and nothing else.
    val written = Files.readAllLines(out, UTF_8).asScala.toSeq
    written.map(read(_).get("raw_record").asText) shouldBe Seq(Quoted, Trailing, Acted)
    // Its error output has the warning, what was logged, and nothing of a record. It is read
    // a line at a time: at that level it is long.
    var (lines, debug, warned, ofSpark) = (0L, 0L, false, false)
    val withARecord = Vector.newBuilder[String]
    Using.resource(Files.lines(err, UTF_8)) { logged =>
      logged.forEach { line =>
        lines += 1
        if (line.contains(" DEBUG ")) debug += 1
        if (line == QuarantineCommand.PersonalData) warned = true
        if (line.contains("org.apache.spark.sql.execution")) ofSpark = true
        if (Held.exists(line.contains)) withARecord += s"line $lines: ${line.take(160)}"
      }
    }
    withARecord.result() shouldBe empty
    warned shouldBe true
    debug should be > 1000L
    ofSpark shouldBe true
  }

  test(
    "as it is deployed, it logs at INFO to its error output, and its standard output is records"
  ) {
    val returns = rejecting()
    val (out, err) = (files.newLog(), files.newLog())

    // On the classpath of the command line alone, which has the application's configuration
    // of its log, and told nothing of it.
    val ended = CommandProcess
      .start(
        asking(returns.id.value, Seq("--records", "10")),
        out,
        errors = Some(err),
        classpath = CommandProcess.Deployed
      )
      .await()

    val errors = Files.readAllLines(err, UTF_8).asScala.toSeq
    withClue(errors.takeRight(30).mkString("\n"))(ended.exit shouldBe Main.Exit.Ok)
    // Its standard output is the records, a line each, and nothing else.
    val written = Files.readAllLines(out, UTF_8).asScala.toSeq
    withClue(written.take(5).mkString("\n")) {
      written.map(read(_).get("raw_record").asText) shouldBe Seq(Quoted, Trailing, Acted)
    }
    // Its error output has the warning, and the log in the pattern of Spark's own default.
    errors should contain(QuarantineCommand.PersonalData)
    Held.foreach(held => errors.mkString("\n") should not include held)
    val logged = errors.collect { case Logged(level, logger) => level -> logger }
    withClue(logged.take(40).mkString("\n")) {
      logged.map(_._1).toSet should contain("INFO")
      logged.map(_._1).toSet -- Set("INFO", "WARN", "ERROR") shouldBe empty
    }
    // From its first line: what is logged before Spark starts, as the pool of the control
    // plane opens, is there as the rest is. Without the application's configuration, it is
    // lost (log4j2 logs errors alone until Spark sets its default).
    val firstOfSpark = logged.indexWhere(_._2 == "SparkContext")
    withClue(logged.take(40).mkString("\n")) {
      firstOfSpark should be > 0
      logged.take(firstOfSpark).map(_._2) should contain("HikariDataSource")
    }
  }

  // ---------------------------------------------------------------------------------------------
  // It reads, and writes nothing
  // ---------------------------------------------------------------------------------------------

  /** Every row of the control plane and of the coordination schema, by its table. */
  private def everyRow(): Map[String, String] = asSuperuser { c =>
    val tables = query(
      c,
      "SELECT table_schema || '.' || table_name FROM information_schema.tables " +
        "WHERE table_schema IN ('control', 'delta_coordination') AND table_type = 'BASE TABLE'"
    ).map(_.head)
    tables.map { table =>
      table -> query(
        c,
        s"SELECT count(*) || ' rows, ' || coalesce(md5(string_agg(t::text, '|' ORDER BY t::text)), '') FROM $table t"
      ).head.head
    }.toMap
  }

  /** Every file of the lake, with its size and when it was last changed. */
  private def everyFile(): Map[Path, (Long, Long)] =
    Using.resource(Files.walk(files.lake)) { paths =>
      paths.iterator.asScala
        .filter(Files.isRegularFile(_))
        .map(path => path -> (Files.size(path) -> Files.getLastModifiedTime(path).toMillis))
        .toMap
    }

  test("the command reads, and writes nothing: no row and no file is other than it was") {
    val returns = rejecting()
    val (rows, lake) = (everyRow(), everyFile())
    rows.keySet should contain allOf ("control.ingestion_run", "control.source_lease")
    lake.keys.exists(_.toString.contains("_quarantine")) shouldBe true

    quarantine(returns).exit shouldBe Main.Exit.Ok
    quarantine(returns, "--records", "10", "--batch", "1").exit shouldBe Main.Exit.Ok
    quarantine(returns, "--batch", "9").exit shouldBe Main.Exit.Permanent

    // No run, no lease and no batch of it; no commit, no checkpoint and no file of a table.
    everyRow() shouldBe rows
    everyFile() shouldBe lake
  }

  test("a source that is disabled is read as any other: it is paused, and what it had is there") {
    val returns = rejecting()
    val text = new String(Files.readAllBytes(files.sources.resolve(s"${returns.id}.yaml")), UTF_8)
    Files.write(
      files.sources.resolve(s"${returns.id}.yaml"),
      (text + "enabled: false\n").getBytes(UTF_8)
    )

    val ran = quarantine(returns)
    withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
    ran.lines.head should include("has 3 records of what was asked")
  }
}
