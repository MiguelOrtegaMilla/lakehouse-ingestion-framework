package lakehouse.ingestion.it.cli

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.Base64

import scala.jdk.CollectionConverters._
import scala.util.Using

import lakehouse.ingestion.cli.{Main, RunSession}
import lakehouse.ingestion.core.config.Source
import lakehouse.ingestion.engine.files.FileSourceConnector
import lakehouse.ingestion.it.control.ControlPlaneSuite
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.{
  CapturedLogs,
  CommandProcess,
  DeploymentFiles,
  LocalSparkSuite
}
import org.apache.logging.log4j.Level
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What the command `run` logs of what its source holds: nothing (discovery §20, "Delta logs
 * values of the rows of a table of Bronze").
 *
 * A row of a source may hold personal data, and so may a record that a read sets apart. A run
 * reads them, writes them to Bronze and to the quarantine of its source, and says how many
 * there were: no value of one is in what it prints, nor in what it logs. The log of a process
 * is not the engine's alone: Spark, Delta and Parquet log in it, and Delta logs each snapshot
 * of a table with the checksum of its version, which has values of its rows unless the session
 * is set otherwise (`Profile.EngineSpark.FilesInChecksum`, `DeltaSnapshotLogContractSpec`).
 *
 * So the sources here hold values that nothing else of a run has, in each form that a source
 * may be of: files of CSV, of JSON and of Parquet, and a table of a database; in rows, and in
 * records that are set apart, with a name of a field among them. And a line of a log is looked
 * at for one of those values as it is, or as its bytes in base64, in hexadecimal, or as the
 * list of them that Java writes.
 *
 * '''What holds, and at which level.''' At INFO, which is the level that a process logs at
 * unless it is told otherwise, no line of any logger has a value. Below it, none has but those
 * of Parquet's writer of records (`org.apache.parquet.io.MessageColumnIO` and
 * `RecordConsumerLoggingWrapper`), which at DEBUG logs each value that it writes to a file: a
 * number as it is, a text as the list of its bytes. That is Parquet's, and is so for every
 * table that a run writes, whatever Delta is set to; the engine does not set the level of a
 * logger, and the reference says which to keep above DEBUG (`docs/operations`).
 *
 * Parquet's writer takes its level once, when its class is loaded. So whether it logs in the
 * JVM of the tests is of what ran there before, and what is asked of a run in this JVM leaves
 * its lines out; the processes of their own, which start at the level that they log at, pin
 * both levels.
 */
final class RunCommandLogsSpec
    extends AnyFunSuite
    with Matchers
    with ControlPlaneSuite
    with RunCommandQueries
    with CommandLedger {

  import RunCommandLogsSpec._

  private lazy val files = new DeploymentFiles(pg, Files.createTempDirectory("run-command-logs-"))
  override protected def deployment: DeploymentFiles = files
  import files.{arguments, connection, drop}

  override def afterAll(): Unit =
    try files.close()
    finally super.afterAll()

  /** Three records of CSV, the third with a value too many: it is set apart, as it was written. */
  private def ofCsv(): Source = {
    val of = files.fileSource(newSourceId(), rejected = Some("0.5"))
    drop(
      of,
      "a.csv",
      s"id,name\n1,${InCsv.value(1)}\n2,${InCsv.value(2)}\n3,${InCsv.value(3)},${ApartInCsv.value(1)}\n"
    )
    of
  }

  /**
   * Three records of JSON: one with text after its object and one with a field twice, which
   * are set apart, the second with the name of its field as why.
   */
  private def ofJson(): Source = {
    val of = files.fileSource(newSourceId(), rejected = Some("0.9"), format = "json")
    val field = FieldInJson.value(1)
    drop(
      of,
      "a.json",
      s"""{"id":"1","name":"${InJson.value(1)}"}
         |{"id":"2","name":"${InJson.value(2)}"} ${ApartInJson.value(1)}
         |{"id":"3","$field":"${InJson.value(3)}","$field":"${ApartInJson.value(2)}"}
         |""".stripMargin
    )
    of
  }

  /** Two rows of Parquet, in a file that Spark wrote before the run is heard. */
  private def ofParquet(): Source = {
    val of = files.fileSource(newSourceId(), format = "parquet")
    val scratch = Files.createTempDirectory("run-command-logs-parquet-")
    val session = LocalSparkSuite
      .baseConf(scratch, "run-command-logs-parquet")
      .foldLeft(SparkSession.builder()) { case (builder, (key, value)) =>
        builder.config(key, value)
      }
      .getOrCreate()
    try {
      import session.implicits._
      val written = scratch.resolve("written")
      Seq(1L -> InParquet.value(1), 2L -> InParquet.value(2))
        .toDF("id", "name")
        .coalesce(1)
        .write
        .parquet(written.toString)
      val part = Using.resource(Files.list(written)) {
        _.iterator().asScala.find(_.getFileName.toString.endsWith(".parquet")).get
      }
      val dropped = Files.move(part, files.directoryOf(of.id).resolve("a.parquet"))
      Files.setLastModifiedTime(dropped, FileTime.from(Instant.now().minusSeconds(3600)))
    } finally {
      RunSession.close(session)
      LocalSparkSuite.deleteRecursively(scratch)
    }
    of
  }

  /** Two rows of a table of a database, each with a text and a number of its own. */
  private def ofATable(): Source = {
    val id = newSourceId()
    val table = s"logged_${id.value.filter(_.isLetterOrDigit)}"
    Using.resource(pg.connect("sales_owner", "sales")) { c =>
      Using.resource(c.createStatement()) { statement =>
        statement.execute(s"CREATE TABLE $table (id int, name text, amount bigint)")
        statement.execute(
          s"INSERT INTO $table VALUES (1, '${InATable.value(1)}', ${Amount}1), " +
            s"(2, '${InATable.value(2)}', ${Amount}2)"
        ): Unit
      }
    }
    files.tableSource(
      id,
      s"public.$table",
      connection("sales_ro", pg.password("sales_ro"), url = pg.jdbcUrl("sales"))
    )
  }

  /** The text of the commits of the table of Bronze of a source: what the run wrote of it. */
  private def commitsOf(of: Source): String = {
    val log = files.lake.resolve(s"bronze/${of.id.value.replace('.', '/')}/_delta_log")
    Using
      .resource(Files.list(log))(_.iterator().asScala.toVector)
      .filter(_.getFileName.toString.endsWith(".json"))
      .map(file => new String(Files.readAllBytes(file), UTF_8))
      .mkString("\n")
  }

  test("a run says no value of what its source holds, and none is logged, at any level") {
    val (csv, json, parquet, table) = (ofCsv(), ofJson(), ofParquet(), ofATable())

    // Every logger of the JVM, the application's, Spark's, Delta's and Parquet's, at its lowest.
    val (ran, all) = CapturedLogs.everything(Level.ALL)(line => formIn(line).nonEmpty) {
      Seq(csv, json, parquet, table).map(of => command(Map.empty)(arguments(of): _*))
    }
    // But for the writer of records of Parquet, which logs as this JVM loaded it: the processes
    // of their own, below, pin what it does.
    val heard = all.copy(kept = all.kept.filterNot(_.logger.startsWith(ParquetRecords)))

    ran.foreach(one => withClue(one.err)(one.exit shouldBe Main.Exit.Ok))
    ran.map(one => one.out.linesIterator.find(_.contains("rows read")).map(counts)) shouldBe Seq(
      Some("3 rows read, 2 written, 1 rejected"),
      Some("3 rows read, 1 written, 2 rejected"),
      Some("2 rows read, 2 written, 0 rejected"),
      Some("2 rows read, 2 written, 0 rejected")
    )
    // What the command prints says none of them.
    ran.foreach(one => formIn(one.out + one.err) shouldBe empty)
    // Nor does a line of any logger. Of one that does, what is around the value: a line of a
    // log may be very long.
    def around(message: String): String = {
      val at = Forms.map(message.indexOf).filter(_ >= 0).min
      message.slice(at - 300, at + 80)
    }
    withClue(
      heard.kept
        .map(line => s"${line.level} ${line.logger}: …${around(line.message)}…")
        .mkString("\n")
    )(heard.kept shouldBe empty)

    // The loggers were heard, as low as they log, among thousands of lines.
    heard.events should be > 10000L
    Seq(
      "lakehouse.ingestion",
      "org.apache.spark.sql.delta",
      "org.apache.spark.sql.execution",
      "org.apache.parquet"
    ).foreach(prefix => withClue(prefix)(heard.loggers.exists(_.startsWith(prefix)) shouldBe true))
    // And the values were there to be logged: they are in Bronze, and in the statistics that
    // Delta keeps of each file in its commit, which it would log with its snapshots.
    commitsOf(csv) should include(InCsv.start)
    commitsOf(json) should include(InJson.start)
    commitsOf(parquet) should include(InParquet.start)
    commitsOf(table) should include(InATable.start)
    commitsOf(table) should include(s"${Amount}1")
    Seq(csv, json, parquet, table).foreach(assertLedger(_))
  }

  test("in a process of its own that logs at INFO, no line of its two outputs has a value") {
    // A source of each connector: files, with a record set apart, and a table.
    Seq(ofCsv(), ofATable()).foreach { of =>
      val logged = inAProcess(of, level = "info")
      logged.withAValue shouldBe empty
      // What Delta logs of a snapshot is there, and has none.
      logged.snapshots should be > 0L
    }
  }

  test("at DEBUG, the lines with a value are those of Parquet's writer of records, and no other") {
    Seq(ofCsv(), ofATable()).foreach { of =>
      val logged = inAProcess(of, level = "debug")
      logged.debug should be > 1000L
      logged.snapshots should be > 0L
      // Each value that is written to a file of the table, and of the quarantine: a text as
      // the list of its bytes, by one of Parquet's two loggers, and a number by both.
      val loggers = logged.withAValue.map(_._1).toSet
      loggers should contain("org.apache.parquet.io.RecordConsumerLoggingWrapper")
      (loggers -- ParquetWriters) shouldBe empty
    }
  }

  test("as it is deployed, what univocity parsed of a value too long is in no line of the log") {
    // A quote that is not closed, with the rest of the file after it longer than the bound: the
    // read of the file fails, and univocity's failure has the end of what it parsed, which
    // Spark logs at WARN and at ERROR with each task that fails.
    val held = "PERSON-PARSED"
    val of = files.fileSource(newSourceId())
    // Pieces of 20 characters, each with the text, to past the bound.
    val rest =
      Seq.tabulate(FileSourceConnector.MaxValueLength / 20 + 2)(i => f"$held-$i%06d").mkString
    drop(of, "a.csv", s"id,name\n1,\"$rest\n2,y\n")
    val (out, err) = (files.newLog(), files.newLog())

    val ended = CommandProcess
      .start(arguments(of), out, errors = Some(err), classpath = CommandProcess.Deployed)
      .await()

    val logged = Files.readAllLines(err, UTF_8).asScala.toSeq
    withClue(logged.takeRight(30).mkString("\n"))(ended.exit shouldBe Main.Exit.Permanent)
    logged.filter(_.contains(held)).map(_.take(200)) shouldBe empty
    // Spark logged the failure of each task, with what univocity said of it, and the
    // configuration of the application took out what follows the marker.
    logged.count(_.contains("content parsed=<not logged>")) should be > 0
    Files.readAllLines(out, UTF_8).asScala.filter(_.contains(held)) shouldBe empty
  }

  /**
   * One run of a source in a process of its own that logs to its error output at `level`, as a
   * deployment that is set so: what its log has, once it ended well with nothing of a value on
   * its standard output.
   */
  private def inAProcess(of: Source, level: String): Lines = {
    val logging = Files.write(
      Files.createTempFile("run-logging-", ".properties"),
      s"""rootLogger.level = $level
         |rootLogger.appenderRef.console.ref = console
         |appender.console.type = Console
         |appender.console.name = console
         |appender.console.target = SYSTEM_ERR
         |appender.console.layout.type = PatternLayout
         |appender.console.layout.pattern = $Logged%level|%c|%msg%n%throwable
         |""".stripMargin.getBytes(UTF_8)
    )
    val (out, err) = (files.newLog(), files.newLog())
    val ended = CommandProcess
      .start(
        arguments(of),
        out,
        jvm = Seq(s"-Dlog4j2.configurationFile=${logging.toUri}"),
        errors = Some(err)
      )
      .await()

    withClue(Files.readAllLines(err, UTF_8).asScala.takeRight(30).mkString("\n")) {
      ended.exit shouldBe Main.Exit.Ok
    }
    batchesOf(of) shouldBe Seq("1" -> "COMMITTED")
    val said = Files.readAllLines(out, UTF_8).asScala.toSeq
    said.exists(_.contains("committed the batch")) shouldBe true
    said.flatMap(formIn) shouldBe empty
    linesOf(err)
  }

  /** What the log of a process has. It is read a line at a time: at DEBUG it is long. */
  private def linesOf(log: Path): Lines = {
    var (debug, snapshots) = (0L, 0L)
    // The logger of the line that is being read: a line with no start of its own, as one of a
    // message of several lines, is of the logger of the line before it.
    var logger = ""
    val withAValue = Vector.newBuilder[(String, String)]
    Using.resource(Files.lines(log, UTF_8)) { logged =>
      logged.forEach { line =>
        if (line.startsWith(Logged)) {
          val parts = line.stripPrefix(Logged).split("\\|", 3)
          if (parts.length == 3) {
            logger = parts(1)
            if (parts(0) == "DEBUG") debug += 1
            if (parts(2).contains("Created snapshot ")) snapshots += 1
          }
        }
        if (formIn(line).nonEmpty) withAValue += logger -> line.take(200)
      }
    }
    Lines(withAValue.result(), debug, snapshots)
  }
}

object RunCommandLogsSpec {

  /**
   * What is asked of the lines of a log of a process.
   *
   * @param withAValue the lines that have a value of a source, each with its logger
   * @param debug how many lines were logged at DEBUG
   * @param snapshots how many lines log a snapshot that Delta made
   */
  final private case class Lines(withAValue: Vector[(String, String)], debug: Long, snapshots: Long)

  /** How a line of the log of a process of the tests starts, before its level and its logger. */
  private val Logged = "@@|"

  /** The loggers of Parquet's writer of records, which logs each value that it writes. */
  private val ParquetRecords = "org.apache.parquet.io."

  private val ParquetWriters = Set(
    "org.apache.parquet.io.MessageColumnIO",
    "org.apache.parquet.io.RecordConsumerLoggingWrapper"
  )

  /**
   * What the values of one place of a source start with: eight characters that nothing else of
   * a run has, so that a line that has them has a value of that place.
   */
  final private case class Held(start: String) {
    def value(i: Int): String = s"$start-$i"
  }

  private val InCsv = Held("Kq3ZpV8t")
  private val ApartInCsv = Held("Hd9TmQ2x")
  private val InJson = Held("Rw5NcB7j")
  private val ApartInJson = Held("Gf4XsL6p")
  private val FieldInJson = Held("Yb8VrD3k")
  private val InParquet = Held("Tn6JhM9c")
  private val InATable = Held("Pz2WgF5s")

  /** What the numbers of the rows of the table start with. */
  private val Amount = "730194582"

  /**
   * How a value may be in a line: as it is, or as its bytes in base64, in hexadecimal, or as
   * the list of them that Java writes, which is how a line that says what a file holds, or
   * what a statement was given, may have it. The bytes are the first six of a value, which
   * are eight characters of base64 whatever follows them.
   */
  private val Forms: Seq[String] =
    Seq(InCsv, ApartInCsv, InJson, ApartInJson, FieldInJson, InParquet, InATable).flatMap { held =>
      val bytes = held.start.getBytes(UTF_8).take(6)
      Seq(
        held.start,
        Base64.getEncoder.encodeToString(bytes),
        bytes.map(byte => f"$byte%02x").mkString,
        bytes.map(byte => f"$byte%02X").mkString,
        bytes.mkString(", ")
      )
    } :+ Amount

  /** The form of a value that a text has, if it has one. */
  private def formIn(text: String): Option[String] = Forms.find(text.contains)

  /** The counts of a line of the command that says them. */
  private def counts(line: String): String =
    """\d+ rows read, \d+ written, \d+ rejected""".r.findFirstIn(line).getOrElse(line)
}
