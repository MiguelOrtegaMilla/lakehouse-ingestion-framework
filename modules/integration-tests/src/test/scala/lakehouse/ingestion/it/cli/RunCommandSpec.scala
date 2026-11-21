package lakehouse.ingestion.it.cli

import java.net.ServerSocket
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.TimeZone

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Using

import lakehouse.ingestion.cli.{Main, RunSession}
import lakehouse.ingestion.core.config.{Profile, Source}
import lakehouse.ingestion.core.control.LeaseTiming
import lakehouse.ingestion.engine.control.LeasedRun
import lakehouse.ingestion.it.control.ControlPlaneSuite
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.{CommandProcess, DeploymentFiles, LocalSparkSuite}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The command `run` from end to end, as an orchestrator starts it: a profile and the sources of
 * a deployment in files, a control plane that `migrate` made, a lake and a landing zone on disk,
 * and the exit code as all that it says to whoever started it (discovery §14.1).
 *
 * The command opens its own Spark session and stops it, so this suite holds none: what a run
 * left is asked of the control plane, and of the log of its table on disk. What the engine does
 * with a batch is tested by the suites of the engine; here, that the command puts it together
 * as they do, and ends in the code of what happened.
 *
 * The runs are in this JVM, through `Main.run`, but for the last test: a process of its own,
 * which ends with the code by `sys.exit`, as a deployment sees it.
 */
final class RunCommandSpec extends AnyFunSuite with Matchers with ControlPlaneSuite {

  private lazy val files = new DeploymentFiles(pg, Files.createTempDirectory("run-command-"))
  import files.{arguments, connection, drop, profileFile, validProfile}

  override def afterAll(): Unit =
    try files.close()
    finally super.afterAll()

  /** A source of CSV files of its own, among the sources of the deployment. */
  private def fileSource(rejected: Option[String] = None): Source =
    files.fileSource(newSourceId(), rejected)

  /** One run of a source, in this JVM, with an environment that has nothing in it. */
  private def launch(of: Source, profile: Path = validProfile): Ran = {
    val ran = command(Map.empty)(arguments(of, profile): _*)
    // The command leaves the JVM as it found it.
    SparkSession.getDefaultSession shouldBe empty
    ran
  }

  /** What the command said, but the warnings of the profile: its lake is under /tmp. */
  private def said(ran: Ran): Seq[String] = ran.errors.filterNot(_.startsWith("warning: "))

  /** The runs of a source, each with how it ended, in the order they started. */
  private def runsOf(of: Source): Seq[(String, Option[String])] = asSuperuser { c =>
    query(
      c,
      "SELECT status, error_class FROM control.ingestion_run WHERE source_id = ? ORDER BY started_at",
      of.id.value
    ).map(row => (row(0), Option(row(1))))
  }

  private def batchesOf(of: Source): Seq[(String, String)] = asSuperuser { c =>
    query(
      c,
      "SELECT batch_seq::text, status FROM control.ingestion_batch WHERE source_id = ? ORDER BY 1",
      of.id.value
    ).map(row => (row(0), row(1)))
  }

  private def rowsWritten(of: Source): Seq[String] = asSuperuser { c =>
    query(
      c,
      "SELECT rows_written::text FROM control.ingestion_run WHERE source_id = ? AND " +
        "status = 'SUCCEEDED' ORDER BY started_at",
      of.id.value
    ).map(_.head)
  }

  /** The commits in the log of the table of Bronze of a source. */
  private def commitsOf(of: Source): Seq[String] = {
    val log = files.lake.resolve(s"bronze/${of.id.value.replace('.', '/')}/_delta_log")
    if (!Files.isDirectory(log)) Nil
    else
      Using.resource(Files.list(log))(
        _.iterator.asScala.map(_.getFileName.toString).filter(_.endsWith(".json")).toVector.sorted
      )
  }

  private def closedPort(): Int = Using.resource(new ServerSocket(0))(_.getLocalPort)

  // ---------------------------------------------------------------------------------------------

  test("a run takes the new files of a source to Bronze and ends in 0; the next writes nothing") {
    val returns = fileSource()
    drop(returns, "a.csv", "id,amount\n1,10\n2,20\n")

    val first = launch(returns)

    withClue(first.err)(first.exit shouldBe Main.Exit.Ok)
    first.out should include(s"of ${returns.id.value} committed the batch")
    first.out should include("2 rows read, 2 written, 0 rejected")
    said(first) shouldBe empty
    batchesOf(returns) shouldBe Seq("1" -> "COMMITTED")
    commitsOf(returns) shouldBe Seq("00000000000000000000.json")

    val second = launch(returns)

    withClue(second.err)(second.exit shouldBe Main.Exit.Ok)
    second.out should include("found nothing new, and ended well with no batch")
    batchesOf(returns) shouldBe Seq("1" -> "COMMITTED")
    commitsOf(returns) shouldBe Seq("00000000000000000000.json")
    runsOf(returns) shouldBe Seq("SUCCEEDED" -> None, "SUCCEEDED" -> None)
    rowsWritten(returns) shouldBe Seq("2", "0")
  }

  test("a run takes a table of a database to Bronze, in UTC whatever zone its JVM started in") {
    Using.resource(pg.connect("sales_owner", "sales")) { c =>
      Using.resource(c.createStatement()) { statement =>
        statement.execute("CREATE TABLE visits (id int, seen timestamp, seen_at timestamptz)")
        // Half past two of the night that the clocks of Madrid go back: an hour that is twice.
        statement.execute(
          "INSERT INTO visits VALUES (1, '2026-10-25 02:30:00', '2026-10-25 02:30:00+02')"
        ): Unit
      }
    }
    val visits = files.tableSource(
      newSourceId(),
      "public.visits",
      connection("sales_ro", pg.password("sales_ro"), url = pg.jdbcUrl("sales"))
    )
    val started = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Madrid"))

    val (ran, zone) =
      try {
        val ran = launch(visits)
        (ran, TimeZone.getDefault.getID)
      } finally TimeZone.setDefault(started)

    withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
    ran.out should include(s"of ${visits.id.value} committed the batch")
    ran.out should include("1 rows read, 1 written, 0 rejected")
    said(ran) shouldBe empty
    // The command set the JVM to UTC before it connected to anything.
    zone shouldBe "UTC"
    batchesOf(visits) shouldBe Seq("1" -> "COMMITTED")
    rowsWritten(visits) shouldBe Seq("1")

    // A time without a zone is taken as UTC, and one with its zone is the moment that it is.
    val read = LocalSparkSuite
      .baseConf(Files.createTempDirectory("run-command-read-"), "run-command-read")
      .foldLeft(SparkSession.builder()) { case (builder, (key, value)) =>
        builder.config(key, value)
      }
      .getOrCreate()
    val row =
      try
        read.read
          .format("delta")
          .load(files.lake.resolve(s"bronze/${visits.id.value.replace('.', '/')}").toUri.toString)
          .head()
      finally RunSession.close(read)
    row.getTimestamp(row.fieldIndex("seen")).toInstant shouldBe
      Instant.parse("2026-10-25T02:30:00Z")
    row.getTimestamp(row.fieldIndex("seen_at")).toInstant shouldBe
      Instant.parse("2026-10-25T00:30:00Z")
  }

  test("a run of a source whose lease another run holds ends in 2, and does nothing") {
    val returns = fileSource()
    drop(returns, "a.csv", "id\n1\n")
    val holder =
      LeasedRun.open(database, starting(returns), versionOf(returns), LeaseTiming(2.minutes)).value

    val ran =
      try launch(returns)
      finally holder.close()

    withClue(ran.err)(ran.exit shouldBe Main.Exit.SkippedLocked)
    ran.out should include(s"was skipped: the run ${holder.start.id} holds the lease of the source")
    said(ran) shouldBe empty
    batchesOf(returns) shouldBe empty
    commitsOf(returns) shouldBe empty
  }

  test("more rejected records than the source allows end in 5, with nothing committed") {
    val returns = fileSource()
    drop(returns, "a.csv", "id,amount\n1,10\n2,20,extra\n")

    val ran = launch(returns)

    withClue(ran.err)(ran.exit shouldBe Main.Exit.DataThreshold)
    said(ran).head should include("failed (data)")
    ran.out shouldBe empty
    batchesOf(returns) shouldBe Seq("1" -> "PLANNED")
    commitsOf(returns) shouldBe empty
    runsOf(returns) shouldBe Seq("FAILED" -> Some("data"))
  }

  test("a run that fails for good ends in 4: data with a column of the engine's") {
    val returns = fileSource()
    drop(returns, "a.csv", "id,_run_id\n1,x\n")

    val ran = launch(returns)

    withClue(ran.err)(ran.exit shouldBe Main.Exit.Permanent)
    said(ran).head should (include("failed (permanent)") and include("_run_id"))
    commitsOf(returns) shouldBe empty
    runsOf(returns) shouldBe Seq("FAILED" -> Some("permanent"))
  }

  test("a control plane that is not there ends in 3, and nothing is started") {
    val returns = fileSource()
    drop(returns, "a.csv", "id\n1\n")
    val nowhere = profileFile(control =
      connection(
        Engine,
        pg.password(Engine),
        url = s"jdbc:postgresql://127.0.0.1:${closedPort()}/$Database",
        connect = "1s"
      )
    )

    val ran = launch(returns, nowhere)

    ran.exit shouldBe Main.Exit.Transient
    said(ran).head should (startWith("error: the control plane") and include("08001"))
    said(ran).last shouldBe "Nothing was run."
    runsOf(returns) shouldBe empty
  }

  test("a coordination database that is not there ends in 3, and nothing is run") {
    val returns = fileSource()
    drop(returns, "a.csv", "id\n1\n")
    val nowhere = profileFile(coordination =
      connection(
        "ingest_coordination",
        pg.password("ingest_coordination"),
        url = s"jdbc:postgresql://127.0.0.1:${closedPort()}/$Database",
        connect = "1s"
      )
    )

    val ran = launch(returns, nowhere)

    ran.exit shouldBe Main.Exit.Transient
    said(ran).head should startWith("error: the coordination database cannot be used")
    said(ran)(1) should (startWith("error: the coordination database said: ") and
      include("SQLState 08001"))
    said(ran).last shouldBe "Nothing was run."
    runsOf(returns) shouldBe empty
    commitsOf(returns) shouldBe empty
  }

  test("a coordination database that refuses the password of its role ends in 4") {
    val returns = fileSource()
    drop(returns, "a.csv", "id\n1\n")
    val refused = profileFile(coordination =
      connection("ingest_coordination", "not-the-password-7c1d", connect = "1s")
    )

    val ran = launch(returns, refused)

    ran.exit shouldBe Main.Exit.Permanent
    said(ran).head should startWith("error: the coordination database cannot be used")
    said(ran)(1) shouldBe "error: the coordination database said: FATAL: password " +
      "authentication failed for user \"ingest_coordination\" (SQLState 28P01)"
    (ran.out + ran.err) should not include "not-the-password-7c1d"
    runsOf(returns) shouldBe empty
  }

  test("a setting from outside the profile that would have Delta forget transactions ends in 4") {
    val returns = fileSource()
    drop(returns, "a.csv", "id\n1\n")
    val key = Profile.TransactionRetentionDefault

    // A system property of the JVM: Spark reads every spark.* of them into its configuration.
    System.setProperty(key, "interval 1 day")
    val ran =
      try launch(returns)
      finally System.clearProperty(key): Unit

    ran.exit shouldBe Main.Exit.Permanent
    said(ran).head shouldBe s"error: the Spark session has $key from outside the profile " +
      "(spark-defaults.conf, a system property of the JVM, or --conf), and it would give " +
      "every table that Delta makes the property delta.setTransactionRetentionDuration. Delta " +
      "would then forget the batches written to a table before that time, and a replay would " +
      "write such a batch again: no table of the engine may have it"
    said(ran).last shouldBe "Nothing was run."
    runsOf(returns) shouldBe empty
    commitsOf(returns) shouldBe empty
  }

  test("a process of its own ends with the code of its run, by sys.exit") {
    val returns = fileSource()
    drop(returns, "a.csv", "id\n1\n2\n")
    val rejecting = fileSource()
    drop(rejecting, "a.csv", "id\n1\n1,2\n")

    val (ingested, ingestedLog) = process(returns)
    withClue(ingestedLog)(ingested shouldBe Main.Exit.Ok)
    batchesOf(returns) shouldBe Seq("1" -> "COMMITTED")
    rowsWritten(returns) shouldBe Seq("2")

    val (dataFailure, failureLog) = process(rejecting)
    withClue(failureLog)(dataFailure shouldBe Main.Exit.DataThreshold)
    runsOf(rejecting) shouldBe Seq("FAILED" -> Some("data"))
  }

  /** One run of a source in a JVM of its own: its exit code, and the end of what it printed. */
  private def process(of: Source): (Int, String) = {
    val ended = CommandProcess.start(arguments(of), files.newLog()).await()
    (ended.exit, ended.log)
  }
}
