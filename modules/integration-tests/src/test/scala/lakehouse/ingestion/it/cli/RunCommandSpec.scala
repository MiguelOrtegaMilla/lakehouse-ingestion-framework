package lakehouse.ingestion.it.cli

import java.io.{ByteArrayOutputStream, PrintStream}
import java.net.ServerSocket
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.TimeZone
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Using

import lakehouse.ingestion.cli.{Main, RunSession, Stop}
import lakehouse.ingestion.core.config.{Profile, Source}
import lakehouse.ingestion.core.control.{Layer, LeaseKey, LeaseTiming}
import lakehouse.ingestion.engine.control.LeasedRun
import lakehouse.ingestion.it.control.ControlPlaneSuite
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.{CommandProcess, DeploymentFiles, LocalSparkSuite}
import org.apache.spark.sql.SparkSession
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}

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
final class RunCommandSpec
    extends AnyFunSuite
    with Matchers
    with Eventually
    with ControlPlaneSuite
    with RunCommandQueries {

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

  // ---------------------------------------------------------------------------------------------
  // A chain of runs, and what stops it (discovery §17, P6.10)
  // ---------------------------------------------------------------------------------------------

  /** A source of files of one record each, of which one batch takes one. */
  private def oneAtATime(count: Int): Source = {
    val source = files.fileSource(newSourceId(), options = ", max_files_per_batch: 1")
    (1 to count).foreach(n => drop(source, f"f$n%02d.csv", s"id\n$n\n"))
    source
  }

  /** One invocation in this JVM, asked to stop as `stop` says, with options after the source. */
  private def invoke(of: Source, stop: Stop = Stop.Never, options: Seq[String] = Nil): Ran = {
    val (out, err) = (new ByteArrayOutputStream, new ByteArrayOutputStream)
    val exit = Main.run(
      arguments(of) ++ options,
      new PrintStream(out, true, "UTF-8"),
      new PrintStream(err, true, "UTF-8"),
      Map.empty[String, String].get,
      stop
    )
    Ran(exit, out.toString("UTF-8"), err.toString("UTF-8"))
  }

  private def committedOf(of: Source): Int = batchesOf(of).count(_._2 == "COMMITTED")

  private def takenFiles(of: Source): Seq[String] = asSuperuser { c =>
    query(
      c,
      "SELECT path FROM control.processed_file WHERE source_id = ? ORDER BY 1",
      of.id.value
    ).map(_.head.split('/').last)
  }

  /** Has the lease of a source run out, if a run holds it. */
  private def expireLease(of: Source): Unit = asSuperuser { c =>
    update(
      c,
      "UPDATE control.source_lease SET expires_at = now() - interval '1 second' " +
        "WHERE lease_key = ? AND holder_run_id IS NOT NULL",
      LeaseKey.of(of.id, Layer.Bronze).value
    ): Unit
  }

  test("an invocation runs its source again while the batch it committed says there is more") {
    val chained = oneAtATime(3)
    // Before the second run, a producer leaves a file that is complete already: it was not
    // there at the first plan, which counted three, so it is the next invocation's.
    val asked = new AtomicInteger
    val leaving = new Stop {
      override def requested: Option[Int] = {
        if (asked.incrementAndGet() == 2) drop(chained, "late.csv", "id\n4\n"): Unit
        None
      }
    }

    val ran = invoke(chained, leaving)

    withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
    ran.lines.count(_.contains("committed the batch")) shouldBe 3
    ran.lines.last shouldBe
      s"The invocation of ${chained.id.value} made 3 runs and committed 3 batches."
    batchesOf(chained) shouldBe Seq("1" -> "COMMITTED", "2" -> "COMMITTED", "3" -> "COMMITTED")
    // Each a run of its own, with its lease given up.
    runsOf(chained) shouldBe Seq.fill(3)("SUCCEEDED" -> None)
    takenFiles(chained) should not contain "late.csv"

    val next = invoke(chained)
    withClue(next.err)(next.exit shouldBe Main.Exit.Ok)
    takenFiles(chained) should contain("late.csv")
  }

  test("--max-batches stops the chain where it says, and the next invocation goes on") {
    val limited = oneAtATime(3)

    val first = invoke(limited, options = Seq("--max-batches", "2"))

    withClue(first.err)(first.exit shouldBe Main.Exit.Ok)
    first.lines.last shouldBe s"The invocation of ${limited.id.value} made 2 runs and " +
      "committed 2 batches: it stopped at --max-batches 2, and the source has more."
    committedOf(limited) shouldBe 2

    val rest = invoke(limited)
    withClue(rest.err)(rest.exit shouldBe Main.Exit.Ok)
    committedOf(limited) shouldBe 3
    takenFiles(limited) shouldBe Seq("f01.csv", "f02.csv", "f03.csv")
  }

  test("a stop asked during a run lets the run finish, and no other start") {
    val stopped = oneAtATime(3)
    val asked = new AtomicInteger
    // Asked before each run: nothing before the first, SIGTERM before the second.
    val afterOne = new Stop {
      override def requested: Option[Int] = Option.when(asked.getAndIncrement() > 0)(15)
    }

    val ran = invoke(stopped, afterOne)

    withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
    ran.lines.last shouldBe s"The invocation of ${stopped.id.value} made 1 run and committed " +
      "1 batch: SIGTERM asked it to stop, and no other run was started."
    batchesOf(stopped) shouldBe Seq("1" -> "COMMITTED")
  }

  test("a stop asked before the first run ends in 128 + the signal, with nothing run") {
    val never = oneAtATime(1)
    val now = new Stop { override def requested: Option[Int] = Some(2) }

    val ran = invoke(never, now)

    ran.exit shouldBe 130
    said(ran).last shouldBe "error: SIGINT came before the first run. Nothing was run."
    runsOf(never) shouldBe empty
  }

  Seq("TERM", "INT").foreach { signal =>
    test(s"a real SIG$signal lets the batch in course finish, and starts no other") {
      val source = oneAtATime(8)
      val process = CommandProcess.start(arguments(source), files.newLog())
      eventually(timeout(Span(120, Seconds))) {
        committedOf(source) should be >= 1
      }

      process.signal(signal)
      val ended = process.await()

      withClue(ended.log)(ended.exit shouldBe Main.Exit.Ok)
      ended.log should include(s"SIG$signal received: the batch in course finishes")
      ended.log should include(s"SIG$signal asked it to stop, and no other run was started")
      // Every batch that was started is committed, and the source has files left.
      batchesOf(source).map(_._2).distinct shouldBe Seq("COMMITTED")
      committedOf(source) should be < 8
    }
  }

  test("a second signal ends the process at once, and the next invocation finishes the rest") {
    val source = oneAtATime(8)
    val process = CommandProcess.start(arguments(source), files.newLog())
    eventually(timeout(Span(120, Seconds))) {
      committedOf(source) should be >= 1
    }

    process.signal("TERM")
    process.signal("TERM")
    val ended = process.await()

    withClue(ended.log)(ended.exit shouldBe 143)
    // What it left, a batch planned or a lease held, is the next run's to finish.
    expireLease(source)
    val rest = invoke(source)
    withClue(rest.err)(rest.exit shouldBe Main.Exit.Ok)
    batchesOf(source).map(_._2).distinct shouldBe Seq("COMMITTED")
    takenFiles(source) shouldBe (1 to 8).map(n => f"f$n%02d.csv")
  }

  test("a signal before the first run ends the process in 128 + the signal, with nothing run") {
    val source = oneAtATime(1)
    val process = CommandProcess.start(arguments(source), files.newLog())

    process.signal("TERM")
    val ended = process.await()

    ended.exit shouldBe 143
    runsOf(source) shouldBe empty
  }
}
