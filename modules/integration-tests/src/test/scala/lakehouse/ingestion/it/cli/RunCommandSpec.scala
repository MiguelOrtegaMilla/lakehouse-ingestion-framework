package lakehouse.ingestion.it.cli

import java.net.ServerSocket
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Using

import lakehouse.ingestion.cli.Main
import lakehouse.ingestion.core.config.{Profile, Source}
import lakehouse.ingestion.core.control.LeaseTiming
import lakehouse.ingestion.engine.control.LeasedRun
import lakehouse.ingestion.it.control.ControlPlaneSuite
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.LocalSparkSuite
import org.apache.spark.launcher.JavaModuleOptions
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

  private lazy val work: Path = Files.createTempDirectory("run-command-")
  private lazy val lakeDirectory = Files.createDirectories(work.resolve("lake"))
  private lazy val landingDirectory = Files.createDirectories(work.resolve("landing"))
  private lazy val sourcesDirectory = Files.createDirectories(work.resolve("sources"))
  private lazy val secrets = Files.createDirectories(work.resolve("secrets"))

  override def afterAll(): Unit =
    try LocalSparkSuite.deleteRecursively(work)
    finally super.afterAll()

  /** A file of the secrets of the test: a deployment has them mounted as files. */
  private def secret(name: String, value: String): String = {
    val file = secrets.resolve(name)
    Files.write(file, value.getBytes(UTF_8))
    s"file:$file"
  }

  private def connection(
      role: String,
      password: String,
      url: String = pg.jdbcUrl(Database),
      connect: String = "5s"
  ): String =
    s"""    url: "$url"
       |    user: $role
       |    password: { secretRef: "${secret(s"$role-${password.hashCode}", password)}" }
       |    tls: { mode: disable }
       |    timeouts: { connect: $connect, statement: 10s }
       |""".stripMargin

  /**
   * The profile of the suite, in a file: its lake and landing zone, and both databases in the
   * PostgreSQL of the suite, each reached as its own role with its password in a file.
   *
   * @param control the connection to the control plane, as it is written under it
   * @param coordination that to the coordination database
   */
  private def profileFile(
      control: String = connection(Engine, pg.password(Engine)),
      coordination: String = connection("ingest_coordination", pg.password("ingest_coordination")),
      lake: Path = lakeDirectory
  ): Path = {
    val file = Files.createTempFile(work, "profile-", ".yaml")
    Files.write(
      file,
      s"""lake: { uri: "${lake.toUri}", deployment: single-machine }
         |landing: { root: "${landingDirectory.toUri}" }
         |databases:
         |  control:
         |$control  coordination:
         |$coordination
         |spark:
         |  master: "local[2]"
         |  settings:
         |    spark.ui.enabled: false
         |    spark.sql.shuffle.partitions: 2
         |    spark.default.parallelism: 2
         |    spark.databricks.delta.snapshotPartitions: 2
         |    spark.sql.session.timeZone: UTC
         |    spark.driver.host: 127.0.0.1
         |    spark.driver.bindAddress: 127.0.0.1
         |    spark.sql.warehouse.dir: "${work.resolve("warehouse")}"
         |""".stripMargin.getBytes(UTF_8)
    )
  }

  private lazy val validProfile = profileFile()

  /**
   * A source of CSV files of its own, in the directory of the sources of the suite, with its
   * directory in the landing zone.
   */
  private def fileSource(rejected: Option[String] = None): Source = {
    val id = newSourceId().value
    val directory = id.replace('.', '_')
    Files.createDirectories(landingDirectory.resolve(directory))
    val text =
      s"""id: $id
         |connector: { type: file, path: $directory, format: csv }
         |load: { mode: new_files }
         |targets: { bronze: { table: bronze/${id.replace('.', '/')} } }
         |${rejected.fold("")(share => s"quality: { max_rejected_ratio: $share }")}
         |""".stripMargin
    Files.write(sourcesDirectory.resolve(s"$id.yaml"), text.getBytes(UTF_8))
    Source.load(s"$id.yaml", text).value.value
  }

  /** A file of the source, last changed an hour ago: complete for a run that starts now. */
  private def drop(of: Source, name: String, text: String): Unit = {
    val path = landingDirectory.resolve(of.id.value.replace('.', '_')).resolve(name)
    Files.write(path, text.getBytes(UTF_8))
    Files.setLastModifiedTime(path, FileTime.from(Instant.now().minusSeconds(3600))): Unit
  }

  private def arguments(of: Source, profile: Path): Seq[String] = Seq(
    "run",
    "--profile",
    profile.toString,
    "--sources",
    sourcesDirectory.toString,
    "--source",
    of.id.value
  )

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
    val log = lakeDirectory.resolve(s"bronze/${of.id.value.replace('.', '/')}/_delta_log")
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

  /**
   * Runs the command in a JVM of its own, as `cli/run` starts it: with the classpath of this
   * one, which has Spark, and the options of a JVM that embeds Spark.
   *
   * @return its exit code, and what it printed
   */
  private def process(of: Source): (Int, String) = {
    val log = Files.createTempFile(work, "process-", ".log")
    val command =
      Seq(Paths.get(sys.props("java.home"), "bin", "java").toString) ++
        JavaModuleOptions.defaultModuleOptionArray().toSeq ++
        Seq("-Xmx768m", "-Duser.timezone=UTC", "-cp", sys.props("java.class.path")) ++
        Seq(Main.getClass.getName.stripSuffix("$")) ++
        arguments(of, validProfile)
    val builder = new ProcessBuilder(command.asJava)
      .redirectErrorStream(true)
      .redirectOutput(log.toFile)
    builder.environment().put("SPARK_LOCAL_IP", "127.0.0.1")
    val started = builder.start()
    if (!started.waitFor(3, TimeUnit.MINUTES)) {
      started.destroyForcibly()
      fail(s"the process did not end in 3 minutes:\n${tail(log)}")
    }
    (started.exitValue(), tail(log))
  }

  private def tail(log: Path): String =
    Files.readAllLines(log, UTF_8).asScala.takeRight(40).mkString("\n")
}
