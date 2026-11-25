package lakehouse.ingestion.it.cli

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, StandardOpenOption}
import java.sql.Connection
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import scala.util.Using

import eu.rekawek.toxiproxy.model.ToxicDirection
import lakehouse.ingestion.cli.{Main, RunSession, Stop}
import lakehouse.ingestion.core.config.{EngineColumns, Source}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.control.{Backfills, LeasedRun}
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.{DeploymentFiles, LocalSparkSuite, ProxiedPostgres}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The command `backfill` from end to end, as an operator runs it (ADR-0021 §5, P7.7): the
 * configuration of a deployment, a table of a real PostgreSQL, the real control plane and
 * LogStore, what it says, and the exit code of each of its outcomes (discovery §14.1).
 * `BackfillSpec` has what a backfill does to a source, with the ledger of every sequence of it;
 * here are its runs chained by the command, what it says of them, and how it ends.
 *
 * The rows are stamped in 2025, far behind the clock of the server: what a chunk reads is
 * decided by the range and the chunk alone. The sources that are cut are in a PostgreSQL of
 * their own, behind a proxy.
 */
final class BackfillCommandSpec
    extends AnyFunSuite
    with Matchers
    with RunCommandQueries
    with CommandLedger {

  private lazy val files = new DeploymentFiles(pg, Files.createTempDirectory("backfill-command-"))
  override protected def deployment: DeploymentFiles = files
  import files.{arguments, connection, validProfile}

  private var behindAProxy: Option[ProxiedPostgres] = None

  /** A source of its own behind a proxy, started for the test that cuts its connection. */
  private def proxied: ProxiedPostgres = behindAProxy.getOrElse {
    val started = ProxiedPostgres.start()
    behindAProxy = Some(started)
    started
  }

  override def afterAll(): Unit =
    try behindAProxy.foreach(_.stop())
    finally
      try files.close()
      finally super.afterAll()

  /** A profile whose steps that fail with an error that passes are tried for four seconds. */
  private lazy val hurried: Path = files.profileFile(
    coordination =
      connection("ingest_coordination", pg.password("ingest_coordination"), connect = "1s"),
    retries = Some("4s")
  )

  private val asked = OperatorRequest.of("ops.oncall", "the history before go-live").toOption.get

  private def day(n: Int): Instant = Instant.parse(f"2025-01-$n%02dT00:00:00Z")
  private val From = day(1)
  private val To = day(4)

  /** The rows of a table of changes: one at from, which is not read, one inside, one at to. */
  private val Rows = Seq(1 -> From, 2 -> day(2).plusSeconds(43200), 3 -> To, 4 -> day(5))

  private def ownerOfSales(): Connection = pg.connect("sales_owner", "sales")

  private def asOwner(connect: () => Connection)(statements: String*): Unit =
    Using.resource(connect())(c =>
      Using.resource(c.createStatement())(s => statements.foreach(s.execute(_): Unit))
    )

  private def tableOf(of: Source): String = of.id.value.replace('.', '_')

  /**
   * A source that reads a table of changes of its own by `updated_at`.
   *
   * @param start its `load.watermark.start`: at `to` unless said, where the regular sequence
   *   reads nothing of the range
   * @param widest its `max_batch_range`; none, unless said
   */
  private def changes(start: Option[Instant] = Some(To), widest: Option[String] = None): Source = {
    val id = newSourceId()
    val table = id.value.replace('.', '_')
    asOwner(() => ownerOfSales())(
      s"CREATE TABLE $table (id int PRIMARY KEY, updated_at timestamptz NOT NULL)",
      Rows.map { case (n, at) => s"($n, '$at')" }.mkString(s"INSERT INTO $table VALUES ", ", ", "")
    )
    files.tableSource(
      id,
      s"public.$table",
      connection("sales_ro", pg.password("sales_ro"), url = pg.jdbcUrl("sales")),
      load = "{ mode: incremental, watermark: { column: updated_at, safety_margin: 1m" +
        start.fold("")(at => s""", start: "$at"""") + " }" +
        widest.fold("")(range => s", max_batch_range: $range") + " }"
    )
  }

  /** One invocation of `backfill`, in this JVM, as an operator writes it. */
  private def backfill(
      of: Source,
      from: Instant = From,
      to: Instant = To,
      chunk: Option[String] = Some("1d"),
      id: String = "history",
      more: Seq[String] = Nil,
      stop: Stop = Stop.Never,
      profile: Path = validProfile
  ): Ran = {
    val (out, err) = (new ByteArrayOutputStream, new ByteArrayOutputStream)
    val exit = Main.run(
      Seq(
        "backfill",
        "--profile",
        profile.toString,
        "--sources",
        files.sources.toString,
        "--source",
        of.id.value,
        "--backfill",
        id,
        "--from",
        from.toString,
        "--to",
        to.toString,
        "--reason",
        "the history before go-live"
      ) ++ chunk.toSeq.flatMap(length => Seq("--chunk", length)) ++ more,
      new PrintStream(out, true, "UTF-8"),
      new PrintStream(err, true, "UTF-8"),
      Map.empty[String, String].get,
      stop
    )
    // The command leaves the JVM as it found it.
    SparkSession.getDefaultSession shouldBe empty
    Ran(exit, out.toString("UTF-8"), err.toString("UTF-8"))
  }

  private def backfillId(id: String): BackfillId = BackfillId.parse(id).toOption.get

  private def recordOf(of: Source, id: String = "history"): Option[BackfillRecord] =
    database.run(Backfills.find(_, of.id, Layer.Bronze, backfillId(id)))

  /** The chunks of a backfill: each number with its status, and the run that committed it. */
  private def chunksOf(of: Source, id: String = "history"): Seq[(Long, String, Option[String])] =
    asSuperuser { c =>
      query(
        c,
        "SELECT batch_seq::text, status, committed_by_run::text FROM control.ingestion_batch " +
          "WHERE txn_app_id = ? ORDER BY batch_seq",
        TxnAppId.backfill(of.id, Layer.Bronze, backfillId(id)).value
      ).map(row => (row(0).toLong, row(1), Option(row(2))))
    }

  /** Who asked for each run of the backfill, and why. */
  private def requestsOf(of: Source, id: String = "history"): Seq[(String, String)] =
    asSuperuser { c =>
      query(
        c,
        "SELECT requested_by, reason FROM control.ingestion_run WHERE txn_app_id = ?",
        TxnAppId.backfill(of.id, Layer.Bronze, backfillId(id)).value
      ).map(row => row(0) -> row(1))
    }

  /** What a table of Bronze has, read by a session of Spark that is closed once it is read. */
  private def bronze[T](of: Source)(read: DataFrame => T): T = {
    val session = LocalSparkSuite
      .baseConf(Files.createTempDirectory("backfill-command-read-"), "backfill-command-read")
      .foldLeft(SparkSession.builder()) { case (builder, (key, value)) =>
        builder.config(key, value)
      }
      .getOrCreate()
    val table = files.lake.resolve(s"bronze/${of.id.value.replace('.', '/')}")
    try read(session.read.format("delta").load(table.toUri.toString))
    finally RunSession.close(session)
  }

  /** How many copies of each row of the table Bronze has, by its id. */
  private def copiesOf(of: Source): Map[Int, Long] =
    bronze(of)(
      _.groupBy("id").count().collect().map(row => row.getInt(0) -> row.getLong(1)).toMap
    )

  private def stateOf(of: Source): Seq[Seq[String]] = asSuperuser { c =>
    query(c, "SELECT source_id FROM control.source_state WHERE source_id = ?", of.id.value)
  }

  /** What the command says of what the backfill reads, once a run has created it or resumed it. */
  private def reads(chunk: String = "1d"): String =
    s"it reads ($From, $To] of updated_at, in chunks of $chunk. The rows whose updated_at is " +
      s"$From exactly are not read; those at $To are."

  // ---------------------------------------------------------------------------------------------
  // 0: a range read in chunks, created and resumed
  // ---------------------------------------------------------------------------------------------

  test("an invocation creates a backfill and reads (from, to] a chunk a run, to its end: 0") {
    val orders = changes()

    val ran = backfill(orders)

    withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
    val record = recordOf(orders).value
    record.completedAt should not be empty
    ran.lines.head shouldBe
      s"The run ${record.createdBy} of ${orders.id} created the backfill history: ${reads()}"
    val chunks = chunksOf(orders)
    chunks.map(chunk => (chunk._1, chunk._2)) shouldBe
      Seq(1L -> "COMMITTED", 2L -> "COMMITTED", 3L -> "COMMITTED")
    chunks.foreach { case (number, _, run) =>
      ran.out should include(
        s"The run ${run.value} of ${orders.id} committed the chunk $number of the backfill " +
          "history: "
      )
    }
    ran.lines.takeRight(2) shouldBe Seq(
      s"The backfill history of ${orders.id} is complete: it has read ($From, $To].",
      s"The invocation of the backfill history of ${orders.id} made 3 runs and committed 3 chunks."
    )
    // The range ends at start: the regular sequence reads none of it, and nothing is warned of.
    ran.errors.filter(_.contains("the regular sequence")) shouldBe empty
    said(ran) shouldBe empty
    // The row at from is not read, and the one at to is; the state of the source is not touched.
    copiesOf(orders) shouldBe Map(2 -> 1L, 3 -> 1L)
    stateOf(orders) shouldBe empty
    requestsOf(orders).distinct shouldBe
      Seq(sys.props.getOrElse("user.name", "") -> "the history before go-live")
    assertLedger(orders)
  }

  test(
    "--max-batches stops a backfill with more to read; the next resumes it, then it is complete"
  ) {
    val orders = changes()

    val first = backfill(orders, more = Seq("--max-batches", "2"))

    withClue(first.err)(first.exit shouldBe Main.Exit.Ok)
    first.lines.last shouldBe
      s"The invocation of the backfill history of ${orders.id} made 2 runs and committed 2 " +
      "chunks: it stopped at --max-batches 2, and the backfill has more to read: the next " +
      "invocation resumes it."
    val created = recordOf(orders).value
    created.completedAt shouldBe None

    // The next asks for no chunk: it takes the backfill's, and reads the last.
    val second = backfill(orders, chunk = None)

    withClue(second.err)(second.exit shouldBe Main.Exit.Ok)
    val last = chunksOf(orders).last
    last._1 shouldBe 3L
    second.lines.head shouldBe
      s"The run ${last._3.value} of ${orders.id} resumed the backfill history, created by the " +
      s"run ${created.createdBy} at ${created.createdAt}: ${reads()}"
    second.lines.last shouldBe
      s"The backfill history of ${orders.id} is complete: it has read ($From, $To]."
    recordOf(orders).value.completedAt should not be empty

    // The command run again, as an orchestrator would, reads nothing and ends well.
    val again = backfill(orders)

    withClue(again.err)(again.exit shouldBe Main.Exit.Ok)
    again.lines should have size 1
    again.lines.head should startWith(
      s"The backfill history of ${orders.id} is complete: it has read ($From, $To]. The run "
    )
    again.lines.head should endWith(" read nothing, and ended well.")
    chunksOf(orders) should have size 3
    copiesOf(orders) shouldBe Map(2 -> 1L, 3 -> 1L)
  }

  test("what the regular runs read of the range too is warned of, and is in Bronze twice") {
    val crossing = changes(start = Some(day(3)))

    val ran = backfill(crossing)

    withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
    ran.errors should contain(
      s"warning: the regular sequence of ${crossing.id} reads the rows whose updated_at is " +
        s"after ${day(3)}, its load.watermark.start: (${day(3)}, $To] of ($From, $To] is read " +
        "by both, and is in Bronze twice, each copy with its run, unless a reset of the state " +
        "took the watermark of the source past it."
    )
    // The regular run reads what is after start: the row at to is there once for each.
    val regular = command(Map.empty)(arguments(crossing): _*)
    withClue(regular.err)(regular.exit shouldBe Main.Exit.Ok)
    copiesOf(crossing) shouldBe Map(2 -> 1L, 3 -> 2L, 4 -> 1L)

    // A source with no start has every row read by its regular sequence.
    val whole = changes(start = None)
    backfill(whole).errors should contain(
      s"warning: the regular sequence of ${whole.id} reads every row of updated_at, as it has " +
        s"no load.watermark.start: all of ($From, $To] is read by both, and is in Bronze " +
        "twice, each copy with its run, unless a reset of the state took the watermark of the " +
        "source past it."
    )
  }

  test("an operator backfills a source that is disabled") {
    val paused = changes()
    Files.write(
      files.sources.resolve(s"${paused.id}.yaml"),
      "enabled: false\n".getBytes(UTF_8),
      StandardOpenOption.APPEND
    )

    val ran = backfill(paused)

    withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
    recordOf(paused).value.completedAt should not be empty
  }

  // ---------------------------------------------------------------------------------------------
  // 4: refused, with nothing planned
  // ---------------------------------------------------------------------------------------------

  test("a backfill that is refused ends in 4, with nothing planned and why said") {
    val orders = changes()
    backfill(orders, more = Seq("--max-batches", "1")).exit shouldBe Main.Exit.Ok
    def refused(ran: Ran, because: String): Unit = {
      withClue(ran.err)(ran.exit shouldBe Main.Exit.Permanent)
      said(ran).head should include(because)
      ran.lines shouldBe empty: Unit
    }

    // Another range, or another chunk, under the id of a backfill that is there.
    refused(backfill(orders, to = day(3)), s"this run asks for the range ($From, ${day(3)}]")
    refused(backfill(orders, chunk = Some("12h")), "chunks of 12h, and its chunks are of 1d")
    chunksOf(orders).map(chunk => (chunk._1, chunk._2)) shouldBe Seq(1L -> "COMMITTED")

    // A range that has not settled, and one with no chunk, under ids of their own.
    val tomorrow = Instant.now().plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.SECONDS)
    refused(
      backfill(orders, to = tomorrow, id = "unsettled"),
      "the clock of the source less safety_margin"
    )
    refused(backfill(orders, chunk = None, id = "unchunked"), "it is given no chunk (--chunk)")
    recordOf(orders, "unsettled") shouldBe None
    recordOf(orders, "unchunked") shouldBe None

    // A source that is read whole has no range to read.
    val whole = files.tableSource(
      newSourceId(),
      s"public.${tableOf(orders)}",
      connection("sales_ro", pg.password("sales_ro"), url = pg.jdbcUrl("sales"))
    )
    refused(backfill(whole), "its connector reads no range of it")
  }

  // ---------------------------------------------------------------------------------------------
  // 2: the lease of the backfill, and not that of the source
  // ---------------------------------------------------------------------------------------------

  test("while a run holds the lease of the backfill the command ends in 2, and plans nothing") {
    val orders = changes()
    val holder = LeasedRun
      .open(
        database,
        starting(orders).copy(
          kind = RunKind.Backfill,
          request = Some(asked),
          backfill = Some(backfillId("history"))
        ),
        versionOf(orders),
        LeaseTiming(2.minutes)
      )
      .value

    val ran =
      try backfill(orders)
      finally holder.close()

    withClue(ran.err)(ran.exit shouldBe Main.Exit.SkippedLocked)
    ran.lines should have size 1
    ran.lines.head should include(
      s"was skipped: the run ${holder.start.id} holds the lease of the backfill history, until "
    )
    ran.lines.head should endWith(" at the latest. Nothing was read or written.")
    chunksOf(orders) shouldBe empty
  }

  test("a run that holds the lease of the source does not stop a backfill of it: 0") {
    val orders = changes()
    val regular =
      LeasedRun.open(database, starting(orders), versionOf(orders), LeaseTiming(2.minutes)).value

    val ran =
      try backfill(orders)
      finally regular.close()

    withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
    chunksOf(orders).map(_._2).distinct shouldBe Seq("COMMITTED")
  }

  // ---------------------------------------------------------------------------------------------
  // Signals, and 3
  // ---------------------------------------------------------------------------------------------

  test("a signal before the first run ends in 128 + n; one after a chunk ends the chain in 0") {
    val orders = changes()
    val now = new Stop { override def requested: Option[Int] = Some(2) }

    val never = backfill(orders, stop = now)

    never.exit shouldBe 130
    said(never).last shouldBe "error: SIGINT came before the first run. Nothing was run."
    recordOf(orders) shouldBe None

    // Asked before each run: nothing before the first, SIGTERM before the second.
    val times = new AtomicInteger
    val afterOne = new Stop {
      override def requested: Option[Int] = Option.when(times.getAndIncrement() > 0)(15)
    }

    val stopped = backfill(orders, stop = afterOne)

    withClue(stopped.err)(stopped.exit shouldBe Main.Exit.Ok)
    stopped.lines.last shouldBe
      s"The invocation of the backfill history of ${orders.id} made 1 run and committed 1 " +
      "chunk: SIGTERM asked it to stop, and no other run was started."
    chunksOf(orders).map(chunk => (chunk._1, chunk._2)) shouldBe Seq(1L -> "COMMITTED")
  }

  test("a source cut while a chunk is read ends in 3; the next invocation reads the chunk once") {
    val server = proxied
    val id = newSourceId()
    val table = id.value.replace('.', '_')
    asOwner(() => server.observe("sales_owner", "sales"))(
      s"CREATE TABLE $table AS SELECT g AS id, repeat('x', 200) AS name, " +
        "timestamptz '2025-01-01T12:00:00Z' AS updated_at FROM generate_series(1, 5000) g"
    )
    val wide = files.tableSource(
      id,
      s"public.$table",
      connection(
        "sales_ro",
        server.postgres.password("sales_ro"),
        url = server.faultyJdbcUrl("sales"),
        connect = "1s"
      ),
      load = "{ mode: incremental, watermark: { column: updated_at, safety_margin: 1m } }"
    )
    // Every connection is closed by the proxy once that much of what it reads has gone through:
    // those of the plan read less, and the one of the rows more.
    server.faulty.toxics().limitData("cut", ToxicDirection.DOWNSTREAM, 100000L): Unit

    val cut =
      try backfill(wide, to = day(2), profile = hurried)
      finally server.faulty.toxics().get("cut").remove()

    withClue(cut.err)(cut.exit shouldBe Main.Exit.Transient)
    // It created the backfill, and says so, before it says why its chunk was not read.
    cut.lines.head should include("created the backfill history")
    atLeast(1, said(cut)) should include(s"the source ${wide.id.value}")
    chunksOf(wide).map(chunk => (chunk._1, chunk._2)) shouldBe Seq(1L -> "PLANNED")

    val done = backfill(wide, to = day(2), profile = hurried)

    withClue(done.err)(done.exit shouldBe Main.Exit.Ok)
    chunksOf(wide).map(chunk => (chunk._1, chunk._2)) shouldBe Seq(1L -> "COMMITTED")
    bronze(wide)(
      _.groupBy(EngineColumns.BatchSeq)
        .count()
        .collect()
        .map(row => row.getLong(0) -> row.getLong(1))
        .toSet
    ) shouldBe Set(1L -> 5000L)
    assertLedger(wide)
  }
}
