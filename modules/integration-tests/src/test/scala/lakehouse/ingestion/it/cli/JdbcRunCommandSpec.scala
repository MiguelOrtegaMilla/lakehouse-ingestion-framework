package lakehouse.ingestion.it.cli

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.sql.Connection
import java.time.Instant
import java.time.temporal.ChronoUnit

import scala.util.Using

import eu.rekawek.toxiproxy.model.ToxicDirection
import lakehouse.ingestion.cli.{Main, RunSession}
import lakehouse.ingestion.core.config.{EngineColumns, Loaded, Source}
import lakehouse.ingestion.it.demo.SalesOrders
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.TableGenerator.{Change, Window}
import lakehouse.ingestion.it.support.{
  CommandProcess,
  DeploymentFiles,
  LocalSparkSuite,
  ProxiedPostgres
}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The command `run` with a source that is a table of a database (discovery §17, P6.11), as an
 * orchestrator starts it, and each way that it ends: 0 when its batches are committed; 3 when
 * the source is cut for real, by a proxy, before its batch is planned or while its rows are
 * read; 4 for what running again does not mend: a password that the source refuses, a table
 * that is not there, and a value that the type of its column cannot hold.
 *
 * The first test runs the example source of the repository (`conf/sources/sales.orders.yaml`)
 * as it is written, but for the address of its database, on the table that the demo of the
 * README makes (`SalesOrders`): what the README says to do on a machine is done here.
 *
 * The sources that are cut are in a PostgreSQL of their own, behind a proxy; the control plane
 * is in the one of the suite, which no test breaks.
 */
final class JdbcRunCommandSpec extends AnyFunSuite with Matchers with RunCommandQueries {

  private lazy val files = new DeploymentFiles(pg, Files.createTempDirectory("jdbc-run-command-"))
  import files.{arguments, connection, validProfile}

  private var behindAProxy: Option[ProxiedPostgres] = None

  /** A source of its own behind a proxy, started for the tests that cut its connection. */
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

  /**
   * A profile whose steps that fail with an error that passes are tried for four seconds: no
   * shorter than the LogStore waits for its database, three times the timeout of a connection.
   */
  private lazy val hurried: Path = files.profileFile(
    coordination =
      connection("ingest_coordination", pg.password("ingest_coordination"), connect = "1s"),
    retries = Some("4s")
  )

  /** One invocation of a source, in this JVM, with `environment` as its environment. */
  private def launch(
      of: Source,
      profile: Path = validProfile,
      environment: Map[String, String] = Map.empty,
      options: Seq[String] = Nil
  ): Ran = {
    val ran = command(environment)(arguments(of, profile) ++ options: _*)
    // The command leaves the JVM as it found it.
    SparkSession.getDefaultSession shouldBe empty
    ran
  }

  /** A connection as `role` to the database `sales` of the suite, with its password in a file. */
  private def toSales(role: String = "sales_ro", password: Option[String] = None): String =
    connection(role, password.getOrElse(pg.password(role)), url = pg.jdbcUrl("sales"))

  /** Runs `statements` as the owner of the tables of `sales`. */
  private def asOwner(connect: () => Connection)(statements: String*): Unit =
    Using.resource(connect())(c =>
      Using.resource(c.createStatement())(s => statements.foreach(s.execute(_): Unit))
    )

  private def ownerOfSales(): Connection = pg.connect("sales_owner", "sales")

  /** What a table of Bronze has, read by a session of Spark that is closed once it is read. */
  private def bronze[T](table: String)(read: DataFrame => T): T = {
    val session = LocalSparkSuite
      .baseConf(Files.createTempDirectory("jdbc-run-command-read-"), "jdbc-run-command-read")
      .foldLeft(SparkSession.builder()) { case (builder, (key, value)) =>
        builder.config(key, value)
      }
      .getOrCreate()
    try read(session.read.format("delta").load(files.lake.resolve(table).toUri.toString))
    finally RunSession.close(session)
  }

  /** Waits until the clock of the database is past `moment`: the clock of this machine steps. */
  private def awaitDatabasePast(moment: Instant): Unit =
    while (!databaseNow().isAfter(moment)) Thread.sleep(500)

  // ---------------------------------------------------------------------------------------------
  // 0: the example of the repository, as the README runs it
  // ---------------------------------------------------------------------------------------------

  test(
    "the example source runs as it is written: every row of the demo's table once, then only what changed"
  ) {
    val now = Using.resource(ownerOfSales())(SalesOrders.databaseNow)
    val made = SalesOrders.create(() => ownerOfSales(), now.minusSeconds(120))
    val example = exampleSource("sales.orders")
    val here = example.replace("jdbc:postgresql://localhost:5432/sales", pg.jdbcUrl("sales"))
    here should not be example
    Files.write(files.sources.resolve("sales.orders.yaml"), here.getBytes(UTF_8))
    val orders = Source.load("sales.orders.yaml", here) match {
      case Right(Loaded(source, _)) => source
      case Left(errors) => fail(errors.toChain.toList.map(_.message).mkString("; "))
    }
    val environment = Map("SALES_RO_PASSWORD" -> pg.password("sales_ro"))

    val started = System.nanoTime()
    val first = launch(orders, environment = environment)
    val took = (System.nanoTime() - started) / 1000000000L

    withClue(first.err)(first.exit shouldBe Main.Exit.Ok)
    val batches = batchesOf(orders)
    info(s"the first invocation committed ${batches.size} batches in ${took}s")
    batches.map(_._2).distinct shouldBe Seq("COMMITTED")
    first.out.linesIterator.toSeq.last shouldBe
      s"The invocation of sales.orders made ${batches.size} runs and committed " +
      s"${batches.size} batches."
    said(first) shouldBe empty
    // Every row of the table, once: the demo left none behind its window.
    rowsWritten(orders).map(_.toLong).sum shouldBe made.rows.toLong
    bronze("bronze/sales/orders") { rows =>
      rows.count() shouldBe made.rows.toLong
      rows.select("order_id").distinct().count() shouldBe made.rows.toLong
    }

    // What the demo changes next is read by the next invocation, once its margin has passed.
    val later = Using.resource(ownerOfSales())(SalesOrders.databaseNow)
    val window = Window(later.minusSeconds(60), later)
    val round = SalesOrders.change(() => ownerOfSales(), window)
    val changed = round.committed.collect {
      case Change.Insert(row) => row.orderId
      case Change.Update(row) => row.orderId
    }.distinct
    awaitDatabasePast(window.upper.plusSeconds(61))

    val second = launch(orders, environment = environment)

    withClue(second.err)(second.exit shouldBe Main.Exit.Ok)
    batchesOf(orders).size shouldBe batches.size + 1
    rowsWritten(orders).last shouldBe changed.size.toString
  }

  // ---------------------------------------------------------------------------------------------
  // 0: a chain of windows, in a process of its own
  // ---------------------------------------------------------------------------------------------

  test("a process of its own reads a table window by window, and ends in 0") {
    val table = "stamped"
    val now = Using.resource(ownerOfSales())(SalesOrders.databaseNow).truncatedTo(ChronoUnit.DAYS)
    // Three windows of 100 days whatever the day: the last ends at the clock, less the margin.
    val start = now.minus(250, ChronoUnit.DAYS)
    asOwner(() => ownerOfSales())(
      s"CREATE TABLE $table (id int PRIMARY KEY, updated_at timestamptz NOT NULL)",
      Seq(50, 150, 240)
        .map(days => s"(${days}, '${start.plus(days.toLong, ChronoUnit.DAYS)}')")
        .mkString(s"INSERT INTO $table VALUES ", ", ", "")
    )
    val stamped = files.tableSource(
      newSourceId(),
      s"public.$table",
      toSales(),
      load = "{ mode: incremental, watermark: { column: updated_at, safety_margin: 1m, " +
        s"""start: "$start" }, max_batch_range: 100d }"""
    )

    val ended = CommandProcess.start(arguments(stamped), files.newLog()).await()

    withClue(ended.log)(ended.exit shouldBe Main.Exit.Ok)
    ended.log should include(
      s"The invocation of ${stamped.id.value} made 3 runs and committed 3 batches."
    )
    batchesOf(stamped) shouldBe Seq("1" -> "COMMITTED", "2" -> "COMMITTED", "3" -> "COMMITTED")
    rowsWritten(stamped) shouldBe Seq("1", "1", "1")
  }

  // ---------------------------------------------------------------------------------------------
  // 3: the source cut for real
  // ---------------------------------------------------------------------------------------------

  /** A source of `table` in the database behind the proxy, reached through the proxy. */
  private def behindTheProxy(server: ProxiedPostgres, table: String): Source =
    files.tableSource(
      newSourceId(),
      s"public.$table",
      connection(
        "sales_ro",
        server.postgres.password("sales_ro"),
        url = server.faultyJdbcUrl("sales"),
        connect = "1s"
      )
    )

  test(
    "a source that cannot be reached before its batch is planned ends in 3, with nothing planned"
  ) {
    val server = proxied
    asOwner(() => server.observe("sales_owner", "sales"))(
      "CREATE TABLE unreached AS SELECT g AS id FROM generate_series(1, 10) g"
    )
    val unreached = behindTheProxy(server, "unreached")

    server.faulty.disable()
    val cut =
      try launch(unreached, hurried)
      finally server.faulty.enable()

    withClue(cut.err)(cut.exit shouldBe Main.Exit.Transient)
    said(cut).head should include(s"the source ${unreached.id.value}")
    batchesOf(unreached) shouldBe empty
    holderOf(unreached) shouldBe None

    // The source is there again, and the next invocation reads it.
    val done = launch(unreached, hurried)
    withClue(done.err)(done.exit shouldBe Main.Exit.Ok)
    batchesOf(unreached) shouldBe Seq("1" -> "COMMITTED")
    rowsWritten(unreached).last shouldBe "10"
  }

  test("a source cut while its rows are read ends in 3; the next invocation reads its table once") {
    val server = proxied
    asOwner(() => server.observe("sales_owner", "sales"))(
      "CREATE TABLE wide AS SELECT g AS id, repeat('x', 200) AS name " +
        "FROM generate_series(1, 5000) g"
    )
    val wide = behindTheProxy(server, "wide")
    // Every connection is closed by the proxy once that much of what it reads has gone through:
    // those of the plan read less, and the one of the rows more.
    server.faulty.toxics().limitData("cut", ToxicDirection.DOWNSTREAM, 100000L): Unit

    val cut =
      try launch(wide, hurried)
      finally server.faulty.toxics().get("cut").remove()

    withClue(cut.err)(cut.exit shouldBe Main.Exit.Transient)
    said(cut).head should include(s"the source ${wide.id.value}")
    batchesOf(wide) shouldBe Seq("1" -> "PLANNED")
    runsOf(wide) shouldBe Seq("FAILED" -> Some("transient"))
    holderOf(wide) shouldBe None

    val done = launch(wide, hurried)
    withClue(done.err)(done.exit shouldBe Main.Exit.Ok)
    // The batch that was planned is read whole, once. It is a snapshot of the table as it is
    // by now, which is what a plan made anew would read: the invocation ends with it, and
    // the table is not read a second time (P6.12).
    done.out.linesIterator.toSeq should have size 1
    batchesOf(wide) shouldBe Seq("1" -> "COMMITTED")
    rowsWritten(wide) shouldBe Seq("5000")
    bronze(s"bronze/${wide.id.value.replace('.', '/')}")(
      _.groupBy(EngineColumns.BatchSeq)
        .count()
        .collect()
        .map(row => row.getLong(0) -> row.getLong(1))
        .toSet
    ) shouldBe Set(1L -> 5000L)
  }

  // ---------------------------------------------------------------------------------------------
  // 4: what running again does not mend
  // ---------------------------------------------------------------------------------------------

  test("a password that the source refuses ends in 4, with nothing planned and no password said") {
    asOwner(() => ownerOfSales())("CREATE TABLE guarded (id int)")
    val wrong = "not-the-password-5e2a"
    val guarded =
      files.tableSource(newSourceId(), "public.guarded", toSales(password = Some(wrong)))

    val refused = launch(guarded)

    withClue(refused.err)(refused.exit shouldBe Main.Exit.Permanent)
    said(refused).head should include("28P01")
    (refused.out + refused.err) should not include wrong
    batchesOf(guarded) shouldBe empty
    runsOf(guarded) shouldBe Seq("FAILED" -> Some("permanent"))
    holderOf(guarded) shouldBe None
  }

  test("a table that is not there ends in 4, with nothing planned") {
    val nowhere = files.tableSource(newSourceId(), "public.never_made", toSales())

    val refused = launch(nowhere)

    withClue(refused.err)(refused.exit shouldBe Main.Exit.Permanent)
    said(refused).head should include("""relation "public.never_made" does not exist""")
    batchesOf(nowhere) shouldBe empty
    runsOf(nowhere) shouldBe Seq("FAILED" -> Some("permanent"))
    holderOf(nowhere) shouldBe None
  }

  test("a value that the type of its column cannot hold ends in 4, its batch kept for text") {
    asOwner(() => ownerOfSales())(
      "CREATE TABLE contracts (id int, valid_to date)",
      "INSERT INTO contracts VALUES (1, '2027-01-01'), (2, 'infinity')"
    )
    val contracts = files.tableSource(newSourceId(), "public.contracts", toSales())

    val refused = launch(contracts)

    // A value is seen when the batch is read, after its plan: the batch waits for the
    // configuration that the failure names, and no run reads another before it.
    withClue(refused.err)(refused.exit shouldBe Main.Exit.Permanent)
    said(refused).head should include("has infinity or -infinity in a row of the batch")
    said(refused).head should include("connector.text_columns")
    batchesOf(contracts) shouldBe Seq("1" -> "PLANNED")
    runsOf(contracts) shouldBe Seq("FAILED" -> Some("permanent"))
    holderOf(contracts) shouldBe None

    val asText = files.tableSource(
      contracts.id,
      "public.contracts",
      toSales(),
      textColumns = Seq("valid_to")
    )
    val done = launch(asText)
    withClue(done.err)(done.exit shouldBe Main.Exit.Ok)
    // The batch that was planned is read, as a snapshot of the table as it is by now, and the
    // invocation ends with it.
    done.out.linesIterator.toSeq should have size 1
    batchesOf(contracts) shouldBe Seq("1" -> "COMMITTED")
    rowsWritten(contracts) shouldBe Seq("2")
  }

  test(
    "a window that a run before left is followed by a plan made anew, which --max-batches may stop"
  ) {
    val start = Using
      .resource(ownerOfSales())(SalesOrders.databaseNow)
      .truncatedTo(ChronoUnit.DAYS)
      .minus(10, ChronoUnit.DAYS)
    asOwner(() => ownerOfSales())(
      "CREATE TABLE leases (id int PRIMARY KEY, ends_on date, updated_at timestamptz NOT NULL)",
      s"INSERT INTO leases VALUES (1, 'infinity', '${start.plus(1, ChronoUnit.DAYS)}'), " +
        s"(2, '2027-01-01', '${start.plus(2, ChronoUnit.DAYS)}')"
    )
    val load = "{ mode: incremental, watermark: { column: updated_at, safety_margin: 1m, " +
      s"""start: "$start" } }"""
    val leases = files.tableSource(newSourceId(), "public.leases", toSales(), load = load)
    // The window is planned, and its batch waits for the column to be read as text.
    launch(leases).exit shouldBe Main.Exit.Permanent
    batchesOf(leases) shouldBe Seq("1" -> "PLANNED")
    val asText = files.tableSource(
      leases.id,
      "public.leases",
      toSales(),
      load = load,
      textColumns = Seq("ends_on")
    )

    // The window of the run before stops where it was planned to. With one batch allowed,
    // the invocation stops after it, and does not say that it knows what is after.
    val limited = launch(asText, options = Seq("--max-batches", "1"))
    withClue(limited.err)(limited.exit shouldBe Main.Exit.Ok)
    limited.out.linesIterator.toSeq.last shouldBe
      s"The invocation of ${leases.id.value} made 1 run and committed 1 batch: it stopped at " +
      "--max-batches 1, after a batch that a run before this invocation left: whether the " +
      "source has more is for the next invocation to plan."
    batchesOf(asText) shouldBe Seq("1" -> "COMMITTED")
    rowsWritten(asText) shouldBe Seq("2")

    // Without the limit, the same batch is followed by a plan made anew: a window is not
    // its source whole, however late it is read.
    asOwner(() => ownerOfSales())("CREATE TABLE leases_too AS SELECT * FROM leases")
    val too = files.tableSource(newSourceId(), "public.leases_too", toSales(), load = load)
    launch(too).exit shouldBe Main.Exit.Permanent
    val tooAsText = files.tableSource(
      too.id,
      "public.leases_too",
      toSales(),
      load = load,
      textColumns = Seq("ends_on")
    )
    val done = launch(tooAsText)
    withClue(done.err)(done.exit shouldBe Main.Exit.Ok)
    // The second run plans as the first of an invocation does. Its horizon is the clock of
    // the source less the margin, which is past that of the plan that was left by now.
    runsOf(tooAsText).count(_._1 == "SUCCEEDED") shouldBe 2
  }
}
