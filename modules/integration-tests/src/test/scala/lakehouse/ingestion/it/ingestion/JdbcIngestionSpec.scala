package lakehouse.ingestion.it.ingestion

import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import java.time.{Instant, OffsetDateTime, ZoneOffset}

import scala.util.Using

import eu.rekawek.toxiproxy.model.ToxicDirection
import io.circe.Json
import lakehouse.ingestion.core.config.{QualifiedName, Source}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.core.jdbc.FullLoadPlan
import lakehouse.ingestion.core.watermark.WatermarkPlan
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.FaultPoint
import lakehouse.ingestion.it.support.ProxiedPostgres
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

/**
 * A run of a table of a database from end to end (P6.6, P6.7): the protocol of ADR-0005 with
 * the JDBC connector, on a real PostgreSQL as its source, a real Delta on the coordinated
 * LogStore, and a real control plane. A table is loaded whole, as one snapshot of it, and by a
 * watermark, as the windows of the clock of its source (ADR-0020).
 *
 * The clock of a source is that of its server, which no test sets. A test of a window either
 * has its bounds decided by the configuration alone, a start and a range that are months
 * behind any clock, or stamps its rows by the bounds that a plan recorded; and it waits for a
 * time by asking the server, and never by its own count.
 *
 * The source is the demo database `sales`, read as `sales_ro`, in the server of the suite. What
 * goes wrong is made to go wrong for real: a statement that the server cuts for running too
 * long, a connection that a proxy cuts while its rows are read, a row that Bronze cannot keep,
 * and a run that dies at a fault point of the engine.
 */
final class JdbcIngestionSpec
    extends AnyFunSuite
    with Matchers
    with Eventually
    with JdbcIngestionSuite {

  import JdbcIngestionSpec._

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(100, Millis))

  private var behindAProxy: Option[ProxiedPostgres] = None

  /** A source of its own behind a proxy, started for the test that cuts its connection. */
  private def proxied: ProxiedPostgres = behindAProxy.getOrElse {
    val started = ProxiedPostgres.start()
    behindAProxy = Some(started)
    started
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Using.resource(pg.connectAsSuperuser("sales")) { c =>
      execute(c, "ALTER ROLE sales_ro IN DATABASE sales SET log_statement = 'all'")
    }
  }

  override protected def stopServers(): Unit =
    try behindAProxy.foreach(_.stop())
    finally super.stopServers()

  // ---------------------------------------------------------------------------------------------
  // A source of a table, and a run of it
  // ---------------------------------------------------------------------------------------------

  /** With the password of the server behind the proxy, once a test has started it. */
  override protected def passwords: Map[String, String] =
    super.passwords ++
      behindAProxy.map(server => "PROXIED_PASSWORD" -> server.postgres.password("sales_ro"))

  /**
   * A source that reads `table` of `sales` whole, under an id of its own unless it is given
   * one.
   *
   * @param statement the longest that a statement of it may run; that of a source, unless said
   */
  private def tableSource(
      table: String,
      id: String = newSourceId().value,
      statement: Option[String] = None,
      url: String = pg.jdbcUrl("sales"),
      password: String = "env:SALES_RO_PASSWORD",
      textColumns: Seq[String] = Nil
  ): Source =
    jdbcSource(
      table,
      id = id,
      url = url,
      password = Some(password),
      statement = statement,
      textColumns = textColumns
    )

  /**
   * A source that reads `table` of `sales` by the watermark `updated_at`.
   *
   * @param watermark the settings of the watermark after its column, as a file writes them
   * @param range the widest stretch of the watermark that a batch takes; none, unless said
   */
  private def watermarkSource(
      table: String,
      id: String = newSourceId().value,
      column: String = "updated_at",
      watermark: String = "safety_margin: 1h",
      range: Option[String] = None
  ): Source =
    jdbcSource(
      table,
      load = s"""load:
                 |  mode: incremental
                 |  watermark: { column: $column, $watermark }
                 |  ${range.fold("")(widest => s"max_batch_range: $widest")}""".stripMargin,
      id = id
    )

  /** A table of changes, as a load by a watermark reads one: each row with when it changed. */
  private def changesTable(name: String): Unit =
    asOwner(
      execute(
        _,
        s"CREATE TABLE $name (id int PRIMARY KEY, name text, updated_at timestamptz NOT NULL)"
      )
    )

  /** A row of a table of changes, stamped as the test says and not by the clock of the server. */
  private def stamped(table: String, id: Int, name: String, at: Instant): Unit =
    asOwner { c =>
      Using.resource(
        c.prepareStatement(
          s"INSERT INTO $table (id, name, updated_at) VALUES (?, ?, ?) " +
            "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, updated_at = EXCLUDED.updated_at"
        )
      ) { statement =>
        statement.setInt(1, id)
        statement.setString(2, name)
        statement.setObject(3, OffsetDateTime.ofInstant(at, ZoneOffset.UTC))
        statement.executeUpdate(): Unit
      }
    }

  /** What the server of the source says the time is. */
  private def clockOfTheSource(): Instant =
    asOwner { c =>
      Using.resource(c.createStatement()) { statement =>
        val rows = statement.executeQuery("SELECT CURRENT_TIMESTAMP")
        rows.next()
        rows.getObject(1, classOf[OffsetDateTime]).toInstant
      }
    }

  /** The window that a batch was planned to read, as the write-ahead log has it. */
  private def windowOf(batch: BatchId): WatermarkPlan =
    WatermarkPlan.fromJson(batchOf(batch).plan).value

  /** A bound as Spark writes it into a statement, in UTC. */
  private def literal(bound: Instant): String = Timestamp.from(bound).toString

  /** The rows of Bronze, each by its id, its name and the batch that wrote it. */
  private def snapshots(of: Source): Seq[(Int, String, Long)] =
    spark.read
      .format("delta")
      .load(bronze(of).uri)
      .select("id", "name", "_batch_seq")
      .collect()
      .map(row => (row.getInt(0), row.getString(1), row.getLong(2)))
      .sortBy(row => (row._3, row._1))
      .toSeq

  private def failed(outcome: Outcome): Outcome.Failed = outcome match {
    case failure: Outcome.Failed => failure
    case other => fail(s"the run did not fail: $other")
  }

  /** Whether the table of a source has a batch of it, by what its log says. */
  private def hasABatch(of: Source): Boolean =
    delta.lastVersion(bronze(of), TxnAppId.of(of.id, Layer.Bronze)).nonEmpty

  private def dies(of: Source, at: FaultPoint): Unit = {
    intercept[Died](launch(of, reading(hookAt(at)(_ => throw new Died))))
    expire(of)
  }

  /** The statements that asked for rows of `table`, as the server logged them after `mark`. */
  private def rowsAskedSince(mark: Int, table: String): Seq[String] =
    pg.serverLog()
      .substring(mark)
      .linesIterator
      .collect {
        case Statement(_, sql) if sql.contains(s""""public"."$table"""") => sql.trim
      }
      .filterNot(_.contains("WHERE 1=0"))
      .toSeq

  /** The statements over `table` that the server logged after `mark`, each by its kind. */
  private def statementsSince(mark: Int, table: String): Seq[String] =
    pg.serverLog()
      .substring(mark)
      .linesIterator
      .collect {
        case Statement(kind, sql) if sql.contains(s""""public"."$table"""") =>
          if (kind.contains("/")) "portal" else "whole"
      }
      .toSeq

  // ---------------------------------------------------------------------------------------------
  // The path where nothing fails
  // ---------------------------------------------------------------------------------------------

  test("a full load takes its table to Bronze as one snapshot, and the next run takes another") {
    asOwner { c =>
      execute(
        c,
        "CREATE TABLE items (id int NOT NULL, name text, price numeric(10,2), seen timestamptz)"
      )
      execute(
        c,
        "INSERT INTO items VALUES (1, 'a', 1.50, now()), (2, 'b', 2.50, now()), (3, 'c', NULL, NULL)"
      )
    }
    val items = tableSource("public.items")
    val mark = logMark()

    val first = ingested(launch(items, reading()))

    first.batch shouldBe batchId(items, 1)
    first.outputs shouldBe Seq(BatchOutput(items.targets.bronze.table, 0))
    // Read and written in one pass, and counted by the commit of the table.
    first.counts shouldBe RunCounts(
      rowsRead = Some(3),
      rowsWritten = Some(3),
      rowsRejected = Some(0),
      bytesRead = None,
      retries = None,
      writeConflicts = Some(0),
      commitContention = Some(0)
    )
    runOf(first.run).counts shouldBe first.counts
    snapshots(items) shouldBe Seq((1, "a", 1L), (2, "b", 1L), (3, "c", 1L))
    FullLoadPlan.fromJson(batchOf(first.batch).plan).value shouldBe
      FullLoadPlan(QualifiedName.parse("public.items").value)
    // A full load leaves nothing to start from.
    stateOf(items).value.watermark shouldBe None
    // The rows were asked of the source once, with a cursor, after their first row was.
    eventually(statementsSince(mark, "items").count(_ == "portal") shouldBe 2)

    // The table changes, and the next run takes it as it is then: another snapshot, whole.
    asOwner { c =>
      execute(c, "DELETE FROM items WHERE id = 1")
      execute(c, "UPDATE items SET name = 'B' WHERE id = 2")
      execute(c, "INSERT INTO items VALUES (4, 'd', 4.50, now())")
    }
    val second = ingested(launch(items, reading()))

    second.batch shouldBe batchId(items, 2)
    second.outputs shouldBe Seq(BatchOutput(items.targets.bronze.table, 1))
    second.counts.rowsWritten shouldBe Some(3)
    snapshots(items) shouldBe Seq(
      (1, "a", 1L),
      (2, "b", 1L),
      (3, "c", 1L),
      (2, "B", 2L),
      (3, "c", 2L),
      (4, "d", 2L)
    )
  }

  test("a table with no rows writes nothing, and its batch is committed") {
    asOwner(execute(_, "CREATE TABLE nothing_yet (id int, name text)"))
    val noRows = tableSource("public.nothing_yet")

    val done = ingested(launch(noRows, reading()))

    done.outputs shouldBe empty
    done.counts shouldBe RunCounts(
      rowsRead = Some(0),
      rowsWritten = Some(0),
      rowsRejected = Some(0)
    )
    hasABatch(noRows) shouldBe false
    batchOf(done.batch).status shouldBe BatchStatus.Committed
  }

  // ---------------------------------------------------------------------------------------------
  // A run that dies
  // ---------------------------------------------------------------------------------------------

  test("a batch that a dead run wrote is rolled forward, and the source is asked nothing") {
    asOwner { c =>
      execute(c, "CREATE TABLE written_once (id int, name text)")
      execute(c, "INSERT INTO written_once VALUES (1, 'a'), (2, 'b')")
    }
    val source = tableSource("public.written_once")
    dies(source, FaultPoint.AfterBronzeCommit)
    // The table changes after the dead run read it: the batch is the snapshot that was written.
    asOwner(execute(_, "INSERT INTO written_once VALUES (3, 'c')"))
    val mark = logMark()

    val outcome = launch(source, reading())

    outcome shouldBe Outcome.RolledForward(
      outcome.run,
      batchId(source, 1),
      Seq(BatchOutput(source.targets.bronze.table, 0)),
      RunCounts(rowsRead = Some(0), bytesRead = Some(0))
    )
    snapshots(source) shouldBe Seq((1, "a", 1L), (2, "b", 1L))
    // No connection to the source was opened: nothing was logged of it.
    Thread.sleep(500)
    statementsSince(mark, "written_once") shouldBe empty
  }

  test("a batch that a dead run did not write is read again, as the table is by then") {
    asOwner { c =>
      execute(c, "CREATE TABLE read_again (id int, name text)")
      execute(c, "INSERT INTO read_again VALUES (1, 'a')")
    }
    val source = tableSource("public.read_again")
    dies(source, FaultPoint.AfterCounts)
    hasABatch(source) shouldBe false
    asOwner(execute(_, "INSERT INTO read_again VALUES (2, 'b')"))

    val done = ingested(launch(source, reading()))

    // The plan is the table, and a replay reads the table: nothing of the dead run was kept.
    done.batch shouldBe batchId(source, 1)
    done.counts.rowsWritten shouldBe Some(2)
    snapshots(source) shouldBe Seq((1, "a", 1L), (2, "b", 1L))
  }

  // ---------------------------------------------------------------------------------------------
  // A load by a watermark
  // ---------------------------------------------------------------------------------------------

  test("an incremental load reads up to its horizon, and the next run reads only what is new") {
    changesTable("orders")
    val start = clockOfTheSource()
    stamped("orders", 1, "a", start.minus(3, ChronoUnit.HOURS))
    stamped("orders", 2, "b", start.minus(2, ChronoUnit.HOURS))
    // Within the margin: its transaction may still be open, as far as a run can tell.
    stamped("orders", 3, "c", start.minus(10, ChronoUnit.MINUTES))
    val orders = watermarkSource("public.orders")
    val mark = logMark()

    val first = ingested(launch(orders, reading()))

    first.batch shouldBe batchId(orders, 1)
    first.outputs shouldBe Seq(BatchOutput(orders.targets.bronze.table, 0))
    first.counts.rowsRead shouldBe Some(2)
    first.counts.rowsWritten shouldBe Some(2)
    snapshots(orders) shouldBe Seq((1, "a", 1L), (2, "b", 1L))
    // The window: the table up to an hour behind the clock of the source. The clock of the
    // engine, which this suite holds days before, bounds nothing.
    val window = windowOf(first.batch)
    window.lower shouldBe None
    window.upper shouldBe window.horizon
    val said = window.upper.plus(1, ChronoUnit.HOURS)
    withClue(s"the source said $said, and the run started at $start: ") {
      said.isBefore(start.minusSeconds(5)) shouldBe false
      said.isAfter(clockOfTheSource().plusSeconds(5)) shouldBe false
    }
    // The commit moved the watermark to the upper bound of the plan.
    watermarkOf(orders) shouldBe Some(window.upper)
    eventually {
      rowsAskedSince(mark, "orders").last should endWith(
        s""" WHERE ("updated_at" IS NOT NULL) AND ("updated_at" <= '${literal(window.upper)}')"""
      )
    }

    // The source changes after the window: a row is updated, and another is new.
    stamped("orders", 2, "B", window.upper.plus(1, ChronoUnit.MICROS))
    stamped("orders", 4, "d", window.upper.plus(2, ChronoUnit.MICROS))
    // The server is asked until its clock, less the margin, is past them.
    eventually {
      clockOfTheSource()
        .minus(1, ChronoUnit.HOURS)
        .isAfter(window.upper.plus(2, ChronoUnit.MICROS)) shouldBe true
    }
    val again = logMark()

    val second = ingested(launch(orders, reading()))

    second.batch shouldBe batchId(orders, 2)
    second.counts.rowsWritten shouldBe Some(2)
    // What changed is appended, as it is now, and what did not change is not read again. The
    // row inside the margin still waits.
    snapshots(orders) shouldBe Seq((1, "a", 1L), (2, "b", 1L), (2, "B", 2L), (4, "d", 2L))
    val next = windowOf(second.batch)
    next.lower shouldBe Some(window.upper)
    watermarkOf(orders) shouldBe Some(next.upper)
    eventually {
      rowsAskedSince(again, "orders").last should endWith(
        s""" WHERE ("updated_at" IS NOT NULL) AND ("updated_at" > '${literal(window.upper)}') """ +
          s"""AND ("updated_at" <= '${literal(next.upper)}')"""
      )
    }
  }

  test("a window with no rows moves the watermark, and touches no table") {
    changesTable("quiet")
    val quiet = watermarkSource(
      "public.quiet",
      watermark = "safety_margin: 1h, start: \"2026-01-01T00:00:00Z\"",
      range = Some("1d")
    )
    // Nothing was changed on the first day of the source, and something on the second.
    stamped("quiet", 1, "a", Instant.parse("2026-01-02T12:00:00Z"))

    val first = ingested(launch(quiet, reading()))

    first.batch shouldBe batchId(quiet, 1)
    first.outputs shouldBe empty
    first.counts shouldBe RunCounts(
      rowsRead = Some(0),
      rowsWritten = Some(0),
      rowsRejected = Some(0)
    )
    batchOf(first.batch).status shouldBe BatchStatus.Committed
    // No table was made, and the source is a day further all the same.
    tableIsThere(quiet) shouldBe false
    watermarkOf(quiet) shouldBe Some(Instant.parse("2026-01-02T00:00:00Z"))

    // The next window starts where that one ended, and has the row.
    val second = ingested(launch(quiet, reading()))
    windowOf(second.batch).lower shouldBe Some(Instant.parse("2026-01-02T00:00:00Z"))
    snapshots(quiet) shouldBe Seq((1, "a", 2L))
    watermarkOf(quiet) shouldBe Some(Instant.parse("2026-01-03T00:00:00Z"))
  }

  test("a window is cut by max_batch_range, and the next run goes on from where it was cut") {
    changesTable("history")
    val from = Instant.parse("2026-01-01T00:00:00Z")
    val aDayOn = from.plus(1, ChronoUnit.DAYS)
    // At the start, which is not the source's to ingest; inside the first day; at its end, to
    // the microsecond; and after it.
    stamped("history", 0, "at the start", from)
    stamped("history", 1, "a", from.plus(12, ChronoUnit.HOURS))
    stamped("history", 2, "b", aDayOn)
    stamped("history", 3, "c", aDayOn.plus(1, ChronoUnit.MICROS))
    stamped("history", 4, "d", from.plus(36, ChronoUnit.HOURS))
    val history = watermarkSource(
      "public.history",
      watermark = "safety_margin: 1h, start: \"2026-01-01T00:00:00Z\"",
      range = Some("1d")
    )

    val first = ingested(launch(history, reading()))

    val cut = windowOf(first.batch)
    (cut.lower, cut.upper) shouldBe (Some(from) -> aDayOn)
    // The plan says that its invocation has more to read: its horizon is months later.
    cut.more shouldBe true
    snapshots(history) shouldBe Seq((1, "a", 1L), (2, "b", 1L))
    watermarkOf(history) shouldBe Some(aDayOn)

    val second = ingested(launch(history, reading()))

    val next = windowOf(second.batch)
    (next.lower, next.upper) shouldBe (Some(aDayOn) -> from.plus(2, ChronoUnit.DAYS))
    snapshots(history) shouldBe Seq((1, "a", 1L), (2, "b", 1L), (3, "c", 2L), (4, "d", 2L))
  }

  test("a chain of runs ends at the horizon of its first plan, though the source goes on writing") {
    changesTable("chained")
    // Two days and a bit behind the clock of the source, in windows of a day: three batches.
    val from = clockOfTheSource().minus(50, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS)
    stamped("chained", 1, "a", from.plus(1, ChronoUnit.HOURS))
    stamped("chained", 2, "b", from.plus(30, ChronoUnit.HOURS))
    stamped("chained", 3, "c", from.plus(48 * 60 + 30, ChronoUnit.MINUTES))
    val chained = watermarkSource(
      "public.chained",
      watermark = s"""safety_margin: 1h, start: "$from"""",
      range = Some("1d")
    )

    // Run by run, as `run` chains them, with a row stamped by the source after each.
    val windows = Iterator
      .iterate(Option(Option.empty[Json])) {
        case Some(horizon) =>
          val done = ingested(launch(chained, reading(), horizon))
          stamped("chained", 100 + done.batch.seq.value.toInt, "new", clockOfTheSource())
          done.next match {
            case Outcome.Next.Within(next) => Some(Some(next))
            case Outcome.Next.Ends => None
            case Outcome.Next.Afresh => fail("a batch of this chain was planned by another")
          }
        case None => None
      }
      .takeWhile(_.nonEmpty)
      .take(10)
      .size

    // Three runs, each of its own batch: the last ends at the horizon of the first plan, and
    // what the source wrote meanwhile is the next invocation's.
    windows shouldBe 3
    val first = windowOf(batchId(chained, 1))
    windowOf(batchId(chained, 3)).upper shouldBe first.horizon
    (1 to 3).map(n => windowOf(batchId(chained, n.toLong)).horizon).distinct shouldBe
      Seq(first.horizon)
    snapshots(chained) shouldBe Seq((1, "a", 1L), (2, "b", 2L), (3, "c", 3L))
  }

  test("a lookback reads the last stretch again, and Bronze keeps both copies") {
    changesTable("read_twice")
    val from = Instant.parse("2026-01-01T00:00:00Z")
    stamped("read_twice", 1, "a", from.plus(12, ChronoUnit.HOURS))
    stamped("read_twice", 2, "b", from.plus(20, ChronoUnit.HOURS))
    stamped("read_twice", 3, "c", from.plus(30, ChronoUnit.HOURS))
    val twice = watermarkSource(
      "public.read_twice",
      watermark = "safety_margin: 1h, lookback: 6h, start: \"2026-01-01T00:00:00Z\"",
      range = Some("1d")
    )

    ingested(launch(twice, reading()))
    val second = ingested(launch(twice, reading()))

    // Six hours before the watermark, and a day after it: the range is counted from the
    // watermark, and not from where the window starts.
    val window = windowOf(second.batch)
    window.lower shouldBe Some(from.plus(18, ChronoUnit.HOURS))
    window.upper shouldBe from.plus(2, ChronoUnit.DAYS)
    snapshots(twice) shouldBe Seq((1, "a", 1L), (2, "b", 1L), (2, "b", 2L), (3, "c", 2L))
  }

  test("a time without a zone that is stamped behind UTC is read by the first batch alone") {
    // Pinned as it is (ADR-0020, point 1): a timestamp without a zone is taken as UTC, and
    // the bounds of a batch are moments of the clock of the source. A table whose writers
    // stamp it in a zone behind UTC has every new row hours before the watermark by the time
    // it is written. The connector warns of such a column at every plan, and cannot tell
    // whose time is in it.
    asOwner(
      execute(
        _,
        "CREATE TABLE local_times (id int PRIMARY KEY, name text, updated_at timestamp NOT NULL)"
      )
    )
    // As an application whose session is in New York stamps a row with now().
    def stampedThere(id: Int, name: String): Unit =
      asOwner(
        execute(
          _,
          s"INSERT INTO local_times VALUES ($id, '$name', now() AT TIME ZONE 'America/New_York')"
        )
      )
    stampedThere(1, "a")
    val there = watermarkSource("public.local_times", watermark = "safety_margin: 1s")

    // The first batch has no lower bound, and the row is hours before its upper one.
    ingested(launch(there, reading())).batch shouldBe batchId(there, 1)
    snapshots(there) shouldBe Seq((1, "a", 1L))
    val reached = watermarkOf(there).value

    stampedThere(2, "b")
    eventually(clockOfTheSource().isAfter(reached.plusSeconds(2)) shouldBe true)
    val second = ingested(launch(there, reading()))

    // A window that the source has no row in: the row is behind it, and stays there.
    windowOf(second.batch).lower shouldBe Some(reached)
    second.counts.rowsRead shouldBe Some(0)
    snapshots(there) shouldBe Seq((1, "a", 1L))
    val stamp = asOwner { c =>
      Using.resource(c.createStatement()) { statement =>
        val rows = statement.executeQuery("SELECT updated_at FROM local_times WHERE id = 2")
        rows.next()
        rows.getObject(1, classOf[java.time.LocalDateTime]).toInstant(ZoneOffset.UTC)
      }
    }
    stamp.isBefore(reached) shouldBe true
  }

  test("a source that is to start later has nothing new, and its run ends well") {
    changesTable("not_yet")
    stamped("not_yet", 1, "a", Instant.parse("2026-01-01T00:00:00Z"))
    val notYet =
      watermarkSource("public.not_yet", watermark = "start: \"2999-01-01T00:00:00Z\"")

    val outcome = launch(notYet, reading())

    outcome shouldBe Outcome.NothingNew(outcome.run)
    runOf(outcome.run).status shouldBe RunStatus.Succeeded
    batchesOf(notYet) shouldBe empty
    stateOf(notYet) shouldBe None
    holderOf(notYet) shouldBe None
  }

  // ---------------------------------------------------------------------------------------------
  // A transaction that commits late, against the clock of the source
  // ---------------------------------------------------------------------------------------------

  /** The safety margin of the sources below, in seconds: a run is planned well within it. */
  private val Margin = 10L

  /**
   * A row inserted by a transaction that stamps it as an application does, with the moment that
   * the transaction began (`CURRENT_TIMESTAMP`), and that is left open while `meanwhile` runs,
   * to be committed after. `meanwhile` is given the moment of the row.
   */
  private def lateRow[T](table: String, id: Int)(meanwhile: Instant => T): (Instant, T) =
    asOwner { c =>
      c.setAutoCommit(false)
      val at = Using.resource(c.createStatement()) { statement =>
        val rows = statement.executeQuery(
          s"INSERT INTO $table (id, name, updated_at) VALUES ($id, 'late', CURRENT_TIMESTAMP) " +
            "RETURNING updated_at"
        )
        rows.next()
        rows.getObject(1, classOf[OffsetDateTime]).toInstant
      }
      val result = meanwhile(at)
      c.commit()
      at -> result
    }

  /** Asks the server until its clock, less the margin, is past `moment`. */
  private def marginPast(moment: Instant): Unit =
    eventually(timeout(Span(3 * Margin, Seconds))) {
      clockOfTheSource().minusSeconds(Margin).isAfter(moment) shouldBe true
    }: Unit

  private def rowsAtTheSource(table: String): Int =
    asOwner { c =>
      Using.resource(c.createStatement()) { statement =>
        val rows = statement.executeQuery(s"SELECT count(*) FROM $table")
        rows.next()
        rows.getInt(1)
      }
    }

  test("a row of a transaction shorter than the margin is read by the batch after its commit") {
    changesTable("late_within")
    val late = watermarkSource("public.late_within", watermark = s"safety_margin: ${Margin}s")

    // The transaction is open while the first batch is planned and read.
    val (at, first) = lateRow("late_within", 1)(_ => ingested(launch(late, reading())))

    // The window ends a margin behind the clock of the source, so before the moment of the row:
    // the transaction had not run for as long as the margin.
    withClue(
      "the first window reached the moment of a row whose transaction was open: it did not end " +
        "a margin behind the clock of the source, or it was planned more than the margin after " +
        "the transaction began: "
    ) {
      windowOf(first.batch).upper.isBefore(at) shouldBe true
    }
    first.outputs shouldBe empty
    marginPast(at)

    val second = ingested(launch(late, reading()))

    // The next window starts where that one ended, and the row, committed by then, is in it.
    windowOf(second.batch).lower shouldBe Some(windowOf(first.batch).upper)
    snapshots(late) shouldBe Seq((1, "late", 2L))
  }

  test("a row of a transaction longer than the margin is lost, and no later batch reads it") {
    changesTable("late_beyond")
    val late = watermarkSource("public.late_beyond", watermark = s"safety_margin: ${Margin}s")

    // The transaction is still open once the clock of the source is a margin past its row.
    val (at, first) = lateRow("late_beyond", 1) { at =>
      marginPast(at)
      ingested(launch(late, reading()))
    }

    // The window that holds the moment of the row was read while the row was not committed.
    val window = windowOf(first.batch)
    window.upper.isAfter(at) shouldBe true
    first.outputs shouldBe empty
    watermarkOf(late) shouldBe Some(window.upper)
    marginPast(window.upper)

    val second = ingested(launch(late, reading()))

    // The source has the row, behind the watermark, and no window reaches back to it. It is the
    // known limit of a load by a column, which CDC is for (ADR-0020), pinned here.
    windowOf(second.batch).lower shouldBe Some(window.upper)
    rowsAtTheSource("late_beyond") shouldBe 1
    tableIsThere(late) shouldBe false
  }

  // ---------------------------------------------------------------------------------------------
  // A run of a window that dies
  // ---------------------------------------------------------------------------------------------

  private val Day = Instant.parse("2026-01-01T00:00:00Z")

  /** A source whose first window is the first day of 2026, whatever the clock of its source. */
  private def aDayOf(table: String): Source = {
    changesTable(table)
    stamped(table, 1, "a", Day.plus(1, ChronoUnit.HOURS))
    stamped(table, 2, "b", Day.plus(2, ChronoUnit.HOURS))
    watermarkSource(
      s"public.$table",
      watermark = "safety_margin: 1h, start: \"2026-01-01T00:00:00Z\"",
      range = Some("1d")
    )
  }

  Seq(FaultPoint.AfterPlan, FaultPoint.AfterRead, FaultPoint.AfterCounts).foreach { point =>
    test(s"a window whose run died at $point is read again, as it was planned and no further") {
      val table = s"died_${point.name.toLowerCase}"
      val source = aDayOf(table)
      dies(source, point)
      val planned = batchOf(batchId(source, 1)).plan
      hasABatch(source) shouldBe false
      watermarkOf(source) shouldBe None
      // The source goes on changing: a row comes into the window, one leaves it for a later
      // time, and one is new, after it.
      stamped(table, 3, "c", Day.plus(3, ChronoUnit.HOURS))
      stamped(table, 2, "B", Day.plus(25, ChronoUnit.HOURS))
      stamped(table, 4, "d", Day.plus(26, ChronoUnit.HOURS))

      val done = ingested(launch(source, reading()))

      // A plan of another invocation's: the next run plans as the first of its own does.
      done.next shouldBe Outcome.Next.Afresh
      // The same batch, under the plan that was written down: the window of the first day,
      // and not one that a clock would give now. Its rows are what the window has by then.
      done.batch shouldBe batchId(source, 1)
      batchOf(done.batch).plan shouldBe planned
      (windowOf(done.batch).lower, windowOf(done.batch).upper) shouldBe
        (Some(Day) -> Day.plus(1, ChronoUnit.DAYS))
      snapshots(source) shouldBe Seq((1, "a", 1L), (3, "c", 1L))
      watermarkOf(source) shouldBe Some(Day.plus(1, ChronoUnit.DAYS))

      // And the row that left the window is in the next, with the one that came after.
      ingested(launch(source, reading())).batch shouldBe batchId(source, 2)
      snapshots(source) shouldBe Seq((1, "a", 1L), (3, "c", 1L), (2, "B", 2L), (4, "d", 2L))
    }
  }

  Seq(FaultPoint.AfterBronzeCommit, FaultPoint.BeforeStateCommit).foreach { point =>
    test(s"a window whose run died at $point is rolled forward, and its source asked nothing") {
      val table = s"died_${point.name.toLowerCase}"
      val source = aDayOf(table)
      dies(source, point)
      hasABatch(source) shouldBe true
      watermarkOf(source) shouldBe None
      stamped(table, 2, "B", Day.plus(25, ChronoUnit.HOURS))
      stamped(table, 4, "d", Day.plus(26, ChronoUnit.HOURS))
      val mark = logMark()

      val outcome = launch(source, reading())

      outcome shouldBe Outcome.RolledForward(
        outcome.run,
        batchId(source, 1),
        Seq(BatchOutput(source.targets.bronze.table, 0)),
        RunCounts(rowsRead = Some(0), bytesRead = Some(0))
      )
      // The watermark is the upper bound of the plan, which is said from the plan alone: no
      // connection to the source was opened.
      watermarkOf(source) shouldBe Some(Day.plus(1, ChronoUnit.DAYS))
      Thread.sleep(500)
      pg.serverLog().substring(mark) should not include table
      // What the dead run wrote is the batch, and the next one starts after its window.
      snapshots(source) shouldBe Seq((1, "a", 1L), (2, "b", 1L))
      ingested(launch(source, reading())).batch shouldBe batchId(source, 2)
      snapshots(source) shouldBe Seq((1, "a", 1L), (2, "b", 1L), (2, "B", 2L), (4, "d", 2L))
    }
  }

  test("a watermark is not gone on from under another column, nor for another table") {
    changesTable("followed")
    stamped("followed", 1, "a", Day.plus(1, ChronoUnit.HOURS))
    asOwner { c =>
      execute(c, "ALTER TABLE followed ADD COLUMN seen_at timestamptz NOT NULL DEFAULT now()")
      execute(c, "CREATE TABLE followed_copy AS SELECT * FROM followed")
    }
    val settings = "safety_margin: 1h, start: \"2026-01-01T00:00:00Z\""
    val followed = watermarkSource("public.followed", watermark = settings, range = Some("1d"))
    ingested(launch(followed, reading()))
    val id = followed.id.value

    // The watermark that is stored is a time of updated_at, of the table followed.
    val byAnotherColumn = watermarkSource(
      "public.followed",
      id = id,
      column = "seen_at",
      watermark = settings,
      range = Some("1d")
    )
    val column = failed(launch(byAnotherColumn, reading()))
    column.failure shouldBe FailureClass.Permanent
    column.message should include(
      "load.watermark.column of the source was updated_at and is seen_at: the watermark " +
        "that is stored is a value of the old column"
    )
    val ofAnotherTable =
      watermarkSource("public.followed_copy", id = id, watermark = settings, range = Some("1d"))
    val table = failed(launch(ofAnotherTable, reading()))
    table.failure shouldBe FailureClass.Permanent
    table.message should include(
      "connector.table of the source was public.followed and is public.followed_copy"
    )

    // Nothing was planned for either, and the source goes on as it was.
    batchesOf(followed) shouldBe Seq((1L, "COMMITTED"))
    ingested(launch(followed, reading())).batch shouldBe batchId(followed, 2)
  }

  test("a window that a run left is not read from a table whose column changed since") {
    changesTable("altered")
    stamped("altered", 1, "a", Day.plus(1, ChronoUnit.HOURS))
    val altered = watermarkSource(
      "public.altered",
      watermark = "safety_margin: 1h, start: \"2026-01-01T00:00:00Z\"",
      range = Some("1d")
    )
    dies(altered, FaultPoint.AfterPlan)

    def refusedFor(change: String, problem: String): Unit = {
      asOwner(execute(_, change))
      val refused = failed(launch(altered, reading()))
      refused.failure shouldBe FailureClass.Permanent
      refused.message shouldBe
        s"the table public.altered of the source ${altered.id} (${pg.jdbcUrl("sales")}, as " +
        s"sales_ro) $problem. The batch of ${altered.id} was planned as a window of that " +
        "column, as the table had it then, and nothing was read"
      // The run ended with its class and gave its lease up; the batch waits as it was.
      runOf(refused.run).failure.value.failure shouldBe FailureClass.Permanent
      batchesOf(altered) shouldBe Seq((1L, "PLANNED"))
      hasABatch(altered) shouldBe false
      holderOf(altered) shouldBe None: Unit
    }

    // The column is gone: there is nothing to bound the rows by.
    refusedFor(
      "ALTER TABLE altered RENAME COLUMN updated_at TO changed_at",
      "has no column updated_at (load.watermark.column). Its columns are id, name, changed_at"
    )
    // It is there, as text: Spark would compare its values itself, over the whole table.
    asOwner(execute(_, "ALTER TABLE altered RENAME COLUMN changed_at TO updated_at"))
    refusedFor(
      "ALTER TABLE altered ALTER COLUMN updated_at TYPE text",
      "has its column updated_at (load.watermark.column) of type text, and a watermark is " +
        "a timestamptz, or a timestamp without a zone, which is taken as UTC: a load by a " +
        "watermark leaves a margin of time behind the clock of its source, and no margin can " +
        "be said in another type (ADR-0020)"
    )

    // With the column as the batch was planned by, the batch is read.
    asOwner(
      execute(
        _,
        "ALTER TABLE altered ALTER COLUMN updated_at TYPE timestamptz " +
          "USING updated_at::timestamptz"
      )
    )
    ingested(launch(altered, reading())).batch shouldBe batchId(altered, 1)
    snapshots(altered) shouldBe Seq((1, "a", 1L))
  }

  // ---------------------------------------------------------------------------------------------
  // The failures of a source, as a run ends with them
  // ---------------------------------------------------------------------------------------------

  test("a statement that the source cuts for running too long may pass, and names its setting") {
    asOwner { c =>
      execute(
        c,
        "CREATE TABLE ten AS SELECT g AS id, 'n' || g AS name FROM generate_series(1, 10) g"
      )
      // A function that the server takes 0.3 s to answer, row by row.
      execute(
        c,
        "CREATE FUNCTION slowly(v int) RETURNS int LANGUAGE sql VOLATILE AS " +
          "$$ SELECT v FROM pg_sleep(0.3) $$"
      )
      execute(c, "CREATE VIEW slow_ten AS SELECT slowly(id) AS id, name FROM ten")
    }
    // The first row comes in time, and the fetch of the ten of them does not.
    val hurried = tableSource("public.slow_ten", statement = Some("1s"))

    val cut = failed(launch(hurried, reading()))

    cut.failure shouldBe FailureClass.Transient
    cut.message should include("took longer over one statement than 1s, and the statement was cut")
    cut.message should include("(SQLState 57014)")
    cut.message should include("raise connector.connection.timeouts.statement")
    // The run failed with its class, its batch stays planned, and nothing of it is in Bronze.
    val run = runOf(cut.run)
    run.status shouldBe RunStatus.Failed
    run.failure.value.failure shouldBe FailureClass.Transient
    batchesOf(hurried) shouldBe Seq((1L, "PLANNED"))
    hasABatch(hurried) shouldBe false
    holderOf(hurried) shouldBe None

    // With the setting raised, the next run reads the batch that was planned.
    val patient = tableSource("public.slow_ten", id = hurried.id.value, statement = Some("30s"))
    val done = ingested(launch(patient, reading()))
    done.batch shouldBe batchId(hurried, 1)
    done.counts.rowsWritten shouldBe Some(10)
  }

  test("a connection that is cut while the rows are read may pass, and nothing is written") {
    val server = proxied
    Using.resource(server.observe("sales_owner", "sales")) { c =>
      execute(
        c,
        "CREATE TABLE wide AS SELECT g AS id, repeat('x', 200) AS name " +
          "FROM generate_series(1, 5000) g"
      )
    }
    val wide = tableSource(
      "public.wide",
      url = server.faultyJdbcUrl("sales"),
      password = "env:PROXIED_PASSWORD"
    )
    // After the first row is asked for: the connection of the rows is closed by the proxy
    // once it has let that much of them through.
    val cutting = reading(hookAt(FaultPoint.AfterCounts) { _ =>
      server.faulty.toxics().limitData(Cut, ToxicDirection.DOWNSTREAM, 100000L): Unit
    })

    val cut =
      try failed(launch(wide, cutting))
      finally server.faulty.toxics().get(Cut).remove()

    cut.failure shouldBe FailureClass.Transient
    cut.message should startWith(
      s"the source ${wide.id} (${server.faultyJdbcUrl("sales")}, as sales_ro)"
    )
    runOf(cut.run).failure.value.failure shouldBe FailureClass.Transient
    batchesOf(wide) shouldBe Seq((1L, "PLANNED"))
    hasABatch(wide) shouldBe false

    // The source is there again, and the next run reads the batch that was planned, whole.
    val done = ingested(launch(wide, reading()))
    done.batch shouldBe batchId(wide, 1)
    done.counts.rowsWritten shouldBe Some(5000)
    spark.read.format("delta").load(bronze(wide).uri).count() shouldBe 5000L
  }

  test("a row that Bronze cannot keep fails its batch for good, and none of its rows is written") {
    asOwner { c =>
      execute(c, "CREATE TABLE contracts (id int, name text, valid_to date)")
      execute(c, "INSERT INTO contracts VALUES (1, 'a', '2027-01-01'), (2, 'b', 'infinity')")
    }
    val contracts = tableSource("public.contracts")

    val refused = failed(launch(contracts, reading()))

    refused.failure shouldBe FailureClass.Permanent
    refused.message should startWith(
      s"the column valid_to of the table public.contracts of the source ${contracts.id} "
    )
    refused.message should include("has infinity or -infinity in a row of the batch")
    runOf(refused.run).failure.value.failure shouldBe FailureClass.Permanent
    batchesOf(contracts) shouldBe Seq((1L, "PLANNED"))
    hasABatch(contracts) shouldBe false

    refused.message should endWith(
      "Add valid_to to connector.text_columns, and the column is read as text, each value " +
        "as the source writes it"
    )

    // The same data gives the same result, until the configuration says what the failure
    // said to: the batch that was planned is then read, with the column as text.
    failed(launch(contracts, reading())).failure shouldBe FailureClass.Permanent
    val asText = tableSource(
      "public.contracts",
      id = contracts.id.value,
      textColumns = Seq("valid_to")
    )
    ingested(launch(asText, reading())).batch shouldBe batchId(contracts, 1)
    spark.read
      .format("delta")
      .load(bronze(contracts).uri)
      .select("id", "valid_to")
      .collect()
      .map(row => row.getInt(0) -> row.getString(1))
      .sortBy(_._1)
      .toSeq shouldBe Seq(1 -> "2027-01-01", 2 -> "infinity")
  }

  test("a column under a name of the engine's fails its run for good, before a batch is planned") {
    asOwner { c =>
      // The two names that a read of files gives a row, which the writer of Bronze takes as
      // they come; and one that the writer adds, in another case.
      execute(c, "CREATE TABLE of_a_file (id int, name text, _source_file text, _rescued_data int)")
      execute(c, "INSERT INTO of_a_file VALUES (1, 'a', 'the table''s own', 3)")
      execute(c, """CREATE TABLE numbered (id int, name text, "_Batch_Seq" int)""")
      execute(c, "INSERT INTO numbered VALUES (1, 'a', 7)")
    }
    Seq(
      "of_a_file" -> """"_source_file", "_rescued_data"""",
      "numbered" -> """"_Batch_Seq""""
    ).foreach { case (table, names) =>
      val named = tableSource(s"public.$table")

      val refused = failed(launch(named, reading()))

      refused.failure shouldBe FailureClass.Permanent
      refused.message should startWith(
        s"the table public.$table of the source ${named.id} (${pg.jdbcUrl("sales")}, as " +
          "sales_ro) has columns under names of the columns that the engine adds to every " +
          s"row of Bronze, $names: _run_id, _batch_seq, _ingested_at, _source_file, " +
          "_source_id, _rescued_data, whatever their case."
      )
      refused.message should endWith(
        "A view of the table can give them other names. Nothing was read"
      )
      // No batch waits for a table that every run would be refused for, and nothing of the
      // source is in Bronze under a name of the engine's.
      batchesOf(named) shouldBe empty
      tableIsThere(named) shouldBe false
      holderOf(named) shouldBe None
    }
  }

  test("a table that is not there fails its run for good, before a batch is planned") {
    val nowhere = tableSource("public.never_made")

    val refused = failed(launch(nowhere, reading()))

    refused.failure shouldBe FailureClass.Permanent
    refused.message should include("""relation "public.never_made" does not exist""")
    runOf(refused.run).status shouldBe RunStatus.Failed
    batchesOf(nowhere) shouldBe empty
    holderOf(nowhere) shouldBe None
  }
}

object JdbcIngestionSpec {

  final class Died extends RuntimeException("the run died here")

  /** The name of the toxic that cuts a connection of the proxy. */
  private val Cut = "cut"

  /** A statement as the server logs it when it starts: run whole, or as a portal. */
  private val Statement = """^\S+ \S+ \S+ \[\d+\] LOG:  (execute [^:]+): (.*)$""".r
}
