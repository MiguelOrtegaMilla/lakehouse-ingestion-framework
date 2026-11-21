package lakehouse.ingestion.it.ingestion

import java.sql.Connection
import java.time.{Clock, ZoneOffset}

import scala.util.Using

import eu.rekawek.toxiproxy.model.ToxicDirection
import lakehouse.ingestion.core.config.{QualifiedName, SecretResolver, Source}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.core.jdbc.FullLoadPlan
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.connector.ConnectorRegistry
import lakehouse.ingestion.engine.jdbc.JdbcSourceConnector
import lakehouse.ingestion.engine.{FaultHooks, FaultPoint, IngestionService}
import lakehouse.ingestion.it.support.ProxiedPostgres
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

/**
 * A run of a table of a database from end to end (P6.6): the protocol of ADR-0005 with the JDBC
 * connector, on a real PostgreSQL as its source, a real Delta on the coordinated LogStore, and
 * a real control plane.
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
    with IngestionSuite {

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

  private def asOwner(body: (String => Unit) => Unit): Unit =
    Using.resource(pg.connect("sales_owner", "sales"))(c => body(execute(c, _)))

  private def execute(c: Connection, sql: String): Unit =
    Using.resource(c.createStatement())(_.execute(sql): Unit)

  // ---------------------------------------------------------------------------------------------
  // A source of a table, and a run of it
  // ---------------------------------------------------------------------------------------------

  private def secrets: SecretResolver = SecretResolver(
    (Map("SALES_RO_PASSWORD" -> pg.password("sales_ro")) ++
      behindAProxy.map(server => "PROXIED_PASSWORD" -> server.postgres.password("sales_ro"))).get
  )

  /**
   * A source that reads `table` of `sales`, under an id of its own unless it is given one.
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
    Source
      .load(
        s"$id.yaml",
        s"""id: $id
           |connector:
           |  type: jdbc
           |  connection:
           |    url: "$url"
           |    user: sales_ro
           |    password: { secretRef: "$password" }
           |    tls: { mode: disable }
           |    ${statement.fold("")(longest => s"timeouts: { statement: $longest }")}
           |  table: $table
           |  text_columns: [${textColumns.mkString(", ")}]
           |load: { mode: full }
           |targets: { bronze: { table: bronze/${id.replace('.', '/')} } }
           |""".stripMargin
      )
      .value
      .value

  private def reading(hooks: FaultHooks = FaultHooks.Off): IngestionService =
    new IngestionService(
      database,
      ConnectorRegistry(new JdbcSourceConnector(secrets)),
      spark,
      lakeRoot,
      timing,
      Clock.fixed(Now, ZoneOffset.UTC),
      hooks
    )

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

  private def logMark(): Int = pg.serverLog().length

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
    asOwner { run =>
      run("CREATE TABLE items (id int NOT NULL, name text, price numeric(10,2), seen timestamptz)")
      run(
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
    asOwner { run =>
      run("DELETE FROM items WHERE id = 1")
      run("UPDATE items SET name = 'B' WHERE id = 2")
      run("INSERT INTO items VALUES (4, 'd', 4.50, now())")
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
    asOwner(_("CREATE TABLE nothing_yet (id int, name text)"))
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
    asOwner { run =>
      run("CREATE TABLE written_once (id int, name text)")
      run("INSERT INTO written_once VALUES (1, 'a'), (2, 'b')")
    }
    val source = tableSource("public.written_once")
    dies(source, FaultPoint.AfterBronzeCommit)
    // The table changes after the dead run read it: the batch is the snapshot that was written.
    asOwner(_("INSERT INTO written_once VALUES (3, 'c')"))
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
    asOwner { run =>
      run("CREATE TABLE read_again (id int, name text)")
      run("INSERT INTO read_again VALUES (1, 'a')")
    }
    val source = tableSource("public.read_again")
    dies(source, FaultPoint.AfterCounts)
    hasABatch(source) shouldBe false
    asOwner(_("INSERT INTO read_again VALUES (2, 'b')"))

    val done = ingested(launch(source, reading()))

    // The plan is the table, and a replay reads the table: nothing of the dead run was kept.
    done.batch shouldBe batchId(source, 1)
    done.counts.rowsWritten shouldBe Some(2)
    snapshots(source) shouldBe Seq((1, "a", 1L), (2, "b", 1L))
  }

  // ---------------------------------------------------------------------------------------------
  // The failures of a source, as a run ends with them
  // ---------------------------------------------------------------------------------------------

  test("a statement that the source cuts for running too long may pass, and names its setting") {
    asOwner { run =>
      run("CREATE TABLE ten AS SELECT g AS id, 'n' || g AS name FROM generate_series(1, 10) g")
      // A function that the server takes 0.3 s to answer, row by row.
      run(
        "CREATE FUNCTION slowly(v int) RETURNS int LANGUAGE sql VOLATILE AS " +
          "$$ SELECT v FROM pg_sleep(0.3) $$"
      )
      run("CREATE VIEW slow_ten AS SELECT slowly(id) AS id, name FROM ten")
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
    asOwner { run =>
      run("CREATE TABLE contracts (id int, name text, valid_to date)")
      run("INSERT INTO contracts VALUES (1, 'a', '2027-01-01'), (2, 'b', 'infinity')")
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
