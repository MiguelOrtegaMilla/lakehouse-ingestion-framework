package lakehouse.ingestion.it.jdbc

import java.net.ServerSocket
import java.sql.{Connection, Date, Timestamp}
import java.time.Instant

import scala.util.Using

import io.circe.Json
import lakehouse.ingestion.core.config._
import lakehouse.ingestion.core.control.FailureClass
import lakehouse.ingestion.core.files.LandedFile
import lakehouse.ingestion.core.jdbc.FullLoadPlan
import lakehouse.ingestion.engine.RunFailedException
import lakehouse.ingestion.engine.connector._
import lakehouse.ingestion.engine.jdbc.{JdbcSourceConnector, SourceColumn, SourceDatabase}
import lakehouse.ingestion.it.support.{BootstrappedPostgres, CapturedLogs, LocalSparkSuite}
import org.apache.logging.log4j.Level
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{EitherValues, OptionValues}

/**
 * The JDBC connector against a real PostgreSQL (P6.6): what it asks of a source, what it reads
 * of a table and into which types, how its connections are opened, and what each failure of a
 * source is for its run.
 *
 * The source is the demo database `sales`, read as `sales_ro`, in a server started as the local
 * one is. The role is given the settings of a session that a read must not stand on
 * (`standard_conforming_strings` off, a session that is ended when it is idle in its
 * transaction, a statement timeout of a second), so that a read that works says that the
 * connector set its own. The server logs every statement of the role, and a test reads there
 * what reached it.
 *
 * What a run does with the connector, from its lease to its commit, is `JdbcIngestionSpec`.
 */
final class JdbcSourceConnectorSpec
    extends AnyFunSuite
    with Matchers
    with EitherValues
    with OptionValues
    with Eventually
    with LocalSparkSuite {

  import JdbcSourceConnectorSpec._

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(100, Millis))

  private lazy val pg = BootstrappedPostgres.start()
  private var encrypted: Option[BootstrappedPostgres] = None

  /** A server that takes TLS alone, started for the tests that need one. */
  private def tlsOnly: BootstrappedPostgres = encrypted.getOrElse {
    val started = BootstrappedPostgres.start(tlsOnly = true)
    encrypted = Some(started)
    Using.resource(started.connect("sales_owner", "sales")) { c =>
      execute(c, "CREATE TABLE many AS SELECT g AS id FROM generate_series(1, 10) g")
    }
    started
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Using.resource(pg.connectAsSuperuser("sales")) { c =>
      Seq(
        "ALTER ROLE sales_ro IN DATABASE sales SET log_statement = 'all'",
        "ALTER ROLE sales_ro IN DATABASE sales SET standard_conforming_strings = off",
        "ALTER ROLE sales_ro IN DATABASE sales SET idle_in_transaction_session_timeout = '1s'",
        "ALTER ROLE sales_ro IN DATABASE sales SET statement_timeout = '1s'"
      ).foreach(execute(c, _))
    }
    Using.resource(pg.connect("sales_owner", "sales"))(c => Fixtures.foreach(execute(c, _)))
  }

  override def afterAll(): Unit =
    try {
      encrypted.foreach(_.stop())
      pg.stop()
    } finally super.afterAll()

  // ---------------------------------------------------------------------------------------------
  // A source of the test, and its connector
  // ---------------------------------------------------------------------------------------------

  private def secretsOf(server: BootstrappedPostgres): SecretResolver =
    SecretResolver(Map("SALES_RO_PASSWORD" -> server.password("sales_ro")).get)

  private def connector(server: BootstrappedPostgres = pg) =
    new JdbcSourceConnector(secretsOf(server))

  /** A source that reads `table` of `sales`, as `sales_ro` unless it says otherwise. */
  private def source(
      table: String,
      url: String = pg.jdbcUrl("sales"),
      user: String = "sales_ro",
      tls: String = "{ mode: disable }",
      load: String = "{ mode: full }",
      password: String = "env:SALES_RO_PASSWORD"
  ): Source =
    Source
      .load(
        "test.read.yaml",
        s"""id: test.read
           |connector:
           |  type: jdbc
           |  connection:
           |    url: "$url"
           |    user: $user
           |    password: { secretRef: "$password" }
           |    tls: $tls
           |  table: $table
           |load: $load
           |targets: { bronze: { table: bronze/test/read } }
           |""".stripMargin
      )
      .value
      .value

  private def bound(of: Source, by: JdbcSourceConnector = connector()): BoundSource =
    ConnectorRegistry(by).bind(of)

  private val planContext = PlanContext(
    Instant.parse("2026-11-21T09:00:00Z"),
    watermark = None,
    new FileRegistry {
      override def processed(directory: RelativePath, names: Seq[String]): Set[LandedFile] =
        Set.empty
    }
  )

  private def plan(of: Source, by: JdbcSourceConnector = connector()): Json =
    bound(of, by).plan(planContext).value

  private def read(of: Source, by: JdbcSourceConnector = connector()): ReadResult.OnePass =
    bound(of, by).read(plan(of, by), ReadContext(spark)) match {
      case once: ReadResult.OnePass => once
      case other => fail(s"the rows of a table are read as they are gone over, and not $other")
    }

  /** What `body` fails with, as the engine would have its connector say it. */
  private def failureOf(of: Source, by: JdbcSourceConnector = connector())(
      body: => Any
  ): RunFailedException =
    intercept[Exception](body) match {
      case classed: RunFailedException => classed
      case other =>
        bound(of, by)
          .failure(other)
          .getOrElse(fail(s"the connector does not know the failure for its source's", other))
    }

  private def where(of: Source): String =
    JdbcSourceConnector.where(of, of.connector.asInstanceOf[JdbcConnectorConfig])

  // ---------------------------------------------------------------------------------------------
  // The plan, and what is asked of the source for it
  // ---------------------------------------------------------------------------------------------

  test("the plan of a full load is its table, and the source is asked for its columns once") {
    val orders = source("public.many")
    val mark = logMark()

    val planned = plan(orders)

    planned.noSpaces shouldBe """{"version":1,"table":"public.many"}"""
    FullLoadPlan.fromJson(planned).value.table.toString shouldBe "public.many"
    // The table is named by its identifiers, each quoted, and no row is asked for. The
    // driver then asks the catalog of the server, with a statement of its own, which of the
    // columns may be null.
    eventually {
      val (ofTheDriver, ofTheConnector) =
        statementsSince(mark).map(_.sql).partition(_.contains("pg_catalog."))
      ofTheConnector shouldBe Seq("""SELECT * FROM "public"."many" WHERE 1=0""")
      ofTheDriver should have size 1
    }
    // What the commit of the plan records is said from the plan alone: nothing to start from.
    bound(orders).progress(planned) shouldBe Progress(watermark = None, files = None)
  }

  test("a table without a schema is found as a statement finds it, and named as it is") {
    plan(source("many")).noSpaces shouldBe """{"version":1,"table":"many"}"""
    read(source("many")).rows.count() shouldBe 10L
  }

  test("what the source says of its columns is what the types of Bronze are decided by") {
    val config = source("public.typed").connector.asInstanceOf[JdbcConnectorConfig]
    val database = SourceDatabase.open(
      config.connection,
      secretsOf(pg).resolve(config.connection.password).value
    )
    val columns =
      try database.columns(config.table)
      finally database.close()

    columns.map(_.name) shouldBe TypesInBronze.map(_._1)
    def said(name: String): SourceColumn = columns.find(_.name == name).value
    // The driver's own words for the types that are told apart by them.
    said("id") shouldBe SourceColumn("id", "int4", 10, 0, nullable = false)
    said("c_numeric_10_2") shouldBe SourceColumn(
      "c_numeric_10_2",
      "numeric",
      10,
      2,
      nullable = true
    )
    said("c_numeric") shouldBe SourceColumn("c_numeric", "numeric", 0, 0, nullable = true)
    said("c_numeric_50_2").precision shouldBe 50
    // A scale below zero is given as one that no numeric has.
    said("c_numeric_neg").scale should be > said("c_numeric_neg").precision
    said("c_time").typeName shouldBe "time"
    said("c_timetz").typeName shouldBe "timetz"
    said("c_date").typeName shouldBe "date"
    said("c_timestamp").typeName shouldBe "timestamp"
    said("c_timestamptz").typeName shouldBe "timestamptz"
    (said("c_bit").typeName, said("c_bit").precision) shouldBe ("bit" -> 3)
    (said("c_bit1").typeName, said("c_bit1").precision) shouldBe ("bit" -> 1)
    said("c_numeric_array").typeName shouldBe "_numeric"
    said("c_time_array").typeName shouldBe "_time"
    said("c_tstz_array").typeName shouldBe "_timestamptz"
  }

  // ---------------------------------------------------------------------------------------------
  // The read: the types of the source, and no value changed without a word
  // ---------------------------------------------------------------------------------------------

  test("each type of PostgreSQL is kept in Bronze as the type that holds every value of it") {
    val rows = read(source("public.typed")).rows

    rows.schema.fields.map(f => f.name -> f.dataType).toSeq shouldBe TypesInBronze
  }

  test("no value is changed: what Spark would round or move is the text of the server") {
    val row = read(source("public.typed")).rows.head()
    def value(column: String): Any = row.get(row.fieldIndex(column))

    // As Spark reads them, and to the last digit.
    value("c_numeric_10_2") shouldBe new java.math.BigDecimal("12.34")
    value("c_date") shouldBe Date.valueOf("2026-10-25")
    value("c_timestamp") shouldBe Timestamp.from(Instant.parse("2026-10-25T02:30:00.123456Z"))
    value("c_timestamptz") shouldBe Timestamp.from(Instant.parse("2026-10-25T00:30:00.123456Z"))
    // The last day of the year 9999 is a day like any other, and is no infinity.
    value("c_date_end") shouldBe Date.valueOf("9999-12-31")
    value("c_ts_end") shouldBe Timestamp.from(Instant.parse("9999-12-31T23:59:59.999Z"))
    value("c_bit1") shouldBe true
    value("c_varbit") shouldBe "11"
    value("c_money") shouldBe "$12.34"
    value("c_interval") shouldBe "1 day 02:03:04"
    value("c_char") shouldBe "ch   "
    value("c_int_array") shouldBe Seq(1, 2)

    // As text, where Spark has no type that holds the value.
    value("c_numeric") shouldBe "1.123456789012345678901"
    value("c_numeric_50_2") shouldBe "12345678901234567890123456789012345678901234567.89"
    value("c_numeric_neg") shouldBe "12300"
    value("c_time") shouldBe "12:34:56.789012"
    value("c_timetz") shouldBe "12:34:56+02"
    value("c_bit") shouldBe "101"
    value("c_numeric_array") shouldBe "{1.5,2.123456789012345678901}"
    value("c_time_array") shouldBe "{12:00:00,24:00:00}"
    value("c_tstz_array") shouldBe "{infinity}"
  }

  test("a number that is not one, and a number too long for any decimal, are kept as text") {
    val rows = read(source("public.numbers")).rows

    rows.orderBy("id").collect().map(_.getString(1)).toSeq shouldBe
      Seq("NaN", "12345678901234567890123.45", "-0.000000000000000000001", null)
  }

  test(
    "a read is one statement with a cursor, and its first row is asked for with one of its own"
  ) {
    val many = read(source("public.many"))
    val mark = logMark()

    many.probe.head(1).length shouldBe 1
    // A portal, from which one row was fetched, and no more.
    eventually {
      statementsSince(mark) shouldBe Seq(Logged("portal", """SELECT "id" FROM "public"."many""""))
    }

    val afterTheProbe = logMark()
    many.rows.collect().map(_.getInt(0)).toSeq.sorted shouldBe (1 to 10)
    eventually {
      statementsSince(afterTheProbe) shouldBe Seq(
        Logged("portal", """SELECT "id" FROM "public"."many"""")
      )
    }
  }

  test("every connection to a source has the settings of a session that a read stands on") {
    val config = source("public.many").connector.asInstanceOf[JdbcConnectorConfig]
    val password = secretsOf(pg).resolve(config.connection.password).value
    def settings(options: Map[String, String]): Row =
      spark.read.format("jdbc").options(options).load().head()

    // What the role has by itself, which a read must not stand on.
    settings(
      Map(
        "url" -> pg.jdbcUrl("sales"),
        "user" -> "sales_ro",
        "password" -> pg.password("sales_ro"),
        "dbtable" -> SessionSettings
      )
    ) shouldBe Row("off", "1s", "1s", "UTC")
    // And what a connection of the connector has, whatever the role has: its own statement
    // timeout (30 minutes, of a source), a backslash that is a backslash, and no end to a
    // session that waits in its transaction.
    settings(
      JdbcSourceConnector.options(config.connection, password, SessionSettings, fetch = 1)
    ) shouldBe Row("on", "0", "30min", "UTC")
  }

  test("a read that waits for longer than the role lets a session be idle is not cut") {
    // The role ends a session that is idle in its transaction for a second, and cuts a
    // statement that runs for one: the read waits two between its fetches, and goes on.
    val config = source("public.many").connector.asInstanceOf[JdbcConnectorConfig]
    val password = secretsOf(pg).resolve(config.connection.password).value
    val slow = spark.read
      .format("jdbc")
      .options(JdbcSourceConnector.options(config.connection, password, "\"public\".\"many\"", 5))
      .load()

    // Five rows a fetch, and each row taken slowly: two seconds between the two fetches.
    slow.rdd.map { _ => Thread.sleep(400); 1 }.reduce(_ + _) shouldBe 10
  }

  // ---------------------------------------------------------------------------------------------
  // Infinity
  // ---------------------------------------------------------------------------------------------

  test(
    "the source is asked of each row whether its dates are finite, in the statement of the rows"
  ) {
    val validity = source("public.validity")
    val finite = read(validity)
    val mark = logMark()

    // The rows that are finite are read as they are, the last day of 9999 among them.
    finite.rows.filter("id < 3").collect().toSeq.sortBy(_.getInt(0)) shouldBe Seq(
      Row(1, Date.valueOf("2026-01-01"), Timestamp.from(Instant.parse("2026-01-01T00:00:00Z"))),
      Row(2, Date.valueOf("9999-12-31"), Timestamp.from(Instant.parse("9999-12-31T23:59:59.999Z")))
    )
    finite.rows.columns.toSeq shouldBe Seq("id", "valid_to", "seen_at")
    eventually {
      statementsSince(mark).map(_.sql).loneOrFail should include(
        """FROM (SELECT "_lakehouse_source".*, CASE """ +
          """WHEN NOT isfinite("_lakehouse_source"."valid_to") THEN 1 """ +
          """WHEN NOT isfinite("_lakehouse_source"."seen_at") THEN 2 END AS "_lakehouse_infinite" """ +
          """FROM "public"."validity" AS "_lakehouse_source") AS "_lakehouse_read""""
      )
    }
  }

  test("a row with infinity fails its batch for good, with its column named") {
    val validity = source("public.validity")

    val failed = failureOf(validity)(read(validity).rows.collect())

    failed.failure shouldBe FailureClass.Permanent
    failed.getMessage shouldBe
      s"the column valid_to of the table public.validity of ${where(validity)} has infinity " +
      "or -infinity in a row of the batch, which PostgreSQL has for a date and Spark has " +
      "not: it would be written to Bronze as another date, the last day of the year 9999 " +
      "for infinity. Nothing of the batch was written"
    // And -infinity, of a timestamp, in a table whose name is no word.
    val past = source("public.long_ago")
    failureOf(past)(read(past).rows.collect()).getMessage should startWith(
      s"the column Seen At of the table public.long_ago of ${where(past)} has infinity or " +
        "-infinity in a row of the batch, which PostgreSQL has for a timestamptz and Spark has not"
    )
  }

  test("a table that has a column under the name of the one that is asked for gets another") {
    val named = read(source("public.flagged")).rows

    named.columns.toSeq shouldBe Seq("id", "_lakehouse_infinite", "day")
    named.collect().toSeq shouldBe Seq(Row(1, "the table's own", Date.valueOf("2026-01-01")))
  }

  // ---------------------------------------------------------------------------------------------
  // A plan that is replayed
  // ---------------------------------------------------------------------------------------------

  test("a plan is read as it was written: another table, or another form, is refused") {
    val many = source("public.many")
    val planned = plan(many)

    val moved = intercept[RunFailedException](
      bound(source("public.typed")).read(planned, ReadContext(spark))
    )
    moved.failure shouldBe FailureClass.Permanent
    moved.getMessage should startWith(
      "the batch of test.read was planned as a snapshot of the table public.many, and the " +
        "configuration of the source reads public.typed now"
    )

    val watermark = Json.obj("version" -> Json.fromInt(1), "column" -> Json.fromString("at"))
    Seq[BoundSource => Any](_.read(watermark, ReadContext(spark)), _.progress(watermark)).foreach {
      asked =>
        val refused = intercept[RunFailedException](asked(bound(many)))
        refused.failure shouldBe FailureClass.Permanent
        refused.getMessage should include("it has no table, has a field column")
    }
  }

  // ---------------------------------------------------------------------------------------------
  // What a table must be
  // ---------------------------------------------------------------------------------------------

  test("columns whose names differ only in their case are refused before a batch is planned") {
    val alike = source("public.alike")

    val refused = intercept[RunFailedException](plan(alike))

    refused.failure shouldBe FailureClass.Permanent
    refused.getMessage shouldBe
      s"the table public.alike of ${where(alike)} has columns whose names differ only in " +
      "their case, Total and total, and a table of Delta cannot have both. Nothing was read"
  }

  test("a watermark is asked of the source: a column that the table has, of a timestamp") {
    def incremental(column: String) = source(
      "public.orders",
      load = s"{ mode: incremental, watermark: { column: $column, safety_margin: 1m } }"
    )

    val missing = intercept[RunFailedException](plan(incremental("modified_at")))
    missing.failure shouldBe FailureClass.Permanent
    missing.getMessage shouldBe
      s"the table public.orders of ${where(incremental("modified_at"))} has no column " +
      "modified_at (load.watermark.column). Its columns are id, updated_at, created_on, " +
      "touched_at. Nothing was planned"

    val aDate = intercept[RunFailedException](plan(incremental("created_on")))
    aDate.failure shouldBe FailureClass.Permanent
    aDate.getMessage should include(
      "has its column created_on (load.watermark.column) of type date, and a watermark is " +
        "a timestamptz"
    )
  }

  test("a watermark that may be null is warned of, and its source is not refused for it") {
    def planOf(column: String): Seq[CapturedLogs.Logged] = {
      val incremental = source(
        "public.orders",
        load = s"{ mode: incremental, watermark: { column: $column, safety_margin: 1m } }"
      )
      val (_, logged) = CapturedLogs.of(classOf[JdbcSourceConnector]) {
        // What reads a window is not built yet: the plan ends there, after what it asks.
        intercept[RunFailedException](plan(incremental)).getMessage should include(
          "is loaded by a watermark, and this version of the connector reads a table whole"
        )
      }
      logged.filter(_.level == Level.WARN)
    }

    planOf("touched_at").map(_.message) shouldBe Seq(
      "test.read: in the table public.orders, the column touched_at (load.watermark.column) " +
        "may be null, and a row with no value in it is in no batch: it is never ingested. " +
        "Every writer of the table has to set it, at every change (ADR-0020)"
    )
    // One that the table says is never null has nothing to be warned of.
    planOf("updated_at") shouldBe empty
  }

  // ---------------------------------------------------------------------------------------------
  // The failures of a source, each with its class
  // ---------------------------------------------------------------------------------------------

  test("a table that is not there, or that the role may not read, will not pass") {
    val missing = source("public.nowhere")
    val refused = intercept[RunFailedException](plan(missing))
    refused.failure shouldBe FailureClass.Permanent
    refused.getMessage shouldBe
      s"""${where(missing)}: ERROR: relation "public.nowhere" does not exist (SQLState 42P01)"""

    val kept = source("public.kept_from_the_reader")
    val denied = intercept[RunFailedException](plan(kept))
    denied.failure shouldBe FailureClass.Permanent
    denied.getMessage shouldBe
      s"${where(kept)}: ERROR: permission denied for table kept_from_the_reader (SQLState 42501)"
  }

  test("a password that the source refuses will not pass, and is in no message") {
    val wrong = "not-the-password-of-the-role"
    val orders = source("public.many")
    val by = new JdbcSourceConnector(SecretResolver(Map("SALES_RO_PASSWORD" -> wrong).get))

    val refused = intercept[RunFailedException](plan(orders, by))

    refused.failure shouldBe FailureClass.Permanent
    refused.getMessage should include("(SQLState 28P01)")
    Iterator
      .iterate[Throwable](refused)(_.getCause)
      .takeWhile(_ != null)
      .flatMap(failure => Option(failure.getMessage))
      .foreach(_ should not include wrong)
  }

  test("a password that cannot be read will not pass, and the source is not asked") {
    val orders = source("public.many")
    val mark = logMark()

    val refused = intercept[RunFailedException](
      plan(orders, new JdbcSourceConnector(SecretResolver(Map.empty[String, String].get)))
    )

    refused.failure shouldBe FailureClass.Permanent
    refused.getMessage should startWith(
      s"the password of ${where(orders)} could not be read, and the source was not asked " +
        "anything: "
    )
    refused.getMessage should include("SALES_RO_PASSWORD")
    statementsSince(mark) shouldBe empty
  }

  test("a database that is not there will not pass; a server that is not there may") {
    val elsewhere = source("public.many", url = pg.jdbcUrl("no_such_database"))
    val unknown = intercept[RunFailedException](plan(elsewhere))
    unknown.failure shouldBe FailureClass.Permanent
    unknown.getMessage should include("(SQLState 3D000)")

    // A port of this machine that nothing listens on.
    val closed = Using.resource(new ServerSocket(0))(_.getLocalPort)
    val nobody = source("public.many", url = s"jdbc:postgresql://localhost:$closed/sales")
    val unreachable = intercept[RunFailedException](plan(nobody))
    unreachable.failure shouldBe FailureClass.Transient
    unreachable.getMessage should startWith(s"${where(nobody)}: ")
    unreachable.getMessage should include("(SQLState 08001)")
  }

  test("a value that the type of its column does not hold fails the read for good") {
    // A bounded numeric is a decimal of Spark, which has no NaN.
    val bounded = source("public.bounded")

    val failed = failureOf(bounded)(read(bounded).rows.collect())

    failed.failure shouldBe FailureClass.Permanent
    failed.getMessage should startWith(
      s"${where(bounded)} has a value that does not fit the type that its column is read into"
    )
    failed.getMessage should include("(SQLState 22003)")
  }

  // ---------------------------------------------------------------------------------------------
  // TLS
  // ---------------------------------------------------------------------------------------------

  test("a source that takes TLS alone is read with its certificate verified, and not without") {
    val server = tlsOnly
    val by = connector(server)
    def over(tls: String) = source("public.many", url = server.jdbcUrl("sales"), tls = tls)

    val verified = over(s"{ mode: verify-full, root_certificate: ${server.rootCertificate} }")
    read(verified, by).rows.count() shouldBe 10L

    // In the clear, the server takes no connection: nothing that running again mends.
    val clear = intercept[RunFailedException](plan(over("{ mode: disable }"), by))
    clear.failure shouldBe FailureClass.Permanent
    clear.getMessage should include("(SQLState 28000)")

    // A certificate that another authority signed is as wrong the next time.
    val other = over(s"{ mode: verify-full, root_certificate: ${server.anotherAuthority()} }")
    val untrusted = intercept[RunFailedException](plan(other, by))
    untrusted.failure shouldBe FailureClass.Permanent
    // And Spark's own connections are opened the same way, and fail the same way.
    val config = other.connector.asInstanceOf[JdbcConnectorConfig]
    val password = secretsOf(server).resolve(config.connection.password).value
    val bySpark = intercept[Exception](
      spark.read
        .format("jdbc")
        .options(JdbcSourceConnector.options(config.connection, password, "\"many\"", 1))
        .load()
    )
    by.failure(other, config, bySpark).value.failure shouldBe FailureClass.Permanent
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private def logMark(): Int = pg.serverLog().length

  /** The statements of `sales_ro` that the server logged after `mark`: each once, by its kind. */
  private def statementsSince(mark: Int): Seq[Logged] =
    pg.serverLog()
      .substring(mark)
      .linesIterator
      .collect { case Statement(kind, sql) =>
        Logged(if (kind.contains("/")) "portal" else "whole", sql.trim)
      }
      // The statements of the connector and of Spark, and not what the driver sets a session
      // up with, nor the start and the end of a transaction.
      .filter(_.sql.contains(" FROM "))
      .toSeq

  private def execute(c: Connection, sql: String): Unit =
    Using.resource(c.createStatement())(_.execute(sql): Unit)
}

object JdbcSourceConnectorSpec {

  /** @param kind `whole`, for a statement run whole, and `portal`, for one read with a cursor */
  final private case class Logged(kind: String, sql: String)

  /** A statement as the server logs it when it starts: run whole, or as a portal. */
  private val Statement = """^\S+ \S+ \S+ \[\d+\] LOG:  (execute [^:]+): (.*)$""".r

  implicit final private class LoneOrFail(statements: Seq[String]) {
    def loneOrFail: String = statements match {
      case Seq(one) => one
      case other => throw new AssertionError(s"one statement was expected, and not $other")
    }
  }

  /** What the settings of the session of a connection are, asked in the place of a table. */
  private val SessionSettings =
    "(SELECT current_setting('standard_conforming_strings') AS strings, " +
      "current_setting('idle_in_transaction_session_timeout') AS idle, " +
      "current_setting('statement_timeout') AS statement, " +
      "current_setting('TimeZone') AS zone) AS settings"

  /** The columns of `typed`, each with the type that Bronze keeps it in. */
  private val TypesInBronze: Seq[(String, DataType)] = Seq(
    "id" -> IntegerType,
    "c_smallint" -> ShortType,
    "c_bigint" -> LongType,
    "c_numeric_10_2" -> DecimalType(10, 2),
    "c_numeric" -> StringType,
    "c_numeric_50_2" -> StringType,
    "c_numeric_neg" -> StringType,
    "c_real" -> FloatType,
    "c_double" -> DoubleType,
    "c_boolean" -> BooleanType,
    "c_text" -> StringType,
    "c_varchar" -> StringType,
    "c_char" -> StringType,
    "c_date" -> DateType,
    "c_date_end" -> DateType,
    "c_timestamp" -> TimestampType,
    "c_timestamptz" -> TimestampType,
    "c_ts_end" -> TimestampType,
    "c_time" -> StringType,
    "c_timetz" -> StringType,
    "c_interval" -> StringType,
    "c_bytea" -> BinaryType,
    "c_uuid" -> StringType,
    "c_json" -> StringType,
    "c_jsonb" -> StringType,
    "c_int_array" -> ArrayType(IntegerType),
    "c_text_array" -> ArrayType(StringType),
    "c_numeric_array" -> StringType,
    "c_time_array" -> StringType,
    "c_tstz_array" -> StringType,
    "c_inet" -> StringType,
    "c_money" -> StringType,
    "c_enum" -> StringType,
    "c_xml" -> StringType,
    "c_point" -> StringType,
    "c_bit" -> StringType,
    "c_bit1" -> BooleanType,
    "c_varbit" -> StringType,
    "c_oid" -> DecimalType(20, 0)
  )

  /** The tables of the suite, made by the owner of the source. */
  private val Fixtures: Seq[String] = Seq(
    "CREATE TABLE many AS SELECT g AS id FROM generate_series(1, 10) g",
    "CREATE TYPE mood AS ENUM ('sad', 'happy')",
    """CREATE TABLE typed (
      |id integer NOT NULL, c_smallint smallint, c_bigint bigint, c_numeric_10_2 numeric(10,2),
      |c_numeric numeric, c_numeric_50_2 numeric(50,2), c_numeric_neg numeric(5,-2), c_real real,
      |c_double double precision, c_boolean boolean, c_text text, c_varchar varchar(20),
      |c_char char(5), c_date date, c_date_end date, c_timestamp timestamp,
      |c_timestamptz timestamptz, c_ts_end timestamptz, c_time time, c_timetz timetz,
      |c_interval interval, c_bytea bytea, c_uuid uuid, c_json json, c_jsonb jsonb,
      |c_int_array integer[], c_text_array text[], c_numeric_array numeric[],
      |c_time_array time[], c_tstz_array timestamptz[], c_inet inet, c_money money,
      |c_enum mood, c_xml xml, c_point point, c_bit bit(3), c_bit1 bit(1), c_varbit varbit(5),
      |c_oid oid)""".stripMargin,
    """INSERT INTO typed VALUES (1, 2, 3, 12.34, 1.123456789012345678901,
      |12345678901234567890123456789012345678901234567.89, 12300, 1.5, 2.5, true, 'text',
      |'varchar', 'ch', '2026-10-25', '9999-12-31', '2026-10-25 02:30:00.123456',
      |'2026-10-25 02:30:00.123456+02', '9999-12-31 23:59:59.999+00', '12:34:56.789012',
      |'12:34:56+02', '1 day 02:03:04', '\x0102', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
      |'{"a":1}', '{"b":2}', '{1,2}', '{"x","y"}', '{1.5,2.123456789012345678901}',
      |'{12:00:00,24:00:00}', '{infinity}', '10.0.0.1', '12.34', 'happy', '<a/>', '(1,2)',
      |B'101', B'1', B'11', 42)""".stripMargin,
    "CREATE TABLE numbers (id int, n numeric)",
    """INSERT INTO numbers VALUES (1, 'NaN'), (2, 12345678901234567890123.45),
      |(3, -0.000000000000000000001), (4, NULL)""".stripMargin,
    "CREATE TABLE bounded (id int, n numeric(10,2))",
    "INSERT INTO bounded VALUES (1, 1.5), (2, 'NaN')",
    "CREATE TABLE validity (id int, valid_to date, seen_at timestamptz)",
    """INSERT INTO validity VALUES (1, '2026-01-01', '2026-01-01 00:00:00+00'),
      |(2, '9999-12-31', '9999-12-31 23:59:59.999+00'),
      |(3, 'infinity', '2026-01-01 00:00:00+00')""".stripMargin,
    """CREATE TABLE long_ago (id int, "Seen At" timestamptz)""",
    """INSERT INTO long_ago VALUES (1, '2026-01-01 00:00:00+00'), (2, '-infinity')""",
    """CREATE TABLE flagged (id int, _lakehouse_infinite text, day date)""",
    """INSERT INTO flagged VALUES (1, 'the table''s own', '2026-01-01')""",
    """CREATE TABLE alike (id int, "Total" int, total int)""",
    """CREATE TABLE orders (id int NOT NULL, updated_at timestamptz NOT NULL, created_on date,
      |touched_at timestamptz)""".stripMargin,
    "CREATE TABLE kept_from_the_reader (id int)",
    "REVOKE ALL ON kept_from_the_reader FROM sales_ro"
  )
}
