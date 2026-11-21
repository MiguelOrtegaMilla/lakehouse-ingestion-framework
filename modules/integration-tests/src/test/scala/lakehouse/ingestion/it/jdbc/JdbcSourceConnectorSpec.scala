package lakehouse.ingestion.it.jdbc

import java.net.ServerSocket
import java.sql.{Connection, Date, DriverManager, SQLException, Timestamp}
import java.time.temporal.ChronoUnit
import java.time.{Instant, OffsetDateTime}
import java.util.{Properties, TimeZone}

import scala.util.Using

import io.circe.Json
import lakehouse.ingestion.core.config._
import lakehouse.ingestion.core.control.FailureClass
import lakehouse.ingestion.core.files.LandedFile
import lakehouse.ingestion.core.jdbc.FullLoadPlan
import lakehouse.ingestion.core.watermark.{StoredWatermark, WatermarkHorizon, WatermarkPlan}
import lakehouse.ingestion.engine.RunFailedException
import lakehouse.ingestion.engine.connector._
import lakehouse.ingestion.engine.control.PostgresJdbc
import lakehouse.ingestion.engine.jdbc.{
  JdbcSourceConnector,
  SecretRefConnectionProvider,
  SourceColumn,
  SourceDatabase
}
import lakehouse.ingestion.it.support.{BootstrappedPostgres, CapturedLogs, LocalSparkSuite}
import org.apache.logging.log4j.Level
import org.apache.spark.sql.{DataFrame, Row}
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
      password: String = "env:SALES_RO_PASSWORD",
      textColumns: Seq[String] = Nil
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
           |  text_columns: [${textColumns.mkString(", ")}]
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

  test("the ten days of 1582 that the calendar of Java has not are read as ten days later") {
    // Pinned as it is, and not as it should be (discovery §20). PostgreSQL counts by the
    // Gregorian calendar from its first day. Its driver gives Spark a date as a
    // `java.sql.Date`, whose calendar goes from 4 October 1582 to the 15th: a day between the
    // two is none of its days, and is taken as the one ten days on, which the table may
    // hold in another row. Any other day, of any year, is the day that the source has.
    def asRead(rows: DataFrame): Seq[(String, String, String)] =
      rows
        .selectExpr("id", "cast(day AS string)", "cast(at AS string)", "cast(at_tz AS string)")
        .collect()
        .sortBy(_.getInt(0))
        .map(row => (row.getString(1), row.getString(2), row.getString(3)))
        .toSeq

    asRead(read(source("public.calendar")).rows) shouldBe Seq(
      ("1582-10-04", "1582-10-04 12:00:00", "1582-10-04 12:00:00"),
      ("1582-10-15", "1582-10-15 12:00:00", "1582-10-15 12:00:00"),
      ("1582-10-24", "1582-10-24 23:59:59.999999", "1582-10-24 23:59:59.999999"),
      ("1582-10-15", "1582-10-15 00:00:00", "1582-10-15 00:00:00"),
      ("1500-02-28", "1500-02-28 12:00:00", "1500-02-28 12:00:00"),
      ("0001-01-01", "0001-01-01 00:00:00", "0001-01-01 00:00:00")
    )
    // As text, which is what keeps them: each day as the source writes it.
    asRead(read(source("public.calendar", textColumns = Seq("day", "at", "at_tz"))).rows) shouldBe
      Seq(
        ("1582-10-04", "1582-10-04 12:00:00", "1582-10-04 12:00:00+00"),
        ("1582-10-05", "1582-10-05 12:00:00", "1582-10-05 12:00:00+00"),
        ("1582-10-14", "1582-10-14 23:59:59.999999", "1582-10-14 23:59:59.999999+00"),
        ("1582-10-15", "1582-10-15 00:00:00", "1582-10-15 00:00:00+00"),
        ("1500-02-28", "1500-02-28 12:00:00", "1500-02-28 12:00:00+00"),
        ("0001-01-01", "0001-01-01 00:00:00", "0001-01-01 00:00:00+00")
      )
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
    val resolver = SecretRefConnectionProvider.register(secretsOf(pg))
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
    ) shouldBe Row("off", "1s", "1s", "UTC", "off")
    // And what a connection of the connector has, whatever the role has: its own statement
    // timeout (30 minutes, of a source), a backslash that is a backslash, no end to a session
    // that waits in its transaction, and transactions that write nothing.
    settings(
      JdbcSourceConnector.options(config.connection, resolver, SessionSettings, fetch = 1)
    ) shouldBe Row("on", "0", "30min", "UTC", "on")
  }

  test("a session of a source writes nothing though its role may, as a guard and not a barrier") {
    val config = source("public.many").connector.asInstanceOf[JdbcConnectorConfig]
    val properties = new Properties
    PostgresJdbc.ofASource(config.connection).foreach { case (name, value) =>
      properties.setProperty(name, value)
    }
    // The owner of the tables of the source, which may write to them and make others.
    properties.setProperty("user", "sales_owner")
    properties.setProperty("password", pg.password("sales_owner"))

    Using.resource(DriverManager.getConnection(pg.jdbcUrl("sales"), properties)) { c =>
      Seq("DELETE FROM many", "CREATE TABLE made_by_a_read (id int)").foreach { sql =>
        intercept[SQLException](execute(c, sql)).getSQLState shouldBe "25006"
      }
      // It is the default of the session, which the session itself may change: it keeps a
      // statement from writing by mistake. What keeps the engine from writing to a source is
      // the role that it connects as, which reads alone (sales_ro, ADR-0013).
      execute(c, "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE")
      execute(c, "CREATE TABLE made_by_a_read (id int)")
      execute(c, "DROP TABLE made_by_a_read")
    }
  }

  test("a read that waits for longer than the role lets a session be idle is not cut") {
    // The role ends a session that is idle in its transaction for a second, and cuts a
    // statement that runs for one: the read waits two between its fetches, and goes on.
    val config = source("public.many").connector.asInstanceOf[JdbcConnectorConfig]
    val resolver = SecretRefConnectionProvider.register(secretsOf(pg))
    val slow = spark.read
      .format("jdbc")
      .options(JdbcSourceConnector.options(config.connection, resolver, "\"public\".\"many\"", 5))
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
      "for infinity. Nothing of the batch was written. Add valid_to to " +
      "connector.text_columns, and the column is read as text, each value as the source " +
      "writes it"
    // And -infinity, of a timestamp, in a column whose name the configuration cannot write.
    val past = source("public.long_ago")
    val unnamed = failureOf(past)(read(past).rows.collect()).getMessage
    unnamed should startWith(
      s"the column Seen-At of the table public.long_ago of ${where(past)} has infinity or " +
        "-infinity in a row of the batch, which PostgreSQL has for a timestamptz and Spark has not"
    )
    unnamed should endWith(
      "connector.text_columns would have the column read as text, each value as the source " +
        "writes it, and takes a name of letters, digits and underscores, which this one is " +
        "not: read the table through a view that gives the column such a name"
    )
  }

  test("a column that the configuration has read as text holds infinity as the source writes it") {
    val validity = source("public.validity", textColumns = Seq("valid_to"))
    val asText = read(validity)
    val mark = logMark()

    asText.rows.schema.fields.map(f => f.name -> f.dataType).toSeq shouldBe Seq(
      "id" -> IntegerType,
      "valid_to" -> StringType,
      "seen_at" -> TimestampType
    )
    asText.rows.collect().toSeq.sortBy(_.getInt(0)).map(_.getString(1)) shouldBe
      Seq("2026-01-01", "9999-12-31", "infinity")
    // The source is asked of the dates that are still read as dates, and of no other.
    eventually {
      val asked = statementsSince(mark).map(_.sql).loneOrFail
      asked should include("""CASE WHEN NOT isfinite("_lakehouse_source"."seen_at") THEN 1 END""")
      asked should not include "valid_to\")"
    }

    // Every date of the table as text: the table is read by its name, and nothing is asked.
    val both = read(source("public.validity", textColumns = Seq("seen_at", "valid_to")))
    val afterIt = logMark()
    both.rows.collect().toSeq.sortBy(_.getInt(0)).map(_.getString(2)) shouldBe Seq(
      "2026-01-01 00:00:00+00",
      "9999-12-31 23:59:59.999+00",
      "2026-01-01 00:00:00+00"
    )
    eventually {
      statementsSince(afterIt).map(_.sql).loneOrFail shouldBe
        """SELECT "id","valid_to","seen_at" FROM "public"."validity""""
    }
  }

  test("a row with infinity is refused for its own column, whatever other is read as text") {
    // A span that is open at both ends. With one of its dates read as text, the source is
    // asked of the other alone, and it is the other that the failure names.
    def refusedWith(asText: String): String = {
      val spans = source("public.spans", textColumns = Seq(asText))
      failureOf(spans)(read(spans).rows.collect()).getMessage
    }
    val table = s"of the table public.spans of ${where(source("public.spans"))}"

    refusedWith("valid_from") should startWith(s"the column valid_to $table has infinity or ")
    refusedWith("valid_from") should include("Add valid_to to connector.text_columns")
    refusedWith("valid_to") should startWith(s"the column valid_from $table has infinity or ")
    refusedWith("valid_to") should include("Add valid_from to connector.text_columns")

    // With both as text, the span is read as the source writes it.
    read(source("public.spans", textColumns = Seq("valid_from", "valid_to"))).rows
      .collect()
      .map(row => row.getString(1) -> row.getString(2))
      .toSeq shouldBe Seq("-infinity" -> "infinity")
  }

  test("a column of any type is read as text if the configuration says so, and only that one") {
    val typed =
      read(source("public.typed", textColumns = Seq("c_numeric_10_2", "c_date", "c_text")))
    val row = typed.rows.head()
    def value(column: String): Any = row.get(row.fieldIndex(column))

    value("c_numeric_10_2") shouldBe "12.34"
    value("c_date") shouldBe "2026-10-25"
    value("c_text") shouldBe "text"
    // The others, as their types say.
    val asText = Set("c_numeric_10_2", "c_date")
    typed.rows.schema.fields.map(f => f.name -> f.dataType).toSeq shouldBe TypesInBronze.map {
      case (name, _) if asText(name) => name -> StringType
      case other => other
    }
  }

  test("a column that the configuration names and the table has not is refused before a plan") {
    val misnamed = source("public.validity", textColumns = Seq("valid_to", "Valid_To", "until"))

    val refused = intercept[RunFailedException](plan(misnamed))

    refused.failure shouldBe FailureClass.Permanent
    refused.getMessage shouldBe
      s"the table public.validity of ${where(misnamed)} has no column Valid_To, until, which " +
      "connector.text_columns names: a column is named as the source writes it, with its " +
      "case. Its columns are id, valid_to, seen_at. Nothing was read"
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
      val (_, logged) = CapturedLogs.of(classOf[JdbcSourceConnector])(plan(incremental))
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

  test("a watermark that is a timestamp without a zone is warned of, at every plan") {
    val changes = source(
      "public.changes",
      load = "{ mode: incremental, watermark: { column: touched_at, safety_margin: 1m } }"
    )
    def warned(): Seq[String] = {
      val (planned, logged) = CapturedLogs.of(classOf[JdbcSourceConnector])(plan(changes))
      // It is warned of, and not refused: the batch is planned.
      WatermarkPlan.fromJson(planned).value.column.value shouldBe "touched_at"
      logged.filter(_.level == Level.WARN).map(_.message)
    }

    val warnings = warned()
    // The column may be null as well, which is warned of beside it.
    warnings should have size 2
    warnings.last shouldBe
      "test.read: in the table public.changes, the column touched_at (load.watermark.column) " +
      "is a timestamp without a zone, and is taken as UTC. If the writers of the table stamp " +
      "it in another zone, a row is read late where that zone is ahead of UTC, by the " +
      "difference, and is never ingested where it is behind: its time is before the " +
      "watermark already when it is written. A timestamptz has no such doubt (ADR-0020)"
    warned() shouldBe warnings
  }

  // ---------------------------------------------------------------------------------------------
  // A load by a watermark: the window of a plan, and what it reads
  // ---------------------------------------------------------------------------------------------

  /** A source that is loaded by a watermark, an hour behind the clock of its source. */
  private def byWatermark(
      table: String = "public.changes",
      column: String = "updated_at",
      more: String = ""
  ): Source =
    source(
      table,
      load = s"{ mode: incremental, watermark: { column: $column, safety_margin: 1h }$more }"
    )

  /** The plan of a window, as a run would have written it down. */
  private def windowOf(column: String, lower: Option[Instant], upper: Instant = Upper): Json =
    WatermarkPlan(SqlIdentifier.parse(column).value, lower, upper, horizon = upper).toJson

  private def windowRead(of: Source, window: Json): ReadResult.OnePass =
    bound(of).read(window, ReadContext(spark)) match {
      case once: ReadResult.OnePass => once
      case other => fail(s"the rows of a window are read as they are gone over, and not $other")
    }

  private def idsOf(rows: DataFrame): Seq[Int] =
    rows.select("id").collect().map(_.getInt(0)).sorted.toSeq

  /** What the source says the time is, asked by the test. */
  private def clockOfTheSource(): Instant =
    Using.resource(pg.connect("sales_owner", "sales")) { c =>
      Using.resource(c.createStatement()) { statement =>
        val rows = statement.executeQuery("SELECT CURRENT_TIMESTAMP")
        rows.next()
        rows.getObject(1, classOf[OffsetDateTime]).toInstant
      }
    }

  /** The statement that asked the source for the rows of `table`, after `mark`. */
  private def rowsStatement(mark: Int, table: String): Logged =
    statementsSince(mark)
      .filter(_.sql.contains(s""""public"."$table""""))
      .filterNot(_.sql.contains("WHERE 1=0"))
      .lastOption
      .getOrElse(throw new AssertionError(s"no statement asked for the rows of $table"))

  test("the plan of a watermark is a window of the clock of the source, and not the engine's") {
    val changes = byWatermark()
    // The clock of the engine is years from that of the source: it bounds nothing.
    val context = planContext.copy(now = Instant.parse("2001-01-01T00:00:00Z"))
    val mark = logMark()
    val before = clockOfTheSource()

    val planned = WatermarkPlan.fromJson(bound(changes).plan(context).value).value

    val after = clockOfTheSource()
    planned.column.value shouldBe "updated_at"
    // The first batch of a source with no start: the table, up to the horizon.
    planned.lower shouldBe None
    planned.upper shouldBe planned.horizon
    planned.more shouldBe false
    // The horizon is what the source said, less the margin. The two looks of the test are
    // around it, by a clock that this machine steps by most of a second.
    val said = planned.horizon.plus(1, ChronoUnit.HOURS)
    withClue(s"the source said $said, between $before and $after: ") {
      said.isBefore(before.minusSeconds(5)) shouldBe false
      said.isAfter(after.plusSeconds(5)) shouldBe false
    }
    // Asked with a constant statement, after the columns of the table.
    eventually {
      everyStatementSince(mark).filterNot(sql =>
        sql.contains("pg_catalog.") || sql.startsWith("SET ")
      ) shouldBe Seq("""SELECT * FROM "public"."changes" WHERE 1=0""", "SELECT CURRENT_TIMESTAMP")
    }

    // What the commit of the plan records is its upper bound, said with nothing asked.
    val quiet = logMark()
    bound(changes).progress(planned.toJson) shouldBe
      Progress(watermark = Some(StoredWatermark(planned.upper).toJson), files = None)
    Thread.sleep(300)
    everyStatementSince(quiet) shouldBe empty
  }

  test("the next window starts from the watermark that the engine gives, as the load says") {
    val changes = byWatermark(more = ", max_batch_range: 1d")
    val reached = Instant.parse("2026-11-01T10:00:00Z")
    val context = planContext.copy(watermark = Some(StoredWatermark(reached).toJson))

    val planned = WatermarkPlan.fromJson(bound(changes).plan(context).value).value

    planned.lower shouldBe Some(reached)
    // Cut by the range, counted from the watermark: the source is weeks behind its clock.
    planned.upper shouldBe Instant.parse("2026-11-02T10:00:00Z")
    planned.more shouldBe true
  }

  test("a later run of a chain plans against the horizon that it is given, and asks no clock") {
    val changes = byWatermark(more = ", max_batch_range: 1d")
    val reached = Instant.parse("2026-11-01T10:00:00Z")
    val chain = WatermarkHorizon(Instant.parse("2026-11-02T22:00:00Z"))
    val context = planContext.copy(
      watermark = Some(StoredWatermark(reached).toJson),
      horizon = Some(chain.toJson)
    )
    val mark = logMark()

    val first = WatermarkPlan.fromJson(bound(changes).plan(context).value).value

    // Cut by the range, and bounded by the horizon of the chain and not by the clock.
    (first.lower, first.upper, first.horizon) shouldBe
      ((Some(reached), Instant.parse("2026-11-02T10:00:00Z"), chain.value))
    bound(changes).more(first.toJson) shouldBe Some(chain.toJson)
    eventually {
      everyStatementSince(mark).filterNot(sql =>
        sql.contains("pg_catalog.") || sql.startsWith("SET ")
      ) shouldBe Seq("""SELECT * FROM "public"."changes" WHERE 1=0""")
    }

    // The next reaches the horizon, and the chain ends with it; and after it, nothing is new.
    val last = WatermarkPlan
      .fromJson(
        bound(changes)
          .plan(context.copy(watermark = Some(StoredWatermark(first.upper).toJson)))
          .value
      )
      .value
    last.upper shouldBe chain.value
    bound(changes).more(last.toJson) shouldBe None
    bound(changes).plan(
      context.copy(watermark = Some(StoredWatermark(last.upper).toJson))
    ) shouldBe None
    // A snapshot of a table is all there is.
    val whole = source("public.changes")
    bound(whole).more(bound(whole).plan(planContext).value) shouldBe None
  }

  test("a horizon that is not one this application wrote plans nothing") {
    val changes = byWatermark()
    val refused =
      failureOf(changes)(
        bound(changes).plan(planContext.copy(horizon = Some(Json.fromString("noon"))))
      )
    refused.failure shouldBe FailureClass.Permanent
    refused.getMessage should include(
      "was given to plan against is not one that this application wrote"
    )
  }

  test("a window is read by its bounds, which reach the source as predicates of its statement") {
    val changes = byWatermark()
    val mark = logMark()

    val read = windowRead(changes, windowOf("updated_at", Some(Lower)))

    // The row at the lower bound is not in the window, the one a microsecond after it is, and
    // so is the one at the upper bound: `(lower, upper]`, to the microsecond.
    idsOf(read.rows) shouldBe Seq(3, 4)
    eventually {
      val rows = rowsStatement(mark, "changes")
      rows.kind shouldBe "portal"
      rows.sql should endWith(
        """ WHERE ("updated_at" IS NOT NULL) AND ("updated_at" > '2026-11-01 10:00:00.123456') """ +
          """AND ("updated_at" <= '2026-11-01 11:00:00.0')"""
      )
    }

    // The first row is asked for with the same bounds.
    val asked = logMark()
    read.probe.head(1) should have length 1
    eventually {
      rowsStatement(asked, "changes").sql should endWith(
        """("updated_at" > '2026-11-01 10:00:00.123456') AND ("updated_at" <= '2026-11-01 11:00:00.0')"""
      )
    }
  }

  test("a first window has no lower bound, and a row with no watermark is in no window") {
    // A timestamp without a zone, which is taken as UTC, and may be null.
    val touched = byWatermark(column = "touched_at")
    val mark = logMark()

    // Every row up to the upper bound, but the one whose watermark is null.
    idsOf(windowRead(touched, windowOf("touched_at", lower = None)).rows) shouldBe Seq(1, 2, 3, 4)
    eventually {
      rowsStatement(mark, "changes").sql should endWith(
        """ WHERE ("touched_at" IS NOT NULL) AND ("touched_at" <= '2026-11-01 11:00:00.0')"""
      )
    }
    // And its bounds are as exact as those of a timestamp with a zone.
    idsOf(windowRead(touched, windowOf("touched_at", Some(Lower))).rows) shouldBe Seq(3, 4)
    idsOf(
      windowRead(touched, windowOf("touched_at", Some(Upper), Upper.plusSeconds(60))).rows
    ) shouldBe Seq(5)
  }

  test("a row whose watermark is infinity is in no window, and -infinity fails the first one") {
    val ever = byWatermark(table = "public.ever")

    // Neither is a time between two bounds.
    idsOf(windowRead(ever, windowOf("updated_at", Some(Lower))).rows) shouldBe Seq(1)
    // No upper bound is after infinity, however late: such a row is never ingested.
    val late = Instant.parse("9999-12-31T23:59:59.999999Z")
    idsOf(windowRead(ever, windowOf("updated_at", Some(Lower), late)).rows) shouldBe Seq(1)

    // A window with no lower bound has the row at -infinity, which Bronze cannot keep.
    val refused = failureOf(ever)(
      idsOf(windowRead(ever, windowOf("updated_at", lower = None)).rows)
    )
    refused.failure shouldBe FailureClass.Permanent
    refused.getMessage shouldBe
      s"the column updated_at of the table public.ever of ${where(ever)} has -infinity in a " +
      "row of the batch, and it is the watermark of the source (load.watermark.column): the " +
      "row was changed at no time that a batch is bounded by, and Spark would write it to " +
      "Bronze as another date. Nothing of the batch was written. A watermark is read as the " +
      "timestamp that it is, and never as text: give the row a time at its source, or have " +
      "the source start after it (load.watermark.start), since a window that has a lower " +
      "bound holds no -infinity"
  }

  test("a plan is read under the load, and the watermark column, that it was planned under") {
    val whole = source("public.changes")
    val changes = byWatermark()
    val snapshot = plan(whole)
    val window = windowOf("updated_at", Some(Lower))
    val asked: Seq[(BoundSource, Json) => Any] =
      Seq(_.read(_, ReadContext(spark)), _.progress(_))

    asked.foreach { ask =>
      // A snapshot of the table is not read as a window, nor a window as a snapshot.
      val asAWindow = intercept[RunFailedException](ask(bound(changes), snapshot))
      asAWindow.failure shouldBe FailureClass.Permanent
      asAWindow.getMessage shouldBe
        "the batch of test.read was planned as a snapshot of its table (load.mode: full), " +
        "and the source has load.mode: incremental now. A plan is read under the load that " +
        "it was planned under, and nothing was read. Give the source the load that it had, " +
        "run it, and change its load once the batch is committed"
      val asASnapshot = intercept[RunFailedException](ask(bound(whole), window))
      asASnapshot.failure shouldBe FailureClass.Permanent
      asASnapshot.getMessage should startWith(
        "the batch of test.read was planned as a window of a watermark (load.mode: " +
          "incremental), and the source has load.mode: full now"
      )

      // A window of one column says nothing of another.
      val ofAnother =
        intercept[RunFailedException](ask(bound(byWatermark(column = "touched_at")), window))
      ofAnother.failure shouldBe FailureClass.Permanent
      ofAnother.getMessage shouldBe
        "the batch of test.read was planned as a window of the column updated_at " +
        "(load.watermark.column), and the configuration of the source has touched_at now. A " +
        "plan is read as it was written, and nothing was read. Give the source the watermark " +
        "column that it had and run it again"

      // And a document of no form of this connector is of no load.
      val ofNone = intercept[RunFailedException](
        ask(bound(changes), Json.obj("version" -> Json.fromInt(1)))
      )
      ofNone.getMessage should startWith(
        "the plan of the batch of test.read is not one that the JDBC connector reads, and " +
          "nothing was read: it has no column"
      )
    }
  }

  test("a clock of the source that is behind its watermark plans nothing, and is warned of") {
    val changes = byWatermark()
    def planned(watermark: Instant): (Option[Json], Seq[String]) = {
      val context = planContext.copy(watermark = Some(StoredWatermark(watermark).toJson))
      val (plan, logged) =
        CapturedLogs.of(classOf[JdbcSourceConnector])(bound(changes).plan(context))
      (plan, logged.filter(_.level == Level.WARN).map(_.message))
    }

    // As if the clock had gone back by a day: the watermark is a day after it.
    val ahead = clockOfTheSource().plus(1, ChronoUnit.DAYS)
    val (none, warnings) = planned(ahead)
    none shouldBe None
    warnings should have size 1
    warnings.head should startWith("test.read: the clock of its source says ")
    warnings.head should include(s", which is before the watermark of the source, $ahead. ")
    warnings.head should include("No batch is planned until it has passed the watermark by ")

    // A source that is read up to its horizon has nothing new, and nothing to be warned of:
    // its watermark is behind the clock, by less than the margin.
    planned(clockOfTheSource().minus(30, ChronoUnit.MINUTES)) shouldBe (None -> Nil)
  }

  test("a window is read only where the JVM and the session of Spark are in UTC") {
    val changes = byWatermark()
    val window = windowOf("updated_at", Some(Lower))
    val mark = logMark()
    def refusal(): String = {
      val refused = intercept[RunFailedException](bound(changes).read(window, ReadContext(spark)))
      refused.failure shouldBe FailureClass.Permanent
      refused.getMessage
    }

    val zoneOfTheJvm = TimeZone.getDefault
    val inMadrid =
      try {
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Madrid"))
        refusal()
      } finally TimeZone.setDefault(zoneOfTheJvm)
    inMadrid should startWith(
      "test.read is loaded by a watermark, and the JVM is in Europe/Madrid, which is not UTC. "
    )
    inMadrid should include("Nothing was read")

    val sessionInMadrid =
      try {
        spark.conf.set("spark.sql.session.timeZone", "Europe/Madrid")
        refusal()
      } finally spark.conf.set("spark.sql.session.timeZone", "UTC")
    sessionInMadrid should startWith(
      "test.read is loaded by a watermark, and the session of Spark is in Europe/Madrid " +
        "(spark.sql.session.timeZone), which is not UTC. "
    )

    // The source was asked nothing, and in UTC the window is read.
    everyStatementSince(mark) shouldBe empty
    idsOf(windowRead(changes, window).rows) shouldBe Seq(3, 4)
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

  test("a server that is busy may pass: no connection left, and a lock not given in time") {
    val many = source("public.many")
    def asSuperuser(statements: String*): Unit =
      Using.resource(pg.connectAsSuperuser("sales"))(c => statements.foreach(execute(c, _)))

    // The role may open no connection more, and the server says so to the one that asks.
    val full =
      try {
        asSuperuser("ALTER ROLE sales_ro CONNECTION LIMIT 0")
        intercept[RunFailedException](plan(many))
      } finally asSuperuser("ALTER ROLE sales_ro CONNECTION LIMIT -1")
    full.failure shouldBe FailureClass.Transient
    full.getMessage shouldBe
      s"""${where(many)}: FATAL: too many connections for role "sales_ro" (SQLState 53300)"""

    // The table is being altered, and the role lets a statement wait 300 ms for its lock.
    val locked =
      try {
        asSuperuser("ALTER ROLE sales_ro IN DATABASE sales SET lock_timeout = '300ms'")
        Using.resource(pg.connect("sales_owner", "sales")) { altering =>
          altering.setAutoCommit(false)
          execute(altering, "LOCK TABLE many IN ACCESS EXCLUSIVE MODE")
          intercept[RunFailedException](plan(many))
        }
      } finally asSuperuser("ALTER ROLE sales_ro IN DATABASE sales RESET lock_timeout")
    locked.failure shouldBe FailureClass.Transient
    locked.getMessage shouldBe
      s"${where(many)}: ERROR: canceling statement due to lock timeout (SQLState 55P03)"

    // Nobody changed anything of the source, and the next plan finds it free.
    plan(many) shouldBe FullLoadPlan(QualifiedName.parse("public.many").value).toJson
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
    failed.getMessage should endWith("add the column to connector.text_columns")
    // Which is what keeps it.
    read(source("public.bounded", textColumns = Seq("n"))).rows
      .collect()
      .map(_.getString(1))
      .toSet shouldBe Set("1.50", "NaN")
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
    val resolver = SecretRefConnectionProvider.register(secretsOf(server))
    val bySpark = intercept[Exception](
      spark.read
        .format("jdbc")
        .options(JdbcSourceConnector.options(config.connection, resolver, "\"many\"", 1))
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

  /** Every statement of `sales_ro` that the server logged after `mark`, as it was written. */
  private def everyStatementSince(mark: Int): Seq[String] =
    pg.serverLog()
      .substring(mark)
      .linesIterator
      .collect { case Statement(_, sql) => sql.trim }
      .toSeq

  private def execute(c: Connection, sql: String): Unit =
    Using.resource(c.createStatement())(_.execute(sql): Unit)
}

object JdbcSourceConnectorSpec {

  /** @param kind `whole`, for a statement run whole, and `portal`, for one read with a cursor */
  final private case class Logged(kind: String, sql: String)

  /** A statement as the server logs it when it starts: run whole, or as a portal. */
  private val Statement = """^\S+ \S+ \S+ \[\d+\] LOG:  (execute [^:]+): (.*)$""".r

  /** The bounds of the window that the tests of a watermark read of `changes`. */
  private val Lower = Instant.parse("2026-11-01T10:00:00.123456Z")
  private val Upper = Instant.parse("2026-11-01T11:00:00Z")

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
      "current_setting('TimeZone') AS zone, " +
      "current_setting('default_transaction_read_only') AS read_only) AS settings"

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
    // The day before the ten that the calendar of Java has not, the first and the last of
    // them, the day after, and two days long before.
    "CREATE TABLE calendar (id int, day date, at timestamp, at_tz timestamptz)",
    """INSERT INTO calendar VALUES
      |(1, '1582-10-04', '1582-10-04 12:00:00', '1582-10-04 12:00:00+00'),
      |(2, '1582-10-05', '1582-10-05 12:00:00', '1582-10-05 12:00:00+00'),
      |(3, '1582-10-14', '1582-10-14 23:59:59.999999', '1582-10-14 23:59:59.999999+00'),
      |(4, '1582-10-15', '1582-10-15 00:00:00', '1582-10-15 00:00:00+00'),
      |(5, '1500-02-28', '1500-02-28 12:00:00', '1500-02-28 12:00:00+00'),
      |(6, '0001-01-01', '0001-01-01 00:00:00', '0001-01-01 00:00:00+00')""".stripMargin,
    "CREATE TABLE spans (id int, valid_from date, valid_to date)",
    "INSERT INTO spans VALUES (1, '-infinity', 'infinity')",
    """CREATE TABLE long_ago (id int, "Seen-At" timestamptz)""",
    """INSERT INTO long_ago VALUES (1, '2026-01-01 00:00:00+00'), (2, '-infinity')""",
    """CREATE TABLE flagged (id int, _lakehouse_infinite text, day date)""",
    """INSERT INTO flagged VALUES (1, 'the table''s own', '2026-01-01')""",
    """CREATE TABLE alike (id int, "Total" int, total int)""",
    """CREATE TABLE orders (id int NOT NULL, updated_at timestamptz NOT NULL, created_on date,
      |touched_at timestamptz)""".stripMargin,
    // A row at each side of each bound of the window `(Lower, Upper]`, to the microsecond, with
    // the same time in a timestamp without a zone; and one whose such timestamp is null.
    """CREATE TABLE changes (id int NOT NULL, updated_at timestamptz NOT NULL,
      |touched_at timestamp, valid_to date)""".stripMargin,
    """INSERT INTO changes VALUES
      |(1, '2026-11-01 10:00:00.123455+00', '2026-11-01 10:00:00.123455', '2027-01-01'),
      |(2, '2026-11-01 10:00:00.123456+00', '2026-11-01 10:00:00.123456', '2027-01-01'),
      |(3, '2026-11-01 10:00:00.123457+00', '2026-11-01 10:00:00.123457', '2027-01-01'),
      |(4, '2026-11-01 11:00:00+00', '2026-11-01 11:00:00', '2027-01-01'),
      |(5, '2026-11-01 11:00:00.000001+00', '2026-11-01 11:00:00.000001', '2027-01-01'),
      |(6, '2026-11-01 11:00:00.000001+00', NULL, '2027-01-01')""".stripMargin,
    "CREATE TABLE ever (id int NOT NULL, updated_at timestamptz NOT NULL)",
    """INSERT INTO ever VALUES (1, '2026-11-01 10:30:00+00'), (2, 'infinity'),
      |(3, '-infinity')""".stripMargin,
    "CREATE TABLE kept_from_the_reader (id int)",
    "REVOKE ALL ON kept_from_the_reader FROM sales_ro"
  )
}
