package lakehouse.ingestion.it.jdbc

import java.sql.{Connection, SQLException}
import java.time.Instant
import java.util.TimeZone
import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._
import scala.util.Using

import lakehouse.ingestion.it.support.{BootstrappedPostgres, LocalSparkSuite}
import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.execution.ExplainMode
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

/**
 * What Spark 4.1's JDBC source and the PostgreSQL driver do with a read of a table: the facts
 * that the JDBC connector is built on (discovery §17, P6).
 *
 * These are characterisation tests, as those of `DeltaSetTransactionContractSpec` are. No code
 * of the engine is in them. Each pins a behaviour that the connector relies on, or one that it
 * must defend against, so that an upgrade of Spark or of the driver that changes it fails here,
 * by the name of the assumption, and not in a batch that reads the wrong rows (ADR-0014,
 * upgrade procedure).
 *
 * '''What reaches the server is read from the server.''' The source is the demo database
 * `sales`, read as `sales_ro`, the role of the connector, in a server started as the local one
 * is. The server logs every statement of that role (`log_statement`), with its form in the
 * protocol: a statement run whole, or a portal and each fetch from it. A test asserts what it
 * logged, and not what Spark says that it sent.
 *
 * The JVM and the session are in UTC, as those of the engine are; the database is not, so that
 * a test can tell which zone a value was read in.
 */
final class SparkJdbcContractSpec
    extends AnyFunSuite
    with Matchers
    with LocalSparkSuite
    with Eventually {

  import SparkJdbcContractSpec._

  // The server's log reaches the container's a moment after the statement.
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(100, Millis))

  private lazy val pg = BootstrappedPostgres.start()

  override def beforeAll(): Unit = {
    super.beforeAll()
    Using.resource(pg.connectAsSuperuser("sales")) { c =>
      execute(c, "ALTER ROLE sales_ro IN DATABASE sales SET log_statement = 'all'")
      execute(c, s"ALTER DATABASE sales SET timezone = '$DatabaseZone'")
    }
    Using.resource(pg.connect("sales_owner", "sales")) { c =>
      Fixtures.foreach(execute(c, _))
    }
  }

  override def afterAll(): Unit =
    try pg.stop()
    finally super.afterAll()

  // ---------------------------------------------------------------------------------------------
  // The statements of a read
  // ---------------------------------------------------------------------------------------------

  test("a read of a table is two statements, each on a connection of its own") {
    val since = logMark()
    jdbc("orders").collect().length shouldBe 3

    eventually {
      statementsSince(since, "orders").map(s => (s.kind, s.sql)) shouldBe Seq(
        // Asked when the frame is made: the columns, and no row.
        "execute <unnamed>" -> "SELECT * FROM orders WHERE 1=0",
        // Asked when the rows are: the columns by name, and the table as it was given.
        "execute <unnamed>" -> """SELECT "id","updated_at","note" FROM orders"""
      )
    }
    statementsSince(since, "orders").map(_.process).distinct should have size 2
  }

  test("a range of a timestamp column reaches the server as predicates, half-open and exact") {
    val since = logMark()
    val lower = Instant.parse("2026-10-25T00:45:00.123456Z")
    val upper = Instant.parse("2026-10-25T01:50:00Z")

    ids(jdbc("events").filter(col("at") > lit(lower) && col("at") <= lit(upper))) shouldBe
      Seq(4, 5, 6, 7)

    eventually {
      statementsSince(since, "events").map(_.sql).last shouldBe
        """SELECT "id" FROM events  WHERE ("at" IS NOT NULL) AND """ +
        """("at" > '2026-10-25 00:45:00.123456') AND ("at" <= '2026-10-25 01:50:00.0')"""
    }
    // The row at the lower bound is not read, the row a microsecond after it is, and so is the
    // row at the upper bound: a bound keeps its microseconds, as PostgreSQL keeps them.
    timesOf(Seq(3, 4, 7)) shouldBe Seq(
      Instant.parse("2026-10-25T00:45:00.123456Z"),
      Instant.parse("2026-10-25T00:45:00.123457Z"),
      Instant.parse("2026-10-25T01:50:00Z")
    )
  }

  // ---------------------------------------------------------------------------------------------
  // The zones that take part
  // ---------------------------------------------------------------------------------------------

  test("the driver sets the zone of a connection to the JVM's, and the database's takes no part") {
    def zoneOfConnection: (String, String) = {
      val row = jdbc("(SELECT setting, source FROM pg_settings WHERE name = 'TimeZone') AS asked")
        .head()
      (row.getString(0), row.getString(1))
    }

    // The driver names a zone when it connects, and the server takes it from the client.
    zoneOfConnection shouldBe ("UTC" -> "client")
    inZones(jvm = Madrid, session = "UTC", java8Api = false)(zoneOfConnection) shouldBe
      (Madrid -> "client")
    inZones(jvm = "UTC", session = Madrid, java8Api = true)(zoneOfConnection) shouldBe
      ("UTC" -> "client")
    // A client that names none is in the zone of the database.
    pg.exec("psql", "-d", "sales", "-tAc", "SHOW TimeZone") shouldBe DatabaseZone
  }

  test("a bound is written in the zone of the JVM, or of the session if dates are of java.time") {
    // An hour before the clocks go back in Madrid, and so not in the hour that repeats.
    val bound = Instant.parse("2026-10-24T23:40:00Z")
    val right = Seq(1, 2, 3, 4, 5, 6, 7, 8)

    // What the session gives Spark's JDBC source is a `java.sql.Timestamp`, written as its
    // `toString`, in the zone of the JVM, unless `spark.sql.datetime.java8API.enabled` has it
    // give an `Instant`, written in the zone of the session. The server reads the literal in the
    // zone of the connection, which is the JVM's.
    val cases = Seq(
      // (the JVM's, the session's, java.time) -> (the literal, the rows read)
      ("UTC", "UTC", false) -> ("2026-10-24 23:40:00.0", right),
      ("UTC", "UTC", true) -> ("2026-10-24 23:40:00", right),
      ("UTC", Madrid, false) -> ("2026-10-24 23:40:00.0", right),
      (Madrid, "UTC", false) -> ("2026-10-25 01:40:00.0", right),
      (Madrid, Madrid, true) -> ("2026-10-25 01:40:00", right),
      // Written in one zone and read in another: off by the difference, two hours one way or
      // the other, with rows lost or read again.
      ("UTC", Madrid, true) -> ("2026-10-25 01:40:00", Seq(7, 8)),
      (Madrid, "UTC", true) -> ("2026-10-24 23:40:00", 0 +: right)
    )
    cases.foreach { case ((jvm, session, java8Api), (literal, rows)) =>
      withClue(s"JVM in $jvm, session in $session, java.time $java8Api: ") {
        val since = logMark()
        inZones(jvm, session, java8Api)(ids(jdbc("events").filter(col("at") > lit(bound)))) shouldBe
          rows
        eventually {
          lastPredicate(since, "events") shouldBe s"""("at" IS NOT NULL) AND ("at" > '$literal')"""
        }
      }
    }
  }

  test(
    "in a zone whose clocks go back, two bounds an hour apart are one literal, and rows are lost"
  ) {
    // The clocks of Madrid go from 03:00 back to 02:00 at 01:00 UTC on 2026-10-25: from 00:00
    // to 02:00 UTC, every time of a clock of Madrid is said twice.
    val first = Instant.parse("2026-10-25T00:40:00Z") // 02:40, summer time
    val second = Instant.parse("2026-10-25T01:40:00Z") // 02:40, winter time

    def readAfter(bound: Instant): (Seq[Int], String) = {
      val since = logMark()
      val rows = inZones(jvm = Madrid, session = Madrid, java8Api = false)(
        ids(jdbc("events").filter(col("at") > lit(bound)))
      )
      (rows, eventually(lastPredicate(since, "events")))
    }

    val (afterFirst, firstLiteral) = readAfter(first)
    val (afterSecond, secondLiteral) = readAfter(second)
    firstLiteral shouldBe """("at" IS NOT NULL) AND ("at" > '2026-10-25 02:40:00.0')"""
    secondLiteral shouldBe firstLiteral
    // PostgreSQL takes a time that is said twice as the later of the two: the first bound reads
    // after the second, and the rows between them are lost.
    afterSecond shouldBe Seq(7, 8)
    afterFirst shouldBe Seq(7, 8)
    // In UTC, the first bound reads them.
    ids(jdbc("events").filter(col("at") > lit(first))) shouldBe Seq(3, 4, 5, 6, 7, 8)
    ids(jdbc("events").filter(col("at") > lit(second))) shouldBe Seq(7, 8)
  }

  // ---------------------------------------------------------------------------------------------
  // Names and values
  // ---------------------------------------------------------------------------------------------

  test("a table is written into the statement as it is given, and a column is quoted by Spark") {
    val since = logMark()
    jdbc("\"Order Lines\"").columns.toSeq shouldBe Seq("OrderId", "Updated At")
    jdbc("\"Order Lines\"").collect().length shouldBe 1
    jdbc("quoted").collect().length shouldBe 1
    eventually {
      statementsSince(since, "\"Order Lines\"").map(_.sql).last shouldBe
        """SELECT "OrderId","Updated At" FROM "Order Lines""""
      // A quote in the name of a column is written twice.
      statementsSince(since, "quoted").map(_.sql).last shouldBe """SELECT "odd""name" FROM quoted"""
    }

    // Unquoted, the name of a table is two words of SQL: the caller quotes it, or nobody does.
    val unquoted = intercept[SparkException](jdbc("Order Lines"))
    serverState(unquoted) shouldBe Some("42601")
  }

  test("a value is written with its quotes doubled, and its backslashes as they are") {
    val since = logMark()
    ids(jdbc("events").filter(col("note") === Injection)) shouldBe Seq(7)
    ids(jdbc("events").filter(col("note") === BackslashInjection)) shouldBe Seq(8)
    eventually {
      statementsSince(since, "events")
        .map(_.sql)
        .filter(_.contains("note"))
        .map(predicateOf) shouldBe
        Seq(
          """("note" IS NOT NULL) AND ("note" = '''; DROP TABLE canary; --')""",
          """("note" IS NOT NULL) AND ("note" = '\''; DROP TABLE canary; --')"""
        )
    }
  }

  test("a value is data only while the session has standard_conforming_strings on") {
    // With it off, a backslash escapes the quote after it: the literal of the value ends early,
    // and what follows is SQL. The protocol takes one statement, so it fails as a syntax error.
    val off = "sessionInitStatement" -> "SET standard_conforming_strings = off"
    val failure = intercept[SparkException] {
      jdbc("events", off).filter(col("note") === BackslashInjection).collect()
    }
    serverState(failure) shouldBe Some("42601")
    ids(jdbc("events", off).filter(col("note") === Injection)) shouldBe Seq(7)
    tableExists("canary") shouldBe true
  }

  // ---------------------------------------------------------------------------------------------
  // Types
  // ---------------------------------------------------------------------------------------------

  test("the type that each type of PostgreSQL arrives as") {
    jdbc("typed").schema.fields.map(f => f.name -> f.dataType).toSeq shouldBe TypesAsRead
  }

  test("values that arrive changed, without a word") {
    val row = jdbc("typed").head()
    def value(column: String): Any = row.get(row.fieldIndex(column))

    // An unbounded numeric is a decimal of 18 places, rounded half up.
    value("c_numeric") shouldBe new java.math.BigDecimal("1.123456789012345679")
    // A timestamp without a zone is read in the zone of the JVM.
    value("c_timestamp") shouldBe java.sql.Timestamp.from(
      Instant.parse("2026-10-25T02:30:00.123456Z")
    )
    value("c_timestamptz") shouldBe java.sql.Timestamp.from(
      Instant.parse("2026-10-25T02:30:00.123456Z")
    )
    // Infinity is the last millisecond of 9999.
    value("c_infinity") shouldBe java.sql.Timestamp.from(Instant.parse("9999-12-31T23:59:59.999Z"))
    // A time of day is a timestamp of 1970-01-01, and its zone is applied.
    value("c_time") shouldBe java.sql.Timestamp.from(Instant.parse("1970-01-01T12:34:56.789Z"))
    value("c_timetz") shouldBe java.sql.Timestamp.from(Instant.parse("1970-01-01T10:34:56Z"))
    // Money is text, with the symbol of the currency of the server's locale.
    value("c_money") shouldBe "$12.34"
    value("c_interval") shouldBe "1 day 02:03:04"
  }

  test("a numeric that does not fit Spark's decimal fails the read") {
    // More than twenty digits before the point, in an unbounded numeric, or a scale below zero.
    Seq("wide_numbers", "negative_scale").foreach { table =>
      withClue(table) {
        val failure = intercept[Exception](jdbc(table).collect())
        stateOf(failure) shouldBe Some("22003")
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The cursor, and the timeouts of the server
  // ---------------------------------------------------------------------------------------------

  test("without fetchsize a read is one statement run whole; with it, a portal and its fetches") {
    val whole = logMark()
    ids(jdbc("many")) shouldBe (1 to 10)
    eventually(statementsSince(whole, "many").map(_.kind) shouldBe Seq(Run, Run))

    val fetched = logMark()
    ids(jdbc("many", "fetchsize" -> "3")) shouldBe (1 to 10)
    // Three rows, three, three and one: a fetch that gives fewer rows than were asked is the
    // last, and the portal closes with it.
    eventually {
      statementsSince(fetched, "many").map(_.kind) shouldBe
        Seq(Run, Portal, Fetch, Fetch, Fetch)
    }
  }

  test("the statement timeout of the server bounds each fetch of a cursor, and not the read") {
    val timeout = "sessionInitStatement" -> s"SET statement_timeout = '$StatementTimeout'"

    // Ten rows at 0.4 s each is twice the timeout, read two at a time: each fetch is quick.
    jdbc("many", "fetchsize" -> "2", timeout).rdd.foreach(_ => Thread.sleep(400))
    // So is a read whole, which the server ends before the rows are taken.
    jdbc("many", timeout).rdd.foreach(_ => Thread.sleep(400))

    // A row of `slow_many` takes the server 0.3 s: one at a time is in time, ten at once is not.
    jdbc("slow_many", "fetchsize" -> "1", timeout).collect().length shouldBe 10
    val cut = intercept[SparkException](jdbc("slow_many", "fetchsize" -> "10", timeout).collect())
    serverState(cut) shouldBe Some("57014")
  }

  test("a read that waits between two fetches is idle in its transaction for the server") {
    val idle = "sessionInitStatement" -> "SET idle_in_transaction_session_timeout = '1s'"
    val cut = intercept[SparkException] {
      jdbc("many", "fetchsize" -> "2", idle).rdd.foreach(_ => Thread.sleep(3000))
    }
    serverState(cut) shouldBe Some("25P03")
  }

  // ---------------------------------------------------------------------------------------------
  // A probe for a first row
  // ---------------------------------------------------------------------------------------------

  test("a probe for a first row ends at it under a cursor, and reads the whole table without one") {
    val probe = logMark()
    jdbc("many", "fetchsize" -> "1").head(1).length shouldBe 1
    jdbc("many", "fetchsize" -> "1").filter(col("id") > 10).head(1) shouldBe empty
    // `isEmpty` asks for no column.
    jdbc("many", "fetchsize" -> "1").isEmpty shouldBe false
    // One fetch of one row each, and none after it.
    eventually {
      statementsSince(probe, "many").filter(_.kind != Run).map(s => (s.kind, s.sql)) shouldBe Seq(
        Portal -> """SELECT "id" FROM many""",
        Portal -> """SELECT "id" FROM many  WHERE ("id" IS NOT NULL) AND ("id" > 10)""",
        Portal -> """SELECT 1 FROM many"""
      )
    }

    // No LIMIT is pushed to the server: without a cursor, the whole result is sent.
    val whole = logMark()
    jdbc("many").head(1).length shouldBe 1
    eventually {
      statementsSince(whole, "many").map(s => (s.kind, s.sql)).last shouldBe
        (Run -> """SELECT "id" FROM many""")
    }
  }

  // ---------------------------------------------------------------------------------------------
  // What is said of a read
  // ---------------------------------------------------------------------------------------------

  test("the password of a read is in no plan of it and in no event of its execution") {
    val heard = new ConcurrentLinkedQueue[String]
    val listener = new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
        case start: SparkListenerSQLExecutionStart => heard.add(start.toString): Unit
        case _ => ()
      }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      val frame = jdbc("orders")
      frame.collect().length shouldBe 3
      val plans = Seq("simple", "extended", "codegen", "cost", "formatted")
        .map(mode => frame.queryExecution.explainString(ExplainMode.fromString(mode)))
      plans.foreach(_ should include("JDBCRelation(orders)"))
      plans.foreach(_ should not include pg.password("sales_ro"))
      eventually(heard.asScala.exists(_.contains("JDBCRelation(orders)")) shouldBe true)
      heard.asScala.foreach(_ should not include pg.password("sales_ro"))
    } finally spark.sparkContext.removeSparkListener(listener)
  }

  test("a failed read has Spark's own state, and the server's in the failure it wraps") {
    val missing = intercept[SparkException](jdbc("nowhere"))
    missing.getSqlState shouldBe "42000"
    missing.getMessage should include("SELECT * FROM nowhere WHERE 1=0")
    serverState(missing) shouldBe Some("42P01")

    val refused = intercept[Exception](jdbc("orders", "password" -> "not the password"))
    serverState(refused) shouldBe Some("28P01")
    messagesOf(refused).foreach(_ should not include "not the password")
  }

  // ---------------------------------------------------------------------------------------------
  // A read written to Delta
  // ---------------------------------------------------------------------------------------------

  test("an append of a read pulls its rows through one cursor, and its commit counts them") {
    val path = tablePath("many")
    val since = logMark()
    jdbc("many", "fetchsize" -> "3").write.format("delta").mode("append").save(path)

    eventually {
      statementsSince(since, "many").map(_.kind) shouldBe Seq(Run, Portal, Fetch, Fetch, Fetch)
    }
    val log = DeltaLog.forTable(spark, path)
    val metrics = log.getChanges(0L).toSeq.flatMap(_._2).collect { case c: CommitInfo =>
      c.operationMetrics
    }
    metrics.flatten.map(_.get("numOutputRows")) shouldBe Seq(Some("10"))
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private def jdbc(table: String, options: (String, String)*): DataFrame =
    spark.read
      .format("jdbc")
      .option("url", pg.jdbcUrl("sales"))
      .option("user", "sales_ro")
      .option("password", pg.password("sales_ro"))
      .option("dbtable", table)
      .options(options.toMap)
      .load()

  private def ids(frame: DataFrame): Seq[Int] =
    frame.select("id").collect().map(_.getInt(0)).sorted.toSeq

  /** When the rows of `events` with these ids were changed, read without Spark. */
  private def timesOf(ids: Seq[Int]): Seq[Instant] =
    Using.resource(pg.connect("sales_ro", "sales")) { c =>
      Using.resource(c.createStatement()) { st =>
        Using.resource(
          st.executeQuery(s"SELECT at FROM events WHERE id IN (${ids.mkString(",")}) ORDER BY id")
        ) { rs =>
          Iterator
            .continually(rs.next())
            .takeWhile(identity)
            .map(_ => rs.getObject(1, classOf[java.time.OffsetDateTime]).toInstant)
            .toSeq
        }
      }
    }

  private def tableExists(name: String): Boolean =
    Using.resource(pg.connect("sales_owner", "sales")) { c =>
      Using.resource(c.createStatement()) { st =>
        Using.resource(st.executeQuery(s"SELECT to_regclass('public.$name') IS NOT NULL")) { rs =>
          rs.next() && rs.getBoolean(1)
        }
      }
    }

  /**
   * `body` with the JVM, the session and the conversion of dates of the session set so, and
   * set back after it. The JVM is one for the whole suite: nothing else runs meanwhile.
   */
  private def inZones[T](jvm: String, session: String, java8Api: Boolean)(body: => T): T = {
    val before = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone(jvm))
    spark.conf.set("spark.sql.session.timeZone", session)
    spark.conf.set("spark.sql.datetime.java8API.enabled", java8Api.toString)
    try body
    finally {
      TimeZone.setDefault(before)
      spark.conf.set("spark.sql.session.timeZone", "UTC")
      spark.conf.unset("spark.sql.datetime.java8API.enabled")
    }
  }

  /** How long the server's log is now: what a statement after this logs is after it. */
  private def logMark(): Int = pg.serverLog().length

  /** The statements of `sales_ro` that the server logged after `mark`, on `table`. */
  private def statementsSince(mark: Int, table: String): Seq[Logged] =
    pg.serverLog()
      .substring(mark)
      .linesIterator
      .collect { case Statement(process, kind, sql) => Logged(process, kind, sql.trim) }
      .filter(_.sql.contains(s"FROM $table"))
      .toSeq

  private def lastPredicate(mark: Int, table: String): String =
    predicateOf(statementsSince(mark, table).map(_.sql).last)

  private def predicateOf(sql: String): String = sql.substring(sql.indexOf("WHERE") + 6)

  private def execute(c: Connection, sql: String): Unit =
    Using.resource(c.createStatement())(_.execute(sql): Unit)
}

object SparkJdbcContractSpec {

  private val Madrid = "Europe/Madrid"

  /** The zone of the database, which neither the JVM nor the session is in. */
  private val DatabaseZone = "America/New_York"

  private val StatementTimeout = "2s"

  private val Injection = "'; DROP TABLE canary; --"
  private val BackslashInjection = "\\'; DROP TABLE canary; --"

  /** A statement as the server logs it: run whole, as a portal, or a fetch from a portal. */
  private val Run = "execute <unnamed>"
  private val Portal = "execute <unnamed>/C_1"
  private val Fetch = "execute fetch from <unnamed>/C_1"

  final private case class Logged(process: String, kind: String, sql: String)

  private val Statement =
    """^\S+ \S+ \S+ \[(\d+)\] LOG:  (execute(?: fetch from)? [^:]+): (.*)$""".r

  /** The SQLState of the innermost failure that has one, as the engine reads it. */
  private def serverState(failure: Throwable): Option[String] =
    causes(failure).collect {
      case sql: SQLException if Option(sql.getSQLState).exists(_.nonEmpty) => sql.getSQLState
    }.lastOption

  /** The SQLState of the first failure that has one, a server's or Spark's. */
  private def stateOf(failure: Throwable): Option[String] =
    causes(failure).collectFirst {
      case sql: SQLException if Option(sql.getSQLState).exists(_.nonEmpty) => sql.getSQLState
      case spark: SparkThrowable if Option(spark.getSqlState).exists(_.nonEmpty) =>
        spark.getSqlState
    }

  private def causes(failure: Throwable): Seq[Throwable] =
    Iterator.iterate(failure)(_.getCause).takeWhile(_ != null).toSeq

  private def messagesOf(failure: Throwable): Seq[String] =
    causes(failure).flatMap(f => Option(f.getMessage))

  private val TypesAsRead: Seq[(String, DataType)] = Seq(
    "c_smallint" -> ShortType,
    "c_integer" -> IntegerType,
    "c_bigint" -> LongType,
    "c_numeric_10_2" -> DecimalType(10, 2),
    "c_numeric" -> DecimalType(38, 18),
    "c_real" -> FloatType,
    "c_double" -> DoubleType,
    "c_boolean" -> BooleanType,
    "c_text" -> StringType,
    "c_varchar" -> StringType,
    "c_char" -> StringType,
    "c_date" -> DateType,
    "c_timestamp" -> TimestampType,
    "c_timestamptz" -> TimestampType,
    "c_infinity" -> TimestampType,
    "c_time" -> TimestampType,
    "c_timetz" -> TimestampType,
    "c_interval" -> StringType,
    "c_bytea" -> BinaryType,
    "c_uuid" -> StringType,
    "c_json" -> StringType,
    "c_jsonb" -> StringType,
    "c_int_array" -> ArrayType(IntegerType),
    "c_text_array" -> ArrayType(StringType),
    "c_inet" -> StringType,
    "c_money" -> StringType,
    "c_enum" -> StringType,
    "c_xml" -> StringType,
    "c_point" -> StringType,
    "c_bit" -> BinaryType,
    "c_oid" -> DecimalType(20, 0)
  )

  /** The tables of the suite, made by the owner of the source. */
  private val Fixtures: Seq[String] = Seq(
    "CREATE TABLE orders (id int, updated_at timestamptz, note text)",
    "INSERT INTO orders VALUES (1, now(), 'a'), (2, now(), 'b'), (3, now(), 'c')",
    "CREATE TABLE events (id int, at timestamptz, note text)",
    """INSERT INTO events VALUES
      |(0, '2026-10-24 22:30:00Z', 'z'),
      |(1, '2026-10-25 00:15:00Z', 'a'), (2, '2026-10-25 00:30:00Z', 'b'),
      |(3, '2026-10-25 00:45:00.123456Z', 'c'), (4, '2026-10-25 00:45:00.123457Z', 'd'),
      |(5, '2026-10-25 01:15:00Z', 'e'), (6, '2026-10-25 01:30:00Z', 'f'),
      |(7, '2026-10-25 01:50:00Z', '''; DROP TABLE canary; --'),
      |(8, '2026-10-25 02:30:00Z', E'\\''; DROP TABLE canary; --')""".stripMargin,
    "CREATE TABLE canary (id int)",
    """CREATE TABLE "Order Lines" ("OrderId" int, "Updated At" timestamptz)""",
    """INSERT INTO "Order Lines" VALUES (1, now())""",
    """CREATE TABLE quoted ("odd""name" int)""",
    """INSERT INTO quoted VALUES (1)""",
    "CREATE TYPE mood AS ENUM ('sad', 'happy')",
    """CREATE TABLE typed (
      |c_smallint smallint, c_integer integer, c_bigint bigint, c_numeric_10_2 numeric(10,2),
      |c_numeric numeric, c_real real, c_double double precision, c_boolean boolean,
      |c_text text, c_varchar varchar(20), c_char char(5), c_date date, c_timestamp timestamp,
      |c_timestamptz timestamptz, c_infinity timestamptz, c_time time, c_timetz timetz,
      |c_interval interval, c_bytea bytea, c_uuid uuid, c_json json, c_jsonb jsonb,
      |c_int_array integer[], c_text_array text[], c_inet inet, c_money money, c_enum mood,
      |c_xml xml, c_point point, c_bit bit(3), c_oid oid)""".stripMargin,
    """INSERT INTO typed VALUES (1, 2, 3, 12.34, 1.123456789012345678901, 1.5, 2.5, true,
      |'text', 'varchar', 'ch', '2026-10-25', '2026-10-25 02:30:00.123456',
      |'2026-10-25 02:30:00.123456+00', 'infinity', '12:34:56.789', '12:34:56+02',
      |'1 day 02:03:04', '\x0102', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"a":1}',
      |'{"b":2}', '{1,2}', '{"x","y"}', '10.0.0.1', '12.34', 'happy', '<a/>', '(1,2)',
      |B'101', 42)""".stripMargin,
    "CREATE TABLE wide_numbers (n numeric)",
    "INSERT INTO wide_numbers VALUES (12345678901234567890123.45)",
    "CREATE TABLE negative_scale (n numeric(5,-2))",
    "INSERT INTO negative_scale VALUES (12300)",
    "CREATE TABLE many AS SELECT g AS id FROM generate_series(1, 10) g",
    // A function that the server takes 0.3 s to answer, row by row.
    "CREATE FUNCTION slowly(v int) RETURNS int LANGUAGE sql VOLATILE AS " +
      "$$ SELECT v FROM pg_sleep(0.3) $$",
    "CREATE VIEW slow_many AS SELECT slowly(id) AS id FROM many"
  )
}
