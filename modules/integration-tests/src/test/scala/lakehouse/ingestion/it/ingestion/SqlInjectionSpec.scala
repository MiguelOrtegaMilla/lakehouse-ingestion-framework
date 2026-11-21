package lakehouse.ingestion.it.ingestion

import java.sql.PreparedStatement
import java.time.{Instant, OffsetDateTime, ZoneOffset}

import scala.util.{Try, Using}

import lakehouse.ingestion.core.config.Source
import lakehouse.ingestion.core.control.FailureClass
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.delta.DeltaNames
import org.apache.spark.sql.{Row => SparkRow}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What reaches a source as SQL, and what does not (ADR-0013 §1), through a run of a source from
 * end to end.
 *
 * Text from outside the engine comes to a statement of a source by two ways: the names that the
 * configuration gives, a table and its watermark column, which the grammar of P3 lets through,
 * and the names that the source gives its own columns, which can be anything. Each is written
 * quoted wherever the engine writes it. A value, of the configuration or of a row, is written
 * into no statement: the bounds of a window reach Spark as predicates, and are times.
 *
 * Every source here connects as `sales_owner`, which owns `canary` and could drop it, and whose
 * role has `standard_conforming_strings` off. As `sales_ro`, which can drop nothing, a test of
 * `canary` would pass whatever the engine wrote. Even so, `canary` is not what tells: the
 * driver sends one statement at a time, so a value written into one would fail it as a syntax
 * error rather than drop a table, and `' OR '1'='1` drops nothing at all. What tells is that
 * each run ends well, with every value in Bronze as it is at the source, and that the server,
 * which logs every statement of `sales_owner`, was sent none of them.
 */
final class SqlInjectionSpec extends AnyFunSuite with Matchers with JdbcIngestionSuite {

  import SqlInjectionSpec._

  override def beforeAll(): Unit = {
    super.beforeAll()
    Using.resource(pg.connectAsSuperuser("sales")) { c =>
      execute(c, "ALTER ROLE sales_owner IN DATABASE sales SET log_statement = 'all'")
      execute(c, "ALTER ROLE sales_owner IN DATABASE sales SET standard_conforming_strings = off")
    }
    asOwner(execute(_, "CREATE TABLE canary (id int)"))
  }

  /** A row written with each value bound, and none in the text of the statement. */
  private def insert(table: String, columns: Seq[String], values: Seq[Any]): Unit =
    asOwner { c =>
      val names = columns.map(quoted).mkString(", ")
      val marks = columns.map(_ => "?").mkString(", ")
      Using.resource(c.prepareStatement(s"INSERT INTO $table ($names) VALUES ($marks)")) {
        statement =>
          values.zipWithIndex.foreach { case (value, i) => bind(statement, i + 1, value) }
          statement.executeUpdate(): Unit
      }
    }

  private def bind(statement: PreparedStatement, at: Int, value: Any): Unit = value match {
    case moment: Instant =>
      statement.setObject(at, OffsetDateTime.ofInstant(moment, ZoneOffset.UTC))
    case time: java.time.LocalTime => statement.setObject(at, time)
    case number: Int => statement.setInt(at, number)
    case text: String => statement.setString(at, text)
    case other => fail(s"no way to bind $other")
  }

  private def canaryIsThere: Boolean =
    asOwner { c =>
      Using.resource(c.createStatement()) { statement =>
        val rows = statement.executeQuery("SELECT to_regclass('public.canary') IS NOT NULL")
        rows.next()
        rows.getBoolean(1)
      }
    }

  /** A source of a table of `sales`, read as `sales_owner`. */
  private def ownersSource(table: String, load: String): Source =
    jdbcSource(table, load, role = "sales_owner")

  private val Whole = FullLoad

  private def byWindow(column: String): String =
    s"""load:
       |  mode: incremental
       |  watermark: { column: $column, safety_margin: 1m, start: "2026-01-01T00:00:00Z" }""".stripMargin

  /** The rows of Bronze, each as the values of `columns` as text, by the first of them. */
  private def bronzeRows(of: Source, columns: Seq[String]): Seq[Seq[String]] =
    spark.read
      .format("delta")
      .load(bronze(of).uri)
      .collect()
      .toSeq
      .map((row: SparkRow) => columns.map(name => text(row.get(row.fieldIndex(name)))))
      .sortBy(_.head)

  private def text(value: Any): String = value match {
    case at: java.sql.Timestamp => at.toInstant.toString
    case other => String.valueOf(other)
  }

  private def loggedSince(mark: Int): String = pg.serverLog().substring(mark)

  /** Whether the server was sent `text`, said without the whole of what it was sent. */
  private def wasSent(sent: String, text: String): Unit =
    withClue(s"the server was not sent $text: ")(sent.contains(text) shouldBe true): Unit
  private def wasNotSent(sent: String, texts: String*): Unit =
    texts.foreach(text =>
      withClue(s"the server was sent $text: ")(sent.contains(text) shouldBe false)
    )

  /** Two runs of the table, whole and by a window, each ending well; and what they sent. */
  private def readTwice(table: String, column: String): (Source, Source, String) = {
    val mark = logMark()
    val whole = ownersSource(table, Whole)
    val window = ownersSource(table, byWindow(column))
    ingested(launch(whole, reading()))
    ingested(launch(window, reading()))
    (whole, window, loggedSince(mark))
  }

  // ---------------------------------------------------------------------------------------------
  // Values
  // ---------------------------------------------------------------------------------------------

  test("a value that would be SQL is read as data, whole and by a window, and is sent nowhere") {
    asOwner(
      execute(_, "CREATE TABLE notes (id int PRIMARY KEY, note text, updated_at timestamptz)")
    )
    Values.zipWithIndex.foreach { case (value, i) =>
      insert("notes", Seq("id", "note", "updated_at"), Seq(i + 1, value, Noon))
    }

    val (whole, window, sent) = readTwice("public.notes", "updated_at")

    val expected = Values.zipWithIndex.map { case (value, i) => Seq(s"${i + 1}", value) }
    bronzeRows(whole, Seq("id", "note")) shouldBe expected
    bronzeRows(window, Seq("id", "note")) shouldBe expected
    // The server was sent the statements of the reads, and no value of a row.
    wasSent(sent, """"public"."notes"""")
    wasNotSent(sent, "canary", "DROP", "'1'='1")
    canaryIsThere shouldBe true
  }

  // ---------------------------------------------------------------------------------------------
  // Names that the configuration gives
  // ---------------------------------------------------------------------------------------------

  test("a table and a column named in mixed case are read by their names, and not by others") {
    asOwner { c =>
      execute(c, """CREATE TABLE "MixedCase" (id int, "UpdatedAt" timestamptz)""")
      execute(
        c,
        """CREATE TABLE mixedcase (id int, "UpdatedAt" timestamptz, updatedat timestamptz)"""
      )
    }
    insert(""""MixedCase"""", Seq("id", "UpdatedAt"), Seq(1, Noon))
    // A table and a column of the same names, case aside, with another row: what an unquoted
    // name would read.
    insert("mixedcase", Seq("id", "UpdatedAt", "updatedat"), Seq(2, Noon, Noon))

    val (whole, window, sent) = readTwice("public.MixedCase", "UpdatedAt")

    bronzeRows(whole, Seq("id", "UpdatedAt")) shouldBe Seq(Seq("1", Noon.toString))
    bronzeRows(window, Seq("id")) shouldBe Seq(Seq("1"))
    wasSent(sent, """"public"."MixedCase"""")
    wasSent(sent, """"UpdatedAt" > '""")
  }

  test("a table and columns named by keywords of SQL are read by their names") {
    asOwner(execute(_, """CREATE TABLE "select" ("order" int, "from" text, "limit" timestamptz)"""))
    insert(""""select"""", Seq("order", "from", "limit"), Seq(1, "where", Noon))

    val (whole, window, sent) = readTwice("public.select", "limit")

    bronzeRows(whole, Seq("order", "from")) shouldBe Seq(Seq("1", "where"))
    bronzeRows(window, Seq("order", "from")) shouldBe Seq(Seq("1", "where"))
    wasSent(sent, """"public"."select"""")
  }

  // ---------------------------------------------------------------------------------------------
  // Names that the source gives
  // ---------------------------------------------------------------------------------------------

  test("the names that a source gives its columns are one name each, wherever they are written") {
    // A quote, in a date that the source is asked whether it is finite; a backtick, in a time
    // that is read as text by a schema that Spark is given; and the marks of a comment, a
    // literal and a qualified name.
    val columns = Seq("id", "a\"b", "it's", "x--y", "p/*q", "t`ick", "a.b", "updated_at")
    asOwner(
      execute(
        _,
        """CREATE TABLE odd_names (id int, "a""b" timestamptz, "it's" text, "x--y" int, """ +
          """"p/*q" text, "t`ick" time, "a.b" text, updated_at timestamptz)"""
      )
    )
    val time = java.time.LocalTime.parse("12:34:56.789")
    insert("odd_names", columns, Seq(1, Noon, Injection, 7, "*/", time, OrTrue, Noon))

    val (whole, window, sent) = readTwice("public.odd_names", "updated_at")

    val expected =
      Seq(Seq("1", Noon.toString, Injection, "7", "*/", "12:34:56.789", OrTrue))
    bronzeRows(whole, columns.init) shouldBe expected
    bronzeRows(window, columns.init) shouldBe expected
    // The date is asked of the source by its name, quoted.
    wasSent(sent, """isfinite("_lakehouse_source"."a""b")""")
    wasNotSent(sent, "canary", "DROP", "'1'='1")
  }

  test("a column whose name a table of Delta cannot have is refused, before a batch is planned") {
    asOwner(execute(_, s"CREATE TABLE unfit (id int, ${quoted(Injection)} text)"))
    insert("unfit", Seq("id", Injection), Seq(1, "a"))
    val mark = logMark()
    val unfit = ownersSource("public.unfit", Whole)

    val outcome = launch(unfit, reading())

    outcome match {
      case Outcome.Failed(_, failure, message) =>
        failure shouldBe FailureClass.Permanent
        message should include(s"cannot have, ${quoted(Injection)}")
        message should include("A view of the table can give them other names")
      case other => fail(s"the run did not fail: $other")
    }
    batchesOf(unfit) shouldBe empty
    tableIsThere(unfit) shouldBe false
    // The source was asked for the columns of the table, and the name went nowhere.
    wasNotSent(loggedSince(mark), "canary")
    canaryIsThere shouldBe true
  }

  test("the characters that the engine refuses in a name are those that Delta refuses") {
    def takes(name: String): Boolean =
      Try(
        spark
          .range(1)
          .toDF(name)
          .write
          .format("delta")
          .save(workDir.resolve(s"names/${name.hashCode}").toString)
      ).isSuccess

    DeltaNames.Refused.foreach(c => withClue(s"${c.toInt}: ")(takes(s"a${c}b") shouldBe false))
    Seq("a\"b", "it's", "t`ick", "a.b", "x--y", "p/*q", "ñandú", "a|b", "a\\b").foreach { name =>
      withClue(s"$name: ")(takes(name) shouldBe true)
      DeltaNames.fits(name) shouldBe true
    }
  }
}

object SqlInjectionSpec {

  private val Injection = "'; DROP TABLE canary; --"
  private val Values = Seq(Injection, "\\'; DROP TABLE canary; --", "' OR '1'='1", "a\"b")
  private val OrTrue = "' OR '1'='1"
  private val Noon = Instant.parse("2026-01-01T12:00:00Z")

  /** A name as one identifier of PostgreSQL. */
  private def quoted(name: String): String = "\"" + name.replace("\"", "\"\"") + "\""
}
