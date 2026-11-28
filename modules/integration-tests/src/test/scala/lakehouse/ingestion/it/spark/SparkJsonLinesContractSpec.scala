package lakehouse.ingestion.it.spark

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.zip.GZIPOutputStream

import scala.util.Using

import com.fasterxml.jackson.core.json.PackageVersion
import com.fasterxml.jackson.core.{JsonFactory, JsonParseException, JsonParser, JsonToken}
import com.fasterxml.jackson.core.io.JsonEOFException
import lakehouse.ingestion.it.support.LocalSparkSuite
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, Row}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What Jackson, as Spark 4.1 carries it, and Spark 4.1 say of the lines of a file of JSON: the
 * facts that the read of a record of JSON as it was written is built on (discovery §17, P8.8;
 * §20).
 *
 * A source of JSON has one record to a line, and no field to say otherwise: its connector reads
 * JSON a line at a time, and no option of Spark is passed through (P8 breakdown). Spark's reader
 * of JSON takes the first value of a line, and says nothing of what comes after it; and it keeps
 * one of the values of a field written twice, or refuses the batch, by where it meets it. So the
 * lines are to be read as text, and each looked at by Jackson before Spark reads its fields.
 * These tests pin what that rests on, with the options of `FileSourceConnector`, and no code of
 * the engine in them: characterisation tests, as those of `SparkUnreadableFileContractSpec`
 * are. An upgrade that changes one of these facts fails here, with the name of the assumption
 * that it breaks (ADR-0014, upgrade procedure).
 */
final class SparkJsonLinesContractSpec extends AnyFunSuite with Matchers with LocalSparkSuite {

  import SparkJsonLinesContractSpec._

  private var count = 0

  /** A file of JSON of its own, with the text given, in gzip if it says so. */
  private def file(text: String, gzip: Boolean = false): Path = {
    count += 1
    val path = workDir.resolve(s"lines-$count.json${if (gzip) ".gz" else ""}")
    if (gzip)
      Using
        .resource(new GZIPOutputStream(Files.newOutputStream(path)))(_.write(text.getBytes(UTF_8)))
    else Files.write(path, text.getBytes(UTF_8))
    path
  }

  /** The rows of a file of JSON, read with its columns as text, as the connector reads them. */
  private def read(path: Path, columns: String*): Seq[Row] =
    spark.read
      .option("ignoreCorruptFiles", "false")
      .option("columnNameOfCorruptRecord", Unparsed)
      .option("mode", "PERMISSIVE")
      .schema(asText(columns))
      .json(s"file:$path")
      .collect()
      .toSeq

  /** The columns of a file of JSON, inferred as the connector infers them: case counts. */
  private def inferred(path: Path): Seq[String] = {
    val exact = spark.newSession()
    exact.conf.set("spark.sql.caseSensitive", "true")
    exact.read
      .option("columnNameOfCorruptRecord", Unparsed)
      .option("primitivesAsString", "true")
      .json(s"file:$path")
      .schema
      .fieldNames
      .toSeq
  }

  /** The lines of a file, as Spark reads a file as text. */
  private def lines(path: Path): Seq[String] =
    spark.read.text(s"file:$path").collect().toSeq.map(_.getString(0))

  // ---------------------------------------------------------------------------------------------
  // Jackson, as Spark carries it
  // ---------------------------------------------------------------------------------------------

  test("the Jackson that Spark carries is com.fasterxml's 2.21.2, and Spark's reader uses it") {
    PackageVersion.VERSION.toString shouldBe "2.21.2"
    new JsonFactory().version().getGroupId shouldBe "com.fasterxml.jackson.core"
    // The parser of Spark's reader of JSON is made by that factory, of that jar.
    classOf[org.apache.spark.sql.catalyst.json.JacksonParser].getMethods
      .exists(_.getParameterTypes.contains(classOf[JsonParser])) shouldBe true
    classOf[JsonFactory].getProtectionDomain.getCodeSource.getLocation.toString should
      endWith("/jackson-core-2.21.2.jar")
  }

  test("strict detection of duplicates refuses a field twice, at any depth of the record") {
    the[JsonParseException] thrownBy first("""{"id":"1","id":"2"}""", strict = true) should
      have message "Duplicate field 'id'\n at [Source: REDACTED " +
      "(`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); line: 1, column: 15]"
    // Within an object of the record as well: the first level is to be counted apart.
    (the[JsonParseException] thrownBy first(
      """{"a":{"x":1,"x":2}}""",
      strict = true
    )).getOriginalMessage shouldBe "Duplicate field 'x'"
    // Without it, the parser takes both, and says nothing.
    first("""{"id":"1","id":"2"}""", strict = false) shouldBe JsonToken.START_OBJECT
  }

  test("after the first value of a line, the parser says what follows it, and where") {
    // Nothing but spaces.
    following("""{"id":"4"}   """) shouldBe After.Nothing
    // Another value: an object written after the first, on the same line.
    following("""{"id":"4"} {"id":"5"}""") shouldBe After.Value(column = 12)
    // Text that is no JSON, or what JSON has only between values: where it starts.
    following("""{"id":"4"} trailing""") shouldBe After.NotJson(column = 12)
    following("""{"id":"4"},""") shouldBe After.NotJson(column = 11)
    following("""[{"id":"4"}] x""") shouldBe After.NotJson(column = 14)
  }

  test("a line that is no whole value fails at its first, and a string may be followed by more") {
    an[JsonEOFException] should be thrownBy first("{", strict = true)
    an[JsonParseException] should be thrownBy first("}", strict = true)
    // A line of a record written over several lines starts with a string, whole by itself: what
    // follows it is no record. Only an object or a list is a value whose end is a record's.
    first(""""id": "1",""", strict = true) shouldBe JsonToken.VALUE_STRING
    following(""""id": "1",""") shouldBe After.NotJson(column = 5)
    // And a line that has nothing has no value.
    first("   ", strict = true) shouldBe null
  }

  // ---------------------------------------------------------------------------------------------
  // What Spark's reader of JSON does with such lines today
  // ---------------------------------------------------------------------------------------------

  test("Spark reads a line with text after its object as its object, and sets nothing apart") {
    read(file("{\"id\":\"4\"} trailing\n"), "id") shouldBe Seq(Row("4", null))
  }

  test("Spark refuses the inference of a field twice, and given the columns keeps the last") {
    val twice = file("{\"id\":\"1\",\"id\":\"2\"}\n")
    (the[AnalysisException] thrownBy inferred(twice)).getCondition shouldBe
      "COLUMN_ALREADY_EXISTS"
    read(twice, "id") shouldBe Seq(Row("2", null))
  }

  test("a record written over several lines is read a line at a time: each line is set apart") {
    // The connector has no field for records over several lines (`multiLine`), and passes no
    // option of Spark through: a file written so is read as lines, each of which is no record.
    val pretty = file("{\n  \"id\": \"1\",\n  \"name\": \"ann\"\n}\n")
    read(pretty, "id", "name") shouldBe Seq(
      Row(null, null, "{"),
      Row(null, null, "  \"id\": \"1\","),
      Row(null, null, "  \"name\": \"ann\""),
      Row(null, null, "}")
    )
    inferred(pretty) shouldBe Seq(Unparsed)
    // As text, the same lines.
    lines(pretty) shouldBe Seq("{", "  \"id\": \"1\",", "  \"name\": \"ann\"", "}")
  }

  // ---------------------------------------------------------------------------------------------
  // The lines, as text
  // ---------------------------------------------------------------------------------------------

  test("Spark reads as text the lines that its reader of JSON reads, and keeps the blank ones") {
    // Ends of lines of Windows, a last line with no end, blank lines and lines of spaces.
    val mixed = file("{\"id\":\"1\"}\r\n\r\n   \n{\"id\":\"2\"}\n{\"id\":\"3\"}")
    read(mixed, "id") shouldBe Seq(Row("1", null), Row("2", null), Row("3", null))
    lines(mixed) shouldBe Seq("{\"id\":\"1\"}", "", "   ", "{\"id\":\"2\"}", "{\"id\":\"3\"}")

    // In gzip, by its name.
    val zipped = file("{\"id\":\"1\"}\n{\"id\":\"2\"}\n", gzip = true)
    lines(zipped) shouldBe Seq("{\"id\":\"1\"}", "{\"id\":\"2\"}")
    read(zipped, "id") shouldBe Seq(Row("1", null), Row("2", null))

    // A mark of the order of bytes at the head of the file is taken off, as the reader of JSON
    // takes it: Jackson would not take it as the start of a value.
    val marked = file("﻿{\"id\":\"1\"}\n{\"id\":\"2\"}\n")
    lines(marked) shouldBe Seq("{\"id\":\"1\"}", "{\"id\":\"2\"}")
    read(marked, "id") shouldBe Seq(Row("1", null), Row("2", null))
    an[JsonParseException] should be thrownBy first("﻿{\"id\":\"1\"}", strict = true)
  }

  test("a line read as text ends at LF, at CRLF and at CR alone, and is held whole in memory") {
    // Where the look at the length of the lines of a batch is to end them (P8.11): as Hadoop's
    // reader of lines does, under Spark's reader of text, with no option of its own.
    lines(file("a\nb\r\nc\rd\n")) shouldBe Seq("a", "b", "c", "d")
    lines(file("a\r\rb")) shouldBe Seq("a", "", "b")
    lines(file("a\n\r\nb\n", gzip = true)) shouldBe Seq("a", "", "b")
    // A line has no bound: one of 3 million characters is one string.
    lines(file("x" * 3000000 + "\n")).map(_.length) shouldBe Seq(3000000)
  }

  test("from_json reads a line as the reader of files does, but for a list, which it sets apart") {
    val session = spark
    import session.implicits._
    // Values that are written one way only: from_json and the reader of files agree on them.
    val written = Seq(
      """{"id":"1","name":"ann"}""",
      "not json",
      "5",
      """[{"id":"2"},{"id":"3"}]""",
      "{}",
      """{"id":"6","o":{"x":1}}""",
      """{"id":"7","l":[1,2]}"""
    )
    val columns = Seq("id", "name", "o", "l")

    val byFiles = read(file(written.mkString("", "\n", "\n")), columns: _*)
    val byLines = fromJson(written, columns)
    // An object, a line that is no JSON, a number, an empty object, and an object and a list
    // kept as the text that they were written as: the same.
    byFiles.filterNot(_.getString(0) == "2").filterNot(_.getString(0) == "3") shouldBe
      byLines.filterNot(_.getString(4) == """[{"id":"2"},{"id":"3"}]""")
    // A list of objects: two rows of the reader of files, and one set apart by from_json.
    byFiles.filter(row => Set("2", "3").contains(row.getString(0))) shouldBe
      Seq(Row("2", null, null, null, null), Row("3", null, null, null, null))
    byLines.filter(_.getString(4) == """[{"id":"2"},{"id":"3"}]""") shouldBe
      Seq(Row(null, null, null, null, """[{"id":"2"},{"id":"3"}]"""))

    // The reader of JSON over the lines as a dataset reads them as it reads them in files, the
    // list among them.
    val ofLines = spark.read
      .option("columnNameOfCorruptRecord", Unparsed)
      .option("mode", "PERMISSIVE")
      .schema(asText(columns))
      .json(written.toDS())
      .collect()
      .toSeq
    ofLines shouldBe byFiles
  }

  test("only the reader of files gives a value that is no string as the text it was written as") {
    val session = spark
    import session.implicits._
    // Corrected with P8.8b: the test above has values written one way only. A number, or an
    // object or a list with spaces in it, is given by the reader of files as it was written,
    // since it reads files as bytes and its exact parsing of text is on; from_json and the
    // reader of a dataset of lines give it as Jackson writes it again.
    spark.conf.get("spark.sql.json.enableExactStringParsing") shouldBe "true"
    val written = Seq(
      """{"n":10.50}""",
      """{"n":1e2}""",
      """{"n": -0 }""",
      """{"n":NaN}""",
      """{"n": { "a" : 1.50 , "b":[1, 2] } }"""
    )
    val asWritten = Seq("10.50", "1e2", "-0", "NaN", """{ "a" : 1.50 , "b":[1, 2] }""")
    val again = Seq("10.5", "100.0", "0", "\"NaN\"", """{"a":1.5,"b":[1,2]}""")

    read(file(written.mkString("", "\n", "\n")), "n").map(_.getString(0)) shouldBe asWritten
    fromJson(written, Seq("n")).map(_.getString(0)) shouldBe again
    spark.read
      .schema(asText(Seq("n")))
      .json(written.toDS())
      .collect()
      .toSeq
      .map(_.getString(0)) shouldBe again
  }

  // ---------------------------------------------------------------------------------------------
  // The fields of an object inside a record (P8.11, for the typing of Silver)
  // ---------------------------------------------------------------------------------------------

  // At the first level, two fields that differ only in their case are one column, and the
  // values of one of them were lost: the connector refuses such a batch (P5.14). Inside an
  // object, the connector reads the object as text, and these pin that nothing of it is lost
  // there, and where it would be: in a read of the object as a struct, as the typing of Silver
  // reads it (P10).

  private val OfCase = """{"id":"1","customer":{"name":"Bea","NAME":"B"}}"""
  private val Twice = """{"id":"1","customer":{"name":"Bea","name":"B"}}"""

  test("an object read as text is the text it was written as, with each field that it has") {
    read(file(OfCase + "\n"), "id", "customer") shouldBe
      Seq(Row("1", """{"name":"Bea","NAME":"B"}""", null))
    read(file(Twice + "\n"), "id", "customer") shouldBe
      Seq(Row("1", """{"name":"Bea","name":"B"}""", null))
  }

  test("read as a struct with one of two names that differ in their case, the other is dropped") {
    val session = spark
    import session.implicits._
    val customer = StructType(Seq(StructField("name", StringType)))
    val record = StructType(Seq(StructField("id", StringType), StructField("customer", customer)))

    // By the reader of files and by from_json, with nothing set apart and nothing said.
    spark.read.schema(record).json(s"file:${file(OfCase + "\n")}").collect().toSeq shouldBe
      Seq(Row("1", Row("Bea")))
    Seq(OfCase)
      .toDF("v")
      .select(from_json(col("v"), record).as("r"))
      .select("r.*")
      .collect() shouldBe
      Array(Row("1", Row("Bea")))
    // A field written twice keeps its last value.
    Seq(Twice)
      .toDF("v")
      .select(from_json(col("v"), record).as("r"))
      .select("r.*")
      .collect() shouldBe
      Array(Row("1", Row("B")))
  }

  test("a struct with both names is refused by the reader of files, and read by from_json") {
    val session = spark
    import session.implicits._
    val both = StructType(Seq(StructField("name", StringType), StructField("NAME", StringType)))
    val record = StructType(Seq(StructField("id", StringType), StructField("customer", both)))

    val refused = the[AnalysisException] thrownBy
      spark.read.schema(record).json(s"file:${file(OfCase + "\n")}").collect()
    refused.getCondition shouldBe "COLUMN_ALREADY_EXISTS"
    Seq(OfCase)
      .toDF("v")
      .select(from_json(col("v"), record).as("r"))
      .select("r.*")
      .collect() shouldBe
      Array(Row("1", Row("Bea", "B")))
    // Inferred as Spark infers it, case aside, the object is refused as well.
    (the[AnalysisException] thrownBy spark.read
      .json(s"file:${file(OfCase + "\n")}")
      .schema).getCondition shouldBe "COLUMN_ALREADY_EXISTS"
  }

  /** The rows of lines read by from_json, with the columns given as text. */
  private def fromJson(lines: Seq[String], columns: Seq[String]): Seq[Row] = {
    val session = spark
    import session.implicits._
    lines
      .toDF("value")
      .select(
        from_json(
          col("value"),
          asText(columns),
          Map("mode" -> "PERMISSIVE", "columnNameOfCorruptRecord" -> Unparsed)
        ).as("row")
      )
      .select("row.*")
      .collect()
      .toSeq
  }
}

object SparkJsonLinesContractSpec {

  /** The column of a record that cannot be parsed, as the connector names it. */
  private val Unparsed = "_lakehouse_unparsed"

  private def asText(columns: Seq[String]): StructType =
    StructType((columns :+ Unparsed).map(StructField(_, StringType)))

  private val factory = new JsonFactory()

  /** The first token of a line, read by a parser that refuses a field twice if `strict`. */
  private def first(line: String, strict: Boolean): JsonToken =
    Using.resource(factory.createParser(line)) { parser =>
      if (strict) parser.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
      val token = parser.nextToken()
      parser.skipChildren()
      token
    }

  /** What follows the first value of a line. */
  sealed private trait After
  private object After {
    case object Nothing extends After
    final case class Value(column: Int) extends After
    final case class NotJson(column: Int) extends After
  }

  /** What follows the first value of a line, which is whole, and at which column it starts. */
  private def following(line: String): After =
    Using.resource(factory.createParser(line)) { parser =>
      parser.nextToken()
      parser.skipChildren()
      try
        Option(parser.nextToken()).fold[After](After.Nothing)(_ =>
          After.Value(parser.currentTokenLocation().getColumnNr)
        )
      catch {
        case _: JsonParseException => After.NotJson(parser.currentTokenLocation().getColumnNr)
      }
    }
}
