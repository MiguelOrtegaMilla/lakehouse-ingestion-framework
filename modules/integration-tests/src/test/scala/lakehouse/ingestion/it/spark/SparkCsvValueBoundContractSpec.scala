package lakehouse.ingestion.it.spark

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import scala.util.Try

import com.univocity.parsers.common.TextParsingException
import com.univocity.parsers.common.input.{CharAppender, DefaultCharAppender, NoopCharAppender}
import com.univocity.parsers.csv.CsvParser
import lakehouse.ingestion.it.support.LocalSparkSuite
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What Spark 4.1's reader of CSV, and univocity 2.9.1 under it, do with a bound on the length
 * of a value (`maxCharsPerColumn`): the facts that the bound of the file connector is built on
 * (discovery §17, P8.11).
 *
 * Spark reads a value of CSV with no bound by default, so a quote that is not closed takes the
 * rest of its file into one value, and one row: a file of some hundreds of megabytes ends its
 * process for want of memory. With a bound, univocity reserves it once for each parser, and a
 * value longer fails the read of its file. Its failure says what it had parsed, up to the 1000
 * characters that Spark has it keep, and Spark logs it whole, at WARN and at ERROR, when a task
 * fails: what the configuration of the log of the application takes out (`content parsed=`).
 * A record with more values than univocity takes (`maxColumns`) fails the same way today, with
 * no content in its failure.
 *
 * The reads are made with the options of `FileSourceConnector`, and no code of the engine is
 * in them: characterisation tests, for an upgrade of Spark or of univocity to fail here with
 * the assumption that it breaks (ADR-0014, upgrade procedure).
 */
final class SparkCsvValueBoundContractSpec extends AnyFunSuite with Matchers with LocalSparkSuite {

  import SparkCsvValueBoundContractSpec._

  private var count = 0

  private def file(text: String): Path = {
    count += 1
    val path = workDir.resolve(s"bound-$count.csv")
    Files.write(path, text.getBytes(UTF_8))
  }

  /** The rows of a file of `id,v`, read as the connector reads them, with a bound if given. */
  private def read(path: Path, bound: Option[Int]): Seq[Seq[String]] = {
    val reader = spark.read
      .option("ignoreCorruptFiles", "false")
      .option("header", "true")
      .option("multiLine", "true")
      .option("enforceSchema", "false")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", Unparsed)
      .schema(StructType(Seq("id", "v", Unparsed).map(StructField(_, StringType))))
    bound
      .fold(reader)(n => reader.option("maxCharsPerColumn", n.toString))
      .csv(s"file:$path")
      .localCheckpoint(eager = true)
      .collect()
      .toSeq
      .map(row => (0 until row.length).map(row.getString))
  }

  /** The columns of a file, inferred as the connector infers them, with a bound. */
  private def inferred(path: Path, bound: Int): Seq[String] =
    spark.read
      .option("header", "true")
      .option("multiLine", "true")
      .option("maxCharsPerColumn", bound.toString)
      .csv(s"file:$path")
      .schema
      .fieldNames
      .toSeq

  /** The failure of univocity that a failure of Spark has among its causes. */
  private def parsing(failure: Throwable): TextParsingException =
    Iterator
      .iterate(failure)(_.getCause)
      .takeWhile(_ != null)
      .collectFirst { case parsing: TextParsingException => parsing }
      .getOrElse(fail(s"no failure of univocity among the causes of $failure"))

  private def failed(body: => Any): TextParsingException =
    parsing(Try(body).failed.getOrElse(fail("the read read")))

  private val Bound = 1000

  test("with no bound, a quote that is not closed takes the rest of its file into one value") {
    // The rest, with the end of its last line: one row, and nothing set apart.
    val rest = "x" * 3000 + "\n2,y\n3,z\n"
    read(file("id,v\n1,\"abc" + rest), None) shouldBe Seq(Seq("1", "abc" + rest, null))
  }

  test("a value as long as the bound reads, and one character more fails the read of its file") {
    read(file("id,v\n1," + "x" * Bound + "\n2,y\n"), Some(Bound)) shouldBe
      Seq(Seq("1", "x" * Bound, null), Seq("2", "y", null))
    // Quoted or not, closed or not: the file fails, and nothing of it is set apart.
    Seq(
      "id,v\n1," + "x" * (Bound + 1) + "\n2,y\n",
      "id,v\n1,\"" + "x" * (Bound + 1) + "\"\n2,y\n",
      "id,v\n1,\"abc" + "x" * Bound + "\n2,y\n"
    ).foreach { text =>
      failed(read(file(text), Some(Bound))).getMessage should include(
        s"exceeds the maximum number of characters defined in your parser settings ($Bound)"
      )
    }
  }

  test("the bound counts the units of UTF-16: a character outside the first plane is two") {
    val pair = new String(Character.toChars(0x1f600))
    read(file("id,v\n1," + pair * (Bound / 2) + "\n"), Some(Bound)) shouldBe
      Seq(Seq("1", pair * (Bound / 2), null))
    failed(read(file("id,v\n1," + pair * (Bound / 2) + "x\n"), Some(Bound)))
  }

  test("a name of a header longer than the bound fails the inference of the columns") {
    failed(inferred(file("id," + "h" * (Bound + 1) + "\n1,y\n"), Bound)).getMessage should
      include("exceeds the maximum number of characters")
  }

  test("the failure says what it had parsed: its last 1000 characters, after ..., at most") {
    new CSVOptions(Map.empty[String, String], true, "UTC").maxErrorContentLength shouldBe 1000
    def content(message: String): String =
      message.substring(message.indexOf(ContentMarker) + ContentMarker.length)

    // What it had parsed is the value, up to the bound: whole, when it is 1000 at most.
    val held = "PERSON-HELD-" + "x" * 3000
    val whole = failed(read(file("id,v\n1," + held + "\n"), Some(Bound))).getMessage
    content(whole) shouldBe held.take(Bound)
    // Longer, its last 1000 characters after "...": 1003, which is what the configuration of
    // the log of the application takes out after the marker (MaxContentAfterMarker).
    val longer = failed(read(file("id,v\n1," + held + "y" * 5000 + "\n"), Some(5000))).getMessage
    content(longer) shouldBe "..." + (held + "y" * 5000).take(5000).takeRight(1000)
    content(longer).length shouldBe MaxContentAfterMarker
  }

  test("today, a record with more values than maxColumns fails with no content in its failure") {
    // 20480, Spark's own default, which the connector does not change.
    val max = new CSVOptions(Map.empty[String, String], true, "UTC").maxColumns
    max shouldBe 20480
    val wide = "PERSON-WIDE," + Seq.fill(max)("v").mkString(",")
    val message = failed(read(file("id,v\n" + wide + "\n"), None)).getMessage
    message should include(s"Index $max out of bounds for length $max")
    message should include("headers=[id, v]")
    message should not include ContentMarker
    message should not include "PERSON-WIDE"
  }

  test("the bound is reserved once for each parser, and shared by its columns, at any width") {
    val record = (1 to 200).map(i => s"value $i").mkString(",")
    val settings =
      new CSVOptions(Map("multiLine" -> "true", "maxCharsPerColumn" -> s"${1 << 20}"), true, "UTC")
    val parser = new CsvParser(settings.asParserSettings)
    parser.parseLine(record).length shouldBe 200

    val appenders = reserved(parser)
    appenders.map(System.identityHashCode).distinct should have size 1
    appenders.head shouldBe a[DefaultCharAppender]
    charsOf(appenders.head.asInstanceOf[DefaultCharAppender]) shouldBe (1 << 20)
  }
}

object SparkCsvValueBoundContractSpec {

  /** The column of a record that cannot be parsed, as the connector names it. */
  private val Unparsed = "_lakehouse_unparsed"

  /** What univocity puts before what it had parsed, in the message of its failure. */
  val ContentMarker = "content parsed="

  /** The most that univocity writes after it: "..." and the last 1000 characters parsed. */
  val MaxContentAfterMarker = 1003

  /** The appenders of the output of a parser that are not the one that keeps nothing. */
  private def reserved(parser: CsvParser): Seq[CharAppender] = {
    val output = field(classOf[com.univocity.parsers.common.AbstractParser[_]], "output", parser)
    field(output.getClass, "appenders", output)
      .asInstanceOf[Array[CharAppender]]
      .toSeq
      .filterNot(_.isInstanceOf[NoopCharAppender])
  }

  private def charsOf(appender: DefaultCharAppender): Int =
    field(classOf[DefaultCharAppender], "chars", appender).asInstanceOf[Array[Char]].length

  private def field(of: Class[_], name: String, in: AnyRef): AnyRef = {
    val declared = of.getDeclaredField(name)
    declared.setAccessible(true)
    declared.get(in)
  }
}
