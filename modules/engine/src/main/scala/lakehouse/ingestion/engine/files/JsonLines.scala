package lakehouse.ingestion.engine.files

import java.io.IOException

import scala.collection.mutable
import scala.util.Using

import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken}
import org.apache.spark.sql.catalyst.json.JSONOptions

/**
 * A line of a file of JSON, read as a record of its source is (discovery §17, P8.8): one object
 * on a line, with each field of its first level once, and nothing after it.
 *
 * Spark's reader takes the first value of a line, and says nothing of what follows it; and of a
 * field written twice it keeps one value, or refuses the batch (`SparkJsonLinesContractSpec`).
 * So each line is read here, by Jackson, and not by Spark's reader:
 *
 *  - '''nothing''' on it but spaces: Spark passes over it, and so does the read;
 *  - '''one object, whole''', each field of its first level once, and nothing after it but
 *    spaces: a record, with the value of each field of its first level as Spark's reader of
 *    files gives it, as text: a string as what it says, null as none, and anything else, a
 *    number, `true`, an object, a list, as the text that it was written as;
 *  - an object whole and '''text after it''': set apart, with the column where the text starts;
 *  - an object whole with '''a field twice''' at its first level: set apart, with the name of the
 *    field. An object inside a record is kept as its text, field twice and all;
 *  - '''anything else''': a line that is no whole object, as a number, a string, a list, a line
 *    of a record written over several lines, or a broken object, is set apart as one that
 *    cannot be parsed. Text after the first value is a record's only when that value is an
 *    object: a line of a record written over several lines may start with a string that is
 *    whole by itself.
 *
 * '''Why the values are read here, and not by Spark.''' Spark's reader of files gives a value
 * that is no string as the text it was written as (`10.50`, `1e2`, an object with its spaces),
 * since it reads files as bytes; Spark's readers of a line that is a value of a row, `from_json`
 * and a dataset of lines, give it as Jackson writes it again (`10.5`, `100.0`). Bronze keeps what
 * a source wrote, as it wrote it (ADR-0008), so a record is read here as the reader of files
 * reads it, and `FileSourceConnectorSpec` holds the two to the same values.
 *
 * The parser is the one that Spark's reader makes, with the options that a source of the
 * connector leaves as Spark has them (`JSONOptions`: quotes of either kind, numbers that are no
 * number): a line is a record here exactly when Spark's reader would read it as one.
 */
private[files] object JsonLines {

  /** What a line of a file of JSON is. */
  sealed trait Line
  object Line {

    /** Nothing but spaces: no record. */
    case object Blank extends Line

    /**
     * One object, whole, each field of its first level once, and nothing after it.
     *
     * @param fields each field of its first level, in the order written, with its value as text,
     *   or none for a value that is null
     */
    final case class Record(fields: Vector[(String, String)]) extends Line {
      def names: Vector[String] = fields.map(_._1)
    }

    /** An object, whole, and text after it, from `column` (1 for the first character). */
    final case class TrailingText(column: Int) extends Line

    /** An object, whole, with `name` written twice at its first level: the first such name. */
    final case class DuplicateField(name: String) extends Line

    /** No whole object: a line that cannot be parsed as a record. */
    case object NotARecord extends Line
  }

  /**
   * The factory of Spark's reader of JSON, with no option set: one for each JVM, made when a
   * line is first looked at there. A factory makes parsers for any thread.
   */
  private lazy val factory: JsonFactory =
    new JSONOptions(Map.empty[String, String], "UTC", "").buildJsonFactory()

  /** What a line is. */
  def of(line: String): Line =
    Using.resource(factory.createParser(line)) { parser =>
      try
        parser.nextToken() match {
          case null => Line.Blank
          case JsonToken.START_OBJECT => record(parser, line)
          case _ => Line.NotARecord
        }
      catch {
        // No whole object, as the parser found it.
        case _: IOException => Line.NotARecord
      }
    }

  /** The rest of an object whose start was read, and what follows it. */
  private def record(parser: JsonParser, line: String): Line = {
    val fields = Vector.newBuilder[(String, String)]
    val names = mutable.HashSet.empty[String]
    var twice = Option.empty[String]
    var token = parser.nextToken()
    while (token == JsonToken.FIELD_NAME) {
      val name = parser.currentName()
      if (!names.add(name) && twice.isEmpty) twice = Some(name)
      fields += name -> value(parser, line)
      token = parser.nextToken()
    }
    // What closes the object is the parser's to check: it fails on anything else.
    twice.fold(following(parser, line, fields.result()))(Line.DuplicateField(_))
  }

  /**
   * The value of the field whose name was just read, as Spark's reader of files gives a value
   * to a column of text: a string as what it says, null as none, and anything else as the text
   * of the line from where the value starts to where it ends, all of it for an object or a list.
   */
  private def value(parser: JsonParser, line: String): String =
    parser.nextToken() match {
      case JsonToken.VALUE_STRING => parser.getText
      case JsonToken.VALUE_NULL => null
      case _ =>
        val from = parser.currentTokenLocation().getCharOffset.toInt
        parser.skipChildren()
        line.substring(from, parser.currentLocation().getCharOffset.toInt)
    }

  /**
   * What follows a whole object: nothing but spaces, or text, from where it starts. Where the
   * object ends is where the parser is, a character at a time on a line of text; what follows
   * is looked at in the line, and not by asking the parser for more, whose place is not moved
   * by a character that it refuses.
   */
  private def following(
      parser: JsonParser,
      line: String,
      fields: Vector[(String, String)]
  ): Line = {
    val end = parser.currentLocation().getColumnNr - 1
    val from = line.indexWhere(c => !Spaces.contains(c), end)
    if (from < 0) Line.Record(fields) else Line.TrailingText(from + 1)
  }

  /** What JSON has for space between its tokens, and nothing else. */
  private val Spaces = Set(' ', '\t', '\n', '\r')

  /** How much of the name of a field the detail of a record keeps: a name may be long. */
  val NameKept = 200

  /** What the detail of a record set apart says, as the quarantine keeps it. */
  def detail(line: Line): Option[String] = line match {
    case Line.TrailingText(column) => Some(s"text after the object, from column $column")
    case Line.DuplicateField(name) =>
      val kept = if (name.length <= NameKept) name else s"${name.take(NameKept)}…"
      Some(s"the field '$kept' twice, at the first level of the record")
    case _ => None
  }
}
