package lakehouse.ingestion.core.config

import java.nio.charset.Charset

import cats.syntax.all._
import io.circe.Json
import lakehouse.ingestion.core.StoredForm
import lakehouse.ingestion.core.config.Read.{Obj, Result}

/**
 * How a source of CSV writes its files (`connector.csv`, ADR-0024 §1): what separates the values
 * of a record, what a value is quoted with, what a quote inside a quoted value is written after,
 * and the encoding of the bytes of every file.
 *
 * It is made only by `read` and `fromJson`, which hold it to its rules, and has no `copy`: three characters that
 * Spark's parser takes, of which the delimiter is neither of the others, each one that the
 * encoding can write. The quote may be the escape, as it is by default (RFC 4180: a quote inside
 * a quoted value is the quote twice).
 */
sealed abstract case class CsvDialect private (
    delimiter: Char,
    quote: Char,
    escape: Char,
    encoding: CsvDialect.Encoding
) {

  /** Whether it is the dialect that a source that says nothing of its own is read in. */
  def isDefault: Boolean = this == CsvDialect.Default

  /** Its four fields, defaults included: as a configuration stores it, and as a plan does. */
  def toJson: Json = Json.obj(
    CsvDialect.Delimiter -> Json.fromString(delimiter.toString),
    CsvDialect.Quote -> Json.fromString(quote.toString),
    CsvDialect.Escape -> Json.fromString(escape.toString),
    CsvDialect.EncodingField -> Json.fromString(encoding.name)
  )

  /** What it is, for a message: each character by its code point. */
  def describe: String =
    s"delimiter ${Unwritten.describe(delimiter.toInt)}, quote ${Unwritten.describe(quote.toInt)}, " +
      s"escape ${Unwritten.describe(escape.toInt)}, encoding ${encoding.name}"
}

object CsvDialect {

  val Delimiter = "delimiter"
  val Quote = "quote"
  val Escape = "escape"
  val EncodingField = "encoding"

  /**
   * The encodings that a source may say (ADR-0024 §2): a closed list, each tested with Spark's
   * reader and the decoders of the JDK (`SparkCsvDialectContractSpec`). A name is that of the
   * charset of the JDK, which is how Spark is given it.
   */
  sealed abstract class Encoding(val name: String) {

    /** The charset of the JDK, which every JVM of the build has (`java.base`). */
    def charset: Charset = Charset.forName(name)
  }

  object Encoding {
    case object Utf8 extends Encoding("UTF-8")
    case object Iso88591 extends Encoding("ISO-8859-1")

    /**
     * What a spreadsheet and the tools of Windows write as CSV in Western Europe. Spark reads it
     * only in a session that may read any charset of the JDK, which every session of a run is
     * (`Profile.EngineSpark.JavaCharsets`).
     */
    case object Windows1252 extends Encoding("windows-1252")

    val All: Seq[Encoding] = Seq(Utf8, Iso88591, Windows1252)

    /** An encoding by its name, whatever its case. Another name of one of them is not one. */
    def parse(text: String): Either[String, Encoding] =
      All
        .find(_.name.equalsIgnoreCase(text))
        .toRight(
          s"is not an encoding that a source may say: one of ${All.map(_.name).mkString(", ")}" +
            All
              .find(encoding => aliases(encoding).exists(_.equalsIgnoreCase(text)))
              .fold("")(encoding => s". '$text' is another name of ${encoding.name}: write that")
        )

    private def aliases(encoding: Encoding): Set[String] = {
      import scala.jdk.CollectionConverters._
      encoding.charset.aliases().asScala.toSet + encoding.charset.name()
    }
  }

  /** RFC 4180 in UTF-8: what a source that says nothing of its dialect is read in. */
  val Default: CsvDialect = new CsvDialect(',', '"', '"', Encoding.Utf8) {}

  /**
   * One character of a dialect, from its text: one character of the basic plane, which is what
   * Spark's parser takes; not the end of a line, which ends a record; and not NUL, which Spark's
   * parser takes for no character, and as the quote turns quoting off.
   *
   * @return the character, or what is wrong, to follow the name of the field
   */
  def character(text: String): Either[String, Char] =
    if (text.isEmpty) Left("is empty: it is one character")
    else if (text.codePointCount(0, text.length) == 1 && text.length == 2)
      Left(
        s"is ${Unwritten.describe(text.codePointAt(0))}, which is outside the basic plane: " +
          "Spark's parser takes a character of 16 bits"
      )
    else if (text.length > 1)
      Left(
        s"is ${text.codePointCount(0, text.length)} characters, and is one" +
          (if (text.startsWith("\\"))
             ": a character written with a backslash, as \\t for a tab, is written between " +
               "double quotes in YAML (\"\\t\"), and between single quotes it is the backslash " +
               "and what follows it"
           else "")
      )
    else
      text.head match {
        case '\r' | '\n' =>
          Left(s"is ${Unwritten.describe(text.head.toInt)}: the end of a line ends a record")
        case '\u0000' =>
          Left(
            "is U+0000 NULL, which Spark's parser takes for no character: as the quote, it " +
              "turns quoting off"
          )
        case c if Character.isSurrogate(c) =>
          Left(s"is ${f"U+${c.toInt}%04X"}, which is half of a pair and no character")
        case c => Right(c)
      }

  /**
   * What is wrong with a dialect beyond each field by itself, each by the name of its field.
   */
  private def rules(
      delimiter: Char,
      quote: Char,
      escape: Char,
      encoding: Encoding
  ): Seq[(String, String)] = {
    val encoder = encoding.charset.newEncoder()
    Seq(
      Option.when(delimiter == quote)(
        Delimiter -> (s"is the quote as well (${Unwritten.describe(quote.toInt)}): a value " +
          "that starts with it could not be told from one that is quoted")
      ),
      Option.when(delimiter == escape && escape != quote)(
        Delimiter -> (s"is the escape as well (${Unwritten.describe(escape.toInt)}): what it " +
          "is written before could not be told from the start of another value")
      )
    ).flatten ++ Seq(Delimiter -> delimiter, Quote -> quote, Escape -> escape).collect {
      case (field, c) if !encoder.canEncode(c) =>
        field -> (s"is ${Unwritten.describe(c.toInt)}, which ${encoding.name}, the encoding " +
          "of the source, cannot write: no file of it has one")
    }
  }

  /**
   * Reads `connector.csv`: its four fields, each one left out its default, and no other.
   */
  def read(section: Obj): Result[CsvDialect] =
    (
      section.orElse(Delimiter, Default.delimiter)(_.text(character)),
      section.orElse(Quote, Default.quote)(_.text(character)),
      section.orElse(Escape, Default.escape)(_.text(character)),
      section.orElse(EncodingField, Default.encoding)(_.text(Encoding.parse))
    ).tupled.andThen { case (delimiter, quote, escape, encoding) =>
      rules(delimiter, quote, escape, encoding)
        .map { case (field, problem) => Read.invalid[Unit](section.path(field), problem) }
        .sequence_
        .map(_ => new CsvDialect(delimiter, quote, escape, encoding) {})
    }

  /**
   * A dialect as `toJson` writes it, held to the rules of one that is read from a file: its four
   * fields and no other.
   *
   * @return the dialect, or what is wrong with the document
   */
  def fromJson(json: Json): Either[String, CsvDialect] = {
    val at = json.hcursor
    def field[A](name: String)(parse: String => Either[String, A]): Either[String, A] =
      at.get[String](name)
        .leftMap(_ => s"$name is not text")
        .flatMap(text => parse(text).leftMap(problem => s"$name $problem"))
    for {
      _ <- at.keys.toRight("is not an object")
      _ <- StoredForm.exactly(at, Delimiter, Quote, Escape, EncodingField)
      delimiter <- field(Delimiter)(character)
      quote <- field(Quote)(character)
      escape <- field(Escape)(character)
      encoding <- field(EncodingField)(Encoding.parse)
      dialect <- rules(delimiter, quote, escape, encoding).headOption
        .map { case (name, problem) => s"$name $problem" }
        .toLeft(new CsvDialect(delimiter, quote, escape, encoding) {})
    } yield dialect
  }
}
