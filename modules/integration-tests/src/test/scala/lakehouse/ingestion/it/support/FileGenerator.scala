package lakehouse.ingestion.it.support

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.Locale

import io.circe.{Json, JsonNumber}

/**
 * Files as a producer leaves them in a landing zone, made from a seed: the same seed gives the
 * same files, byte for byte, on any machine (`java.util.Random` is specified to).
 *
 * A '''delivery''' is the files that a producer leaves at one time. What is in it is said by a
 * `Mix`, in numbers; the seed decides the rest: the order of the files, how many records each
 * has, their values, and where the records that cannot be parsed are.
 *
 *  - '''A column more, a column less.''' Some files have one column that the others do not
 *    (`Added`), some lack one that the others have: a producer that changed what it writes. The
 *    files that lack a column all lack the same one.
 *  - '''Records that cannot be parsed.''' Of CSV, a record with a value more than its header,
 *    or with two values alone; of JSON, a line that is cut short.
 *  - '''Empty files.''' Of no bytes; of CSV, with a header and no record as well, by turns.
 *  - '''Repeated files.''' Another file of the delivery again, with the same bytes and under
 *    another name: a producer that sent a file twice.
 *
 * With the files comes what is in them (`Generated`): the records of each that parse, value by
 * value, and the text of those that do not. A test compares what the engine wrote with that,
 * and not with what it reads back from the lake.
 *
 * Values are as a source has them: names with commas, quotes and letters beyond ASCII, a note
 * of two lines. A file of CSV is written as RFC 4180 has it, in UTF-8, unless the delivery is in
 * a dialect of its own (`Dialect`): a value is quoted if it must be, with its quotes doubled, or
 * after the escape of the dialect. No value is empty, which CSV cannot tell from no value at all.
 */
object FileGenerator {

  sealed abstract class Format(val name: String)
  object Format {
    case object Csv extends Format("csv")
    case object Json extends Format("json")
  }

  /**
   * How the files of CSV of a delivery are written (ADR-0024): what separates the values, what a
   * value is quoted with, what a quote or the escape inside a quoted value is written after, and
   * the encoding of the bytes. A delivery in a dialect of its own has values with each of its
   * characters in them, and only values that its encoding can write.
   */
  final case class Dialect(
      delimiter: Char = ',',
      quote: Char = '"',
      escape: Char = '"',
      encoding: Charset = UTF_8
  )

  object Dialect {

    /** RFC 4180 in UTF-8: what a delivery is written in unless it says otherwise. */
    val Rfc4180: Dialect = Dialect()
  }

  /** The columns of a delivery: every file has them, but those that lack one. */
  val Columns: Seq[String] = Seq("id", "customer", "amount", "placed_at", "note")

  /** The column that only the files with a column more have. */
  val Added = "channel"

  /**
   * What a delivery has. Every number is of files, but `corrupt` and `records`.
   *
   * @param files files with the columns of the delivery
   * @param added files with those and one more
   * @param missing files without one of them
   * @param corrupt records that cannot be parsed, among the files above
   * @param empty files with no record
   * @param repeated files that are one of the files with records again, under another name
   * @param records the most records that parse in a file; a file has one at least
   */
  final case class Mix(
      files: Int,
      added: Int = 0,
      missing: Int = 0,
      corrupt: Int = 0,
      empty: Int = 0,
      repeated: Int = 0,
      records: Int = 12
  ) {
    private[FileGenerator] val withRecords: Int = files + added + missing
    require(Seq(files, added, missing, corrupt, empty, repeated).forall(_ >= 0), "no negative")
    require(records >= 1, "a file with records has one at least")
    require(withRecords >= 1 || corrupt + repeated == 0, "no file to put a record in, or to repeat")
  }

  sealed trait Kind
  object Kind {
    case object Plain extends Kind
    case object WithAdded extends Kind
    final case class Without(column: String) extends Kind
    case object Empty extends Kind
    final case class Again(of: String) extends Kind
  }

  /**
   * A file, and what is in it.
   *
   * @param text what the file holds, which is written in the encoding of its delivery
   * @param columns the columns of its records; none, for a file of no bytes
   * @param records the records that parse, each a value for every column of the file, in the
   *   order they are written
   * @param corrupt the text of each record that cannot be parsed, as it is in the file
   */
  final case class Generated(
      name: String,
      text: String,
      columns: Seq[String],
      records: Vector[Map[String, String]],
      corrupt: Vector[String],
      kind: Kind
  )

  final case class Delivery(
      format: Format,
      files: Vector[Generated],
      dialect: Dialect = Dialect.Rfc4180
  ) {

    /** The bytes of a file, as it is left in a landing zone. */
    def bytesOf(file: Generated): Array[Byte] = file.text.getBytes(dialect.encoding)

    /** Every column of a file of the delivery, in the order they first appear. */
    def columns: Seq[String] = files.flatMap(_.columns).distinct

    /** How many records parse, and how many do not, over every file. */
    def parsed: Int = files.map(_.records.size).sum
    def unparsed: Int = files.map(_.corrupt.size).sum

    def of(kind: Kind => Boolean): Vector[Generated] = files.filter(file => kind(file.kind))
  }

  /**
   * The delivery of a seed.
   *
   * @param number which delivery of its producer it is, from 1: the names of the files and the
   *   ids of the records of two deliveries of one seed are apart
   * @param dialect how its files of CSV are written: RFC 4180 in UTF-8 unless it says otherwise,
   *   which gives the same files as before dialects, byte for byte
   */
  def deliver(
      seed: Long,
      format: Format,
      mix: Mix,
      number: Int = 1,
      dialect: Dialect = Dialect.Rfc4180
  ): Delivery = {
    require(number >= 1 && number <= 99, "a delivery from 1 to 99")
    require(format == Format.Csv || dialect == Dialect.Rfc4180, "a dialect is of CSV")
    val values = new Values(dialect)
    // The number is spread over every bit of the seed (by the constant of the golden ratio), so
    // that the deliveries of one seed, and the first deliveries of seeds next to each other,
    // have nothing in common.
    val random = new java.util.Random(seed ^ (number * 0x9e3779b97f4a7c15L))
    val missing = Columns(1 + random.nextInt(Columns.size - 1))

    val kinds = shuffled(
      random,
      Vector.fill(mix.files)(Kind.Plain: Kind) ++
        Vector.fill(mix.added)(Kind.WithAdded) ++
        Vector.fill(mix.missing)(Kind.Without(missing)) ++
        Vector.fill(mix.empty)(Kind.Empty) ++
        // Which file each is again is decided once the files are there.
        Vector.fill(mix.repeated)(Kind.Again(""))
    )
    // The file that each record that cannot be parsed is in: one of those that have records.
    val corruptIn = Vector.fill(mix.corrupt)(random.nextInt(mix.withRecords))
    val ids = Iterator.from(1).map(n => f"d$number%02d-$n%06d")
    def name(position: Int, suffix: String = ""): String =
      f"orders-d$number%02d-${position + 1}%04d$suffix.${format.name}"

    var withRecords = 0
    var headerOnly = random.nextBoolean()
    val first = kinds.zipWithIndex.map {
      case (Kind.Empty, at) =>
        // A file of CSV may have its header, that of the delivery, and nothing else; one of
        // JSON has nothing at all.
        // The seed decides for the first, and the next is the other kind: two have both.
        headerOnly = !headerOnly
        val header = format == Format.Csv && headerOnly
        val text = if (header) csvLine(dialect, Columns) + "\n" else ""
        Some(
          Generated(
            name(at),
            text,
            if (header) Columns else Nil,
            Vector.empty,
            Vector.empty,
            Kind.Empty
          )
        )
      case (Kind.Again(_), _) => None
      case (kind, at) =>
        val columns = kind match {
          case Kind.WithAdded => Columns :+ Added
          case Kind.Without(column) => Columns.filterNot(_ == column)
          case _ => Columns
        }
        val corrupt = corruptIn.count(_ == withRecords)
        withRecords += 1
        Some(
          written(
            random,
            format,
            dialect,
            values,
            name(at),
            columns,
            kind,
            1 + random.nextInt(mix.records),
            corrupt,
            ids
          )
        )
    }
    val repeatable = first.flatten.filter(_.kind != Kind.Empty)
    val files = first.zipWithIndex.map {
      case (Some(file), _) => file
      case (None, at) =>
        val original = repeatable(random.nextInt(repeatable.size))
        original.copy(name = name(at, "-again"), kind = Kind.Again(original.name))
    }
    Delivery(format, files, dialect)
  }

  /** A file with records: those that parse, and among them, wherever, those that do not. */
  private def written(
      random: java.util.Random,
      format: Format,
      dialect: Dialect,
      values: Values,
      name: String,
      columns: Seq[String],
      kind: Kind,
      parsed: Int,
      corrupt: Int,
      ids: Iterator[String]
  ): Generated = {
    // Left is a record that cannot be parsed, as its text; Right, one that can, as its values.
    val lines: Vector[Either[String, Map[String, String]]] = shuffled(
      random,
      Vector.fill(parsed)(true) ++ Vector.fill(corrupt)(false)
    ).map { parses =>
      val record = columns.map(column => column -> values.of(random, column, ids)).toMap
      if (parses) Right(record)
      else
        Left(format match {
          // One value more than the header, or two alone: never a quote, which the parser
          // would follow to the end of the file.
          case Format.Csv =>
            val delimiter = dialect.delimiter.toString
            if (random.nextBoolean())
              (columns.map(_ => word(random)) :+ "unexpected").mkString(delimiter)
            else s"${record("id")}$delimiter${word(random)}"
          // Cut short, anywhere after its first character: no such line is JSON.
          case Format.Json =>
            val whole = jsonLine(columns, record)
            whole.take(1 + random.nextInt(whole.length - 1))
        })
    }
    val text = format match {
      case Format.Csv =>
        (csvLine(dialect, columns) +: lines.map(
          _.fold(identity, r => csvLine(dialect, columns.map(r)))
        ))
          .mkString("", "\n", "\n")
      case Format.Json =>
        lines.map(_.fold(identity, jsonLine(columns, _))).mkString("", "\n", "\n")
    }
    Generated(
      name,
      text,
      columns,
      lines.collect { case Right(r) => r },
      lines.collect { case Left(t) => t },
      kind
    )
  }

  private val Customers = Vector(
    "Ana Ruiz",
    "Núñez, María",
    "Bar \"El Rincón\" S.L.",
    "O'Brien & Sons",
    "李雷",
    "Müller GmbH",
    "Ferretería \"La Llave\""
  )
  private val Notes =
    Vector(
      "none",
      "leave at the door",
      "call first, then deliver",
      "two\nlines",
      "said \"fragile\""
    )
  private val Channels = Vector("web", "store", "phone")
  private val Epoch = Instant.parse("2026-01-01T00:00:00Z")

  /**
   * The values that a delivery takes: those above for RFC 4180 in UTF-8, as before dialects;
   * for a dialect of its own, those that its encoding can write, and some with each of the
   * characters of the dialect, its own letters and the euro among them.
   */
  final private class Values(dialect: Dialect) {

    private def ofTheDialect(more: Vector[String]): Vector[String] => Vector[String] =
      default =>
        if (dialect == Dialect.Rfc4180) default
        else (default ++ more).filter(dialect.encoding.newEncoder().canEncode(_))

    private val customers = ofTheDialect(
      Vector(
        s"Pe\u00f1a${dialect.delimiter} Hijos",
        s"${dialect.quote}La Llave${dialect.quote} S.A.",
        s"C:${dialect.escape}temp | O'Brien",
        "Caf\u00e9 \u201cEl Rinc\u00f3n\u201d"
      )
    )(Customers)
    private val notes = ofTheDialect(
      Vector("10 \u20ac, then\nmore", s"tab\there${dialect.escape}${dialect.quote}", "\u2014")
    )(Notes)

    def of(random: java.util.Random, column: String, ids: Iterator[String]): String = {
      def oneOf(values: Vector[String]): String = values(random.nextInt(values.size))
      column match {
        case "id" => ids.next()
        case "customer" => oneOf(customers)
        case "amount" =>
          "%d.%02d".formatLocal(Locale.ROOT, random.nextInt(5000), random.nextInt(100))
        case "placed_at" => Epoch.plusSeconds(random.nextInt(300 * 86400).toLong).toString
        case "note" => oneOf(notes)
        case Added => oneOf(Channels)
        case other => throw new IllegalArgumentException(s"no column $other")
      }
    }
  }

  private def word(random: java.util.Random): String =
    Vector.fill(3 + random.nextInt(5))(('a' + random.nextInt(26)).toChar).mkString

  /**
   * A record of CSV in a dialect: a value is quoted if it has the delimiter, the quote, the
   * escape or the end of a line in it, and inside it the escape and the quote are written after
   * the escape. Under RFC 4180, whose escape is the quote, that is the quote twice.
   */
  private def csvLine(dialect: Dialect, values: Seq[String]): String = {
    val quote = dialect.quote.toString
    val escape = dialect.escape.toString
    val special = Set(dialect.delimiter, dialect.quote, dialect.escape, '\r', '\n')
    values
      .map { value =>
        if (value.exists(special)) {
          val escaped =
            if (dialect.escape == dialect.quote) value.replace(quote, quote + quote)
            else value.replace(escape, escape + escape).replace(quote, escape + quote)
          quote + escaped + quote
        } else value
      }
      .mkString(dialect.delimiter.toString)
  }

  /** One object to a line; the amount is a number, with the digits it was given. */
  private def jsonLine(columns: Seq[String], record: Map[String, String]): String =
    Json
      .obj(columns.map {
        case "amount" =>
          "amount" -> Json.fromJsonNumber(JsonNumber.fromDecimalStringUnsafe(record("amount")))
        case column => column -> Json.fromString(record(column))
      }: _*)
      .noSpaces

  /** Fisher and Yates, here and not the library's, so that a seed means one order for good. */
  private def shuffled[A](random: java.util.Random, items: Vector[A]): Vector[A] = {
    val array = items.toBuffer
    for (i <- (array.size - 1) to 1 by -1) {
      val j = random.nextInt(i + 1)
      val held = array(i)
      array(i) = array(j)
      array(j) = held
    }
    array.toVector
  }
}
