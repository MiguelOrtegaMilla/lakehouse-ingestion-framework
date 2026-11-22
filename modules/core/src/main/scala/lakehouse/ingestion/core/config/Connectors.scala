package lakehouse.ingestion.core.config

import java.time.Instant

import scala.concurrent.duration._

import cats.syntax.all._
import io.circe.Json
import lakehouse.ingestion.core.config.Read.{Obj, Problem, Result}
import lakehouse.ingestion.core.watermark.WatermarkPlanner

/** What a connector is told about the source it reads: the `connector` section of a source. */
trait ConnectorConfig {

  /** The `type` of the section, which chose the kind that read it. */
  def kind: String

  /** What is worth a second look in it, by the path of the field. */
  def warnings: Seq[Problem] = Nil

  /**
   * The section as it is stored and hashed (`ConfigVersion`): every field with the name it has
   * in a file, defaults included, and `type` left out, which is added from `kind`. What the
   * kind reads back from this must be this configuration again.
   */
  def canonical: Json

  /**
   * The fields of `canonical` that say where the rows come from, each by its path in the
   * section, as `connection.url`. A source that is loaded by a watermark and changes one of
   * them has a watermark of somewhere else: going on from it would skip rows without a word
   * (`ConfigVersion.breakingChanges`). How the place is reached, a user or a timeout, is not
   * one of them.
   */
  def origin: Seq[String]

  /**
   * A place that the source has to itself among the sources of its kind, by the path of its
   * field in the section and its value: one from which what is read is taken once, so that two
   * sources that read it would both take the same thing. The directory of a file source is
   * one: each file is ingested once, by the source whose directory it is in. A table of a
   * database is not: two sources may read one table, each for its own purpose.
   */
  def exclusive: Option[(String, String)] = None

  /**
   * What is wrong in the section that shows only beside the load of its source, by the path of
   * the field from the top of the file, as `connector.text_columns[1]`. What is wrong in a
   * field by itself is said when the section is read (`ConnectorKind.read`).
   */
  def rules(load: Load): Seq[Problem] = Nil
}

/**
 * A kind of connector, as far as configuration goes: its name in `type:`, how the fields of its
 * section are read, and which ways of loading it has.
 *
 * A new kind of source is one of these and its connector, registered where the application is
 * put together. Nothing in the model of a source, nor in the engine, names a kind (discovery
 * §12.2).
 */
trait ConnectorKind {

  /** What a source writes in `connector.type`. */
  def name: String

  /** The ways of loading that a connector of this kind can plan. */
  def loads: Set[Load.Mode]

  /**
   * Reads the section. `type` is already read; every other field that is not asked for here is
   * an error. No I/O: this runs when a configuration is validated.
   */
  def read(section: Obj): Result[ConnectorConfig]

  /**
   * How a moment is made into the watermark of a source of this kind that is loaded so, in the
   * form that its connector stores it in, for an operator's reset of the state of the source
   * (ADR-0021 §3); none, for a load that keeps no watermark. No I/O, as `read`: a reset does not
   * open the source.
   *
   * The function gives the watermark, or why the moment cannot be one.
   */
  def watermarkAt(load: Load): Option[Instant => Either[String, Json]] = None
}

object ConnectorKind {

  /** The kinds that the framework has: a table of a database, and files that are left to it. */
  val BuiltIn: Seq[ConnectorKind] = Seq(JdbcConnector, FileConnector)
}

/**
 * A table of a relational database, read over JDBC (PostgreSQL in the MVP).
 *
 * @param table the table, or `schema.table`: a validated name, never text that reaches SQL
 *   (ADR-0013 §1)
 * @param textColumns the columns of the table that are read as text, the value as the source
 *   writes it, whatever their type: for a column that holds what the type that Spark would
 *   read it into has not, as `infinity` in a date (discovery §17, P6.6)
 */
final case class JdbcConnectorConfig(
    connection: DatabaseConnection,
    table: QualifiedName,
    textColumns: Vector[SqlIdentifier] = Vector.empty
) extends ConnectorConfig {

  override def kind: String = JdbcConnector.name

  override def warnings: Seq[Problem] =
    DatabaseConnection.warnings("connector.connection", connection).toSeq

  override def canonical: Json =
    Json.obj(
      "connection" -> connection.canonical,
      "table" -> Json.fromString(table.toString),
      "text_columns" -> Json.fromValues(textColumns.map(name => Json.fromString(name.value)))
    )

  /**
   * A column is named once; it is the source's, and not one that the engine adds to a row; and
   * it is not the watermark, which is read as the timestamp that it is: its bounds reach the
   * source as times, and a text has none.
   */
  override def rules(load: Load): Seq[Problem] =
    textColumns.zipWithIndex.flatMap { case (name, index) =>
      val field = s"connector.${JdbcConnector.TextColumns}[$index]"
      val first = textColumns.indexOf(name)
      Seq(
        Option.when(first < index)(
          Problem(
            field,
            s"is $name again: it is connector.${JdbcConnector.TextColumns}[$first] already"
          )
        ),
        EngineColumns.refused(field, name.value),
        load match {
          case Load.Incremental(watermark, _) if watermark.column == name =>
            Some(
              Problem(
                field,
                s"is $name, which is load.watermark.column: a watermark is read as the " +
                  "timestamp that it is, and the bounds of a batch reach the source as times"
              )
            )
          case _ => None
        }
      ).flatten
    }

  /** The database and the table. The role, the password, TLS and the timeouts are how it is reached. */
  override def origin: Seq[String] = Seq("connection.url", "table")
}

object JdbcConnector extends ConnectorKind {

  override val name: String = "jdbc"

  override val loads: Set[Load.Mode] = Set(Load.Mode.Full, Load.Mode.Incremental)

  /** The field of the columns that are read as text. */
  val TextColumns = "text_columns"

  /** A load by a watermark keeps the upper bound of its last batch (`StoredWatermark`). */
  override def watermarkAt(load: Load): Option[Instant => Either[String, Json]] = load match {
    case incremental: Load.Incremental =>
      Some(at => WatermarkPlanner.resetTo(incremental, at).map(_.toJson))
    case _ => None
  }

  override def read(section: Obj): Result[ConnectorConfig] =
    (
      section.required("connection")(_.obj(DatabaseConnection.read(Timeouts.OfASource))),
      section.required("table")(_.text(QualifiedName.parse)),
      section.orElse(TextColumns, Vector.empty[SqlIdentifier])(
        _.list(_.text(SqlIdentifier.parse))
      )
    ).mapN(JdbcConnectorConfig.apply)
}

/**
 * Files that are left in a directory of the landing zone. The directory is listed, and not the
 * directories under it.
 *
 * @param path the directory, relative to the profile's `landing.root` (ADR-0013 §2)
 * @param pattern which files of the directory, by their name; all of them if it is left out
 * @param minFileAge how long a file goes unchanged before it is taken as complete: one that is
 *   still being written would be ingested cut short (ADR-0008, the "complete file" rule)
 * @param maxFilesPerBatch the most files that one batch takes. A plan names every file of its
 *   batch, and is written down whole before anything is read (ADR-0005 §3)
 */
final case class FileConnectorConfig(
    path: RelativePath,
    format: FileConnectorConfig.Format,
    pattern: Option[FileConnectorConfig.Pattern],
    minFileAge: FiniteDuration = FileConnectorConfig.DefaultMinFileAge,
    maxFilesPerBatch: Int = FileConnectorConfig.DefaultMaxFilesPerBatch
) extends ConnectorConfig {

  override def kind: String = FileConnector.name

  override def warnings: Seq[Problem] =
    Option
      .when(minFileAge == Duration.Zero)(
        Problem(
          "connector.min_file_age",
          "is 0s: a file that is still being written may be taken, and ingested cut short"
        )
      )
      .toSeq

  override def canonical: Json = Json.obj(
    "path" -> Json.fromString(path.value),
    "format" -> Json.fromString(format.name),
    "pattern" -> pattern.fold(Json.Null)(p => Json.fromString(p.value)),
    "min_file_age" -> Json.fromString(Durations.render(minFileAge)),
    "max_files_per_batch" -> Json.fromInt(maxFilesPerBatch)
  )

  /** The directory and the names. How soon a file is taken, and how many at once, are not. */
  override def origin: Seq[String] = Seq("path", "pattern")

  /**
   * The directory, whatever the pattern: whether two patterns can match one name has an
   * answer, and code to keep it right that buys nothing (discovery §17, P5). A directory inside
   * another source's is not this one, and is allowed: neither source lists the other's.
   */
  override def exclusive: Option[(String, String)] = Some("path" -> path.value)
}

object FileConnectorConfig {

  /** Long enough for a copy of a file that is not small, and short for one that waits. */
  val DefaultMinFileAge: FiniteDuration = 1.minute

  /** A file whose writer stopped for longer than this is not one that is being written. */
  val MaxMinFileAge: FiniteDuration = 1.day

  val DefaultMaxFilesPerBatch = 1000

  /**
   * A plan of this many files, each with a name of the longest a name may be, stays well within
   * what the control plane stores of a plan (8 Mi characters).
   */
  val MaxFilesPerBatch = 10000

  sealed abstract class Format(val name: String)

  object Format {
    case object Csv extends Format("csv")
    case object Json extends Format("json")
    case object Parquet extends Format("parquet")

    val All: Seq[Format] = Seq(Csv, Json, Parquet)
  }

  /**
   * A pattern for the name of a file: letters, digits, `_`, `-`, `.`, and `*` and `?` for any
   * run of characters and for any one. It has no `/`, so it never leads out of its directory.
   */
  final class Pattern private (val value: String) extends AnyVal {

    /**
     * Whether `name` is one of the files of the pattern: the whole name, character for
     * character, with `*` for any run of characters, none included, and `?` for any one. A
     * character is a code point, and case counts.
     */
    def matches(name: String): Boolean =
      Pattern.matches(value.codePoints.toArray, name.codePoints.toArray)

    override def toString: String = value
  }

  object Pattern {

    /**
     * The usual match of a glob without classes: it goes forward, and on a mismatch it goes back
     * to the last `*` and lets it take one character more. Each `*` is gone back to at most once
     * for each character of the name, so it takes time of the length of the name times the
     * length of the pattern, and never more, whatever the name is.
     */
    private def matches(pattern: Array[Int], name: Array[Int]): Boolean = {
      // p, n: where the pattern and the name are; star: the last `*` seen, and the place in the
      // name that it was taking from.
      @scala.annotation.tailrec
      def go(p: Int, n: Int, star: Int, from: Int): Boolean =
        if (n < name.length) {
          if (p < pattern.length && (pattern(p) == '?' || pattern(p) == name(n)))
            go(p + 1, n + 1, star, from)
          else if (p < pattern.length && pattern(p) == '*') go(p + 1, n, p, n)
          else if (star >= 0) go(star + 1, from + 1, star, from + 1)
          else false
        } else pattern.drop(p).forall(_ == '*')
      go(0, 0, -1, 0)
    }

    private def allowed(c: Int): Boolean =
      (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
        "_-.*?".indexOf(c) >= 0

    def parse(text: String): Either[String, Pattern] =
      if (text.isEmpty) Left("is empty")
      else if (text.length > RelativePath.MaxSegmentBytes)
        Left(s"is ${text.length} bytes long, and a name is ${RelativePath.MaxSegmentBytes} at most")
      else
        Unwritten.firstOutside(text, allowed) match {
          case Some((codePoint, position)) =>
            Left(
              s"has ${Unwritten.describe(codePoint)} at position $position: a pattern is for " +
                "the name of a file, of letters, digits, _ - . and the wildcards * and ?"
            )
          case None if text.startsWith(".") || text.startsWith("_") =>
            Left(s"starts with '${text.head}': such files are hidden to Spark and Delta")
          case None => Right(new Pattern(text))
        }
  }
}

object FileConnector extends ConnectorKind {

  override val name: String = "file"

  override val loads: Set[Load.Mode] = Set(Load.Mode.NewFiles)

  override def read(section: Obj): Result[ConnectorConfig] =
    (
      section.required("path")(_.text(RelativePath.parse)),
      section.required("format")(_.oneOf(FileConnectorConfig.Format.All.map(f => f.name -> f): _*)),
      section.optional("pattern")(_.text(FileConnectorConfig.Pattern.parse)),
      section.orElse("min_file_age", FileConnectorConfig.DefaultMinFileAge)(
        _.duration(Duration.Zero, FileConnectorConfig.MaxMinFileAge)
      ),
      section.orElse("max_files_per_batch", FileConnectorConfig.DefaultMaxFilesPerBatch)(
        _.integer(1, FileConnectorConfig.MaxFilesPerBatch)
      )
    ).mapN(FileConnectorConfig.apply)
}
