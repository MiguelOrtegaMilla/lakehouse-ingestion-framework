package lakehouse.ingestion.core.config

import cats.syntax.all._
import lakehouse.ingestion.core.config.Read.{Obj, Problem, Result}

/** What a connector is told about the source it reads: the `connector` section of a source. */
trait ConnectorConfig {

  /** The `type` of the section, which chose the kind that read it. */
  def kind: String

  /** What is worth a second look in it, by the path of the field. */
  def warnings: Seq[Problem] = Nil
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
 */
final case class JdbcConnectorConfig(connection: DatabaseConnection, table: QualifiedName)
    extends ConnectorConfig {

  override def kind: String = JdbcConnector.name

  override def warnings: Seq[Problem] =
    DatabaseConnection.warnings("connector.connection", connection).toSeq
}

object JdbcConnector extends ConnectorKind {

  override val name: String = "jdbc"

  override val loads: Set[Load.Mode] = Set(Load.Mode.Full, Load.Mode.Incremental)

  override def read(section: Obj): Result[ConnectorConfig] =
    (
      section.required("connection")(_.obj(DatabaseConnection.read)),
      section.required("table")(_.text(QualifiedName.parse))
    ).mapN(JdbcConnectorConfig.apply)
}

/**
 * Files that are left in a directory of the landing zone.
 *
 * @param path the directory, relative to the profile's `landing.root` (ADR-0013 §2)
 * @param pattern which files of the directory, by their name; all of them if it is left out
 */
final case class FileConnectorConfig(
    path: RelativePath,
    format: FileConnectorConfig.Format,
    pattern: Option[FileConnectorConfig.Pattern]
) extends ConnectorConfig {
  override def kind: String = FileConnector.name
}

object FileConnectorConfig {

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
    override def toString: String = value
  }

  object Pattern {

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
      section.optional("pattern")(_.text(FileConnectorConfig.Pattern.parse))
    ).mapN(FileConnectorConfig.apply)
}
