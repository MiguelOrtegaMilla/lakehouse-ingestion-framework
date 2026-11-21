package lakehouse.ingestion.engine.jdbc

/**
 * A column of a table of a source, as the source says it: what the metadata of a statement
 * over the table has of it (`SourceDatabase.columns`).
 *
 * @param name as the source writes it, with its case; any text, since it is the source's
 * @param typeName the name that PostgreSQL has for its type, as its driver gives it: `int4`,
 *   `numeric`, `timestamptz`, and `_numeric` for an array of `numeric`
 * @param precision of a `numeric`, its digits, and zero if it has no bound; of a `bit`, its
 *   length
 * @param nullable whether the source lets it be null, as far as the source says
 */
final case class SourceColumn(
    name: String,
    typeName: String,
    precision: Int,
    scale: Int,
    nullable: Boolean
) {

  /** Whether it is an array, and the type of what it holds if it is. */
  def element: Option[String] = Option.when(typeName.startsWith("_"))(typeName.drop(1))
}

/**
 * How Bronze keeps a column of a table of PostgreSQL, by its type (discovery §17, P6.2 and
 * P6.6): as Spark's JDBC source reads it, or as text.
 *
 * '''No value is changed in Bronze without a word.''' Spark reads some types of PostgreSQL
 * into a type of its own that does not hold every value of them, and changes the value, or
 * fails the read (pinned by `SparkJdbcContractSpec`). A column of such a type is read as text
 * instead, which is the value as the server writes it, to the last digit: Bronze is faithful
 * to its source, and Silver is where a value gets the type that suits it (ADR-0008).
 *
 * It is decided by the type of the column alone, and never by its values: the columns of a
 * table of Bronze do not depend on what a batch happened to hold.
 */
object BronzeTypes {

  sealed trait Kept

  /** As Spark's JDBC source reads the type, which holds every value of it. */
  case object AsSparkReads extends Kept

  /**
   * As text: the value as the server writes it.
   *
   * @param why what Spark would do with the type otherwise
   */
  final case class AsText(why: String) extends Kept

  /** The most digits that a decimal of Spark has. */
  val MaxDecimalDigits = 38

  def of(column: SourceColumn): Kept =
    column.element match {
      case Some(element) => ofAnArray(column, element)
      case None => ofAValue(column, column.typeName)
    }

  /**
   * Whether a value of the column may be `infinity` or `-infinity`, which PostgreSQL has for a
   * date and for a timestamp and Spark has not: Spark reads each as a date of its own choosing,
   * the last of the year 9999 for one, which a row may hold for itself. It cannot be told by
   * the type, so each row is asked of the source (`JdbcSql.flagged`).
   */
  def mayBeInfinite(column: SourceColumn): Boolean =
    column.element.isEmpty && Finite.contains(column.typeName)

  private val Finite = Set("date", "timestamp", "timestamptz")

  private def ofAValue(column: SourceColumn, typeName: String): Kept = typeName match {
    case "numeric" if column.precision == 0 =>
      AsText(
        "a numeric with no bound has more digits than a decimal of Spark, which rounds it " +
          "to 18 places, and fails the read beyond 20 digits before the point"
      )
    case "numeric" if column.precision > MaxDecimalDigits =>
      AsText(
        s"a numeric of ${column.precision} digits has more than a decimal of Spark, which " +
          s"has $MaxDecimalDigits"
      )
    case "numeric" if column.scale < 0 || column.scale > column.precision =>
      AsText("a numeric with a scale below zero is one that a decimal of Spark does not have")
    case "time" | "timetz" =>
      AsText(
        "a time of day is read by Spark as a timestamp of 1970-01-01, with its zone applied " +
          "if it has one"
      )
    case "bit" if column.precision > 1 =>
      AsText("a string of bits is read by Spark as the bytes of its characters")
    case _ => AsSparkReads
  }

  private def ofAnArray(column: SourceColumn, element: String): Kept =
    if (Finite.contains(element))
      AsText(
        s"an array of $element may hold infinity, which Spark reads as another date, and a " +
          "value of an array cannot be asked of the source one by one"
      )
    else
      ofAValue(column, element) match {
        case AsText(why) => AsText(s"it is an array, and $why")
        case AsSparkReads => AsSparkReads
      }
}

/** What a load by a watermark asks of its column (ADR-0020), from what the source says of it. */
object WatermarkColumn {

  /** The types that a watermark is of: a timestamp, and nothing else (ADR-0020, point 1). */
  val Types: Set[String] = Set("timestamptz", "timestamp")

  /**
   * @return why the column cannot be a watermark, or a warning of it if there is one: a column
   *   that may be null
   */
  def check(columns: Seq[SourceColumn], name: String): Either[String, Option[String]] =
    columns.find(_.name == name) match {
      case None =>
        Left(
          s"has no column $name (load.watermark.column). Its columns are " +
            columns.map(_.name).mkString(", ")
        )
      case Some(column) if !Types.contains(column.typeName) =>
        Left(
          s"has its column $name (load.watermark.column) of type ${column.typeName}, and a " +
            "watermark is a timestamptz, or a timestamp without a zone, which is taken as " +
            "UTC: a load by a watermark leaves a margin of time behind the clock of its " +
            "source, and no margin can be said in another type (ADR-0020)"
        )
      case Some(column) if column.nullable =>
        Right(
          Some(
            s"the column $name (load.watermark.column) may be null, and a row with no value " +
              "in it is in no batch: it is never ingested. Every writer of the table has to " +
              "set it, at every change (ADR-0020)"
          )
        )
      case Some(_) => Right(None)
    }
}
