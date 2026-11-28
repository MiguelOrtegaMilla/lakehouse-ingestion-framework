package lakehouse.ingestion.engine.bronze

import java.util.Locale

import scala.collection.mutable

import lakehouse.ingestion.core.config.SchemaPolicy.{OnMissingColumn, OnNewColumn, OnTypeChange}
import lakehouse.ingestion.core.config.{EngineColumns, SchemaPolicy}
import lakehouse.ingestion.engine.delta.DeltaNames
import org.apache.spark.sql.types._

/**
 * The schema policy of a source as a function with no I/O (ADR-0025): what the rows of a batch
 * are to be in their table, or every reason why the table does not take them.
 *
 * It is given the parts of a read, in the order of its plan; the schema of the table, if there is
 * one; and the policy of the source. It is made at each attempt of a write, against the snapshot
 * on which the write commits (ADR-0025 §1, §10), and gives the same for the same three.
 *
 *   - '''Names.''' A column of the source is given its name in Bronze by `schema.renames`, matched
 *     by its name exactly as the read has it (§5, §6). A name of the engine's, case aside, and one
 *     that a table of Delta cannot have, are refused: a rename is the way on.
 *   - '''One row.''' Names that differ only in their case are one column for Delta. In a part
 *     whose rows have every one of its columns, two such are two columns of one row, and are
 *     refused; and so are two names of one rename.
 *   - '''The batch.''' Its columns are those of its parts, by name and case aside, in the order in
 *     which they are first met, under the first spelling met. A column of two types has the wider,
 *     where one is a widening of the other (§3); two that are not refuse the batch (§7).
 *   - '''The table.''' Where there is none, the batch makes it as it is, whatever the policy says.
 *     Otherwise a column takes the spelling of the table. A type that the table has, or one
 *     narrower than it, is cast to the type of the table, under any policy; a wider one is
 *     widened under `widen`, and refused under `fail`; any other is refused. A column that the
 *     table does not have is added, rescued or refused; one that the read lacks is null or
 *     refused.
 *
 * The columns of the engine are not compared: a read gives those of the source alone, and those
 * of the table are the engine's. Whether the batch has rows is not looked at either. A batch with
 * none writes nothing (ADR-0025 §11).
 */
object SchemaDecision {

  /** A part of a read: a schema, and what the part is, for the messages. */
  sealed trait Part {
    def label: String
    def schema: StructType
  }

  object Part {

    /**
     * Each row has every column of it: a table of a database, a file of Parquet, the files of
     * CSV under one header.
     */
    final case class Whole(label: String, schema: StructType) extends Part

    /**
     * Each record has some of its columns: the records of JSON. No record has two of them that
     * are to be one, which the read makes sure of: it sets apart one with two that differ only
     * in their case (ADR-0025 §6), and fails its batch for one with two names of one rename (§5).
     * The columns are in the order in which the records first have them.
     */
    final case class Sparse(label: String, schema: StructType) extends Part
  }

  /**
   * A column of the rows as they are written.
   *
   * @param name its name in the table
   * @param dataType its type in the table, to which the values read are cast
   * @param read the names of the read that give it, in the order in which they were met: one,
   *   or the spellings and the names of a rename that are one column; none, for a column of the
   *   table that the read lacks, which has nothing in it
   */
  final case class Column(name: String, dataType: DataType, read: Vector[String])

  /** A column whose type is widened, from the type of the table to that of the read. */
  final case class Widening(column: String, from: DataType, to: DataType)

  /**
   * What a batch changes of its table, and what of its read goes elsewhere: what its commit
   * records (ADR-0025 §11), by names and types alone.
   *
   * @param added the columns added to the table, in the order of the read
   * @param widened the columns whose type is widened
   * @param rescued the columns of the read whose values go to `_rescued_data`
   * @param missing the columns of the table that the read lacks
   * @param rescuedData whether `_rescued_data` is added to the table, which a table of types has
   *   only once something has been rescued into it
   */
  final case class Change(
      added: Vector[StructField],
      widened: Vector[Widening],
      rescued: Vector[String],
      missing: Vector[String],
      rescuedData: Boolean
  ) {

    /** Whether the table is changed: its write is the one that is to merge into its schema. */
    def changesTable: Boolean = added.nonEmpty || widened.nonEmpty || rescuedData
  }

  object Change {
    val Unchanged: Change =
      Change(Vector.empty, Vector.empty, Vector.empty, Vector.empty, rescuedData = false)
  }

  /**
   * The rows of a batch, as the table is to take them.
   *
   * @param columns the columns of the rows, in the order of the table, and then those added
   * @param rescued the columns of the read that go to `_rescued_data`, under the name and with
   *   the type that the read gives them
   */
  final case class Adjustment(columns: Vector[Column], rescued: Vector[Column], change: Change) {

    /** The schema of the rows as written, without the columns of the engine. */
    def schema: StructType = StructType(columns.map(c => StructField(c.name, c.dataType)))
  }

  /** Why the table does not take the rows of a batch: one reason at least. */
  final case class Refusal(reasons: Vector[Reason]) {
    require(reasons.nonEmpty, "a refusal has a reason")

    def message: String = reasons.map(_.message).mkString("; ")
  }

  /** One reason why the table does not take the rows of a batch. */
  sealed trait Reason {
    def message: String
  }

  object Reason {

    /** A name of a column of the source that Bronze cannot have as it is. */
    sealed trait NameOfTheSource extends Reason {
      def column: String
    }

    /** A column of the source under a name of the engine's, case aside. */
    final case class EngineName(column: String, engine: String) extends NameOfTheSource {
      def message: String =
        s"the column $column of the source is under a name of the engine's ($engine, case " +
          "aside), and needs another in Bronze (schema.renames)"
    }

    /** A column whose name a table of Delta cannot have. */
    final case class NameRefused(column: String) extends NameOfTheSource {
      def message: String =
        s"the column $column of the source has a space, a tab, a line break or one of ,;{}()= " +
          "in its name, which a table of Delta does not take, and needs another in Bronze " +
          "(schema.renames)"
    }

    /** Two columns of one row whose names differ only in their case. */
    final case class OneNameTwice(columns: Vector[String], part: String) extends Reason {
      def message: String =
        s"the columns ${columns.mkString(", ")} of one row of $part differ only in their case, " +
          "and a table of Delta takes them for one"
    }

    /** Two columns of one row that are renamed to one column of Bronze. */
    final case class RenamedTogether(columns: Vector[String], to: String, part: String)
        extends Reason {
      def message: String =
        s"the columns ${columns.mkString(", ")} of one row of $part are both $to in Bronze " +
          "(schema.renames)"
    }

    /** A column of two types in the parts of a batch, neither a widening of the other. */
    final case class TwoTypes(
        column: String,
        first: DataType,
        firstPart: String,
        second: DataType,
        secondPart: String
    ) extends Reason {
      def message: String =
        s"the column $column is ${first.sql} in $firstPart and ${second.sql} in $secondPart, " +
          "and neither type is a widening of the other"
    }

    /** A column that the table does not have, under `on_new_column: fail`. */
    final case class NewColumn(column: String, dataType: DataType, part: String) extends Reason {
      def message: String =
        s"the column $column (${dataType.sql}) of $part is not in the table, and the policy " +
          "of the source takes no new column (schema.on_new_column: fail)"
    }

    /** A column of the table that the read lacks, under `on_missing_column: fail`. */
    final case class MissingColumn(column: String) extends Reason {
      def message: String =
        s"the column $column of the table is not in the read, and the policy of the source " +
          "takes no column missing (schema.on_missing_column: fail)"
    }

    /** A widening of a type, under `on_type_change: fail`. */
    final case class TypeChanged(column: String, from: DataType, to: DataType, part: String)
        extends Reason {
      def message: String =
        s"the column $column is ${from.sql} in the table and ${to.sql} in $part, a widening " +
          "that the policy of the source does not make (schema.on_type_change: fail)"
    }

    /** A change of a type that is no widening, under any policy. */
    final case class Incompatible(column: String, table: DataType, read: DataType, part: String)
        extends Reason {
      def message: String =
        s"the column $column is ${table.sql} in the table and ${read.sql} in $part, which is " +
          "no widening that the engine makes: another column of Bronze takes it " +
          "(schema.renames)"
    }
  }

  import Reason._

  /**
   * @param parts the parts of the read, in the order of its plan: one at least
   * @param table the schema of the table, with the columns of the engine; none, if there is no
   *   table yet
   */
  def decide(
      parts: Seq[Part],
      table: Option[StructType],
      policy: SchemaPolicy
  ): Either[Refusal, Adjustment] = {
    require(parts.nonEmpty, "a read has a part at least")
    val reasons = Vector.newBuilder[Reason]
    val reads = named(parts.toVector, policy)
    val refusedNames = namesRefused(reads)
    reasons ++= refusedNames
    // A column whose name is refused is said once, and not compared again as a column.
    val usable = reads.filterNot(read => refusedNames.exists(_.column == read.name))
    reasons ++= rowsRefused(usable)
    val (batch, mixed) = unified(usable)
    reasons ++= mixed
    val adjusted = table.fold(made(batch))(against(batch, _, policy))
    adjusted.left.foreach(reasons ++= _)
    val all = reasons.result()
    adjusted match {
      case Right(adjustment) if all.isEmpty => Right(adjustment)
      case _ => Left(Refusal(all))
    }
  }

  /** A column of a part of the read, with its name in Bronze. */
  final private case class Read(
      part: Int,
      label: String,
      whole: Boolean,
      name: String,
      spelling: String,
      dataType: DataType,
      renamed: Boolean
  ) {
    def key: String = keyOf(spelling)
  }

  /** A column of the batch: one name, case aside, over its parts. */
  final private case class Unified(
      spelling: String,
      dataType: DataType,
      typePart: String,
      firstPart: String,
      read: Vector[String]
  ) {
    def key: String = keyOf(spelling)
  }

  private def keyOf(name: String): String = name.toLowerCase(Locale.ROOT)

  private def named(parts: Vector[Part], policy: SchemaPolicy): Vector[Read] =
    for {
      (part, index) <- parts.zipWithIndex
      field <- part.schema.fields.toVector
    } yield {
      val renamed = policy.renames.get(field.name)
      Read(
        index,
        part.label,
        part.isInstanceOf[Part.Whole],
        field.name,
        renamed.fold(field.name)(_.value),
        field.dataType,
        renamed.nonEmpty
      )
    }

  /** The names of the source that Bronze cannot have, each once. */
  private def namesRefused(reads: Vector[Read]): Vector[NameOfTheSource] =
    reads
      .distinctBy(_.spelling)
      .flatMap { read =>
        EngineColumns.reserved(read.spelling).map(EngineName(read.name, _)).toVector ++
          (if (DeltaNames.fits(read.spelling)) Vector.empty else Vector(NameRefused(read.name)))
      }

  /** Two columns of one row of a part, wherever every row of the part has every column. */
  private def rowsRefused(reads: Vector[Read]): Vector[Reason] =
    reads
      .filter(_.whole)
      .groupBy(_.part)
      .toVector
      .sortBy(_._1)
      .flatMap { case (_, ofPart) =>
        inOrder(ofPart)(_.key).collect {
          case (_, same) if same.size > 1 && same.exists(_.renamed) =>
            RenamedTogether(same.map(_.name), same.head.spelling, same.head.label)
          case (_, same) if same.size > 1 =>
            OneNameTwice(same.map(_.name), same.head.label)
        }
      }

  /** The elements by their key, in the order in which each key is first met. */
  private def inOrder[A](as: Vector[A])(key: A => String): Vector[(String, Vector[A])] = {
    val groups = mutable.LinkedHashMap.empty[String, Vector[A]]
    as.foreach(a => groups.updateWith(key(a))(had => Some(had.getOrElse(Vector.empty) :+ a)))
    groups.toVector
  }

  /** The columns of the batch, and its columns of two types that neither widens to the other. */
  private def unified(reads: Vector[Read]): (Vector[Unified], Vector[Reason]) = {
    val mixed = Vector.newBuilder[Reason]
    val columns = inOrder(reads)(_.key).map { case (_, same) =>
      val first = same.head
      val start = Unified(first.spelling, first.dataType, first.label, first.label, Vector.empty)
      val (column, _) = same.foldLeft((start, false)) { case ((column, refused), read) =>
        val withName =
          if (column.read.contains(read.name)) column
          else column.copy(read = column.read :+ read.name)
        if (sameType(withName.dataType, read.dataType) || widens(read.dataType, withName.dataType))
          (withName, refused)
        else if (widens(withName.dataType, read.dataType))
          (withName.copy(dataType = read.dataType, typePart = read.label), refused)
        else {
          if (!refused)
            mixed += TwoTypes(
              withName.spelling,
              withName.dataType,
              withName.typePart,
              read.dataType,
              read.label
            )
          (withName, true)
        }
      }
      column
    }
    (columns, mixed.result())
  }

  /** The batch that makes its table. */
  private def made(batch: Vector[Unified]): Either[Vector[Reason], Adjustment] =
    Right(
      Adjustment(
        batch.map(c => Column(c.spelling, c.dataType, c.read)),
        Vector.empty,
        Change.Unchanged
      )
    )

  /** The batch, against the table that it is written to. */
  private def against(
      batch: Vector[Unified],
      table: StructType,
      policy: SchemaPolicy
  ): Either[Vector[Reason], Adjustment] = {
    val reasons = Vector.newBuilder[Reason]
    val widened = Vector.newBuilder[Widening]
    val missing = Vector.newBuilder[String]
    val ofTheTable = table.fields.toVector.filter(f => EngineColumns.reserved(f.name).isEmpty)
    val byKey = batch.map(c => c.key -> c).toMap

    val kept = ofTheTable.map { field =>
      byKey.get(keyOf(field.name)) match {
        case Some(read) if sameType(field.dataType, read.dataType) =>
          Column(field.name, field.dataType, read.read)
        case Some(read) if widens(read.dataType, field.dataType) =>
          // Narrower than its column: cast, and no change (ADR-0025 §2).
          Column(field.name, field.dataType, read.read)
        case Some(read) if widens(field.dataType, read.dataType) =>
          policy.onTypeChange match {
            case OnTypeChange.Widen =>
              widened += Widening(field.name, field.dataType, read.dataType)
            case OnTypeChange.Fail =>
              reasons += TypeChanged(field.name, field.dataType, read.dataType, read.typePart)
          }
          Column(field.name, read.dataType, read.read)
        case Some(read) =>
          reasons += Incompatible(field.name, field.dataType, read.dataType, read.typePart)
          Column(field.name, field.dataType, read.read)
        case None =>
          policy.onMissingColumn match {
            case OnMissingColumn.Null => missing += field.name
            case OnMissingColumn.Fail => reasons += MissingColumn(field.name)
          }
          Column(field.name, field.dataType, Vector.empty)
      }
    }

    val inTheTable = ofTheTable.map(f => keyOf(f.name)).toSet
    val added = Vector.newBuilder[Column]
    val rescued = Vector.newBuilder[Column]
    batch.filterNot(c => inTheTable(c.key)).foreach { read =>
      val column = Column(read.spelling, read.dataType, read.read)
      policy.onNewColumn match {
        case OnNewColumn.Add => added += column
        case OnNewColumn.Rescue => rescued += column
        case OnNewColumn.Fail => reasons += NewColumn(read.spelling, read.dataType, read.firstPart)
      }
    }

    val all = reasons.result()
    if (all.nonEmpty) Left(all)
    else {
      val (adds, rescues) = (added.result(), rescued.result())
      val hasRescuedData =
        table.fields.exists(f => EngineColumns.reserved(f.name).contains(EngineColumns.RescuedData))
      Right(
        Adjustment(
          kept ++ adds,
          rescues,
          Change(
            adds.map(c => StructField(c.name, c.dataType)),
            widened.result(),
            rescues.map(_.name),
            missing.result(),
            rescuedData = rescues.nonEmpty && !hasRescuedData
          )
        )
      )
    }
  }

  /**
   * Whether `to` is a widening of `from` that the engine makes (ADR-0025 §3): those that Delta
   * 4.4.1 makes with every value kept and no feature but type widening (P9.1,
   * `DeltaSchemaContractSpec`). Inside a struct, an array or a map, the shape is the same and
   * one change at least is a widening: the fields of a struct under the same names in the same
   * order, the keys of a map of the same type.
   */
  def widens(from: DataType, to: DataType): Boolean = (from, to) match {
    case (ByteType, ShortType | IntegerType | LongType | DoubleType) => true
    case (ShortType, IntegerType | LongType | DoubleType) => true
    case (IntegerType, LongType | DoubleType) => true
    case (FloatType, DoubleType) => true
    case (ByteType | ShortType | IntegerType, d: DecimalType) => d.precision - d.scale >= 10
    case (LongType, d: DecimalType) => d.precision - d.scale >= 20
    case (f: DecimalType, t: DecimalType) =>
      !sameType(f, t) && t.scale >= f.scale && t.precision - t.scale >= f.precision - f.scale
    case (ArrayType(f, _), ArrayType(t, _)) => widens(f, t)
    case (MapType(fk, fv, _), MapType(tk, tv, _)) => sameType(fk, tk) && widens(fv, tv)
    case (f: StructType, t: StructType) =>
      f.length == t.length &&
      f.fields.zip(t.fields).forall { case (a, b) =>
        a.name == b.name && (sameType(a.dataType, b.dataType) || widens(a.dataType, b.dataType))
      } &&
      f.fields.zip(t.fields).exists { case (a, b) => widens(a.dataType, b.dataType) }
    case _ => false
  }

  /** Whether two types are one, whatever they say of nulls and whatever metadata they carry. */
  def sameType(a: DataType, b: DataType): Boolean = (a, b) match {
    case (ArrayType(x, _), ArrayType(y, _)) => sameType(x, y)
    case (MapType(k1, v1, _), MapType(k2, v2, _)) => sameType(k1, k2) && sameType(v1, v2)
    case (s: StructType, t: StructType) =>
      s.length == t.length && s.fields.zip(t.fields).forall { case (x, y) =>
        x.name == y.name && sameType(x.dataType, y.dataType)
      }
    case _ => a == b
  }
}
