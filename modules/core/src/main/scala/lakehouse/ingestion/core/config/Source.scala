package lakehouse.ingestion.core.config

import java.time.Instant

import scala.collection.immutable.SortedMap
import scala.concurrent.duration._

import cats.data.{Chain, NonEmptyChain}
import cats.syntax.all._
import io.circe.Json
import lakehouse.ingestion.core.config.Read.{Node, Obj, Problem, Result}

/**
 * What is ingested from one place, and into what: one file of `conf/sources/` (ADR-0007,
 * discovery §13.1). Adding a source is writing one of these, not code.
 *
 * @param keys the columns that tell one row from another: what Silver merges by
 * @param deletes how rows that the source deleted are found, when the load does not find them
 *   by itself (ADR-0009)
 * @param columns what is said of single columns, by their name: which hold personal data
 * @param freshnessSla how old the data of the source may get before it is late
 */
final case class Source(
    id: SourceId,
    enabled: Boolean,
    owner: Option[String],
    connector: ConnectorConfig,
    load: Load,
    keys: Vector[SqlIdentifier],
    deletes: Option[Deletes],
    schema: SchemaPolicy,
    columns: SortedMap[String, ColumnSpec],
    quality: Quality,
    targets: Targets,
    freshnessSla: Option[FiniteDuration]
)

/**
 * The name of a source: words of lower-case letters, digits and underscores, joined by dots, as
 * `sales.orders`. It becomes part of file names, of table paths and of the keys of leases, so
 * it is written one way: no upper case, nothing that a path or a shell reads otherwise.
 */
final class SourceId private (val value: String) extends AnyVal {
  override def toString: String = value
}

object SourceId {

  val MaxBytes = 100

  private val Written = """[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*""".r

  def parse(text: String): Either[String, SourceId] =
    if (text.isEmpty) Left("is empty")
    else if (text.length > MaxBytes)
      Left(s"is ${text.length} bytes long, and an id is $MaxBytes at most")
    else if (Written.matches(text)) Right(new SourceId(text))
    else
      Left(
        "must be words of lower-case letters, digits and underscores, each starting with a " +
          "letter and joined by dots, as sales.orders"
      )
}

/** How the rows of a source are taken. */
sealed trait Load {
  def mode: Load.Mode
}

object Load {

  sealed abstract class Mode(val name: String)

  object Mode {
    case object Full extends Mode("full")
    case object Incremental extends Mode("incremental")
    case object NewFiles extends Mode("new_files")

    val All: Seq[Mode] = Seq(Full, Incremental, NewFiles)
  }

  /** Everything, every time. */
  case object Full extends Load {
    override def mode: Mode = Mode.Full
  }

  /**
   * What changed since the last time, by a column that only grows.
   *
   * @param maxBatchRange the widest stretch of the watermark that one batch takes, counted from
   *   the watermark. The first batch of a source without `start` has no watermark to count from
   */
  final case class Incremental(watermark: Watermark, maxBatchRange: Option[FiniteDuration])
      extends Load {
    override def mode: Mode = Mode.Incremental
  }

  /** The files that have not been taken yet. */
  case object NewFiles extends Load {
    override def mode: Mode = Mode.NewFiles
  }

  /**
   * @param column a column of a timestamp: what a margin can be said in (discovery §17, P6)
   * @param lookback how far before the last watermark a load starts again, for rows that
   *   arrived late
   * @param safetyMargin how far behind the present a load stops, for transactions that are
   *   still open
   * @param start where the first batch of the source starts: the rows at or before it are not
   *   the source's to ingest. Without it, the first batch reads the whole table. Once the source
   *   has a watermark, it is not looked at again
   */
  final case class Watermark(
      column: SqlIdentifier,
      lookback: FiniteDuration,
      safetyMargin: FiniteDuration,
      start: Option[Instant]
  )

  /**
   * What is worth a second look in a load that is valid, by the path of the field: a watermark
   * that loses rows of transactions still open when their window is read, and a cut of batches
   * that the first batch does not have.
   */
  def warnings(load: Load): Seq[Problem] = load match {
    case Incremental(watermark, range) =>
      Seq(
        Option.when(watermark.safetyMargin == Duration.Zero && watermark.lookback == Duration.Zero)(
          Problem(
            "load.watermark",
            "has safety_margin and lookback both at 0s: a row whose transaction commits after " +
              "the window that holds its time was read is never ingested. safety_margin longer " +
              "than the longest transaction that writes the table loses none"
          )
        ),
        Option.when(range.nonEmpty && watermark.start.isEmpty)(
          Problem(
            "load.max_batch_range",
            "is set, and load.watermark.start is not: the first batch reads the whole table up " +
              "to the present in one statement, however long its history is. max_batch_range " +
              "cuts the batches after it, counted from the watermark"
          )
        )
      ).flatten
    case _ => Nil
  }
}

/** How deletes of the source are found when the load does not find them by itself (ADR-0009). */
sealed trait Deletes

object Deletes {

  /** A run of its own compares the keys of the source with those of Silver. */
  case object KeyReconciliation extends Deletes

  /** The source marks a deleted row in a column of its own. */
  final case class DeletedFlag(column: SqlIdentifier) extends Deletes
}

/**
 * What is done when the source does not look as it did (discovery §10; the full policy is P9).
 *
 * @param renames columns that changed their name, old to new: a rename cannot be told from a
 *   column that went and another that came, unless somebody says so
 */
final case class SchemaPolicy(
    onNewColumn: SchemaPolicy.OnNewColumn,
    onMissingColumn: SchemaPolicy.OnMissingColumn,
    onTypeChange: SchemaPolicy.OnTypeChange,
    renames: SortedMap[String, SqlIdentifier]
)

object SchemaPolicy {

  sealed abstract class OnNewColumn(val name: String)

  object OnNewColumn {
    case object Add extends OnNewColumn("add")
    case object Rescue extends OnNewColumn("rescue")
    case object Fail extends OnNewColumn("fail")
    val All: Seq[OnNewColumn] = Seq(Add, Rescue, Fail)
  }

  sealed abstract class OnMissingColumn(val name: String)

  object OnMissingColumn {
    case object Null extends OnMissingColumn("null")
    case object Fail extends OnMissingColumn("fail")
    val All: Seq[OnMissingColumn] = Seq(Null, Fail)
  }

  sealed abstract class OnTypeChange(val name: String)

  object OnTypeChange {
    case object Widen extends OnTypeChange("widen")
    case object Fail extends OnTypeChange("fail")
    val All: Seq[OnTypeChange] = Seq(Widen, Fail)
  }

  /** What discovery §13.1 shows: a new column is taken, a missing one is null, a type stays. */
  val Default: SchemaPolicy =
    SchemaPolicy(OnNewColumn.Add, OnMissingColumn.Null, OnTypeChange.Fail, SortedMap.empty)
}

/**
 * @param pii what kind of personal data the column holds, as a word of the team's own: `email`,
 *   `phone`, `national_id`
 * @param mask how Silver hides it
 */
final case class ColumnSpec(pii: Option[String], mask: Option[ColumnSpec.Mask])

object ColumnSpec {

  sealed abstract class Mask(val name: String)

  object Mask {

    /** A keyed hash: the same value gives the same token, and the token does not give the value. */
    case object Hmac extends Mask("hmac")

    /** The value is left out. */
    case object Redact extends Mask("redact")

    val All: Seq[Mask] = Seq(Hmac, Redact)
  }

  private val Tag = """[a-z][a-z0-9_]{0,39}""".r

  def tag(text: String): Either[String, String] =
    if (Tag.matches(text)) Right(text)
    else
      Left("must be a word of lower-case letters, digits and underscores, as email or national_id")
}

/** @param maxRejectedRatio the share of rows of a batch that may be rejected before it is not committed */
final case class Quality(maxRejectedRatio: Double) {

  /**
   * Whether `rejected` of the `read` rows of a batch are more than the source allows. The share
   * that it allows is allowed whole: one row of a hundred is not beyond `0.01`.
   *
   * It is counted in decimals, as the ratio was written, and not in the doubles that it is kept
   * in: a batch at its limit would be over it, or not, by the rounding of a product.
   */
  def exceededBy(rejected: Long, read: Long): Boolean = {
    require(rejected >= 0 && rejected <= read, s"$rejected rows of $read were rejected")
    BigDecimal(rejected) > BigDecimal(maxRejectedRatio) * BigDecimal(read)
  }
}

object Quality {

  /** Nothing is rejected unless a source says how much may be. */
  val Default: Quality = Quality(maxRejectedRatio = 0.0)
}

/** Where a source goes: always to Bronze, and to Silver if it says so. */
final case class Targets(bronze: Targets.Bronze, silver: Option[Targets.Silver])

object Targets {

  /** @param table relative to the profile's `lake.uri` (ADR-0011) */
  final case class Bronze(table: RelativePath)

  final case class Silver(table: RelativePath, partitionBy: Vector[SqlIdentifier])
}

object Source {

  /**
   * Reads a source from the text of its file.
   *
   * @param file the name of the file, for the errors
   * @param kinds the kinds of connector that the application has
   * @return the source with its warnings, or everything that is wrong in the file
   */
  def load(
      file: String,
      text: String,
      kinds: Seq[ConnectorKind] = ConnectorKind.BuiltIn
  ): Either[NonEmptyChain[ConfigError], Loaded[Source]] = {
    def located(problem: Problem): ConfigError =
      ConfigError(file, Option(problem.field).filter(_ != "the file"), problem.problem)

    SafeYaml.parse(text) match {
      case Left(refusal) => Left(NonEmptyChain.one(ConfigError(file, None, refusal.message)))
      case Right(json) =>
        read(json, kinds).toEither.leftMap(_.map(located)).flatMap { case (source, kind) =>
          NonEmptyChain
            .fromChain(rules(source, kind).map(located))
            .toLeft(Loaded(source, warnings(source).map(located).toVector))
        }
    }
  }

  /** What is worth a second look in a source that is valid: its connector's, and its load's. */
  def warnings(source: Source): Seq[Problem] =
    source.connector.warnings ++ Load.warnings(source.load)

  // -------------------------------------------------------------------------------------------
  // The file, field by field
  // -------------------------------------------------------------------------------------------

  private def read(json: Json, kinds: Seq[ConnectorKind]): Result[(Source, ConnectorKind)] =
    Read.document(json) { top =>
      (
        top.required("id")(_.text(SourceId.parse)),
        top.orElse("enabled", true)(_.boolean),
        top.optional("owner")(_.text(owner)),
        top.required("connector")(_.obj(connector(kinds))),
        top.required("load")(_.obj(load)),
        top.orElse("keys", Vector.empty[SqlIdentifier])(_.list(_.text(SqlIdentifier.parse))),
        top.optional("deletes")(_.obj(deletes)),
        top.orElse("schema", SchemaPolicy.Default)(_.obj(schema)),
        top.orElse("columns", Vector.empty[(String, ColumnSpec)])(_.entries(column)),
        top.orElse("quality", Quality.Default)(
          _.obj(
            _.orElse("max_rejected_ratio", Quality.Default.maxRejectedRatio)(_.ratio)
              .map(Quality.apply)
          )
        ),
        top.required("targets")(_.obj(targets)),
        top.optional("freshness_sla")(_.duration(1.second, Durations.Max))
      ).mapN {
        (
            id,
            enabled,
            owner,
            connector,
            load,
            keys,
            deletes,
            schema,
            columns,
            quality,
            targets,
            sla
        ) =>
          val (config, kind) = connector
          (
            Source(
              id,
              enabled,
              owner,
              config,
              load,
              keys,
              deletes,
              schema,
              SortedMap.from(columns),
              quality,
              targets,
              sla
            ),
            kind
          )
      }
    }

  private def owner(text: String): Either[String, String] =
    if (text.isEmpty || text.length > 100) Left("must be 1 to 100 characters")
    else
      Unwritten
        .firstOutside(text, c => c >= 0x20 && c < 0x7f)
        .fold[Either[String, String]](Right(text)) { case (codePoint, position) =>
          Left(s"has ${Unwritten.describe(codePoint)} at position $position: it is printable ASCII")
        }

  private def connector(
      kinds: Seq[ConnectorKind]
  )(section: Obj): Result[(ConnectorConfig, ConnectorKind)] =
    section
      .required("type")(_.oneOf(kinds.map(kind => kind.name -> kind): _*))
      // Without its kind the other fields of the section cannot be told from mistakes.
      .leftMap { problems => section.restUnjudged(); problems }
      .andThen(kind => kind.read(section).map(_ -> kind))

  private def load(fields: Obj): Result[Load] = {
    val watermark = fields.optional("watermark")(_.obj { w =>
      (
        w.required("column")(_.text(SqlIdentifier.parse)),
        w.orElse("lookback", Duration.Zero)(_.duration(Duration.Zero, Durations.Max)),
        w.orElse("safety_margin", Duration.Zero)(_.duration(Duration.Zero, Durations.Max)),
        w.optional("start")(_.text(Timestamps.parse))
      ).mapN(Load.Watermark.apply)
    })
    val range = fields.optional("max_batch_range")(_.duration(1.second, Durations.Max))
    val mode = fields.required("mode")(_.oneOf(Load.Mode.All.map(mode => mode.name -> mode): _*))

    (mode, watermark, range).tupled.andThen {
      case (Load.Mode.Incremental, Some(watermark), range) =>
        Read.valid(Load.Incremental(watermark, range))
      case (Load.Mode.Incremental, None, _) =>
        Read.invalid(
          fields.path("watermark"),
          "is missing: an incremental load goes by a column that only grows"
        )
      case (other, watermark, range) =>
        val whole: Load = if (other == Load.Mode.Full) Load.Full else Load.NewFiles
        val misplaced = Chain.fromSeq(
          Seq(watermark.map(_ => "watermark"), range.map(_ => "max_batch_range")).flatten.map {
            name =>
              Problem(
                fields.path(name),
                s"is for mode incremental alone, and mode is ${other.name}"
              )
          }
        )
        NonEmptyChain.fromChain(misplaced).fold(Read.valid(whole))(_.invalid)
    }
  }

  private def deletes(fields: Obj): Result[Deletes] = {
    val column = fields.optional("column")(_.text(SqlIdentifier.parse))
    val strategy =
      fields.required("strategy")(_.oneOf("key_reconciliation" -> false, "deleted_flag" -> true))
    (strategy, column).tupled.andThen {
      case (true, Some(column)) => Read.valid(Deletes.DeletedFlag(column))
      case (true, None) =>
        Read.invalid(
          fields.path("column"),
          "is missing: deleted_flag needs the column that marks a deleted row"
        )
      case (false, Some(_)) =>
        Read.invalid(fields.path("column"), "is for the strategy deleted_flag alone")
      case (false, None) => Read.valid(Deletes.KeyReconciliation)
    }
  }

  private def schema(fields: Obj): Result[SchemaPolicy] = {
    import SchemaPolicy._
    (
      fields.orElse("on_new_column", Default.onNewColumn)(
        _.oneOf(OnNewColumn.All.map(v => v.name -> v): _*)
      ),
      fields.orElse("on_missing_column", Default.onMissingColumn)(
        _.oneOf(OnMissingColumn.All.map(v => v.name -> v): _*)
      ),
      fields.orElse("on_type_change", Default.onTypeChange)(
        _.oneOf(OnTypeChange.All.map(v => v.name -> v): _*)
      ),
      fields.orElse("renames", Vector.empty[(String, SqlIdentifier)])(_.entries(rename))
    ).mapN((added, missing, changed, renames) =>
      SchemaPolicy(added, missing, changed, SortedMap.from(renames))
    )
  }

  private def rename(old: String, to: Node): Result[(String, SqlIdentifier)] =
    (named(old, to), to.text(SqlIdentifier.parse)).mapN((old, to) => old.value -> to)

  private def column(name: String, spec: Node): Result[(String, ColumnSpec)] =
    (
      named(name, spec),
      spec.obj { fields =>
        (
          fields.optional("pii")(_.text(ColumnSpec.tag)),
          fields
            .optional("silver")(
              _.obj(_.optional("mask")(_.oneOf(ColumnSpec.Mask.All.map(m => m.name -> m): _*)))
            )
            .map(_.flatten)
        ).mapN(ColumnSpec.apply)
      }
    ).mapN((name, spec) => name.value -> spec)

  /** A key of a mapping that is the name of a column. */
  private def named(name: String, at: Node): Result[SqlIdentifier] =
    SqlIdentifier.parse(name).fold(reason => at.fail(s"is under a name that $reason"), Read.valid)

  private def targets(fields: Obj): Result[Targets] =
    (
      fields.required("bronze")(
        _.obj(_.required("table")(_.text(RelativePath.parse)).map(Targets.Bronze))
      ),
      fields.optional("silver")(_.obj { silver =>
        (
          silver.required("table")(_.text(RelativePath.parse)),
          silver.orElse("partition_by", Vector.empty[SqlIdentifier])(
            _.list(_.text(SqlIdentifier.parse))
          )
        ).mapN(Targets.Silver.apply)
      })
    ).mapN(Targets.apply)

  // -------------------------------------------------------------------------------------------
  // What is about more than one field of a source
  // -------------------------------------------------------------------------------------------

  private def rules(source: Source, kind: ConnectorKind): Chain[Problem] = {
    val silver = source.targets.silver
    val bronze = source.targets.bronze.table.segments

    def repeated(field: String, names: Vector[SqlIdentifier]): Seq[Problem] =
      names.zipWithIndex.collect {
        case (name, index) if names.indexOf(name) < index =>
          Problem(
            s"$field[$index]",
            s"is $name again: it is $field[${names.indexOf(name)}] already"
          )
      }

    Chain.fromSeq(
      Seq(
        Option.when(!kind.loads(source.load.mode))(
          Problem(
            "load.mode",
            s"is ${source.load.mode.name}, and a connector of type ${kind.name} loads by " +
              kind.loads.toSeq.map(_.name).sorted.mkString(" or ")
          )
        ),
        Option.when(silver.nonEmpty && source.keys.isEmpty)(
          Problem(
            "keys",
            "is empty, and targets.silver needs the columns that tell one row from another to merge by"
          )
        ),
        silver
          .filter(s => s.table.segments.startsWith(bronze) || bronze.startsWith(s.table.segments))
          .map { _ =>
            Problem(
              "targets.silver.table",
              "is the Bronze table, or inside it, or around it: each layer has a table of its own"
            )
          },
        source.deletes.collect {
          case Deletes.KeyReconciliation if source.load.mode != Load.Mode.Incremental =>
            Problem(
              "deletes.strategy",
              s"is key_reconciliation, which is for incremental loads, and load.mode is ${source.load.mode.name}"
            )
        },
        source.deletes.filter(_ => silver.isEmpty).map { _ =>
          Problem(
            "deletes",
            "is set, and there is no targets.silver: deletes are marked in Silver (ADR-0009)"
          )
        }
      ).flatten ++
        // Every column that is masked, not the first alone: a file's mistakes are all said.
        source.columns.toSeq.collect {
          case (name, ColumnSpec(_, Some(_))) if silver.isEmpty =>
            Problem(
              s"columns.$name.silver.mask",
              "is set, and there is no targets.silver to mask it in"
            )
        } ++ repeated("keys", source.keys) ++
        silver.toSeq.flatMap(s => repeated("targets.silver.partition_by", s.partitionBy)) ++
        engineColumns(source)
    )
  }

  /**
   * The columns of the engine where a column of the source goes (`EngineColumns`): a key that
   * is the engine's column says nothing of the row, and a column of the source by one of those
   * names would be two columns of one name in a table. `_source_id` is what a table that
   * several sources merge into is partitioned by, and is right there, written as the engine
   * writes it.
   *
   * The old name of a rename is the source's, and may be one of them: renaming is the way out
   * for a source that has such a column (P9).
   */
  private def engineColumns(source: Source): Seq[Problem] = {
    def engines(field: String, name: String): Option[Problem] =
      EngineColumns.reserved(name).map { column =>
        val which = if (column == name) column else s"$column, case aside"
        Problem(
          field,
          s"is $name, which is $which: a column that the engine adds to every row, and not one " +
            "of the source. A source that has a column of that name renames it (schema.renames)"
        )
      }

    def named(field: String, names: Vector[SqlIdentifier]): Seq[Problem] =
      names.zipWithIndex.flatMap { case (name, index) => engines(s"$field[$index]", name.value) }

    val partitions = source.targets.silver.toVector.flatMap(_.partitionBy.zipWithIndex).flatMap {
      case (name, _) if name.value == EngineColumns.SourceId => None
      case (name, index) if EngineColumns.reserved(name.value).contains(EngineColumns.SourceId) =>
        Some(
          Problem(
            s"targets.silver.partition_by[$index]",
            s"is ${name.value}, which is the engine's ${EngineColumns.SourceId}: it is written " +
              "as the engine writes it"
          )
        )
      case (name, index) => engines(s"targets.silver.partition_by[$index]", name.value)
    }

    named("keys", source.keys) ++
      (source.load match {
        case Load.Incremental(watermark, _) =>
          engines("load.watermark.column", watermark.column.value)
        case _ => None
      }) ++
      source.deletes.collect { case Deletes.DeletedFlag(column) => column }.flatMap { column =>
        engines("deletes.column", column.value)
      } ++
      source.columns.keys.flatMap(name => engines(s"columns.$name", name)) ++
      source.schema.renames.toSeq.flatMap { case (old, to) =>
        engines(s"schema.renames.$old", to.value)
      } ++ partitions
  }
}
