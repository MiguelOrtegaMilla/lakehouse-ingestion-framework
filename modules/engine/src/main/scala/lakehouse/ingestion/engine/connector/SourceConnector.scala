package lakehouse.ingestion.engine.connector

import java.time.Instant

import scala.reflect.ClassTag

import io.circe.Json
import lakehouse.ingestion.core.config.{ConnectorConfig, ConnectorKind, RelativePath, Source}
import lakehouse.ingestion.core.files.{FilePlan, LandedFile}
import lakehouse.ingestion.engine.RunFailedException
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * What reads one kind of source for the engine (discovery §12.2): it plans a batch, and reads
 * what a plan says. Its configuration is its `ConnectorKind`'s, which `core` reads and
 * validates with no I/O; this is the half that has I/O.
 *
 * A new kind of source is one of these, and its registration where the application is put
 * together (`ConnectorRegistry`). The engine names no kind (RNF-04).
 *
 * What a connector guarantees:
 *
 *  - '''A plan says what is to be read, whole, and nothing is read before it is written
 *    down.''' It is JSON of the connector's own form, which the control plane stores in the
 *    write-ahead log and does not read (ADR-0005 §3). It has a version of its form: it outlives
 *    the process that wrote it, and may be replayed by another version of the application.
 *  - '''A plan is a function of what the source has, of what the engine tells it, and of
 *    nothing else''': two runs that plan from the same state, at the same moment, plan the same
 *    batch.
 *  - '''A read of a plan reads that plan, and is the same each time it is replayed''', for as
 *    long as the source still has what the plan names. It has no effect: what was read reaches
 *    nothing but the rows that it gives.
 *  - '''A read says whether its rows are held apart from the source''' (`ReadResult`). Rows
 *    that are, the engine counts before it writes one. Rows that are read from the source each
 *    time they are gone over, the engine goes over once, as it writes them.
 *  - '''A source that no longer has what a plan names is said so, and nothing of the plan is
 *    read as something else''' (`SourceChangedException`, ADR-0018): a file written again, or
 *    gone. What follows is the engine's: the batch is given up if nothing of it was written,
 *    and the next run plans anew.
 *  - '''What the commit of a plan records is a function of the plan alone''' (`progress`):
 *    where the next batch starts from, and the files that the batch took. A batch whose data a
 *    run finds written is then committed with nothing read (ADR-0005 §3, step 2).
 *  - '''Whether there is more after a plan is a function of the plan alone''' (`more`), and is
 *    said against a horizon that the first plan of an invocation fixed: the next run of the
 *    chain is given it (`PlanContext.horizon`), and plans against it, so that a chain ends
 *    though the source goes on changing (discovery §17, P6.10). A connector with no horizon
 *    says that there is no more, and its source is never chained.
 *  - '''A connector never writes to the lake, nor to the control plane.''' The state of a
 *    source, the files it took and the record of a run are the engine's, which moves them in
 *    one transaction with the batch (ADR-0005). What a connector needs of them is given to it,
 *    to read (`PlanContext`).
 *  - '''A failure whose class is known is said with it''' (`RunFailedException`): a directory
 *    or a table that is not there is permanent. One that is not known is let through, for the
 *    classification of errors (P8). Rows that are read as they are written fail in the hands
 *    of the engine, and the engine asks their connector what the failure is (`failure`).
 *
 * What a connector needs that is not in the configuration of a source, as the root of the
 * landing zone or a way to resolve secrets, it is given when it is made.
 *
 * @tparam C the configuration of its kind: the one class of `ConnectorConfig` that its kind
 *   reads
 */
abstract class SourceConnector[C <: ConnectorConfig](implicit configClass: ClassTag[C]) {

  /** How a source of this kind is configured, and what `connector.type` names it. */
  def kind: ConnectorKind

  /**
   * The next batch of a source: what it is to read, or none if it has nothing new.
   *
   * @param config the source's own `connector` section, which is `source.connector`
   */
  def plan(source: Source, config: C, context: PlanContext): Option[Json]

  /**
   * The rows of a plan that this connector wrote, for this source.
   *
   * @throws lakehouse.ingestion.engine.RunFailedException a permanent failure, if the plan is
   *   not one that this connector reads
   * @throws SourceChangedException if the source no longer has what the plan names, as the plan
   *   names it. Nothing of the plan was given to be written
   */
  def read(source: Source, config: C, plan: Json, context: ReadContext): ReadResult

  /**
   * What the commit of a plan records of its source. It reads the plan and nothing else: no
   * file, no table, no clock.
   *
   * @throws lakehouse.ingestion.engine.RunFailedException a permanent failure, if the plan is
   *   not one that this connector reads
   */
  def progress(source: Source, config: C, plan: Json): Progress

  /**
   * Whether the invocation that planned this plan has more to read after it, as the horizon
   * that the next run of the chain is to plan against; none if the chain ends here. It reads
   * the plan and nothing else, as `progress` does.
   *
   * @throws lakehouse.ingestion.engine.RunFailedException a permanent failure, if the plan is
   *   not one that this connector reads
   */
  def more(source: Source, config: C, plan: Json): Option[Json] = None

  /**
   * What a failure that came up while the engine went over the rows of a read of this connector
   * is, if it is one of the source's: its class, and what to do about it. The engine asks it of
   * rows that are read as they are gone over (`ReadResult.OnePass`), whose source fails after
   * `read` has answered: a connection that is cut, a statement that runs too long, a value
   * that the read refuses.
   *
   * It is asked of failures of anything: of the write of a table as much as of the read, since
   * the two are one pass. A connector answers for what its source said, and for nothing else.
   *
   * @return none, for a failure that is not the source's, which is then let through as it is
   */
  def failure(source: Source, config: C, error: Throwable): Option[RunFailedException] = None

  /**
   * This connector for one source: none if the source is of another kind. The engine holds a
   * source by what this gives, and never sees `C`.
   */
  final def bind(source: Source): Option[BoundSource] =
    source.connector match {
      case configClass(config) if config.kind == kind.name => Some(new Bound(source, config))
      case _ => None
    }

  final private class Bound(of: Source, config: C) extends BoundSource(of) {
    override def plan(context: PlanContext): Option[Json] =
      SourceConnector.this.plan(source, config, context)

    override def read(plan: Json, context: ReadContext): ReadResult =
      SourceConnector.this.read(source, config, plan, context)

    override def progress(plan: Json): Progress =
      SourceConnector.this.progress(source, config, plan)

    override def more(plan: Json): Option[Json] =
      SourceConnector.this.more(source, config, plan)

    override def failure(error: Throwable): Option[RunFailedException] =
      SourceConnector.this.failure(source, config, error)
  }
}

/** A source with the connector of its kind: what the engine plans and reads it by. */
abstract class BoundSource private[connector] (val source: Source) {

  /** @see [[SourceConnector.plan]] */
  def plan(context: PlanContext): Option[Json]

  /** @see [[SourceConnector.read]] */
  def read(plan: Json, context: ReadContext): ReadResult

  /** @see [[SourceConnector.progress]] */
  def progress(plan: Json): Progress

  /** @see [[SourceConnector.more]] */
  def more(plan: Json): Option[Json]

  /** @see [[SourceConnector.failure]] */
  def failure(error: Throwable): Option[RunFailedException]
}

/**
 * What the engine tells a connector to plan by. Each part is what one kind of source needs;
 * a connector reads the ones that are its own, and only reads them.
 *
 * @param now when the run plans: what "old enough" and "up to now" are measured against. It is
 *   given, and not read from a clock, so that a plan is a function of what it is given
 * @param watermark where the last batch of the source left it, as the connector wrote it: none
 *   before the first, and for a source that has none
 * @param files the files that batches of the source have taken
 * @param horizon how far the invocation reads, as the plan before this one in its chain said
 *   (`SourceConnector.more`); none for the first plan of an invocation, which fixes it
 */
final case class PlanContext(
    now: Instant,
    watermark: Option[Json],
    files: FileRegistry,
    horizon: Option[Json] = None
)

/**
 * The files that batches of a source took (`processed_file`), to be asked of a listing.
 *
 * It is asked by name, of the names that a directory lists, so that what is asked grows with
 * the directory and not with the history of the source (P5.3).
 */
trait FileRegistry {

  /** The files of `directory` with these names that batches of the source have taken. */
  def processed(directory: RelativePath, names: Seq[String]): Set[LandedFile]
}

/** What the engine gives a connector to read with. */
final case class ReadContext(spark: SparkSession)

/**
 * What a read gives: the rows of a plan, and how they may be gone over.
 *
 * Rows are a query of Spark's, and each time they are gone over, to count them or to write
 * them, the query is run. What that costs, and whether it gives the same rows, is the
 * connector's to say, by which of the two it answers with: the engine counts the rows of one
 * before it writes them, and never goes twice over the rows of the other.
 */
sealed trait ReadResult {

  /**
   * The rows as the source gave them, with the columns that are a read's to add
   * (`_source_file`, `_rescued_data`: `EngineColumns`). The columns of the engine are the
   * writer's to add, and a read never has them.
   */
  def rows: DataFrame
}

object ReadResult {

  /**
   * Rows that Spark holds apart from the source: going over them again asks the source for
   * nothing, and gives the same rows. With them, the records that could not be parsed into
   * rows, from the same read, so that both say the same thing of the source.
   *
   * The engine counts both before it writes either: the share of the records that were
   * rejected is known before a row is in Bronze (ADR-0008).
   *
   * @param unparsed the records that could not be parsed, as they were written in the source:
   *   `_source_file` and `_record`, both text. None for a source whose records are never text
   *   to parse, as a file of Parquet
   */
  final case class Kept(rows: DataFrame, unparsed: Option[DataFrame]) extends ReadResult

  /**
   * Rows that are read from the source each time they are gone over: a table of a database,
   * which is asked with a statement, answers with what it has by then, and bears the cost. To
   * hold them apart first would be to hold the batch in Spark, whatever its size.
   *
   * The engine goes over them once, as it writes them, and the count of the batch is that of
   * the commit of its table. It has no records that could not be parsed: what such a source
   * gives is rows already.
   *
   * A batch with no rows writes nothing, so the engine asks for a first row before it writes
   * (`probe`). The two are two questions to the source, and it may answer the second
   * otherwise: a batch that had a row and has none when it is written is a commit of no rows,
   * which is a batch written as any other.
   *
   * @param probe the same rows, asked in the way that gives the first of them at the least
   *   cost to the source: the engine takes one row of it (`head(1)`), and no more. It is
   *   `rows`, for a connector that has no cheaper way
   */
  final case class OnePass(rows: DataFrame, probe: DataFrame) extends ReadResult

  /** The column of an unparsed record that has its text, as it was in the source. */
  val Record = "_record"
}

/**
 * What the commit of a batch records of its source, besides the batch itself: how far the
 * source has been ingested once the batch is in its tables. The engine writes it, in the
 * transaction that commits the batch (ADR-0005 §3, step 7); a connector only says what it is.
 *
 * @param watermark where the next batch starts from, in the connector's own form; none, for a
 *   source that has none
 * @param files the files that the batch took, for a source of files: they are recorded as
 *   taken, and no batch after it takes them again
 */
final case class Progress(watermark: Option[Json], files: Option[FilePlan]) {

  /** The bytes that the batch reads, where its plan says them: the sizes of its files. */
  def bytes: Option[Long] = files.map(_.bytes)
}

/**
 * The source no longer has what a plan names, as the plan names it (ADR-0018): a file that was
 * written again, or is gone, since it was planned. A read raises it in place of rows, with
 * nothing of the plan given to be written.
 *
 * It is not a failure with a class (`RunFailedException`). It says one thing of a plan, which
 * a plan made now would not have wrong, and what follows is the engine's to decide: the batch
 * is given up, if nothing of it is in its tables, and the next run plans anew. So a connector
 * raises it for that alone. A plan that it cannot read, or a source that is not there, are
 * failures: no new plan mends them.
 *
 * @param summary what happened, for a person: it names what changed, and never a value of a
 *   row nor where a link leads
 * @param changed each thing of the plan that is not as it was planned
 */
final class SourceChangedException(
    val summary: String,
    val changed: Seq[SourceChangedException.Changed]
) extends RuntimeException(summary) {
  require(changed.nonEmpty, "a source that changed has something that changed")
}

object SourceChangedException {

  /**
   * @param item what the plan names, as the plan names it: a file, by its name
   * @param change how it is not as it was planned
   */
  final case class Changed(item: String, change: String)
}
