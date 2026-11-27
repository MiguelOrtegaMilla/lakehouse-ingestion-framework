package lakehouse.ingestion.engine.connector

import java.time.Instant

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

import io.circe.Json
import lakehouse.ingestion.core.config.{ConnectorConfig, ConnectorKind, RelativePath, Source}
import lakehouse.ingestion.core.files.{FilePlan, LandedFile}
import lakehouse.ingestion.core.watermark.BackfillRange
import lakehouse.ingestion.engine.failure.RunFailedException
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
 *  - '''Whether a plan is its source whole, as it is when the plan is read, is a function of
 *    the plan alone''' (`wholeWhenRead`): a snapshot of a table is, and a window or a list of
 *    files is what it was planned as, however late it is read. A batch that a run before left,
 *    and that this run has read, is then all that a plan made anew would read, and the chain
 *    ends with it.
 *  - '''A connector never writes to the lake, nor to the control plane.''' The state of a
 *    source, the files it took and the record of a run are the engine's, which moves them in
 *    one transaction with the batch (ADR-0005). What a connector needs of them is given to it,
 *    to read (`PlanContext`).
 *  - '''A failure whose class is known is said with it''' (`RunFailedException`): a directory
 *    or a table that is not there is permanent. One that is not known is let through, to where
 *    the run ends (`Failures`, ADR-0022 §2). Rows that are read as they are written fail in the
 *    hands of the engine, and the engine asks their connector what the failure is (`failure`).
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
   * @throws lakehouse.ingestion.engine.failure.RunFailedException a permanent failure, if the plan is
   *   not one that this connector reads
   * @throws SourceChangedException if the source no longer has what the plan names, as the plan
   *   names it. Nothing of the plan was given to be written
   * @throws UnreadableFilesException if files of the plan cannot be read, and the source skips
   *   such files. Nothing of the plan was given to be written
   */
  def read(source: Source, config: C, plan: Json, context: ReadContext): ReadResult

  /**
   * What the commit of a plan records of its source. It reads the plan and nothing else: no
   * file, no table, no clock.
   *
   * @throws lakehouse.ingestion.engine.failure.RunFailedException a permanent failure, if the plan is
   *   not one that this connector reads
   */
  def progress(source: Source, config: C, plan: Json): Progress

  /**
   * Whether the invocation that planned this plan has more to read after it, as the horizon
   * that the next run of the chain is to plan against; none if the chain ends here. It reads
   * the plan and nothing else, as `progress` does.
   *
   * @throws lakehouse.ingestion.engine.failure.RunFailedException a permanent failure, if the plan is
   *   not one that this connector reads
   */
  def more(source: Source, config: C, plan: Json): Option[Json] = None

  /**
   * The plan of a batch that reads again what `plan` read, as the source says to read it now: a
   * reprocess (ADR-0021 §4), which reads the files of a batch that was committed again, and is
   * asked for when what they were read as is to be mended. It reads the plan and nothing else,
   * as `progress` does.
   *
   * @return the plan itself, unless the connector says otherwise. A connector whose plan says how
   *   its source is read (ADR-0024 §3) gives a plan of the same input, read as its configuration
   *   says now
   * @throws lakehouse.ingestion.engine.failure.RunFailedException a permanent failure, if the plan is
   *   not one that this connector reads
   */
  def again(source: Source, config: C, plan: Json): Json = plan

  /**
   * Whether a read of this plan gives its source whole, as the source is when it is read, and
   * not as it was when the plan was made: a snapshot of a table, whose plan names the table
   * and nothing of what is in it. It reads the plan and nothing else, as `progress` does.
   *
   * The engine asks it of a batch that a run before left planned, once this run has read it.
   * Such a batch is followed by a plan made anew, since what another invocation planned stops
   * short of what there is by now: a window ends where it was planned to, and a list of files
   * has the files of then. A plan that is its source whole stops short of nothing: read now,
   * it is what a plan made anew would read, and reading it again is the source read twice.
   *
   * @return false unless the connector says otherwise: a plan made anew is never wrong
   * @throws lakehouse.ingestion.engine.failure.RunFailedException a permanent failure, if the plan is
   *   not one that this connector reads
   */
  def wholeWhenRead(source: Source, config: C, plan: Json): Boolean = false

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
   * How this connector plans the chunks of a backfill of a source (ADR-0021 §5): none, for a
   * source that it does not read a range of. It is a capability that a connector does not have
   * unless it says so, and a source is not backfilled without it.
   *
   * What it plans is a batch as any other: its plan is written down, read by `read`, and
   * recorded by `progress` and `more`. A connector that has it says, of the plan of a chunk,
   * that there is more while the chunk ends before the end of its range, and that there is no
   * more once it ends there: that is what completes a backfill.
   */
  def ranges(source: Source, config: C): Option[RangePlanner] = None

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

    override def again(plan: Json): Json = SourceConnector.this.again(source, config, plan)

    override def wholeWhenRead(plan: Json): Boolean =
      SourceConnector.this.wholeWhenRead(source, config, plan)

    override def failure(error: Throwable): Option[RunFailedException] =
      SourceConnector.this.failure(source, config, error)

    override def ranges: Option[RangePlanner] = SourceConnector.this.ranges(source, config)
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

  /** @see [[SourceConnector.again]] */
  def again(plan: Json): Json

  /** @see [[SourceConnector.wholeWhenRead]] */
  def wholeWhenRead(plan: Json): Boolean

  /** @see [[SourceConnector.failure]] */
  def failure(error: Throwable): Option[RunFailedException]

  /** @see [[SourceConnector.ranges]] */
  def ranges: Option[RangePlanner]
}

/**
 * How a connector plans the chunks of a backfill of one source (ADR-0021 §5): what time it is
 * where the rows are stamped, and the next chunk of a range. It plans, and reads nothing: a
 * chunk is read by `SourceConnector.read`, as any batch.
 */
trait RangePlanner {

  /**
   * Where the range of a backfill that is created now ends, at the latest: the clock of the
   * source less what its rows may still be stamped behind it (`safety_margin`, ADR-0020). The
   * source is asked.
   *
   * @throws lakehouse.ingestion.engine.failure.RunFailedException with its class, if the source cannot
   *   be asked, or says a time that no range ends at
   */
  def horizon(): Instant

  /**
   * The plan of the next chunk of a backfill, or none once its progress is at the end of its
   * range. As `SourceConnector.plan`, it is a function of what it is given and of what the
   * source has, and asks the source what a plan of it needs to: the table and its columns.
   *
   * @param chunk how long a chunk is, at most
   * @param progress where the last chunk that was committed left the backfill, as this
   *   connector wrote it (`Progress.watermark`); none before the first
   * @throws lakehouse.ingestion.engine.failure.RunFailedException with its class, if the source cannot
   *   be asked, has no table that a chunk can be read from, or the progress is not one that
   *   this connector wrote
   */
  def chunk(range: BackfillRange, chunk: FiniteDuration, progress: Option[Json]): Option[Json]
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

  /**
   * The files of `directory` with these names that batches of the source have taken, or could
   * not read and skipped (ADR-0023): a plan leaves out both.
   */
  def processed(directory: RelativePath, names: Seq[String]): Set[LandedFile]
}

/**
 * What the engine gives a connector to read with.
 *
 * @param verify what a read asks before each job of Spark that it starts beyond those of the
 *   read of its plan, as the read of each file of a plan alone (ADR-0023 §3): it throws if the
 *   run may no longer start one, having lost its lease (ADR-0006). The jobs that are running
 *   when a lease is lost are stopped; one that is started after it is not, unless this is asked.
 *   The engine gives the lease of the run; a read outside a run, nothing to ask
 * @param pause what a read asks to wait before it reads again what failed, as a file read alone
 *   a second and a third time (ADR-0023 §3): the run waits that long, or what is left of its
 *   budget of retries if that is less (ADR-0022 §5), and looks at its lease while it does. A read
 *   outside a run does not wait
 */
final case class ReadContext(
    spark: SparkSession,
    verify: () => Unit = () => (),
    pause: FiniteDuration => Unit = _ => ()
)

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
   * @param unparsed the records that could not be parsed, or that were set apart for what they
   *   are, as they were written in the source: `_source_file` and `_record`, both text, and, for
   *   a connector that knows more of why, `_error_type`, one of `Quarantine.ErrorType`, and
   *   `_error_detail`, both text. Without them a record is one that could not be parsed. None for
   *   a source whose records are never text to parse, as a file of Parquet
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

  /** The column of a record set apart that says why, as `Quarantine.ErrorType` names it. */
  val ErrorType = "_error_type"

  /** The column of a record set apart that says what more is known of why. */
  val ErrorDetail = "_error_detail"
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

/**
 * Files of a plan cannot be read, and their source skips such files (ADR-0023): a file that
 * Spark fails to read alone, at every one of its reads, one that the landing zone refuses to
 * open, or one whose bytes are not of the encoding of its source (ADR-0024 §4). A read raises
 * it in place of rows, with nothing of the plan given to be written.
 *
 * It is not a failure with a class, as `SourceChangedException` is not: what follows is the
 * engine's. The batch is given up, if nothing of it is in its tables, each file is recorded as
 * one that could not be read, with why, and the next plan is made without them. A connector
 * raises it for a source that says to skip such files alone; under `fail`, the default, the
 * same files are a permanent failure that names them.
 *
 * @param summary what happened, for a person: it names each file, and never a value of a row,
 *   the text of an exception, nor where a link leads
 * @param unreadable each file of the plan that cannot be read, by the name that the plan has it
 *   under, with why
 */
final class UnreadableFilesException(
    val summary: String,
    val unreadable: Seq[UnreadableFilesException.Unreadable]
) extends RuntimeException(summary) {
  require(unreadable.nonEmpty, "files that cannot be read are some files")
}

object UnreadableFilesException {

  /**
   * @param item the file, by the name that the plan has it under
   * @param why what Spark said of it, by the condition of its error and the type of its cause,
   *   what the landing zone found, or where its first byte that is not of its encoding is, with
   *   what to do about it; never the text of an exception nor a byte, which may hold what the
   *   file holds
   */
  final case class Unreadable(item: String, why: String)
}
