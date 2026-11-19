package lakehouse.ingestion.engine.connector

import java.time.Instant

import scala.reflect.ClassTag

import io.circe.Json
import lakehouse.ingestion.core.config.{ConnectorConfig, ConnectorKind, RelativePath, Source}
import lakehouse.ingestion.core.files.LandedFile
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
 *  - '''A connector never writes to the lake, nor to the control plane.''' The state of a
 *    source, the files it took and the record of a run are the engine's, which moves them in
 *    one transaction with the batch (ADR-0005). What a connector needs of them is given to it,
 *    to read (`PlanContext`).
 *  - '''A failure whose class is known is said with it''' (`RunFailedException`): a directory
 *    or a table that is not there is permanent. One that is not known is let through, for the
 *    classification of errors (P8).
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
   */
  def read(source: Source, config: C, plan: Json, context: ReadContext): ReadResult

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
  }
}

/** A source with the connector of its kind: what the engine plans and reads it by. */
abstract class BoundSource private[connector] (val source: Source) {

  /** @see [[SourceConnector.plan]] */
  def plan(context: PlanContext): Option[Json]

  /** @see [[SourceConnector.read]] */
  def read(plan: Json, context: ReadContext): ReadResult
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
 */
final case class PlanContext(now: Instant, watermark: Option[Json], files: FileRegistry)

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
 * The rows of a read, as the source gave them, with the columns that are a read's to add
 * (`_source_file`, `_rescued_data`: `EngineColumns`). The columns of the engine are the
 * writer's to add, and a read never has them.
 */
final case class ReadResult(rows: DataFrame)
