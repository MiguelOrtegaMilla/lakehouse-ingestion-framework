package lakehouse.ingestion.core.control

import lakehouse.ingestion.core.config.SourceId

/** A layer of the lake that a source is ingested into (ADR-0008). */
sealed abstract class Layer(val name: String) {
  override def toString: String = name
}

object Layer {

  case object Bronze extends Layer("bronze")
  case object Silver extends Layer("silver")

  val All: Seq[Layer] = Seq(Bronze, Silver)

  def parse(text: String): Either[String, Layer] =
    All.find(_.name == text).toRight(s"must be one of ${All.map(_.name).mkString(", ")}")
}

/**
 * What an operator calls a backfill (ADR-0021 §5): a sequence of batches of its own, which reads
 * a range of the watermark of a source beside its regular sequence. The operator gives it, and
 * the command creates the backfill the first time and resumes it each time after: an id that
 * the application made would have an orchestrator that runs the command again create another.
 *
 * It is unique within its source and layer, and is a segment of the name of the sequence, so it
 * has no `/`. It is written in lower case, as an id of a source is, so that two ids that differ
 * in their case alone are not two backfills to a person and one to a reader that ignores case:
 * letters, digits, `.`, `_` and `-`, starting with a letter or a digit, as `history-2024`.
 */
final class BackfillId private (val value: String) extends AnyVal {
  override def toString: String = value
}

object BackfillId {

  val MaxLength = 63

  private val Written = """[a-z0-9][a-z0-9._-]*""".r

  def parse(text: String): Either[String, BackfillId] =
    if (text.isEmpty) Left("is empty")
    else if (text.length > MaxLength)
      Left(s"is ${text.length} characters long, and an id is $MaxLength at most")
    else if (Written.matches(text)) Right(new BackfillId(text))
    else
      Left(
        "must be lower-case letters, digits, '.', '_' and '-', starting with a letter or a " +
          "digit, as history-2024"
      )
}

/**
 * What a lease is taken on: a sequence of batches of one source in one layer (ADR-0006). The
 * key is the name of the sequence, as its `TxnAppId` has it: `<source_id>/<layer>` for the
 * regular one, and `<source_id>/<layer>/backfill/<backfill_id>` for a backfill (ADR-0021 §5).
 * Whoever holds the lease of a key is the one run that may advance the state of its sequence:
 * the state of the source, or the progress of the backfill.
 *
 * It is not a lock on a table. Several sources may write one table at the same time, each
 * under its own key, and so may a backfill beside the regular sequence of its own source;
 * keeping their commits apart is the LogStore's work (ADR-0004).
 */
final class LeaseKey private (val value: String) extends AnyVal {
  override def toString: String = value
}

object LeaseKey {

  def of(source: SourceId, layer: Layer): LeaseKey = new LeaseKey(Keys.regular(source, layer))

  /** The key of the lease of a backfill: the name of its sequence. */
  def backfill(source: SourceId, layer: Layer, id: BackfillId): LeaseKey =
    new LeaseKey(Keys.backfill(source, layer, id))

  /** A key as it is read back from the database. */
  def parse(text: String): Either[String, LeaseKey] =
    Keys.sequence(text).map(_ => new LeaseKey(text))
}

/**
 * The name under which the batches of a sequence are written to Delta (ADR-0005 §2):
 * `<source_id>/<layer>` for the regular sequence of a source in a layer, and
 * `<source_id>/<layer>/backfill/<backfill_id>` for a backfill of it (ADR-0021 §5). Delta keeps,
 * in the log of each table, the last `txnVersion` it committed for each such name, and skips a
 * write that is not beyond it: that is what makes a batch that is written twice a batch that is
 * in the table once.
 *
 * It is by sequence, not by table. Several sources write one table, and each needs a sequence of
 * its own; a backfill has one so that it runs next to the incremental load of the same table
 * without either disturbing the other's numbers. It is the same text as the key of the lease of
 * the sequence, and it is another thing: the key says who may advance the state, and this says
 * which sequence a write belongs to.
 */
final class TxnAppId private (val value: String) extends AnyVal {

  /** The backfill whose sequence this is; none for the regular sequence of its source. */
  def backfill: Option[BackfillId] =
    Keys.sequence(value).fold(reason => throw new IllegalStateException(reason), _._3)

  override def toString: String = value
}

object TxnAppId {

  def of(source: SourceId, layer: Layer): TxnAppId = new TxnAppId(Keys.regular(source, layer))

  /** The name of the sequence of a backfill, which is also the key of its lease. */
  def backfill(source: SourceId, layer: Layer, id: BackfillId): TxnAppId =
    new TxnAppId(Keys.backfill(source, layer, id))

  /** A name as it is read back from the database. */
  def parse(text: String): Either[String, TxnAppId] =
    Keys.sequence(text).map(_ => new TxnAppId(text))
}

/**
 * How many times the lease of a key has been taken, counted by the database (ADR-0006). It is
 * the fencing token: every statement that advances the state of a source says the epoch its run
 * holds, and does nothing if the lease has been taken again since. A run that lost its lease
 * and does not know it, a zombie, cannot advance anything.
 */
final class Epoch private (val value: Long) extends AnyVal {
  override def toString: String = value.toString
}

object Epoch {

  implicit val ordering: Ordering[Epoch] = Ordering.by(_.value)

  /** The epoch of the first run that takes a lease: an epoch is 1 or more. */
  def of(value: Long): Either[String, Epoch] =
    if (value >= 1) Right(new Epoch(value)) else Left(s"is $value, and an epoch is 1 or more")
}

/**
 * The place of a batch in the sequence of its `TxnAppId`: 1, 2, 3… It is given by the control
 * plane when a batch is planned, and it is the `txnVersion` that the batch is written to Delta
 * with. A number belongs to one plan, for ever (ADR-0005 §3).
 */
final class BatchSeq private (val value: Long) extends AnyVal {

  def next: BatchSeq = new BatchSeq(value + 1)

  override def toString: String = value.toString
}

object BatchSeq {

  implicit val ordering: Ordering[BatchSeq] = Ordering.by(_.value)

  val First: BatchSeq = new BatchSeq(1)

  def of(value: Long): Either[String, BatchSeq] =
    if (value >= 1) Right(new BatchSeq(value))
    else Left(s"is $value, and the first batch of a sequence is 1")
}

private object Keys {

  /** The word between the layer and the id, in the name of the sequence of a backfill. */
  private val Backfill = "backfill"

  def regular(source: SourceId, layer: Layer): String = s"$source/$layer"

  def backfill(source: SourceId, layer: Layer, id: BackfillId): String =
    s"${regular(source, layer)}/$Backfill/$id"

  /**
   * `<source_id>/<layer>`, or `<source_id>/<layer>/backfill/<backfill_id>`: no id has a `/`, so
   * there is one way to take the text apart.
   */
  def sequence(text: String): Either[String, (SourceId, Layer, Option[BackfillId])] = {
    def sourceAndLayer(source: String, layer: String) =
      for {
        source <- SourceId.parse(source).left.map(reason => s"has a source id that $reason")
        layer <- Layer.parse(layer).left.map(reason => s"has a layer that $reason")
      } yield (source, layer)

    text.split("/", -1).toList match {
      case source :: layer :: Nil =>
        sourceAndLayer(source, layer).map { case (s, l) => (s, l, None) }
      case source :: layer :: Backfill :: id :: Nil =>
        sourceAndLayer(source, layer).flatMap { case (s, l) =>
          BackfillId
            .parse(id)
            .left
            .map(reason => s"has a backfill id that $reason")
            .map(id => (s, l, Some(id)))
        }
      case _ =>
        Left(
          "must be <source_id>/<layer>, as sales.orders/bronze, or " +
            "<source_id>/<layer>/backfill/<backfill_id>, as sales.orders/bronze/backfill/history-2024"
        )
    }
  }
}
