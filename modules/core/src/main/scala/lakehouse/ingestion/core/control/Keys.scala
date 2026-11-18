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
 * What a lease is taken on: the state of one source in one layer, `<source_id>/<layer>`
 * (ADR-0006). Whoever holds the lease of a key is the one run that may advance that state.
 *
 * It is not a lock on a table. Several sources may write one table at the same time, each
 * under its own key; keeping their commits apart is the LogStore's work (ADR-0004).
 *
 * A backfill has a key of its own, `backfill/<backfill_id>`, since it does not touch the
 * incremental state. That form comes with backfills (P7).
 */
final class LeaseKey private (val value: String) extends AnyVal {
  override def toString: String = value
}

object LeaseKey {

  def of(source: SourceId, layer: Layer): LeaseKey = new LeaseKey(s"$source/$layer")

  /** A key as it is read back from the database. */
  def parse(text: String): Either[String, LeaseKey] =
    Keys.sourceAndLayer(text).map { case (source, layer) => of(source, layer) }
}

/**
 * The name under which the batches of one source in one layer are written to Delta,
 * `<source_id>/<layer>` (ADR-0005 §2). Delta keeps, in the log of each table, the last
 * `txnVersion` it committed for each such name, and skips a write that is not beyond it: that
 * is what makes a batch that is written twice a batch that is in the table once.
 *
 * It is by source and layer, not by table. Several sources write one table, and each needs a
 * sequence of its own. For a regular run it is the same text as the key of its lease, and it is
 * another thing: the key says who may advance the state, and this says which sequence a write
 * belongs to. A backfill has a name of its own, so that it runs next to the incremental load of
 * the same table without either disturbing the other's sequence; that form comes in P7.
 */
final class TxnAppId private (val value: String) extends AnyVal {
  override def toString: String = value
}

object TxnAppId {

  def of(source: SourceId, layer: Layer): TxnAppId = new TxnAppId(s"$source/$layer")

  /** A name as it is read back from the database. */
  def parse(text: String): Either[String, TxnAppId] =
    Keys.sourceAndLayer(text).map { case (source, layer) => of(source, layer) }
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

  /** `<source_id>/<layer>`: an id has no `/`, so there is one way to take the text apart. */
  def sourceAndLayer(text: String): Either[String, (SourceId, Layer)] =
    text.split("/", -1).toList match {
      case source :: layer :: Nil =>
        for {
          source <- SourceId.parse(source).left.map(reason => s"has a source id that $reason")
          layer <- Layer.parse(layer).left.map(reason => s"has a layer that $reason")
        } yield (source, layer)
      case _ => Left("must be <source_id>/<layer>, as sales.orders/bronze")
    }
}
