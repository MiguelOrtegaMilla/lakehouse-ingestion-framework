package lakehouse.ingestion.logstore.coordination

/**
 * The turn of a writer at some work that one writer at a time does (ADR-0019).
 *
 * An application that gives such work to one writer after another numbers them: whoever takes
 * the work over has an epoch greater than the one before. The LogStore holds a writer to its
 * turn where a commit happens. The epoch in force under a key is announced
 * (`CoordinationStore.announce`), and a commit that names an epoch takes its version only while
 * that epoch is the one in force. A writer that lost its turn and does not know it yet, because
 * it was stalled and not stopped, commits nothing more.
 *
 * What a key is, is the application's. For the engine it is the key of the lease of a source,
 * and the epoch is the one of the lease (ADR-0006). The LogStore reads neither.
 *
 * @param key what the turn is at. One key numbers the writers of one piece of work, whatever
 *   tables they write
 * @param epoch the turn: 1 or more, and greater for each writer that takes the work over
 */
final case class WriterEpoch(key: String, epoch: Long) {
  require(key.trim.nonEmpty, "the epoch of a writer needs the key that it is an epoch of")
  require(epoch >= 1, s"an epoch is 1 or more, not $epoch")

  override def toString: String = s"epoch $epoch of $key"
}
