package lakehouse.ingestion.logstore.coordination

/**
 * A table whose commits are coordinated (ADR-0004 §3.5, layer 2).
 *
 * @param registeredVersion the last version that was not coordinated: `NotYetCreated` for a
 *   table created through the LogStore, N for one adopted at version N. Every commit above it
 *   has an entry.
 * @param deltaTableId the table's `metaData.id`, when it was recorded
 * @param taintReason why writes to the table are refused, if a commit made outside the LogStore
 *   was detected on it
 * @param generation which registration of the table this is: a number that no registration of
 *   any table had before. A table gets one when it is registered, and a new one whenever an
 *   operator adopts it again. A writer takes a version only under the generation it read, so
 *   one that looked at the registry before an operator's change cannot act on it afterwards
 *   (ADR-0004 §3.5).
 */
final case class Registration(
    table: TableUri,
    registeredVersion: Long,
    deltaTableId: Option[String],
    taintReason: Option[String],
    generation: Long
) {
  def isTainted: Boolean = taintReason.isDefined
}

object Registration {

  /** The `registeredVersion` of a table that had no commit when it was registered. */
  val NotYetCreated: Long = -1L
}
