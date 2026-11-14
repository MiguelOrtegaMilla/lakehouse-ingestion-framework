package lakehouse.ingestion.logstore.coordination

/**
 * The key a Delta table is coordinated under: its canonical URI (ADR-0004 §3.6).
 *
 * Two writers exclude each other only if they name the table with the same key, so a key is
 * never built from the path a caller happened to pass.
 */
final case class TableUri(value: String) {
  require(value.nonEmpty, "a table URI cannot be empty")

  override def toString: String = value
}
