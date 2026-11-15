package lakehouse.ingestion.logstore.coordination

/**
 * The key a Delta table is coordinated under: its canonical URI (ADR-0004 §3.6).
 *
 * Two writers exclude each other only if they name the table with the same key, so a key is
 * never built from the path a caller happened to pass. `TableIdentity.of` is the one way to get
 * a key: it gives the same one for every spelling of a table's path.
 *
 * The constructor is therefore closed to code outside this module. It is a plain class and not
 * a case class, whose `apply` and `copy` would be two more constructors, and public ones.
 */
final class TableUri private[logstore] (val value: String) {
  require(value.nonEmpty, "a table URI cannot be empty")

  override def equals(other: Any): Boolean = other match {
    case that: TableUri => value == that.value
    case _ => false
  }

  override def hashCode: Int = value.hashCode

  override def toString: String = value
}
