package lakehouse.ingestion.logstore.coordination

/**
 * Who asked for an operation on a table, and why: what the maintenance log keeps of it, beside
 * what was done and when (`delta_coordination.maintenance_log`).
 *
 * What a person typed is the caller's to check, and to say what is wrong with: this holds only
 * what the log itself refuses, a name or a reason that says nothing or is too long.
 */
final case class MaintenanceRequest(requestedBy: String, reason: String) {
  import MaintenanceRequest._

  require(saysSomething(requestedBy, MaxRequestedBy), s"who asked must be a name: '$requestedBy'")
  require(saysSomething(reason, MaxReason), "the reason must say something, in 1000 characters")
}

object MaintenanceRequest {

  /** As the log holds them, in characters. */
  val MaxRequestedBy = 200
  val MaxReason = 1000

  private def saysSomething(text: String, max: Int): Boolean =
    text.exists(!_.isWhitespace) && text.codePointCount(0, text.length) <= max
}
