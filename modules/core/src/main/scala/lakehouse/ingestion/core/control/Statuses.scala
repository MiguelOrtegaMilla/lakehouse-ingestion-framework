package lakehouse.ingestion.core.control

/**
 * How a run stands (ADR-0005, ADR-0006). The names are those of `control.ingestion_run.status`
 * and of every query on the audit trail: they are part of a contract, and a test pins them.
 *
 * A run is born with the answer of its lease, and has no status before it:
 *
 *  - `RUNNING`: it took the lease, and holds an epoch;
 *  - `SKIPPED_LOCKED`: another run holds it. Nothing was read or written, and the run ends
 *    there. It is not a failure (exit code 2).
 *
 * A run that is running ends in one of three ways, and each has one who may say so:
 *
 *  - `SUCCEEDED`, said by the run itself, in the one transaction that advances the state of its
 *    source, marks its batch committed and marks the run, all on condition of its epoch
 *    (ADR-0005 §3, step 7). A run that lost its lease cannot say it;
 *  - `FAILED`, said by the run itself, with the class of its error;
 *  - `ABANDONED`, said by the run that takes the lease over after it expired. The run that held
 *    it died, or stopped renewing, and said nothing: it ended halfway. Its batch, if it had
 *    planned one, is still `PLANNED`, and the run that took over recovers it.
 *
 * What ended does not change. A zombie that reports late finds its run `ABANDONED`, and that
 * is what stays: it is what happened, as far as the system can tell.
 */
sealed abstract class RunStatus(val name: String, val ended: Boolean) {
  override def toString: String = name
}

object RunStatus {

  case object Running extends RunStatus("RUNNING", ended = false)
  case object Succeeded extends RunStatus("SUCCEEDED", ended = true)
  case object Failed extends RunStatus("FAILED", ended = true)
  case object SkippedLocked extends RunStatus("SKIPPED_LOCKED", ended = true)
  case object Abandoned extends RunStatus("ABANDONED", ended = true)

  val All: Seq[RunStatus] = Seq(Running, Succeeded, Failed, SkippedLocked, Abandoned)

  /** The statuses a run may be born with: what its lease answered. */
  val AtBirth: Set[RunStatus] = Set(Running, SkippedLocked)

  def parse(name: String): Either[String, RunStatus] =
    All.find(_.name == name).toRight(s"must be one of ${All.map(_.name).mkString(", ")}")

  /** Whether a run that stands as `from` may come to stand as `to`. */
  def mayBecome(from: RunStatus, to: RunStatus): Boolean =
    from == Running && to.ended && !AtBirth(to)
}

/**
 * How a batch stands (ADR-0005 §3). The names are those of `control.ingestion_batch.status`.
 *
 *  - `PLANNED`: its plan is written down, before anything is read. It is the write-ahead step:
 *    whatever happens next, the plan is there to be run again exactly as it was;
 *  - `COMMITTED`: its data is in the table and the state of its source has moved past it, in
 *    one transaction;
 *  - `ABANDONED`: an operator gave it up, to reset a source or to reprocess it (P7). Its number
 *    is not used again.
 *
 * At most one batch of a `TxnAppId` is `PLANNED` at a time: the next is planned when it is
 * committed.
 */
sealed abstract class BatchStatus(val name: String, val ended: Boolean) {
  override def toString: String = name
}

object BatchStatus {

  case object Planned extends BatchStatus("PLANNED", ended = false)
  case object Committed extends BatchStatus("COMMITTED", ended = true)
  case object Abandoned extends BatchStatus("ABANDONED", ended = true)

  val All: Seq[BatchStatus] = Seq(Planned, Committed, Abandoned)

  def parse(name: String): Either[String, BatchStatus] =
    All.find(_.name == name).toRight(s"must be one of ${All.map(_.name).mkString(", ")}")

  def mayBecome(from: BatchStatus, to: BatchStatus): Boolean = from == Planned && to.ended
}
