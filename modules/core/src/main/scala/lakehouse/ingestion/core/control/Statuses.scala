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
 * What a run is for (ADR-0005 §2, ADR-0021). A regular run ingests what is new, in full or by its
 * watermark or its files. The others are what an operator does to a source, each a run under
 * the lease of the sequence that it acts on, which says who asked for it and why: giving a
 * planned batch up, resetting the state of a source, reading the files of a committed batch
 * again, and reading a range of its watermark on a sequence of its own, a backfill. The names are
 * those of `run_kind` in the control plane.
 *
 * A kind acts on the regular sequence of its source, on that of a backfill of it, or on either:
 * a batch is given up in the sequence that it is planned in, and the state of a source, or its
 * files, are those of its regular sequence.
 *
 * @param operators whether an operator asks for it, and it says who and why
 * @param plans whether it plans a batch, which then has its kind
 * @param ofRegular whether it may act on the regular sequence of its source
 * @param ofBackfills whether it may act on the sequence of a backfill
 */
sealed abstract class RunKind(
    val name: String,
    val operators: Boolean,
    val plans: Boolean,
    val ofRegular: Boolean,
    val ofBackfills: Boolean
) {
  override def toString: String = name
}

object RunKind {

  case object Regular
      extends RunKind(
        "regular",
        operators = false,
        plans = true,
        ofRegular = true,
        ofBackfills = false
      )
  case object Abandon
      extends RunKind(
        "abandon",
        operators = true,
        plans = false,
        ofRegular = true,
        ofBackfills = true
      )
  case object Reset
      extends RunKind(
        "reset",
        operators = true,
        plans = false,
        ofRegular = true,
        ofBackfills = false
      )
  case object Reprocess
      extends RunKind(
        "reprocess",
        operators = true,
        plans = true,
        ofRegular = true,
        ofBackfills = false
      )
  case object Backfill
      extends RunKind(
        "backfill",
        operators = true,
        plans = true,
        ofRegular = false,
        ofBackfills = true
      )

  val All: Seq[RunKind] = Seq(Regular, Abandon, Reset, Reprocess, Backfill)

  /** The kinds that a batch may have: those of the runs that plan one. */
  val OfBatches: Seq[RunKind] = All.filter(_.plans)

  def parse(name: String): Either[String, RunKind] =
    All.find(_.name == name).toRight(s"must be one of ${All.map(_.name).mkString(", ")}")
}

/**
 * Why a run failed, as far as what to do next goes (discovery §14). The names are those of
 * `error_class` in the audit trail. Telling an error of one class from one of another, and the
 * kinds of conflict apart, is the classification of errors (P8).
 */
sealed abstract class FailureClass(val name: String) {
  override def toString: String = name
}

object FailureClass {

  /** It may pass: a connection lost, a timeout, the coordination database not answering. */
  case object Transient extends FailureClass("transient")

  /** Another writer changed the table between the plan and the commit. */
  case object Conflict extends FailureClass("conflict")

  /** Running again does not mend it: credentials, a missing table, a configuration. */
  case object Permanent extends FailureClass("permanent")

  /** More rows were rejected than the source allows. */
  case object Data extends FailureClass("data")

  val All: Seq[FailureClass] = Seq(Transient, Conflict, Permanent, Data)

  def parse(name: String): Either[String, FailureClass] =
    All.find(_.name == name).toRight(s"must be one of ${All.map(_.name).mkString(", ")}")
}

/**
 * How a batch stands (ADR-0005 §3). The names are those of `control.ingestion_batch.status`.
 *
 *  - `PLANNED`: its plan is written down, before anything is read. It is the write-ahead step:
 *    whatever happens next, the plan is there to be run again exactly as it was;
 *  - `COMMITTED`: its data is in the table and the state of its source has moved past it, in
 *    one transaction;
 *  - `ABANDONED`: it was given up with nothing of it in its tables, and says by whom, when and
 *    why (`BatchAbandonment`). By the run that held the lease of its source, when the source no
 *    longer had what the plan names: the next run plans anew (ADR-0018). Or by an operator, to
 *    reset a source or to reprocess it (P7). Its plan stays as it was written, and its number
 *    is not used again.
 *
 * At most one batch of a `TxnAppId` is `PLANNED` at a time: the next is planned when it is
 * committed, or given up.
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
