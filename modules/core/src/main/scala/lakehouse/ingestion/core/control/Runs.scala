package lakehouse.ingestion.core.control

import java.time.Instant

import lakehouse.ingestion.core.config.SourceId

/**
 * Which process ran a run: `<host>:<pid>:<when the process started, in milliseconds>`.
 *
 * It answers "which instance" in the audit trail, and tells two runs of one host apart when
 * they overlap. The start is there because a process id is used again: in a container every
 * process is number 1.
 */
final class InstanceId private (val value: String) extends AnyVal {
  override def toString: String = value
}

object InstanceId {

  val MaxLength = 200

  def of(host: String, pid: Long, started: Instant): InstanceId = {
    // A host name has no ':' and nothing a terminal would obey; what is not one is not kept.
    val name = host.filter(c => c.isLetterOrDigit || c == '.' || c == '-' || c == '_').take(120)
    new InstanceId(s"${if (name.isEmpty) "unknown-host" else name}:$pid:${started.toEpochMilli}")
  }

  /** An id as it is read back from the database. */
  def parse(text: String): Either[String, InstanceId] =
    if (text.isEmpty) Left("is empty")
    else if (text.length > MaxLength) Left(s"is longer than $MaxLength characters")
    else if (text.exists(c => c < 0x21 || c == 0x7f))
      Left("has a space or a control character in it")
    else Right(new InstanceId(text))
}

/**
 * What is known of a run when it starts, and never changes: who runs, what, with which
 * configuration (discovery §13.2). The control plane lets none of it be written twice.
 *
 * @param appVersion the version of the application that runs it
 * @param configHash the hash of the configuration it runs with, whose snapshot is stored
 */
final case class RunStart(
    id: RunId,
    source: SourceId,
    layer: Layer,
    kind: RunKind,
    instance: InstanceId,
    appVersion: String,
    configHash: String
) {

  /** The sequence that its batch belongs to. That of a backfill is its own (P7). */
  def txnAppId: TxnAppId = TxnAppId.of(source, layer)

  /** What it must hold the lease of. */
  def leaseKey: LeaseKey = LeaseKey.of(source, layer)
}

/**
 * What a run counted. Each is there when something counted it: a count that is missing says
 * that nothing did, which is not zero.
 *
 * They are written with the end of the run, in one statement: what ended does not change.
 *
 * @param retries steps that were run again after a transient failure
 * @param writeConflicts writes that were run again because another writer changed the table
 * @param commitContention versions of a table that another writer took first
 */
final case class RunCounts(
    rowsRead: Option[Long] = None,
    rowsWritten: Option[Long] = None,
    rowsRejected: Option[Long] = None,
    bytesRead: Option[Long] = None,
    retries: Option[Int] = None,
    writeConflicts: Option[Int] = None,
    commitContention: Option[Int] = None
)

object RunCounts {
  val Nothing: RunCounts = RunCounts()
}

/** How a run that was running says it ended. That it was abandoned is said by another. */
sealed trait RunEnd {
  def status: RunStatus
  def counts: RunCounts
}

object RunEnd {

  final case class Succeeded(counts: RunCounts = RunCounts.Nothing) extends RunEnd {
    override def status: RunStatus = RunStatus.Succeeded
  }

  /** @param message what went wrong, for a person. Never a secret */
  final case class Failed(
      failure: FailureClass,
      message: String,
      counts: RunCounts = RunCounts.Nothing
  ) extends RunEnd {
    override def status: RunStatus = RunStatus.Failed
  }
}

/** Why a run failed, as the audit trail has it. */
final case class RunFailure(failure: FailureClass, message: String)

/**
 * A run as the control plane has it.
 *
 * @param epoch the epoch of the lease it took; none, if it was skipped
 * @param batch the batch it planned or recovered, once it has one
 * @param startedAt by the clock of the database, as every time of the control plane
 */
final case class RunRecord(
    start: RunStart,
    txnAppId: TxnAppId,
    batch: Option[BatchSeq],
    status: RunStatus,
    epoch: Option[Epoch],
    startedAt: Instant,
    finishedAt: Option[Instant],
    counts: RunCounts,
    failure: Option[RunFailure]
)
