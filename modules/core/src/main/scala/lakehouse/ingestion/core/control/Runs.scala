package lakehouse.ingestion.core.control

import java.time.Instant

import lakehouse.ingestion.core.config.{SourceId, Unwritten}

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
 * Who asked an operator's action of a source, and why (ADR-0021 §1).
 *
 * Who is the user of the operating system that ran the process, as the process reports it: a
 * datum of the audit trail, which says where to ask, and not an identity that anything verified.
 * Anyone who can run the command with the secret of the engine's role can say they are anyone.
 *
 * @param requestedBy who, as the process says it
 * @param reason why, for a person to read: required, and one line
 */
final case class OperatorRequest private (requestedBy: String, reason: String)

object OperatorRequest {

  val MaxReason = 1000
  val MaxRequestedBy = 200

  /**
   * A request from what the process says of its user and what the command was told.
   *
   * The user is kept as a name: what a name of a user does not have, a character that a
   * terminal would obey among them, is left out, and a user that is nothing that can be kept is
   * `unknown`. The reason is refused rather than changed: it is what a person wrote, and is to
   * be read as they wrote it.
   */
  def of(user: String, reason: String): Either[String, OperatorRequest] = {
    val name = user.filter(c => c.isLetterOrDigit || ".-_@\\".contains(c)).take(MaxRequestedBy)
    val text = reason.trim
    if (text.isEmpty) Left("--reason says nothing: an operator's action says why it was asked for")
    else if (text.codePointCount(0, text.length) > MaxReason)
      Left(s"--reason is longer than $MaxReason characters")
    else
      Unwritten.firstOutside(text, printable) match {
        case Some((codePoint, at)) =>
          Left(
            s"--reason has ${Unwritten.describe(codePoint)} at position $at: a reason is one " +
              "line of text, with no character that a terminal obeys"
          )
        case None => Right(new OperatorRequest(if (name.isEmpty) "unknown" else name, text))
      }
  }

  /** A request as it is read back from the database. */
  def stored(requestedBy: String, reason: String): OperatorRequest =
    new OperatorRequest(requestedBy, reason)

  private def printable(codePoint: Int): Boolean =
    !(Character.getType(codePoint) match {
      case Character.CONTROL | Character.FORMAT | Character.LINE_SEPARATOR |
          Character.PARAGRAPH_SEPARATOR | Character.SURROGATE | Character.UNASSIGNED =>
        true
      case _ => false
    })
}

/**
 * What is known of a run when it starts, and never changes: who runs, what, with which
 * configuration (discovery §13.2). The control plane lets none of it be written twice.
 *
 * @param appVersion the version of the application that runs it
 * @param configHash the hash of the configuration it runs with, whose snapshot is stored
 * @param request who asked for it and why: for an operator's action, and for no other run
 * @param backfill the backfill whose sequence it acts on; none, for the regular sequence of its
 *   source. Which a run may act on is its kind's to say (`RunKind.ofRegular`, `ofBackfills`)
 */
final case class RunStart(
    id: RunId,
    source: SourceId,
    layer: Layer,
    kind: RunKind,
    instance: InstanceId,
    appVersion: String,
    configHash: String,
    request: Option[OperatorRequest] = None,
    backfill: Option[BackfillId] = None
) {
  require(
    kind.operators == request.nonEmpty,
    s"a run of the kind $kind says who asked for it and why exactly when an operator asks for it"
  )
  require(
    if (backfill.isEmpty) kind.ofRegular else kind.ofBackfills,
    s"a run of the kind $kind does not act on " +
      backfill.fold("the regular sequence of its source")(id => s"the sequence of the backfill $id")
  )

  /** The sequence that its batch belongs to: the regular one, or that of its backfill. */
  def txnAppId: TxnAppId =
    backfill.fold(TxnAppId.of(source, layer))(TxnAppId.backfill(source, layer, _))

  /** What it must hold the lease of: its sequence, by the same name. */
  def leaseKey: LeaseKey =
    backfill.fold(LeaseKey.of(source, layer))(LeaseKey.backfill(source, layer, _))
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
