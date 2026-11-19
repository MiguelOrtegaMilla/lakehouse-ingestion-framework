package lakehouse.ingestion.logstore.coordination

import java.io.IOException

/**
 * The shared state of the commit protocol (ADR-0004 §3): which tables are coordinated and which
 * writer owns each commit version. It is where the protocol gets the mutual exclusion that the
 * storage does not give.
 *
 * What every implementation guarantees, and `CoordinationStoreContract` tests:
 *
 *  - '''Atomic and linearised.''' Each operation takes effect at one instant, in one order that
 *    all callers observe, whatever thread or process they are in.
 *  - '''Repeatable.''' An operation called again with the same arguments gives the same answer
 *    and changes nothing more. A caller that lost the reply to a call repeats it to learn what
 *    happened.
 *  - '''Append-only.''' Nothing is deleted or rewritten here. An entry goes from incomplete to
 *    complete, a table from clean to tainted, and the epoch of a key to a later one, never
 *    back. Undoing any of them is an operator's decision, taken elsewhere and with other
 *    privileges.
 *  - '''Fail closed.''' An implementation that cannot answer throws an `IOException`. It never
 *    reports "not found" for something it could not look up.
 */
trait CoordinationStore {

  /** The registration of `table`, or `None` if the table is not coordinated. */
  @throws[IOException]
  def registration(table: TableUri): Option[Registration]

  /**
   * Registers `table` unless it already is. The first registration stays: a later call does not
   * change its version or its id.
   *
   * @param registeredVersion `Registration.NotYetCreated`, or the version the table is adopted at
   */
  @throws[IOException]
  def register(
      table: TableUri,
      registeredVersion: Long,
      deltaTableId: Option[String]
  ): RegisterResult

  @throws[IOException]
  def entry(table: TableUri, version: Long): Option[CommitEntry]

  /** The entry with the highest version, compared as a number. */
  @throws[IOException]
  def latestEntry(table: TableUri): Option[CommitEntry]

  /**
   * Gives the claimed version to the claim if nobody has it, if the table's registration is
   * still of the generation the claim names, and, for a claim that names an epoch, if that
   * epoch is the one in force under its key: the linearisation point of a commit. Every
   * condition is decided in the one step that takes the version, also against an operator's
   * change of the registration and against an announcement of an epoch that are in flight. A
   * claim that already owns the version, from an earlier call whose reply was lost, is told so
   * again.
   */
  @throws[IOException]
  def take(claim: Claim): TakeResult

  /**
   * Announces that `epoch` is in force under its key (ADR-0019), unless a later one is. An
   * epoch only grows: announcing one that is not beyond the one in force changes nothing.
   *
   * It and `take` exclude each other, for a claim that names an epoch of the same key. Once
   * this has returned an epoch, every claim under an earlier one has either taken its version,
   * and its entry is there for whoever reads the registry from now on, or is refused, now and
   * for ever. A writer that announces its epoch before it asks a table anything is therefore
   * answered for good, as far as the writers before it go.
   *
   * @return the epoch in force under the key once this took effect: that of `epoch`, or a later
   *   one that was announced before. A writer that is answered a later one has lost its turn
   */
  @throws[IOException]
  def announce(epoch: WriterEpoch): Long

  /**
   * Marks the entry of `version` complete, provided it is the one that points to `tempPath`.
   *
   * @return whether that entry exists; if it does, it is complete from now on
   */
  @throws[IOException]
  def complete(table: TableUri, version: Long, tempPath: String): Boolean

  /**
   * Taints `table`, which must be registered. The first reason stays: tainting a tainted table
   * changes nothing.
   */
  @throws[IOException]
  def taint(table: TableUri, reason: String): Unit
}

sealed trait RegisterResult

object RegisterResult {

  /** @param registration the registration this call made, with the generation it was given */
  final case class Registered(registration: Registration) extends RegisterResult

  /** @param existing the registration that stays, with its own version and id */
  final case class AlreadyRegistered(existing: Registration) extends RegisterResult
}

sealed trait TakeResult

object TakeResult {

  /** The version belongs to the claim. */
  case object Taken extends TakeResult

  /** The version belongs to another claim. */
  final case class TakenBy(owner: CommitEntry) extends TakeResult

  /** The table is not registered, so no version of it can be taken (ADR-0004 §3.5, layer 2). */
  case object NotRegistered extends TakeResult

  /**
   * The table is registered, and not under the generation of the claim: since the writer read
   * the registration, an operator adopted the table again, or reset its registry and the table
   * was registered anew. What the writer checked no longer holds, so it gets no version.
   */
  case object RegistrationChanged extends TakeResult

  /**
   * The claim names an epoch that is not the one in force under its key (ADR-0019): a later one
   * was announced, and the writer has lost its turn, or none was, and the writer never
   * announced its own. It gets no version, and will get none: an epoch only grows.
   *
   * @param inForce the epoch in force under the key of the claim; none, if none was announced
   */
  final case class EpochNotInForce(inForce: Option[Long]) extends TakeResult
}
