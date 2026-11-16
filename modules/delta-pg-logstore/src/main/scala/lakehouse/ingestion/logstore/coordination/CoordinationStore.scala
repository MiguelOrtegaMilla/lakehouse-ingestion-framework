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
 *    complete and a table from clean to tainted, never back. Undoing either is an operator's
 *    decision, taken elsewhere and with other privileges.
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
   * Gives the claimed version to the claim if nobody has it, and if the table's registration is
   * still of the generation the claim names: the linearisation point of a commit. Both
   * conditions are decided in the one step that takes the version, also against an operator's
   * change of the registration that is in flight. A claim that already owns the version, from
   * an earlier call whose reply was lost, is told so again.
   */
  @throws[IOException]
  def take(claim: Claim): TakeResult

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
}
