package lakehouse.ingestion.logstore.coordination

import java.io.IOException

/**
 * What an operator does to the registry (ADR-0004 §3.5): the changes that `CoordinationStore`
 * leaves out on purpose, made with privileges the commit path does not have (ADR-0013 §3).
 *
 * Each operation happens whole or not at all, and fails closed like the store: what cannot be
 * done or looked up is an `IOException`. None of them looks at the table's log. Deciding that
 * an operation is right for a table is `TableMaintenance`'s part.
 *
 * '''A change is made on what was examined, or not at all.''' The operator decides by looking at
 * a table's entries and at its log, and a writer that was not stopped may take or finish a
 * version in between. Adopting a table again and forgetting one therefore name the entries the
 * operator examined. The change holds the table's registration against writers while it looks
 * at the entries again, and is called off if they are not those. Applied, it leaves the table
 * with another generation of registration or with none, so a writer that had looked at the
 * registry before gets no version afterwards (`CoordinationStore.take`).
 *
 * '''A change is recorded with it.''' Each operation that changes the registry writes who asked
 * for it, why, and what it found and changed to the maintenance log, as part of the change: a
 * change that is made is in the log, and one that is called off or refused is not. Collecting the
 * temporary files of a table changes nothing of the registry, and is recorded once it is done
 * (`collected`).
 */
trait RegistryMaintenance {

  @throws[IOException]
  def registration(table: TableUri): Option[Registration]

  @throws[IOException]
  def entry(table: TableUri, version: Long): Option[CommitEntry]

  /** The entries of `table` that are not complete, lowest version first. */
  @throws[IOException]
  def unfinished(table: TableUri): Seq[CommitEntry]

  /**
   * The entries of `table` that a log whose latest commit is `version` contradicts: the ones
   * above `version` and the ones that are not complete, lowest version first.
   */
  @throws[IOException]
  def contradicted(table: TableUri, version: Long): Seq[CommitEntry]

  /**
   * Registers a table that exists and was never coordinated, at the version its log is at, and
   * records it.
   *
   * @return whether it was registered; `false` if it already was, and nothing changed
   */
  @throws[IOException]
  def adopt(
      table: TableUri,
      version: Long,
      deltaTableId: Option[String],
      request: MaintenanceRequest
  ): Boolean

  /**
   * Makes the table's log, as it is at `version`, the table: the registration moves to
   * `version` and gets a new generation, the taint is cleared, and the entries that contradict
   * the log are removed (`contradicted`). It is recorded, with the version it moved from, the
   * taint and the entries it removed.
   *
   * @param deltaTableId the table's id, if it is to be recorded anew
   * @param examined the contradicted entries the caller looked at; the change is made only if
   *   they are still the contradicted entries
   * @return what was cleared and removed
   */
  @throws[IOException]
  def readopt(
      table: TableUri,
      version: Long,
      deltaTableId: Option[String],
      examined: Seq[CommitEntry],
      request: MaintenanceRequest
  ): RegistryChange[Readoption]

  /**
   * Removes everything the registry holds about `table`: its taint, its entries, its
   * registration. It is recorded, with what the registration was, its taint, how many entries
   * it had and those of them that were not complete.
   *
   * @param examined the unfinished entries the caller looked at; the change is made only if
   *   they are still the unfinished entries
   */
  @throws[IOException]
  def forget(
      table: TableUri,
      examined: Seq[CommitEntry],
      request: MaintenanceRequest
  ): RegistryChange[Unit]

  /**
   * Whether the maintenance log may be written as the role that this registry is reached as:
   * what an operation that changes nothing of the registry, and is recorded after it is made,
   * asks before it does anything (`TableMaintenance.collectTemps`). It changes nothing.
   */
  @throws[IOException]
  def mayRecord(): Boolean

  /**
   * Records that the temporary files of `table` were collected (`TableMaintenance.collectTemps`),
   * once they are: nothing of the registry changes, so the record is a change of its own.
   *
   * @param temps each file by its name, with what became of it, as the log names it
   */
  @throws[IOException]
  def collected(table: TableUri, request: MaintenanceRequest, temps: Seq[(String, String)]): Unit
}

/** What became of a change that an operator asked for after examining a table's entries. */
sealed trait RegistryChange[+A]

object RegistryChange {

  final case class Applied[A](result: A) extends RegistryChange[A]

  /** The table is not registered. Nothing was changed. */
  case object NotRegistered extends RegistryChange[Nothing]

  /**
   * The entries are not the ones the caller examined: a writer took a version, or a commit was
   * finished, in the meantime. Nothing was changed.
   *
   * @param now the entries in their place
   */
  final case class EntriesChanged(now: Seq[CommitEntry]) extends RegistryChange[Nothing]
}

/**
 * What adopting a table again took out of the registry.
 *
 * @param clearedTaint why the table was tainted, if it was
 * @param removedEntries the entries that the log contradicted, lowest version first
 */
final case class Readoption(clearedTaint: Option[String], removedEntries: Seq[CommitEntry])
