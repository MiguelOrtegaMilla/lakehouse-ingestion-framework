package lakehouse.ingestion.logstore.coordination

import java.io.IOException

/**
 * What an operator does to the registry (ADR-0004 §3.5): the changes that `CoordinationStore`
 * leaves out on purpose, made with privileges the commit path does not have (ADR-0013 §3).
 *
 * Each operation happens whole or not at all, and fails closed like the store: what cannot be
 * done or looked up is an `IOException`. None of them looks at the table's log. Deciding that
 * an operation is right for a table is `TableMaintenance`'s part.
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
   * Registers a table that exists and was never coordinated, at the version its log is at.
   *
   * @return whether it was registered; `false` if it already was, and nothing changed
   */
  @throws[IOException]
  def adopt(table: TableUri, version: Long, deltaTableId: Option[String]): Boolean

  /**
   * Makes the table's log, as it is at `version`, the table: the registration moves to
   * `version`, the taint is cleared, and the entries that contradict the log are removed, which
   * are the ones above `version` and the ones that are not complete.
   *
   * @param deltaTableId the table's id, if it is to be recorded anew
   * @return what was cleared and removed, or `None` if the table is not registered
   */
  @throws[IOException]
  def readopt(table: TableUri, version: Long, deltaTableId: Option[String]): Option[Readoption]

  /**
   * Removes everything the registry holds about `table`: its taint, its entries, its
   * registration.
   *
   * @return whether the table was registered
   */
  @throws[IOException]
  def forget(table: TableUri): Boolean
}

/**
 * What adopting a table again took out of the registry.
 *
 * @param clearedTaint why the table was tainted, if it was
 * @param removedEntries the entries that the log contradicted, lowest version first
 */
final case class Readoption(clearedTaint: Option[String], removedEntries: Seq[CommitEntry])
