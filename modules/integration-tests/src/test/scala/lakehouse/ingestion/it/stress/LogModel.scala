package lakehouse.ingestion.it.stress

/**
 * What the verifier knows about a stress table, as plain data with no Spark in it, so that every
 * check can be unit-tested on hand-built inputs (`VerifierSpec`).
 *
 * File paths are relative to the table root and decoded, whichever side they come from.
 */
object LogModel {

  final case class Txn(appId: String, version: Long)

  /** One `_delta_log/<version>.json` that parsed. */
  final case class Commit(
      version: Long,
      txns: Seq[Txn],
      added: Seq[String],
      removed: Seq[String],
      operation: Option[String]
  )

  /** A commit file that could not be read as a commit: a malformed line, or no actions at all. */
  final case class UnreadableCommit(version: Long, detail: String)

  /**
   * The Delta log as the raw commit files say, read without Delta's own log replay.
   *
   * @param versions every version that has a commit file, readable or not
   */
  final case class LogView(
      versions: Seq[Long],
      commits: Seq[Commit],
      unreadable: Seq[UnreadableCommit]
  ) {

    /** The data files of the latest version, replaying adds and removes in version order. */
    def activeFiles: Set[String] =
      commits.sortBy(_.version).foldLeft(Set.empty[String]) { (files, c) =>
        files -- c.removed ++ c.added
      }

    def everAdded: Set[String] = commits.flatMap(_.added).toSet
  }

  /** Rows the table holds for one `(writer_id, txn_version)`. */
  final case class RowCount(rows: Long, distinctRowSeqs: Long)

  /** The table as a Delta reader sees it: its latest snapshot. */
  final case class TableView(
      activeFiles: Set[String],
      rows: Map[(String, Long), RowCount]
  )

  /** Data files present on disk under the table root, outside `_delta_log`. */
  final case class DiskView(dataFiles: Set[String])
}
