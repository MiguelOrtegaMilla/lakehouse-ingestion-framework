package lakehouse.ingestion.it.stress

/**
 * What the verifier knows about a stress table, as plain data with no Spark in it, so that every
 * check can be unit-tested on hand-built inputs (`VerifierSpec`).
 *
 * File paths are relative to the table root and decoded, whichever side they come from.
 */
object LogModel {

  final case class Txn(appId: String, version: Long)

  /**
   * One `_delta_log/<version>.json` that parsed.
   *
   * @param epoch the epoch that its writer said it committed under, in the commit itself
   *   (`Turn.metadata`, in the `userMetadata` of the commit); none, for a commit that says none
   */
  final case class Commit(
      version: Long,
      txns: Seq[Txn],
      added: Seq[String],
      removed: Seq[String],
      operation: Option[String],
      epoch: Option[Long] = None
  )

  /** A commit file that could not be read as a commit: a malformed line, or no actions at all. */
  final case class UnreadableCommit(version: Long, detail: String)

  /**
   * The Delta log as the raw commit files say, read without Delta's own log replay.
   *
   * @param versions every version that has a commit file, readable or not
   * @param checksumMismatches versions whose commit file Hadoop's `LocalFileSystem` rejects: its
   *   `.crc` sidecar does not match its bytes. An observation, not an anomaly. On `file://` a
   *   commit overwritten by another writer usually leaves one, because `ChecksumFs` renames the
   *   commit file and its sidecar in two steps, and the sidecar's rename does not overwrite.
   *   It is not certain: when both sidecar renames also race, the last writer's sidecar wins and
   *   matches the commit file it wrote.
   * @param commitSidecars versions whose commit file has a `.crc` sidecar at all, whether it
   *   matches or not. Delta's default LogStore leaves one next to every commit. A commit
   *   published by `PostgresLogStore` has none (ADR-0015), so one there was written by somebody
   *   else, or by a publish that went another way than it should.
   * @param staleSidecars the other files of the log, by their path under `_delta_log`, that have
   *   a sidecar which does not match them: Delta's version checksums, checkpoints, the pointer
   *   to the last checkpoint. Delta writes some of these through Hadoop's checksummed file
   *   system, so a sidecar there is ordinary; one that is stale makes the file unreadable to
   *   every reader that checks it.
   */
  final case class LogView(
      versions: Seq[Long],
      commits: Seq[Commit],
      unreadable: Seq[UnreadableCommit],
      checksumMismatches: Seq[Long] = Nil,
      commitSidecars: Seq[Long] = Nil,
      staleSidecars: Seq[String] = Nil
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

  /**
   * A way of reading the table through Delta. The two differ only in Hadoop's local file system,
   * which decides whether `.crc` sidecars are checked.
   */
  sealed abstract class Reader(val label: String, val description: String)

  object Reader {

    /** What any Spark job reading the table on `file://` gets by default. */
    case object Default
        extends Reader(
          "default reader",
          "Delta over Hadoop's `LocalFileSystem`, which checks the `.crc` sidecar of every file " +
            "it reads"
        )

    /** What a reader on object storage gets: S3 and MinIO have no sidecars to check. */
    case object SidecarFree
        extends Reader(
          "sidecar-free reader",
          "Delta over `RawLocalFileSystem`, which ignores `.crc` sidecars, as every reader on " +
            "object storage does: there are none"
        )

    val all: Seq[Reader] = Seq(Default, SidecarFree)
  }

  /** The table as a Delta reader sees it: its latest snapshot. */
  final case class TableView(
      activeFiles: Set[String],
      rows: Map[(String, Long), RowCount]
  )

  /** Data files present on disk under the table root, outside `_delta_log`. */
  final case class DiskView(dataFiles: Set[String])

  /**
   * One row of the registry of commits: who has `version`, and under which run.
   *
   * @param tookMillis from the moment the version was taken to the moment its entry was marked
   *   complete, on the database's clock; `None` while it is not complete
   */
  final case class RegistryEntry(
      version: Long,
      complete: Boolean,
      runId: Option[String],
      tookMillis: Option[Long] = None,
      epochKey: Option[String] = None,
      epoch: Option[Long] = None
  )

  /**
   * The turn of one process at its writer, in a run whose writers commit under epochs
   * (ADR-0019), as the process itself wrote it down before its first commit.
   *
   * @param writer the writer, by its id
   * @param epoch the epoch that the process announced and commits under
   * @param run the run that it attributes its commits to
   * @param told the batch that the table said the writer was at, asked once the announcement
   *   of the epoch had returned: zero, if it had none. The process commits from the batch after
   * @param announceMillis how long the announcement took. One that met a commit of the epoch
   *   before it in flight waited for it
   */
  final case class Turn(
      writer: String,
      epoch: Long,
      run: String,
      told: Long,
      announceMillis: Long
  ) {

    /** As a line of the file of the turns of a writer. */
    def line: String = s"$epoch $run $told $announceMillis"
  }

  object Turn {

    /** A line of the file of the turns of `writer`; none, if it is not one. */
    def parse(writer: String, line: String): Option[Turn] =
      line.trim.split(" ").toList match {
        case epoch :: run :: told :: millis :: Nil =>
          for {
            epoch <- epoch.toLongOption
            told <- told.toLongOption
            millis <- millis.toLongOption
          } yield Turn(writer, epoch, run, told, millis)
        case _ => None
      }

    /** What a commit says of the epoch of its writer, as the `userMetadata` of the commit. */
    def metadata(epoch: Long): String = s"epoch=$epoch"

    private val Metadata = """epoch=(\d+)""".r

    /** The epoch that a commit says, from its `userMetadata`. */
    def epochIn(userMetadata: String): Option[Long] = userMetadata match {
      case Metadata(epoch) => epoch.toLongOption
      case _ => None
    }
  }

  /**
   * The epochs of a run: what its processes said of their turns, and what the coordination
   * database has in force at the end, read with plain SQL.
   *
   * @param keys the key that the epochs of each writer are of, by the id of the writer
   * @param turns every turn of every writer
   * @param inForce the epoch in force under each of those keys when the run was over
   */
  final case class EpochsView(
      keys: Map[String, String],
      turns: Seq[Turn],
      inForce: Map[String, Long]
  )

  /**
   * What the coordination database holds about a table, read with plain SQL and not through
   * the LogStore's own store: the verifier does not ask the component under test what it did.
   *
   * @param registeredVersion the version the table is registered at (-1 if it was created
   *   through the LogStore), or `None` if the table is not registered at all
   * @param taint why the table refuses writes, if it does
   * @param entries every entry of the table, lowest version first
   */
  final case class RegistryView(
      registeredVersion: Option[Long],
      taint: Option[String],
      entries: Seq[RegistryEntry]
  )
}
