package lakehouse.ingestion.logstore

import java.io.{FileNotFoundException, IOException}
import java.nio.charset.StandardCharsets.UTF_8
import java.util

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.Using

import lakehouse.ingestion.logstore.coordination.{
  CommitEntry,
  MaintenanceRequest,
  Readoption,
  RegistryChange,
  RegistryMaintenance,
  TableUri
}
import lakehouse.ingestion.logstore.storage.LogStorage
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

/**
 * An operator's request does not fit the table: its log or its registration is not in the state
 * the operation is for. Nothing was changed.
 */
class MaintenanceRefusedException(message: String) extends IOException(message)

/**
 * The table is being written while an operator changes its registry: its entries changed each
 * time they were examined. Nothing was changed, and asked again once its writers are stopped, the
 * operation may be made.
 */
final class TableBeingWrittenException(message: String) extends MaintenanceRefusedException(message)

/** A table that was adopted: its key, and the version of its log it is registered at. */
final case class Adoption(table: TableUri, version: Long)

/**
 * A table that was adopted again.
 *
 * @param version the version of its log it is registered at now
 * @param removed what the registry held that the log contradicted
 */
final case class Readopted(table: TableUri, version: Long, removed: Readoption)

/** What became of a temporary file when the temporary files of a table were collected. */
sealed trait TempFate {
  def removed: Boolean

  /** As the maintenance log names it. */
  def name: String
}

object TempFate {

  /** Removed: its commit is published, so the file had a second name it no longer needs. */
  case object RemovedPublished extends TempFate {
    val removed = true
    val name = "removed_published"
  }

  /**
   * Removed: its commit is recorded as published, is below the table's last checkpoint, and is
   * no longer in the log. Delta's log cleanup removed the commit, and the checkpoint holds
   * everything it said.
   */
  case object RemovedExpired extends TempFate {
    val removed = true
    val name = "removed_expired"
  }

  /** Removed: no entry points at it, and it is older than any commit could take. */
  case object RemovedOrphan extends TempFate {
    val removed = true
    val name = "removed_orphan"
  }

  /** Kept: its commit is taken and not published yet. These are the commit's bytes. */
  case object KeptUnfinished extends TempFate {
    val removed = false
    val name = "kept_unfinished"
  }

  /**
   * Kept: its entry is complete, and the commit file is another file, or is missing where
   * Delta's log cleanup does not explain it. It is the only copy of a commit that was lost, and
   * somebody will want to see it.
   */
  case object KeptOnlyCopy extends TempFate {
    val removed = false
    val name = "kept_only_copy"
  }

  /** Kept: no entry points at it, and a writer may be about to take a version for it. */
  case object KeptRecent extends TempFate {
    val removed = false
    val name = "kept_recent"
  }
}

/**
 * What an operator does to a coordinated table (ADR-0004 §3.5, §3.8): adopt one that was made
 * without the LogStore, adopt one again once its log has been verified, reset the registry of
 * one that was dropped, and collect the temporary files that nothing needs.
 *
 * These are the steps of the runbooks, for a table whose writers are stopped. Each looks at the
 * table's log first and refuses what the log does not bear out, so that a slip of the operator
 * does not discard a commit or un-register a live table. None runs on the commit path.
 *
 * A writer that was not stopped is not harmed, and does no harm. Adopting a table again and
 * resetting one change the registry only if its entries are still the ones this class examined;
 * if a writer took or finished a version in between, the table is examined again. Once the
 * change is made, a writer that had looked at the registry before it gets no version
 * (`RegistrationChangedException`).
 *
 * Each operation is recorded in the maintenance log, with who asked for it and why
 * (`MaintenanceRequest`): in the change that it makes to the registry, so that one that is
 * refused or called off leaves no record; and, for the temporary files, once they are collected.
 *
 * @param registry the registry, reached with an operator's privileges
 */
final class TableMaintenance(
    registry: RegistryMaintenance,
    storageOf: FileSystem => LogStorage = LogStorage.of
) {

  import TableMaintenance._

  /**
   * The id of the table (`metaData.id`) as its first commit says it, read as the LogStore reads
   * it when a table is created through it: what `adopt` records, for a caller that has no Delta
   * to ask. None when the first commit is no longer in the log, as once Delta's log cleanup has
   * removed it, or says no id in the form that Delta writes it in; the registry allows none.
   */
  @throws[IOException]
  def firstCommitId(tablePath: Path, fs: FileSystem): Option[String] = {
    val first = new Target(tablePath, fs).commitFile(0L)
    try
      Using.resource(fs.open(first)) { in =>
        DeltaTableId.in(new String(in.readAllBytes(), UTF_8).linesIterator.toSeq)
      }
    catch { case _: FileNotFoundException => None }
  }

  /**
   * Registers a table that exists and is not coordinated, at the version its log is at, so that
   * it can be written through the LogStore from the next version on.
   *
   * @param deltaTableId the table's `metaData.id`, which the caller reads with Delta
   */
  @throws[IOException]
  def adopt(
      tablePath: Path,
      fs: FileSystem,
      deltaTableId: Option[String],
      request: MaintenanceRequest
  ): Adoption = {
    val target = new Target(tablePath, fs)
    val latest = target.latestCommit.getOrElse {
      refuse(s"there is no table to adopt at ${target.table}: its log holds no commit")
    }
    if (!registry.adopt(target.table, latest, deltaTableId, request))
      refuse(s"${target.table} is registered already. A registered table is adopted again")
    Adoption(target.table, latest)
  }

  /**
   * Makes the log of a registered table, as it stands, the table: for a table that is tainted,
   * or whose registry and log disagree, once an operator has looked at the log and accepts it.
   * The taint is cleared, and the entries the log contradicts are removed and returned.
   *
   * A commit that is taken and can still be published is not the operator's to discard: the
   * request is refused, and a read of the table publishes it.
   */
  @throws[IOException]
  def readopt(
      tablePath: Path,
      fs: FileSystem,
      deltaTableId: Option[String],
      request: MaintenanceRequest
  ): Readopted = {
    val target = new Target(tablePath, fs)
    val table = target.table
    val noTable = s"there is no table to adopt at $table: its log holds no commit. " +
      "If the table was dropped, reset its registry"
    if (target.latestCommit.isEmpty) refuse(noTable)
    if (registry.registration(table).isEmpty)
      refuse(s"$table is not registered. A table that was never coordinated is adopted")

    // The log, then the entries it contradicts, then the change, which is made only if those
    // are still the entries. Readers are not stopped for this and a writer may not be: a commit
    // that one of them publishes or takes while this looks makes it look again.
    @tailrec
    def examine(attemptsLeft: Int): Readopted = {
      val latest = target.latestCommit.getOrElse(refuse(noTable))
      val contradicted = registry.contradicted(table, latest)
      contradicted.filter(entry => !entry.complete && target.canStillBePublished(entry)).foreach {
        entry =>
          refuse(
            s"version ${entry.version} of $table is committed and can still be published " +
              s"(${entry.tempPath}). Read the table once, which publishes it, then adopt it again"
          )
      }
      // Recorded as published, above the listing, and in the log: published after the listing.
      val logGrew = contradicted.exists(entry => entry.complete && target.isInLog(entry))
      val change =
        if (logGrew) RegistryChange.EntriesChanged(contradicted)
        else registry.readopt(table, latest, deltaTableId, examined = contradicted, request)
      change match {
        case RegistryChange.Applied(removed) => Readopted(table, latest, removed)
        case RegistryChange.NotRegistered =>
          refuse(s"$table stopped being registered while it was being adopted again")
        case RegistryChange.EntriesChanged(_) if attemptsLeft > 1 => examine(attemptsLeft - 1)
        case RegistryChange.EntriesChanged(_) =>
          throw new TableBeingWrittenException(beingWritten(table, "adopted again"))
      }
    }
    examine(Examinations)
  }

  /**
   * Removes everything the registry holds about a table that no longer exists, so that a table
   * can be created at its path again (scenario F9).
   */
  @throws[IOException]
  def reset(tablePath: Path, fs: FileSystem, request: MaintenanceRequest): Unit = {
    val target = new Target(tablePath, fs)
    val table = target.table

    // As in `readopt`: the registry is reset only if its unfinished entries are the ones looked
    // at here. A writer that outlived the table may be about to publish a commit into its path.
    @tailrec
    def examine(attemptsLeft: Int): Unit = {
      target.latestCommit.foreach { latest =>
        refuse(
          s"$table still has a log, with commits up to version $latest. " +
            "A table that exists is adopted again, not reset"
        )
      }
      val unfinished = registry.unfinished(table)
      unfinished.filter(target.canStillBePublished).foreach { entry =>
        refuse(
          s"version ${entry.version} of $table is committed and can still be published " +
            s"(${entry.tempPath}): the table is not gone, or one of its writers is still running"
        )
      }
      registry.forget(table, examined = unfinished, request) match {
        case RegistryChange.Applied(_) => ()
        case RegistryChange.NotRegistered =>
          refuse(s"$table is not registered: there is nothing to reset")
        case RegistryChange.EntriesChanged(_) if attemptsLeft > 1 => examine(attemptsLeft - 1)
        case RegistryChange.EntriesChanged(_) =>
          throw new TableBeingWrittenException(beingWritten(table, "reset"))
      }
    }
    examine(Examinations)
  }

  /**
   * Removes the temporary files of a table that nothing needs any more (ADR-0004 §3.8):
   *
   *  - the file of a commit that is published, which the commit file is a second name of;
   *  - the file of a commit that Delta's log cleanup has removed from the log. Delta removes
   *    the commits below the last checkpoint once they are older than the log retention, thirty
   *    days by default. Such a file is told from the only copy of a commit that was lost by
   *    where it is: below the version in `_last_checkpoint`, which is the line Delta's cleanup
   *    itself never crosses. It must also be older than `orphansOlderThan`, so that a commit
   *    somebody deleted by mistake a moment ago is still there to be put back;
   *  - a file that no entry points at and that is older than `orphansOlderThan`: what a writer
   *    left when it died before taking its version (scenario F1), or when it was writing a file
   *    that is not a commit.
   *
   * What became of each file is recorded once the files are removed: a collection that fails
   * halfway has no record, and what it removed is what nobody needed. A role that may not write
   * the record is refused before a file is looked at: a collection that could not be recorded
   * is not made.
   *
   * @param orphansOlderThan far longer than a commit can take between writing its temporary
   *   file and taking its version: a younger file may belong to a writer that is doing so now
   * @return what became of every temporary file, by its name
   */
  @throws[IOException]
  def collectTemps(
      tablePath: Path,
      fs: FileSystem,
      request: MaintenanceRequest,
      orphansOlderThan: FiniteDuration = DefaultOrphanAge
  ): Map[String, TempFate] = {
    val target = new Target(tablePath, fs)
    if (!registry.mayRecord())
      refuse(
        s"the temporary files of ${target.table} are not collected: the role that this runs as " +
          "cannot write the maintenance log, and a collection is recorded. Nothing was removed"
      )
    val now = System.currentTimeMillis()
    def isOld(temp: FileStatus): Boolean =
      now - temp.getModificationTime >= orphansOlderThan.toMillis
    // Read once, before the files: a checkpoint written meanwhile only makes this cautious.
    val lastCheckpoint = target.lastCheckpoint

    val fates = target.temps.map { temp =>
      val name = temp.getPath.getName
      val fate = target.entryPointingAt(name) match {
        case Some(entry) if !entry.complete => TempFate.KeptUnfinished
        case Some(entry) =>
          val commit = target.commitFile(entry.version)
          val expired = lastCheckpoint.exists(entry.version < _) && isOld(temp)
          if (target.sameContent(temp.getPath, commit)) TempFate.RemovedPublished
          else if (expired && !fs.exists(commit)) TempFate.RemovedExpired
          else TempFate.KeptOnlyCopy
        case None =>
          if (isOld(temp)) TempFate.RemovedOrphan else TempFate.KeptRecent
      }
      if (fate.removed) target.storage.discard(temp.getPath)
      name -> fate
    }
    registry.collected(target.table, request, fates.map { case (name, fate) => name -> fate.name })
    fates.toMap
  }

  /** A table as an operation looks at it: its key, and its log on the storage. */
  final private class Target(tablePath: Path, fs: FileSystem) {
    // First of all: a location the LogStore cannot write to has nothing to maintain.
    val storage: LogStorage = storageOf(fs)
    val table: TableUri = TableIdentity.of(tablePath, fs)
    private val log = new Path(fs.makeQualified(tablePath), TableIdentity.LogDirectory)
    private val tempDirectory = new Path(log, CommitProtocol.TempDirectory)

    def commitFile(version: Long): Path = new Path(log, f"$version%020d.json")

    /** The highest version that has a commit file in the log. */
    def latestCommit: Option[Long] =
      list(log).flatMap(file => PostgresLogStore.commitVersion(file.getPath)).maxOption

    def temps: Seq[FileStatus] = list(tempDirectory).filter(_.isFile)

    /**
     * The version of the table's last checkpoint, as Delta's pointer to it says: Delta's log
     * cleanup removes commits below that version and no others. `None` if there is no pointer,
     * or if it is not written the way Delta writes it: then nothing is taken for expired.
     */
    def lastCheckpoint: Option[Long] =
      try
        new String(bytesOf(new Path(log, LastCheckpointFile)), UTF_8) match {
          case LastCheckpointVersion(digits) => digits.toLongOption
          case _ => None
        }
      catch { case _: FileNotFoundException => None }

    /** The entry whose temporary file is the one named `name`, if there is one. */
    def entryPointingAt(name: String): Option[CommitEntry] = {
      val tempPath = s"${CommitProtocol.TempDirectory}/$name"
      name match {
        case TempOfCommit(digits) =>
          digits.toLongOption.flatMap(registry.entry(table, _)).filter(_.tempPath == tempPath)
        case _ => None
      }
    }

    /** Whether a reader of the table would publish the commit of `entry`, or find it published. */
    def canStillBePublished(entry: CommitEntry): Boolean = {
      val temp = new Path(log, entry.tempPath)
      val commit = commitFile(entry.version)
      fs.exists(temp) && (!fs.exists(commit) || sameContent(temp, commit))
    }

    /** Whether the commit file of `entry` is in the log. */
    def isInLog(entry: CommitEntry): Boolean = fs.exists(commitFile(entry.version))

    def sameContent(one: Path, other: Path): Boolean =
      try
        fs.getFileStatus(one).getLen == fs.getFileStatus(other).getLen &&
          util.Arrays.equals(bytesOf(one), bytesOf(other))
      catch { case _: FileNotFoundException => false }

    private def bytesOf(file: Path): Array[Byte] = Using.resource(fs.open(file))(_.readAllBytes())

    private def list(directory: Path): Seq[FileStatus] =
      try fs.listStatus(directory).toSeq
      catch { case _: FileNotFoundException => Nil }
  }
}

object TableMaintenance {

  /** A day: the age below which a temporary file without an entry is left alone. */
  val DefaultOrphanAge: FiniteDuration = 1.day

  /** The temporary file of a commit: `<digits>.json.<unique>`. */
  private val TempOfCommit = """(\d+)\.json\..+""".r

  /** Delta's pointer to the last checkpoint of a table, in its `_delta_log`. */
  private val LastCheckpointFile = "_last_checkpoint"

  /**
   * The version in that pointer. The module has no JSON parser and needs none for this: Delta
   * writes `version` as the first field. A pointer of any other shape gives no version.
   */
  private val LastCheckpointVersion = """(?s)^\{"version":(\d+)[,}].*""".r

  /**
   * How many times a table is examined before the operation gives up: its entries changed
   * under every one of them, which is a table that is being written.
   */
  private val Examinations = 3

  private def beingWritten(table: TableUri, operation: String): String =
    s"$table is being written while it is $operation: its entries changed each of the " +
      s"$Examinations times they were examined. Stop its writers first"

  private def refuse(why: String): Nothing = throw new MaintenanceRefusedException(why)
}
