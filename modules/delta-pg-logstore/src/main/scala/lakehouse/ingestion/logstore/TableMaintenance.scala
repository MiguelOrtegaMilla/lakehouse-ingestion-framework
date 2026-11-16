package lakehouse.ingestion.logstore

import java.io.{FileNotFoundException, IOException}
import java.util

import scala.concurrent.duration._
import scala.util.Using

import lakehouse.ingestion.logstore.coordination.{
  CommitEntry,
  Readoption,
  RegistryMaintenance,
  TableUri
}
import lakehouse.ingestion.logstore.storage.LogStorage
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

/**
 * An operator's request does not fit the table: its log or its registration is not in the state
 * the operation is for. Nothing was changed.
 */
final class MaintenanceRefusedException(message: String) extends IOException(message)

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
}

object TempFate {

  /** Removed: its commit is published, so the file had a second name it no longer needs. */
  case object RemovedPublished extends TempFate { val removed = true }

  /** Removed: no entry points at it, and it is older than any commit could take. */
  case object RemovedOrphan extends TempFate { val removed = true }

  /** Kept: its commit is taken and not published yet. These are the commit's bytes. */
  case object KeptUnfinished extends TempFate { val removed = false }

  /**
   * Kept: its entry is complete, and the commit file is missing or is another file. It is the
   * only copy of a commit that was lost, and somebody will want to see it.
   */
  case object KeptOnlyCopy extends TempFate { val removed = false }

  /** Kept: no entry points at it, and a writer may be about to take a version for it. */
  case object KeptRecent extends TempFate { val removed = false }
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
 * @param registry the registry, reached with an operator's privileges
 */
final class TableMaintenance(
    registry: RegistryMaintenance,
    storageOf: FileSystem => LogStorage = LogStorage.of
) {

  import TableMaintenance._

  /**
   * Registers a table that exists and is not coordinated, at the version its log is at, so that
   * it can be written through the LogStore from the next version on.
   *
   * @param deltaTableId the table's `metaData.id`, which the caller reads with Delta
   */
  @throws[IOException]
  def adopt(tablePath: Path, fs: FileSystem, deltaTableId: Option[String]): Adoption = {
    val target = new Target(tablePath, fs)
    val latest = target.latestCommit.getOrElse {
      refuse(s"there is no table to adopt at ${target.table}: its log holds no commit")
    }
    if (!registry.adopt(target.table, latest, deltaTableId))
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
  def readopt(tablePath: Path, fs: FileSystem, deltaTableId: Option[String]): Readopted = {
    val target = new Target(tablePath, fs)
    val table = target.table
    val noTable = s"there is no table to adopt at $table: its log holds no commit. " +
      "If the table was dropped, reset its registry"
    if (target.latestCommit.isEmpty) refuse(noTable)
    if (registry.registration(table).isEmpty)
      refuse(s"$table is not registered. A table that was never coordinated is adopted")
    registry.unfinished(table).filter(target.canStillBePublished).foreach { entry =>
      refuse(
        s"version ${entry.version} of $table is committed and can still be published " +
          s"(${entry.tempPath}). Read the table once, which publishes it, then adopt it again"
      )
    }
    // The version to adopt at is read from the log now, after the unfinished entries. Readers
    // are not stopped for this, and one may have published a commit in the meantime: read
    // before, the version would leave that commit above the registration, and its entry among
    // the ones removed. No entry was left that a reader can still publish, so the log does not
    // grow after this listing.
    val latest = target.latestCommit.getOrElse(refuse(noTable))
    val removed = registry.readopt(table, latest, deltaTableId).getOrElse {
      refuse(s"$table stopped being registered while it was being adopted again")
    }
    Readopted(table, latest, removed)
  }

  /**
   * Removes everything the registry holds about a table that no longer exists, so that a table
   * can be created at its path again (scenario F9).
   */
  @throws[IOException]
  def reset(tablePath: Path, fs: FileSystem): Unit = {
    val target = new Target(tablePath, fs)
    target.latestCommit.foreach { latest =>
      refuse(
        s"${target.table} still has a log, with commits up to version $latest. " +
          "A table that exists is adopted again, not reset"
      )
    }
    if (!registry.forget(target.table))
      refuse(s"${target.table} is not registered: there is nothing to reset")
  }

  /**
   * Removes the temporary files of a table that nothing needs any more (ADR-0004 §3.8):
   *
   *  - the file of a commit that is published, which the commit file is a second name of;
   *  - a file that no entry points at and that is older than `orphansOlderThan`: what a writer
   *    left when it died before taking its version (scenario F1), or when it was writing a file
   *    that is not a commit.
   *
   * @param orphansOlderThan far longer than a commit can take between writing its temporary
   *   file and taking its version: a younger file may belong to a writer that is doing so now
   * @return what became of every temporary file, by its name
   */
  @throws[IOException]
  def collectTemps(
      tablePath: Path,
      fs: FileSystem,
      orphansOlderThan: FiniteDuration = DefaultOrphanAge
  ): Map[String, TempFate] = {
    val target = new Target(tablePath, fs)
    val now = System.currentTimeMillis()
    target.temps.map { temp =>
      val name = temp.getPath.getName
      val fate = target.entryPointingAt(name) match {
        case Some(entry) if !entry.complete => TempFate.KeptUnfinished
        case Some(entry) =>
          if (target.sameContent(temp.getPath, target.commitFile(entry.version)))
            TempFate.RemovedPublished
          else TempFate.KeptOnlyCopy
        case None =>
          if (now - temp.getModificationTime >= orphansOlderThan.toMillis) TempFate.RemovedOrphan
          else TempFate.KeptRecent
      }
      if (fate.removed) target.storage.discard(temp.getPath)
      name -> fate
    }.toMap
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

  private def refuse(why: String): Nothing = throw new MaintenanceRefusedException(why)
}
