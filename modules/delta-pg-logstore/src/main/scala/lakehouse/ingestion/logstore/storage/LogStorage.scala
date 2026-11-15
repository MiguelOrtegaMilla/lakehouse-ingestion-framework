package lakehouse.ingestion.logstore.storage

import java.io.IOException

import lakehouse.ingestion.logstore.UnsupportedTableLocationException
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path, RawLocalFileSystem}

/**
 * How log files reach the storage: the one part of the commit protocol that depends on the
 * scheme (ADR-0004 §3.4).
 *
 * The protocol takes its mutual exclusion from the coordination store, not from here. What it
 * asks of the storage, and every implementation gives:
 *
 *  - '''Whole files only.''' Under its final name a file is complete or absent. No reader, in
 *    this process or another, ever finds part of one.
 *  - '''On disk before it is reported.''' What an operation says it wrote is still there after
 *    the machine loses power: the bytes and the name.
 *  - '''`publish` replaces nothing.''' A file that is already under the name stays as it is, and
 *    the caller is told whether it is the same commit.
 *  - '''Plain errors.''' A failure is an `IOException`, and never a
 *    `java.nio.file.FileAlreadyExistsException`: Delta reads that one as "this version is
 *    taken" and moves on to the next (ADR-0004 §3.2).
 *
 * Paths are qualified paths of the file system the storage was obtained for. The protocol
 * chooses them, the temporary ones included.
 */
trait LogStorage {

  /**
   * Whether a file written in place, without these operations, can be read before it is
   * complete. It is the answer Delta asks a LogStore for (`isPartialWriteVisible`).
   */
  def partialWritesVisible: Boolean

  /**
   * Writes `lines` to the new file `temp`, each followed by a line break, creating its
   * directory if needed. Nothing reads a temporary file before it is published, so only its
   * being complete and on disk when this returns matters.
   *
   * @throws java.io.IOException also if `temp` already exists: it is not overwritten
   */
  @throws[IOException]
  def writeTemp(temp: Path, lines: Iterator[String]): Unit

  /**
   * Makes the content of `temp` appear as `target`, at once and only if nothing is there.
   * `temp` stays: it is the commit's bytes for whoever finds the coordination entry incomplete.
   */
  @throws[IOException]
  def publish(temp: Path, target: Path): PublishResult

  /**
   * Makes the content of `temp` the content of `target`, at once, whether `target` exists or
   * not: a reader finds the old file or the new one. `temp` is used up.
   */
  @throws[IOException]
  def replace(temp: Path, target: Path): Unit
}

object LogStorage {

  /**
   * The storage for the log files that `fs` holds, which is how a location is known to be one
   * this LogStore can write to. The configuration guard asks the same question at startup
   * (ADR-0004 §3.5, layer 1).
   *
   * Only `file:` has a storage until E1 (ADR-0004 §2), and only when Hadoop's own local file
   * system serves it. `LocalLogStorage` goes to the disk directly, taking the path in the URI
   * for a path of the operating system. Under a replacement of that file system, which may map
   * paths or act on its own when a file is created or renamed, this LogStore and the readers
   * that go through Hadoop could disagree about where a commit is.
   *
   * @throws UnsupportedTableLocationException if there is no storage for `fs`
   */
  @throws[IOException]
  def of(fs: FileSystem): LogStorage = fs.getUri.getScheme match {
    case LocalLogStorage.Scheme if isHadoopsLocalFileSystem(fs) => LocalLogStorage.shared
    case LocalLogStorage.Scheme =>
      throw new UnsupportedTableLocationException(
        s"${LocalLogStorage.Scheme}: is served by ${fs.getClass.getName} here, and this " +
          "LogStore writes log files only where Hadoop's own local file system does " +
          s"(${classOf[LocalFileSystem].getName} or ${classOf[RawLocalFileSystem].getName})"
      )
    case other =>
      throw new UnsupportedTableLocationException(
        s"this LogStore has no storage for the scheme $other (file system ${fs.getUri}): " +
          s"it coordinates tables on ${LocalLogStorage.Scheme}: only"
      )
  }

  /** The exact classes: a subclass is a replacement, whatever it inherits. */
  private def isHadoopsLocalFileSystem(fs: FileSystem): Boolean =
    if (fs.getClass == classOf[LocalFileSystem])
      fs.asInstanceOf[LocalFileSystem].getRawFileSystem.getClass == classOf[RawLocalFileSystem]
    else fs.getClass == classOf[RawLocalFileSystem]
}

/** What `LogStorage.publish` found or did. */
sealed trait PublishResult

object PublishResult {

  /** This call gave the file its name. */
  case object Published extends PublishResult

  /**
   * The name was already there with the same bytes: another process published this commit. It
   * is on disk now, whoever published it.
   */
  case object AlreadyPublished extends PublishResult

  /**
   * The name is there with other bytes. Nobody who follows the protocol writes them, since one
   * coordination entry means one temporary file: a writer outside it did. Nothing was changed.
   */
  case object ForeignFile extends PublishResult

  /** `temp` does not exist, so there is nothing to publish. Nothing was changed. */
  case object TempFileLost extends PublishResult
}
