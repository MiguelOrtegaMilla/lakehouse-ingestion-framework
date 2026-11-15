package lakehouse.ingestion.logstore

import java.io.IOException
import java.nio.file.{Files, NoSuchFileException, Path => LocalPath, Paths}

import scala.annotation.tailrec

import lakehouse.ingestion.logstore.coordination.TableUri
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * A table location that cannot be given a coordination key, so nothing may be read from it or
 * written to it through the LogStore. Retrying does not help: the location or the deployment
 * has to change.
 */
final class UnsupportedTableLocationException(message: String, cause: Throwable = null)
    extends IOException(message, cause)

/**
 * From a table's path, however it is spelled, to the key the table is coordinated under
 * (ADR-0004 §3.6).
 *
 * Mutual exclusion is per key. Two paths that reach the same `_delta_log` and get different
 * keys are two writers that do not exclude each other, so this is the only place where a key is
 * made.
 *
 * On `file:`, the one scheme implemented (ADR-0004 §2), the key is Hadoop's qualified form of
 * the table's real directory, such as `file:/home/user/lake/bronze/orders`:
 *
 *  - '''Qualified by the file system.''' A relative path, a missing scheme, `file:///`, a
 *    trailing or doubled slash, `.` and `..` all end in the same absolute path. Hadoop removes
 *    `..` from the text of the path before any I/O, so this sees the path that will be opened.
 *  - '''Symbolic links resolved''', by the operating system, for the part of the path that
 *    exists. A table that is about to be created has its key already: the part that does not
 *    exist yet is kept as named, and creating it does not change the key.
 *  - '''Refused when no single key exists''': a `_delta_log` that is itself a symbolic link, a
 *    link whose target is missing, another scheme, a URI that names a host.
 *
 * Two paths can still reach one directory without a link between them, as when a file system is
 * mounted twice. Path resolution cannot see that. The table registry does: the second path has
 * a key that is not registered, and its commits are refused (ADR-0004 §3.5, layer 2).
 */
object TableIdentity {

  private val Scheme = "file"
  private val LogDirectory = "_delta_log"

  /**
   * The key of the table at `tablePath`.
   *
   * @param fs the file system of `tablePath`, which qualifies it
   * @throws UnsupportedTableLocationException if the location has no key, as described above
   * @throws java.io.IOException if the path cannot be resolved, for example for lack of
   *   permission on a directory above it
   */
  @throws[IOException]
  def of(tablePath: Path, fs: FileSystem): TableUri = {
    val location = qualified(tablePath, fs)
    val directory = realPath(Paths.get(location.toUri.getPath), location)

    // Through such a link two table directories would share one log, under two keys.
    if (Files.isSymbolicLink(directory.resolve(LogDirectory)))
      throw new UnsupportedTableLocationException(
        s"cannot coordinate the table at $location: its $LogDirectory is a symbolic link"
      )
    new TableUri(new Path(Scheme, null, directory.toString).toString)
  }

  /** `path` as its file system names it: absolute and with a scheme, on the local disk. */
  private def qualified(path: Path, fs: FileSystem): Path = {
    val location =
      try fs.makeQualified(path)
      catch {
        // Hadoop's "Wrong FS": the path names another file system than the one given, or a host.
        case e: IllegalArgumentException =>
          throw new UnsupportedTableLocationException(
            s"cannot coordinate the table at $path: ${e.getMessage}",
            e
          )
      }
    val uri = location.toUri
    if (uri.getScheme != Scheme || uri.getAuthority != null)
      throw new UnsupportedTableLocationException(
        s"cannot coordinate the table at $location: this LogStore coordinates tables on " +
          s"$Scheme: only, with no host in the URI"
      )
    location
  }

  /**
   * `path` with every symbolic link resolved in the part of it that exists, and the rest as
   * named.
   *
   * @param location the table's location as the caller named it, for the error message
   * @param missing the names below `path` that are already known not to exist, outermost first
   */
  @tailrec
  private def realPath(path: LocalPath, location: Path, missing: List[LocalPath] = Nil): LocalPath =
    existing(path) match {
      case Some(real) => missing.foldLeft(real)(_.resolve(_))
      case None =>
        // The link exists and what it points to does not. Keeping its name would give a key
        // that changes on the day the target is created.
        if (Files.isSymbolicLink(path))
          throw new UnsupportedTableLocationException(
            s"cannot coordinate the table at $location: $path is a symbolic link to a path " +
              "that does not exist"
          )
        // A root directory always exists, so a path that does not exist has a parent.
        realPath(path.getParent, location, path.getFileName :: missing)
    }

  /**
   * The real path of `path`, or `None` if it does not exist. Any other failure, such as a loop
   * of links or a directory that may not be searched, is an error: the key is not guessed.
   */
  private def existing(path: LocalPath): Option[LocalPath] =
    try Some(path.toRealPath())
    catch { case _: NoSuchFileException => None }
}
