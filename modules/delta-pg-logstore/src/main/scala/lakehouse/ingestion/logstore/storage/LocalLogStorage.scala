package lakehouse.ingestion.logstore.storage

import java.io.{BufferedOutputStream, IOException}
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.StandardCopyOption.ATOMIC_MOVE
import java.nio.file.StandardOpenOption.{CREATE_NEW, READ, WRITE}
import java.nio.file.{
  FileAlreadyExistsException,
  Files,
  NoSuchFileException,
  Path => LocalPath,
  Paths
}

import scala.util.Using
import scala.util.control.NonFatal

import lakehouse.ingestion.logstore.storage.PublishResult._
import org.apache.hadoop.fs.Path

/**
 * `LogStorage` on a local POSIX file system (ADR-0015).
 *
 * It works on the disk through `java.nio`, not through Hadoop's local file system, for two
 * reasons:
 *
 *  - '''The hard link.''' A commit is published as a second name of its temporary file.
 *    `link(2)` is one system call: the name appears with the whole file behind it, and the call
 *    fails if the name is taken. Hadoop's file system has no such operation. Its rename
 *    replaces what is there.
 *  - '''No `.crc` sidecars.''' Hadoop's checksummed file system keeps one next to each file it
 *    writes and moves it in a second step, so a crash between the two steps leaves a file that
 *    every checksum-verifying reader refuses (the RED spike). A file without a sidecar is read
 *    unchecked, by every reader.
 *
 * '''On disk.''' A temporary file is forced to disk, and so is the directory that holds its
 * name, before `writeTemp` returns: the coordination entry inserted next must never point at a
 * file a power cut can take. A published or replaced name is forced the same way.
 *
 * Every process that reads or writes a table needs write access to its temporary files: Linux
 * refuses a hard link to a file the caller neither owns nor may write
 * (`fs.protected_hardlinks`), and a reader that finds an unpublished commit publishes it. One
 * operating-system user for all of them is the simple way.
 *
 * @param toDisk forces a file or a directory to disk; only the tests give another one
 */
final class LocalLogStorage private[storage] (toDisk: LocalPath => Unit) extends LogStorage {

  def this() = this(LocalLogStorage.force)

  import LocalLogStorage._

  /** A file written in place on a local disk can be read while it grows. */
  override val partialWritesVisible: Boolean = true

  override def writeTemp(temp: Path, lines: Iterator[String]): Unit = {
    val file = local(temp)
    val directory = file.getParent
    plainErrors(s"write the temporary file $temp") {
      val created = !Files.isDirectory(directory)
      if (created) Files.createDirectories(directory): Unit
      writeNew(file, lines)

      toDisk(file)
      toDisk(directory)
      // The directory is itself a new name in the one above.
      if (created) toDisk(directory.getParent)
    }
  }

  override def publish(temp: Path, target: Path): PublishResult = {
    val (file, name) = (local(temp), local(target))
    val result =
      try {
        Files.createLink(name, file)
        Published
      } catch {
        case _: FileAlreadyExistsException =>
          // The same file under both names, or a copy of it, is this commit. Anything else was
          // written by somebody who did not take the version.
          if (Files.mismatch(file, name) == -1L) AlreadyPublished else ForeignFile
        // The link also fails with this when the directory of `name` is missing.
        case _: NoSuchFileException if !Files.exists(file) => TempFileLost
      }
    // Also when another process made the link: it may have died before forcing it, and the
    // caller is about to record the commit as complete.
    if (result == Published || result == AlreadyPublished) toDisk(name.getParent)
    result
  }

  override def replace(temp: Path, target: Path): Unit = {
    val (file, name) = (local(temp), local(target))
    plainErrors(s"replace $target") {
      // A sidecar that Hadoop's file system wrote for the file being replaced would not match
      // the new one, and readers that verify it would refuse the file. It goes first: the old
      // file without a sidecar is still readable, the new file under the old sidecar is not.
      Files.deleteIfExists(sidecarOf(name)): Unit
      // rename(2): it replaces what is under the name, in one step.
      Files.move(file, name, ATOMIC_MOVE): Unit
      toDisk(name.getParent)
    }
  }

  /** Writes the new file `file`. A write that fails removes what it wrote of it. */
  private def writeNew(file: LocalPath, lines: Iterator[String]): Unit = {
    // If the file exists this fails, and there is nothing of this call's to remove.
    val out = new BufferedOutputStream(Files.newOutputStream(file, CREATE_NEW, WRITE))
    try
      Using.resource(out) { stream =>
        lines.foreach { line =>
          stream.write(line.getBytes(UTF_8))
          stream.write('\n'.toInt)
        }
      }
    catch {
      case NonFatal(failure) =>
        try Files.deleteIfExists(file): Unit
        catch { case NonFatal(cleanup) => failure.addSuppressed(cleanup) }
        throw failure
    }
  }
}

object LocalLogStorage {

  val Scheme = "file"

  /** The storage is stateless: one instance serves every table of the JVM. */
  val shared: LocalLogStorage = new LocalLogStorage

  /**
   * `fsync(2)` on a file or on a directory. A directory is opened for reading, which is the
   * only way to open one and is enough to force it.
   */
  private def force(path: LocalPath): Unit =
    Using.resource(FileChannel.open(path, if (Files.isDirectory(path)) READ else WRITE)) {
      _.force(true)
    }

  /** The path on disk of a `file:` path: the path of its URI. */
  private def local(path: Path): LocalPath = {
    val uri = path.toUri
    require(uri.getScheme == null || uri.getScheme == Scheme, s"not a path on $Scheme: $path")
    Paths.get(uri.getPath)
  }

  /** Where Hadoop's checksummed file system keeps the checksums of `file`. */
  private def sidecarOf(file: LocalPath): LocalPath =
    file.resolveSibling(s".${file.getFileName}.crc")

  /**
   * Runs `body`. A file that is in the way becomes an ordinary I/O error: thrown as it is,
   * Delta would take it to mean that the commit's version belongs to another writer.
   */
  private def plainErrors[T](action: => String)(body: => T): T =
    try body
    catch {
      case e: FileAlreadyExistsException =>
        throw new IOException(s"cannot $action: ${e.getFile} is in the way", e)
    }
}
