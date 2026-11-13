package lakehouse.ingestion.it.stress

import java.net.URI
import java.util.concurrent.atomic.AtomicLong

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.local.LocalConfigKeys
import org.apache.hadoop.fs.{
  ChecksumFs,
  DelegateToFileSystem,
  FsServerDefaults,
  Path,
  RawLocalFileSystem
}

/**
 * Hadoop's local `AbstractFileSystem` (`LocalFs`) with a delay before the rename that publishes a
 * Delta commit file, to widen the race window of the RED spike (ADR-0004 §7.3).
 *
 * Delta's default LogStore on `file://` (`HDFSLogStore`) writes `_delta_log/<N>.json` through a
 * `FileContext`: it checks that the file does not exist, writes a temporary file and renames it
 * with `Rename.NONE`. On the local file system that rename checks for the destination twice more
 * (in `AbstractFileSystem.renameInternal` and in `FileSystem.rename(src, dst, options)`), then
 * calls `RawLocalFileSystem.rename(src, dst)`, a POSIX `rename(2)` that replaces an existing
 * destination. The delay sits there, after the last existence check and before the replacing
 * rename, so two writers that both passed the checks both rename. Without the delay the window
 * is the same, only microseconds wide.
 *
 * It is `LocalFs` rebuilt from public parts, because `LocalFs` and `RawLocalFs` have
 * package-private constructors: a `ChecksumFs` (which keeps the `.crc` sidecars of `LocalFs`)
 * over a `DelegateToFileSystem` with the same overrides as `RawLocalFs`, delegating to a
 * `RawLocalFileSystem` whose two-argument `rename` sleeps first. Nothing else changes.
 *
 * Writers select it with `-Dspark.hadoop.fs.AbstractFileSystem.file.impl=<this class>` and set
 * the delay with `-Dspark.hadoop.<DelayKey>=<ms>` (`jvmOptions`). It affects only the
 * `FileContext` path, which `HDFSLogStore.write` uses; readers keep using `LocalFileSystem`.
 */
final class DelayingLocalFs(uri: URI, conf: Configuration)
    extends ChecksumFs(new DelayingLocalFs.Raw(uri, conf))

object DelayingLocalFs {

  /** Hadoop setting with the delay in milliseconds; absent or 0 means no delay. */
  val DelayKey = "lakehouse.stress.commitRenameDelayMs"

  /** The writer JVM options that install this file system with `delay`. */
  def jvmOptions(delayMs: Long): Seq[String] = Seq(
    s"-Dspark.hadoop.fs.AbstractFileSystem.file.impl=${classOf[DelayingLocalFs].getName}",
    s"-Dspark.hadoop.$DelayKey=$delayMs"
  )

  private val CommitFile = """\d{20}\.json""".r

  private val delayed = new AtomicLong

  /** Commit-file renames delayed so far in this JVM; a writer reports it in its probe. */
  def delayedRenames: Long = delayed.get

  /** Only the publication of a commit file: not checksums, checkpoints or `_last_checkpoint`. */
  def isCommitFile(dst: Path): Boolean =
    dst.getParent != null && dst.getParent.getName == "_delta_log" &&
      CommitFile.matches(dst.getName)

  /** `RawLocalFs` with the delaying file system underneath; the overrides are `RawLocalFs`'s. */
  final private class Raw(uri: URI, conf: Configuration)
      extends DelegateToFileSystem(uri, new DelayingRawLocalFileSystem, conf, "file", false) {
    override def getUriDefaultPort: Int = -1
    override def getServerDefaults(f: Path): FsServerDefaults = LocalConfigKeys.getServerDefaults()
    @deprecated("overrides a deprecated method", since = "Hadoop 2.7")
    override def getServerDefaults: FsServerDefaults = LocalConfigKeys.getServerDefaults()
    override def isValidName(src: String): Boolean = true
  }

  final private class DelayingRawLocalFileSystem extends RawLocalFileSystem {
    private var delayMs = 0L

    override def initialize(uri: URI, conf: Configuration): Unit = {
      super.initialize(uri, conf)
      delayMs = conf.getLong(DelayKey, 0L)
    }

    override def rename(src: Path, dst: Path): Boolean = {
      if (delayMs > 0 && isCommitFile(dst)) {
        delayed.incrementAndGet(): Unit
        Thread.sleep(delayMs)
      }
      super.rename(src, dst)
    }
  }
}
