package lakehouse.ingestion.logstore

import java.util.concurrent.atomic.AtomicReference

import io.delta.storage.HadoopFileSystemLogStore
import lakehouse.ingestion.logstore.storage.LogStorage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

/**
 * `PostgresLogStore` as Delta builds it, by its name, from the configuration of a session, with
 * a writer that a test can stop at a fault point of one commit (`HookedLogStore.crashAt`): the
 * way a run of the engine dies inside its commit to a table, in the JVM of the test.
 *
 * `PostgresLogStore` is final, and is not extended: this one writes and lists through one of its
 * own, built with the hooks of this object, and reads as it does, as Hadoop does. While nothing
 * is armed, it is `PostgresLogStore`.
 */
final class HookedLogStore(initHadoopConf: Configuration)
    extends HadoopFileSystemLogStore(initHadoopConf) {

  private val coordinated = new PostgresLogStore(
    initHadoopConf,
    PostgresLogStore.sharedCoordination,
    LogStorage.of,
    _ => HookedLogStore.Hooks,
    RetryPolicy.Default
  )

  override def write(
      path: Path,
      actions: java.util.Iterator[String],
      overwrite: java.lang.Boolean,
      hadoopConf: Configuration
  ): Unit = {
    // The commit that the hooks are asked at is this one: they are on this thread.
    HookedLogStore.writing.set(path)
    try coordinated.write(path, actions, overwrite, hadoopConf)
    finally HookedLogStore.writing.remove()
  }

  override def listFrom(path: Path, hadoopConf: Configuration): java.util.Iterator[FileStatus] =
    coordinated.listFrom(path, hadoopConf)

  override def isPartialWriteVisible(path: Path, hadoopConf: Configuration): java.lang.Boolean =
    coordinated.isPartialWriteVisible(path, hadoopConf)
}

object HookedLogStore {

  /** A crash, armed for the next commit to a file that `of` says, at `point`. */
  final private case class Crash(point: FaultPoint, of: Path => Boolean)

  private val armed = new AtomicReference[Option[Crash]](None)
  private val writing = new ThreadLocal[Path]

  /**
   * The next commit to a file that `of` says stops at `point` as a process that dies there:
   * nothing after it runs (`SimulatedCrash`). It is armed for one commit, and stops one.
   */
  def crashAt(point: FaultPoint)(of: Path => Boolean): Unit = armed.set(Some(Crash(point, of)))

  /** No commit stops any more, whether one did or not. */
  def disarm(): Unit = armed.set(None)

  private val Hooks: FaultHooks = reached => {
    val now = armed.get
    now.foreach { crash =>
      val ofThisCommit = Option(writing.get).exists(crash.of)
      if (crash.point == reached && ofThisCommit && armed.compareAndSet(now, None))
        throw new SimulatedCrash(reached)
    }
  }
}
