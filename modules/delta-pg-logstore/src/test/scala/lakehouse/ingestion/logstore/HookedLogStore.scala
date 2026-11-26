package lakehouse.ingestion.logstore

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import io.delta.storage.HadoopFileSystemLogStore
import lakehouse.ingestion.logstore.storage.LogStorage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

/**
 * `PostgresLogStore` as Delta builds it, by its name, from the configuration of a session, with
 * a writer that a test can stop at a fault point of one commit (`HookedLogStore.crashAt`): the
 * way a run of the engine dies inside its commit to a table, in the JVM of the test. Or hold
 * there while something else is done (`HookedLogStore.runAt`): the way another writer commits
 * to the table between the moment a commit read it and the moment it takes its version.
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

  /** What is to be done at `point` of the next `left` commits to a file that `of` says. */
  final private case class Action(
      point: FaultPoint,
      of: Path => Boolean,
      left: Int,
      run: Path => Unit
  )

  private val armed = new AtomicReference[Option[Crash]](None)
  private val actions = new AtomicReference[Option[Action]](None)
  private val acting = new AtomicBoolean(false)
  private val writing = new ThreadLocal[Path]

  /**
   * The next commit to a file that `of` says stops at `point` as a process that dies there:
   * nothing after it runs (`SimulatedCrash`). It is armed for one commit, and stops one.
   */
  def crashAt(point: FaultPoint)(of: Path => Boolean): Unit = armed.set(Some(Crash(point, of)))

  /**
   * At `point` of each of the next `times` commits to a file that `of` says, `action` is done on
   * the thread of the commit, with the file of the commit, and the commit goes on once it
   * returns. What `action` commits itself, on whatever thread, has nothing done to it: another
   * writer that commits in the middle of this one, as a test makes one, is let through.
   *
   * Delta holds a lock of its JVM on the log of a table while it commits to it, on `file://`:
   * another commit made by Delta in this JVM, while this one waits, waits for it in turn. The
   * other writer commits through a LogStore of its own, as another process would.
   */
  def runAt(point: FaultPoint, times: Int)(of: Path => Boolean)(action: Path => Unit): Unit = {
    require(times > 0, s"an action is done at $times commits")
    actions.set(Some(Action(point, of, times, action)))
  }

  /** No commit stops any more, nor has anything done to it, whether one did or not. */
  def disarm(): Unit = {
    armed.set(None)
    actions.set(None)
  }

  private val Hooks: FaultHooks = reached => {
    val now = armed.get
    now.foreach { crash =>
      val ofThisCommit = Option(writing.get).exists(crash.of)
      if (crash.point == reached && ofThisCommit && armed.compareAndSet(now, None))
        throw new SimulatedCrash(reached)
    }
    val planned = actions.get
    planned.foreach { action =>
      val ofThisCommit = Option(writing.get).exists(action.of)
      if (action.point == reached && ofThisCommit && acting.compareAndSet(false, true))
        try {
          val next = Option.when(action.left > 1)(action.copy(left = action.left - 1))
          if (actions.compareAndSet(planned, next)) action.run(writing.get)
        } finally acting.set(false)
    }
  }
}
