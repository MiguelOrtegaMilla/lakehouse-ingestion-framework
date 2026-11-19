package lakehouse.ingestion.logstore

import java.util.concurrent.atomic.LongAdder
import java.util.{Objects, UUID}

import lakehouse.ingestion.logstore.coordination.WriterEpoch

/**
 * The run that a thread's Delta commits are made for (ADR-0004 §3.7).
 *
 * Delta gives a LogStore the file to write and nothing about who asked for it. An application
 * that wants its commits attributed makes one context for a run and writes inside it:
 *
 * {{{
 * val commits = CommitContext.forRun(runId)
 * commits.attribute { rejected.write.format("delta").mode("append").save(quarantine) }
 * commits.attribute { rows.write.format("delta").mode("append").save(bronze) }
 * audit.finished(runId, commitContention = commits.contention)
 * }}}
 *
 * While the block runs, each commit that `PostgresLogStore` writes on the same thread records
 * the run in its coordination entry, and each version such a commit loses to another writer is
 * counted here. Of those, the ones lost at the `INSERT` are counted apart: they are the times
 * that two writers were inside the same commit at once.
 *
 * '''The thread is the caller's.''' Delta writes a commit on the thread that called the
 * operation (ADR-0004 §8, item 3), and that thread alone is in the context. A thread started
 * inside the block does not inherit it: pools outlive the block, and a later commit under the
 * wrong run is worse than a commit under none.
 *
 * '''The run is best effort.''' Naming a run cannot fail a commit, and a commit outside any
 * context is as good as one inside: its entry has no run. A commit that a writer took and
 * another process finished keeps the run of the writer that took it.
 *
 * '''The epoch is not''' (ADR-0019). A context that says under which epoch its run commits
 * holds every commit made in it to that epoch: one takes its version only while the epoch is
 * the one in force under its key, and is refused otherwise
 * (`EpochNotInForceException`). It is how a run that lost its turn at some work, and does not
 * know it yet, is kept from committing anything more. A context that names no epoch, and a
 * commit outside any context, are held to none.
 *
 * One context may be entered on several threads at once, and any number of times.
 *
 * @param epoch the epoch that the commits of the run are made under, if it has one
 */
final class CommitContext private (val runId: UUID, val epoch: Option[WriterEpoch]) {

  private val lostInLog = new LongAdder
  private val lostAtInsert = new LongAdder

  /**
   * Runs `body` with this thread's commits attributed to the run. Inside another context, this
   * one applies until `body` ends, and then the outer one again.
   */
  def attribute[A](body: => A): A = {
    val outer = CommitContext.onThisThread.get()
    CommitContext.onThisThread.set(this)
    try body
    finally
      if (outer == null) CommitContext.onThisThread.remove()
      else CommitContext.onThisThread.set(outer)
  }

  /**
   * How many times a commit of this run found its version taken by another writer, so far. Each
   * time, Delta looked for a logical conflict with the winner and, finding none, tried the
   * next version: this is the cost of sharing a table, not a failure (ADR-0010).
   */
  def contention: Long = lostInLog.sum() + lostAtInsert.sum()

  /**
   * How many of those versions the writer lost at the `INSERT`: when it looked, the version's
   * commit was not in the log, so it prepared its own commit and claimed the version, and the
   * claim found another writer's. The two writers were inside the commit of one version at the
   * same time, which is the moment a LogStore without mutual exclusion loses a commit. The
   * rest of `contention` are versions whose commit was already in the log when the writer
   * looked: it was merely behind, as it would be on any LogStore.
   */
  def contentionAtInsert: Long = lostAtInsert.sum()

  /** @param atInsert whether the claim found the version taken, and not the look at the log */
  private[logstore] def versionLost(atInsert: Boolean): Unit =
    if (atInsert) lostAtInsert.increment() else lostInLog.increment()

  override def toString: String = s"CommitContext($runId${epoch.fold("")(e => s", $e")})"
}

object CommitContext {

  // Not inheritable, on purpose: see the class.
  private val onThisThread = new ThreadLocal[CommitContext]

  /** A context for the run, with nothing counted. One per run, kept for all its writes. */
  def forRun(runId: UUID): CommitContext =
    new CommitContext(Objects.requireNonNull(runId, "a commit context needs its run"), None)

  /**
   * A context for a run that commits under an epoch: its commits are refused once that epoch is
   * not the one in force under its key. The run announces the epoch before it writes
   * (`WriterEpochs.announce`).
   */
  def forRun(runId: UUID, epoch: WriterEpoch): CommitContext =
    new CommitContext(
      Objects.requireNonNull(runId, "a commit context needs its run"),
      Some(Objects.requireNonNull(epoch, "a commit context under an epoch needs the epoch"))
    )

  /** The context the calling thread is in, if any. */
  private[logstore] def current: Option[CommitContext] = Option(onThisThread.get())
}
