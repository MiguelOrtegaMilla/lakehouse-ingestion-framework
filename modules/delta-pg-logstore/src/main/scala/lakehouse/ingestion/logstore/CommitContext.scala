package lakehouse.ingestion.logstore

import java.util.concurrent.atomic.LongAdder
import java.util.{Objects, UUID}

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
 * '''Best effort.''' Nothing here can fail a commit, and a commit outside any context is as
 * good as one inside: its entry has no run. A commit that a writer took and another process
 * finished keeps the run of the writer that took it.
 *
 * One context may be entered on several threads at once, and any number of times.
 */
final class CommitContext private (val runId: UUID) {

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

  override def toString: String = s"CommitContext($runId)"
}

object CommitContext {

  // Not inheritable, on purpose: see the class.
  private val onThisThread = new ThreadLocal[CommitContext]

  /** A context for the run, with nothing counted. One per run, kept for all its writes. */
  def forRun(runId: UUID): CommitContext =
    new CommitContext(Objects.requireNonNull(runId, "a commit context needs its run"))

  /** The context the calling thread is in, if any. */
  private[logstore] def current: Option[CommitContext] = Option(onThisThread.get())
}
