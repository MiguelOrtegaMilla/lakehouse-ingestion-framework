package lakehouse.ingestion.cli

import java.io.PrintStream

import scala.annotation.tailrec

import io.circe.Json

import lakehouse.ingestion.engine.IngestionService.Outcome

/**
 * The runs of an invocation (discovery §17, P6.10): one, and another after it for as long as the
 * batch that it committed says that there is more, within what the first plan of the invocation
 * saw. Each is a whole run, with its lease and its batch. The chain stops at `--max-batches`, and
 * before a run once a signal has asked it to; its code is that of its last run (`exitOf`), or
 * 128 + the signal if it asked before the first.
 *
 * What a run is, and what is said of each, is the command's: the regular runs of a source for
 * `run`, the chunks of a backfill for `backfill`.
 */
private[cli] object Chain {

  /**
   * How far an invocation goes: no further than `maxBatches` committed, and no run after `stop`
   * has asked.
   */
  final case class Limits(maxBatches: Option[Int], stop: Stop)

  /**
   * How the lines of the chain name what it is of.
   *
   * @param invocation the invocation, as a line starts with it: "The invocation of sales.orders"
   * @param batch what a run commits, one of it: "batch"
   * @param batches more than one of it: "batches"
   * @param more what is left, when `--max-batches` stops a chain whose last batch says that
   *   there is more: "and the source has more."
   * @param afterAnother what is left, when it stops after a batch that a run before the
   *   invocation left, of which no plan of the invocation says anything
   */
  final case class Words(
      invocation: String,
      batch: String,
      batches: String,
      more: String,
      afterAnother: String
  )

  /**
   * Runs the chain.
   *
   * @param say what is said of a run once it has ended, on `out` or on `err`
   * @param attempt one run, given what the run before it said of the next: the horizon of the
   *   chain, or none for the first run, and for a first plan of the invocation after a batch of
   *   another
   */
  def apply(limits: Limits, words: Words, out: PrintStream, err: PrintStream)(
      say: Outcome => Unit
  )(attempt: Option[Json] => Outcome): Int = {
    def ofTheInvocation(ran: Int, committed: Int): String =
      s"${words.invocation} made ${counted(ran, "run", "runs")} and committed " +
        counted(committed, words.batch, words.batches)

    @tailrec
    def next(horizon: Option[Json], ran: Int, committed: Int, last: Option[Int]): Int =
      limits.stop.requested match {
        case Some(signal) =>
          last match {
            case Some(code) =>
              out.println(
                s"${ofTheInvocation(ran, committed)}: ${Stop.name(signal)} asked it to stop, " +
                  "and no other run was started."
              )
              code
            case None =>
              err.println(
                s"error: ${Stop.name(signal)} came before the first run. Nothing was run."
              )
              128 + signal
          }
        case None =>
          val outcome = attempt(horizon)
          say(outcome)
          val code = RunCommand.exitOf(outcome)
          val done = committed + (outcome match {
            case _: Outcome.Ingested | _: Outcome.RolledForward => 1
            case _ => 0
          })
          // What the next run plans against, if there is one: the horizon of the chain, or
          // none, for a first plan of the invocation after a batch of another.
          val more: Option[Option[Json]] = outcome match {
            case Outcome.Ingested(_, _, _, _, Outcome.Next.Within(within)) => Some(Some(within))
            case Outcome.Ingested(_, _, _, _, Outcome.Next.Afresh) => Some(None)
            case _: Outcome.RolledForward => Some(None)
            case _ => None
          }
          more match {
            case Some(within) if limits.maxBatches.forall(done < _) =>
              next(within, ran + 1, done, Some(code))
            case Some(within) =>
              // The plan of the batch says that there is more. After a batch of another
              // invocation no plan of this one says anything: the next plans anew, and finds
              // out.
              val left = if (within.nonEmpty) words.more else words.afterAnother
              out.println(
                s"${ofTheInvocation(ran + 1, done)}: it stopped at --max-batches " +
                  s"${limits.maxBatches.getOrElse(done)}, $left"
              )
              code
            case None =>
              if (ran > 0)
                out.println(s"${ofTheInvocation(ran + 1, done)}.")
              code
          }
      }
    next(horizon = None, ran = 0, committed = 0, last = None)
  }

  private def counted(count: Int, one: String, many: String): String =
    if (count == 1) s"1 $one" else s"$count $many"
}
