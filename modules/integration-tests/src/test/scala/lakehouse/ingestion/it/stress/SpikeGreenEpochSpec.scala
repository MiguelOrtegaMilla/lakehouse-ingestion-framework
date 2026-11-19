package lakehouse.ingestion.it.stress

import scala.collection.mutable
import scala.concurrent.duration._

import lakehouse.ingestion.it.stress.GreenSpikeSuite.Spiked
import lakehouse.ingestion.it.stress.LogModel.Turn
import lakehouse.ingestion.it.stress.StressHarness.{Chaos, HarnessFailure}
import lakehouse.ingestion.it.support.StressTest
import lakehouse.ingestion.logstore.FaultPoint
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Spike GREEN, the writers that are taken over (ADR-0019): every process commits under an epoch
 * of the key of its writer, as a run of the engine commits under the epoch of its lease, and
 * while it commits another process takes its writer over under the next epoch.
 *
 * The process that is taken over is not killed, and is told nothing. It is the run that lost
 * its lease and does not know it: it goes on committing, and what stops it is the LogStore,
 * which refuses its next commit. The process that takes over does what a run of the engine
 * does when it holds its lease: it announces its epoch, asks the table which batch the writer
 * is at, writes both down, and goes on from the batch after.
 *
 * '''The invariant''', on top of the criterion of the spike (no commit lost, none twice, no
 * other anomaly): '''no commit under an epoch is made once a later epoch of its key was
 * announced'''. The verifier holds it by what the processes wrote down: a commit under an
 * earlier epoch, of a batch beyond the one that the process of a later epoch was told by the
 * table, was taken behind the back of the process that asked. The epoch of a commit is the one
 * that its own file says, and it must be the one that the registry recorded, under the run of
 * the process that had that turn.
 *
 * '''The variants.'''
 *
 *   - ''The announcement that meets a commit in flight'', made to happen every time. The claim
 *     of the process that is to be taken over is held with its version taken and its
 *     transaction open: it has read the epoch in force, which was its own, and has an entry
 *     that nobody else sees yet. The next epoch is announced then. The announcement must wait
 *     for the claim, and the process that announced must then be told of that very batch.
 *     Nothing of the LogStore is changed for it: the hold is a trigger of the test's own in
 *     the database. With the lock taken out of the claim, the announcement does not wait, the
 *     process is told of the batch before, and this run reports `CommitAfterLaterEpoch`.
 *   - ''The takeovers as they come'', at the size of the natural race: each writer is taken
 *     over several times while all of them commit, at moments that nobody chose.
 *
 * Each run, with its table, journals, turns, logs and probes, stays in `target/stress/<run>/`,
 * next to its `report.md`.
 */
final class SpikeGreenEpochSpec extends AnyFunSuite with Matchers with GreenSpikeSuite {

  import SpikeGreenEpochSpec._

  /** How long the claim of a process that is taken over is held with its version taken. */
  private val Held = 1500.millis

  /**
   * What every commit of the run with the forced takeovers waits before it takes its version:
   * a process that is to take a writer over needs many seconds to start, and the writer must
   * still be committing when it has.
   */
  private val Paced = 1500.millis

  /** The commits of each writer in that run: more than it makes while it is taken over. */
  private val ForcedCommits = 50

  /** The writers of that run, in the order they are taken over: one of them twice. */
  private val ForcedOrder = Seq(0, 1, 2, 0)

  /** A writer is taken over while it has at least this many batches left to commit. */
  private val LeftToCommit = 3

  /** And once it has acknowledged this many. */
  private val CommittedBefore = 2

  /**
   * Has a process take `writer` over under its next epoch, while the one that has the turn is
   * committing, and waits for that one to stop.
   *
   * @param meetingACommit whether the announcement is made while a claim of the process that
   *   has the turn is held, with its version taken and not committed
   */
  private def takeOver(
      run: StressRun,
      chaos: Chaos,
      settings: StressHarness.Settings,
      writer: Int,
      meetingACommit: Boolean
  ): Takeover = {
    val before = chaos.epochOf(writer)
    // Not before it has committed: a process that is taken over at once was never a writer.
    if (chaos.acknowledged(writer).size < CommittedBefore)
      chaos.awaitAcknowledged(writer, CommittedBefore, within = 5.minutes)
    chaos.standBy(writer)
    val left = settings.commits - chaos.acknowledged(writer).size
    val (turn, inFlight) =
      if (!meetingACommit) chaos.takeOver(writer) -> None
      else {
        if (left < LeftToCommit)
          throw new HarnessFailure(
            s"${StressTable.writerId(writer)} had $left batches left when the process that " +
              "was to take it over could: too few to be taken over while it commits"
          )
        holdClaims(run.epochKey(writer), before.toLong, Held)
        try {
          awaitHeldClaim(within = 2.minutes)
          // The batch whose commit is held: the one after the last that the writer was told
          // of, since its write has not returned.
          val held = chaos.acknowledged(writer).size + 1L
          chaos.takeOver(writer) -> Some(held)
        } finally releaseClaims(run.epochKey(writer))
      }
    // The process that was taken over finds out at its next commit, and no sooner.
    val ended = chaos.awaitSuperseded(writer).find(_._1 == before).map(_._2)
    Takeover(
      writer,
      before,
      turn,
      ended.getOrElse(fail("the process taken over is not known")),
      inFlight
    )
  }

  /** What every run with takeovers must leave. */
  private def everyBatchOnceAndNoEpochBack(spiked: Spiked, commits: Int): Assertion = {
    val report = spiked.report
    isGreen(report)
    // By name, though `isGreen` has them: these are what the run is for.
    report.summary.lost shouldBe 0
    report.count("DuplicateCommit") shouldBe 0
    report.count("CommitAfterLaterEpoch") shouldBe 0
    report.count("EpochWentBack") shouldBe 0
    spiked.outcomes.map(_.exitCode).distinct shouldBe Seq(StressWriter.ExitCode.Ok)
    spiked.outcomes.map(_.journal.acked) shouldBe
      spiked.outcomes.map(_ => (1L to commits.toLong).toVector)
    report.summary.writerCommitsInLog shouldBe commits * spiked.outcomes.size
  }

  private def describe(takeovers: Seq[Takeover]): String =
    takeovers
      .map { t =>
        s"`${StressTable.writerId(t.writer)}` to epoch ${t.turn.epoch} (told batch " +
          s"${t.turn.told}, announcement ${t.turn.announceMillis} ms, the process before it " +
          s"exited with ${t.exitOfTheOneBefore})"
      }
      .mkString("; ")

  test(
    "an epoch announced while a commit of the one before is taken: that commit is found, no other made",
    StressTest
  ) {
    val takeovers = mutable.ArrayBuffer.empty[Takeover]
    val settings = coordinated(
      writers = 3,
      commits = ForcedCommits,
      alsoWith = Seq(delayAt(FaultPoint.BeforeInsert, Paced))
    ).copy(epochs = true)
    installClaimHold()

    val spiked = spikeRun(
      "epoch-forced",
      "Spike GREEN — PostgresLogStore, an epoch announced while a commit of the one before is taken",
      settings,
      chaos = (run, chaos) =>
        ForcedOrder.foreach { writer =>
          takeovers += takeOver(run, chaos, settings, writer, meetingACommit = true)
        }
    ) { (_, _) =>
      Seq(
        "Every process commits under an epoch of the key of its writer, and is taken over by a " +
          "process under the next while it commits. It is not killed, and it is told nothing: " +
          "it is a run that lost its lease and does not know it.",
        s"Each of the ${takeovers.size} takeovers is made at the worst moment. A trigger of the " +
          "test's own holds the claim of the process that has the turn for " +
          s"${Held.toMillis} ms, with its version taken and its transaction open: the commit " +
          "has read the epoch in force, which is its own, and has an entry that nobody else " +
          "sees yet. Seen there from outside, the next epoch is announced. The announcement " +
          "must wait for the claim, and the process that announced must then be told by the " +
          "table of the very batch that was in flight; the process that was taken over must " +
          "have its next commit refused. A claim that did not wait would let the announcement " +
          "through, and its commit would then be made behind the back of whoever had asked.",
        s"The takeovers: ${describe(takeovers.toSeq)}.",
        s"Every commit also waits ${Paced.toMillis} ms before it takes its version, so that a " +
          "writer is still committing when the process that takes it over has started."
      )
    }

    everyBatchOnceAndNoEpochBack(spiked, ForcedCommits)
    takeovers.map(_.writer).toSeq shouldBe ForcedOrder
    takeovers.foreach { takeover =>
      withClue(s"${StressTable.writerId(takeover.writer)} to epoch ${takeover.turn.epoch}: ") {
        // The announcement met the claim that was held, and waited for it.
        takeover.turn.announceMillis should be >= Held.toMillis / 3
        // What the table then said is the batch that was in flight when the epoch was
        // announced, one that its writer had not been told of yet, committed by the process
        // that was taken over under its own run: the question saw the commit.
        Some(takeover.turn.told) shouldBe takeover.inFlight
        val turnBefore = spiked.outcomes(takeover.writer).turns.find(_.epoch == takeover.before)
        commitOfBatch(spiked.run, takeover.writer, takeover.turn.told).flatMap(_.run) shouldBe
          turnBefore.map(_.run)
        // And that process committed nothing more: the LogStore refused it.
        takeover.exitOfTheOneBefore shouldBe StressWriter.ExitCode.OutOfTurn
      }
    }
    // One process for each turn, and the epoch in force is that of the last one.
    spiked.outcomes.map(_.processes.map(_.epoch)) shouldBe Seq(Seq(1, 2, 3), Seq(1, 2), Seq(1, 2))
    spiked.report.summary.epochs.map(_.inForce.values.toSeq.sorted) shouldBe Some(Seq(2L, 2L, 3L))
  }

  if (NaturalRuns > 0)
    test(
      s"takeovers as they come: $Writers writers, each taken over $TakeoversOfAWriter times while all commit",
      StressTest
    ) {
      val takeovers = mutable.ArrayBuffer.empty[Takeover]
      val settings = coordinated(Writers, Commits).copy(
        epochs = true,
        // A run of the size of CI takes far longer than the harness waits by default.
        runTimeout = 15.minutes.max(Commits * TimeForACommit)
      )

      val spiked = spikeRun(
        "epoch-natural",
        s"Spike GREEN — PostgresLogStore, $Writers writers under epochs, taken over as they commit",
        settings,
        chaos = (run, chaos) =>
          for (_ <- 1 to TakeoversOfAWriter; writer <- 0 until Writers)
            // A writer that is done has nothing left to be taken over at.
            if (chaos.isAlive(writer) && chaos.acknowledged(writer).size < Commits)
              takeovers += takeOver(run, chaos, settings, writer, meetingACommit = false)
      ) { (_, _) =>
        val refused = takeovers.count(_.exitOfTheOneBefore == StressWriter.ExitCode.OutOfTurn)
        Seq(
          s"No fault injection: $Writers writers, $Commits commits each, every process under an " +
            "epoch of the key of its writer. One writer after another is taken over by a " +
            s"process under its next epoch, $TakeoversOfAWriter times each, while all of them " +
            "commit: the process that had the turn goes on until the LogStore refuses it.",
          s"${takeovers.size} takeovers were made, and $refused of the processes that were " +
            "taken over had a commit refused for their epoch; the others had no batch left " +
            s"that the table did not have already. The takeovers: ${describe(takeovers.toSeq)}.",
          "This is the weak evidence, as the natural race is: the moment at which an " +
            "announcement meets a claim in flight is a fraction of a millisecond wide. It is " +
            "made to happen every time in the run next to this one, and in " +
            "`WriterEpochRaceSpec`. What this run adds is the epochs under the load of the " +
            "natural race, with every commit of every writer held to one."
        )
      }

      everyBatchOnceAndNoEpochBack(spiked, Commits)
      // A process that was taken over stopped because it was refused, or because it was done.
      takeovers
        .map(_.exitOfTheOneBefore)
        .toSet
        .subsetOf(
          Set(StressWriter.ExitCode.OutOfTurn, StressWriter.ExitCode.Ok)
        ) shouldBe true
      // The run tested something: writers were taken over, and the LogStore refused some.
      takeovers.size should be >= Writers
      takeovers.count(_.exitOfTheOneBefore == StressWriter.ExitCode.OutOfTurn) should be >= 1
      info(s"takeovers: ${describe(takeovers.toSeq)}")
    }
}

object SpikeGreenEpochSpec {

  /**
   * One takeover of a writer.
   *
   * @param before the epoch of the process that had the turn
   * @param turn what the process that took over wrote down of its own
   * @param exitOfTheOneBefore how the process that was taken over ended
   * @param inFlight the batch whose commit was held when the epoch was announced, for a
   *   takeover that was made to meet one
   */
  final case class Takeover(
      writer: Int,
      before: Int,
      turn: Turn,
      exitOfTheOneBefore: Int,
      inFlight: Option[Long] = None
  )

  /** The environment variable with how many times each writer is taken over as it comes. */
  val TakeoversVariable = "STRESS_TAKEOVERS"

  /** Whether the run with the takeovers as they come is made: as the natural runs are. */
  val NaturalRuns: Int = SpikeGreenSpec.NaturalRuns

  /** The size of that run: the one of a natural run (`SpikeGreenSpec`). */
  val Writers: Int = SpikeGreenSpec.NaturalWriters
  val Commits: Int = SpikeGreenSpec.NaturalCommits

  val TakeoversOfAWriter: Int =
    sys.env.get(TakeoversVariable).flatMap(_.toIntOption).filter(_ > 0).getOrElse(2)

  /** What the run is given for each commit of a writer before the harness gives it up. */
  private val TimeForACommit = 15.seconds
}
