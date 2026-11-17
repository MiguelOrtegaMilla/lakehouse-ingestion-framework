package lakehouse.ingestion.it.stress

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.{Random, Using}

import lakehouse.ingestion.it.stress.CoordinatedStressSuite.BatchCommit
import lakehouse.ingestion.it.stress.GreenSpikeSuite.Spiked
import lakehouse.ingestion.it.stress.SpikeGreenKillSpec.Side
import lakehouse.ingestion.it.stress.StressHarness.WriterOutcome
import lakehouse.ingestion.it.stress.WriterKiller.{InFlight, Killed}
import lakehouse.ingestion.it.support.StressTest
import lakehouse.ingestion.logstore.FaultPoint
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Spike GREEN, the writers that die (ADR-0004 §7.2): a writer is killed with `kill -9` inside a
 * commit, and a process is started in its place that repeats the batch that was in flight, as
 * the engine repeats a batch whose outcome it does not know.
 *
 * The widened window of `SpikeGreenSpec` shows that writers which are all alive cannot lose a
 * commit to each other. What is left is a writer that leaves a commit half made. The protocol
 * has three points where a death leaves something different behind, and a writer is killed at
 * each (`WriterKiller`), from outside and running the LogStore that is deployed:
 *
 *   - '''before the `INSERT`''': the commit is prepared and no version is taken. The commit
 *     never happened; the batch must not be in the table until the writer is started again,
 *     and then once;
 *   - '''after the `INSERT`''': the version is taken and its commit is not in the log. The
 *     commit happened; the other writers must publish it, and the writer that is started
 *     again must find its batch in the table and write nothing;
 *   - '''after the publish''': the commit is in the log and its entry is not acknowledged.
 *     The same, and somebody else acknowledges it.
 *
 * Each run is held to the criterion of the spike, and every batch of every writer must be in
 * the table exactly once. Whose commit the batch in flight turned out to be is read from the
 * registry: the run of the process that was killed, at the version it was seen to take, or
 * the run of the one started in its place.
 *
 * The writers that are alive finish the commit of a dead one as soon as they look at the
 * table, which may be before the orchestrator reads what the dead one left. So each run
 * reports both what was seen when the kill was decided and what was read after it, and holds
 * the time between the two well inside the time the victim is held for: the victim did not
 * finish the commit itself.
 *
 * A last run kills writers several times at moments nobody chose, anywhere in a batch. Each
 * victim is a process that is committing: a first version of the run drew its moments from the
 * start, and every kill fell on a process that was still starting and had written nothing.
 */
final class SpikeGreenKillSpec extends AnyFunSuite with Matchers with GreenSpikeSuite {

  private val Writers = 3
  private val Commits = 8

  /** The writer that is killed, in the runs that kill one at a chosen point. */
  private val Victim = 0

  /**
   * How long the victim is held at the point of each of its commits: long enough to be seen
   * there from outside and killed, short enough to get through the commits before.
   */
  private val Held = 3.seconds

  /** The victim has made this many commits when it is killed: it dies in the next one. */
  private val CommitsBeforeTheKill = 2

  /** Kills the victim at `point`, starts a process in its place, and runs to the end. */
  private def killedAt(point: FaultPoint, variant: String, title: String, what: String)(
      found: Killed => String
  ): (Spiked, Killed) = {
    var killed = Option.empty[Killed]
    val settings = coordinated(Writers, Commits).copy(
      alsoFor = Map(Victim -> Seq(delayAt(point, Held)))
    )
    val spiked = spikeRun(
      variant,
      title,
      settings,
      chaos = (run, chaos) =>
        Using.resource(killerOf(run)) { killer =>
          killed = Some(killer.killAt(chaos, Victim, point, CommitsBeforeTheKill))
          chaos.restart(Victim)
        }
    ) { (_, _) =>
      val kill = killed.getOrElse(fail("no writer was killed"))
      Seq(
        s"`${StressTable.writerId(Victim)}` runs with the fault hook `${point.key}` set to " +
          s"${Held.toMillis} ms, which holds it $what in each of its commits. It is the " +
          "LogStore that is deployed, loaded by name; nothing in the writer knows it will die.",
        s"The orchestrator looked at the registry and at the log directory from outside. Once " +
          s"the writer had acknowledged ${kill.acknowledged} batches it saw " +
          s"${kill.seen.fold("nothing")(_.describe)}, and killed process ${kill.pid} with " +
          s"`kill -9`: the process was gone ${kill.goneAfter.toMillis} ms after that look began.",
        s"Read right after the process was gone: ${kill.entriesOfProcess} versions taken by " +
          s"it, for ${kill.acknowledged} batches acknowledged; ${kill.left}. ${found(kill)}",
        s"A process was then started in its place, which repeated batch ${kill.batchInFlight} " +
          "and went on to the end."
      )
    }
    (spiked, killed.getOrElse(fail("no writer was killed")))
  }

  /** What every run with a kill must leave, whatever the point. */
  private def everyBatchOnce(spiked: Spiked, kills: Seq[Int]): Assertion = {
    isGreen(spiked.report)
    spiked.outcomes.map(_.exitCode) shouldBe Seq.fill(Writers)(StressWriter.ExitCode.Ok)
    spiked.outcomes.map(_.journal.acked.size) shouldBe
      Seq.fill(Writers)(spiked.report.summary.acknowledged / Writers)
    spiked.report.summary.writerCommitsInLog shouldBe spiked.report.summary.acknowledged
    spiked.outcomes.map(_.kills) shouldBe kills
  }

  /**
   * The version the victim was seen to have taken when the kill was decided, with its commit
   * in the log or not, as the point says. It died there: well inside the time it was held for.
   */
  private def takenWhenKilled(kill: Killed, published: Boolean): Long = {
    kill.goneAfter should be < Held / 2
    kill.seen match {
      case Some(InFlight(version, `published`)) => version
      case other => fail(s"the victim was killed on seeing $other")
    }
  }

  /** Who finished the commit that the victim died in, as the read after the kill tells. */
  private def finishedBy(kill: Killed, version: Long): String =
    kill.unfinished.find(_.version == version) match {
      case Some(InFlight(_, false)) =>
        "The commit existed, and was not in the log: whoever looked at the table next had to " +
          "publish it."
      case Some(InFlight(_, true)) =>
        "The commit was in the log, and its entry not complete: whoever looked at the table " +
          "next had to complete it."
      case None =>
        "Its entry was complete by then: another process had finished the commit in the " +
          "milliseconds between the look and the read. The victim, held, could not have."
    }

  /** The runs of the victim: of the process that was killed, and of the one in its place. */
  private def runsOfVictim(spiked: Spiked): (String, String) = {
    val victim: WriterOutcome = spiked.outcomes(Victim)
    victim.runs should have size 2
    (victim.runs.head, victim.runs(1))
  }

  test(
    "a writer killed before its INSERT made no commit, and its batch is written once",
    StressTest
  ) {
    val (spiked, kill) = killedAt(
      FaultPoint.BeforeInsert,
      "killed-before-insert",
      "Spike GREEN — PostgresLogStore, a writer killed before the INSERT",
      "between its look at the log and the `INSERT` that takes a version, with its commit ready"
    ) { _ =>
      "No version was taken for the batch in flight: the commit never happened."
    }

    everyBatchOnce(spiked, kills = Seq(1, 0, 0))
    // No version was taken for the batch in flight, and no entry of the process is unfinished.
    kill.acknowledged shouldBe CommitsBeforeTheKill
    kill.entriesOfProcess shouldBe kill.acknowledged
    kill.unfinished shouldBe empty
    // The commit it had ready stayed where it was, and belongs to nobody.
    kill.preparedLeft should not be empty
    // So the batch is in the table by the process that was started afterwards.
    val (_, inItsPlace) = runsOfVictim(spiked)
    commitOfBatch(spiked.run, Victim, kill.batchInFlight).flatMap(_.run) shouldBe Some(inItsPlace)
  }

  test("a writer killed after its INSERT made a commit, which others publish, once", StressTest) {
    val (spiked, kill) = killedAt(
      FaultPoint.AfterInsert,
      "killed-after-insert",
      "Spike GREEN — PostgresLogStore, a writer killed after the INSERT",
      "between the `INSERT` that takes its version and the publication of its commit"
    )(kill => kill.seen.collect { case InFlight(version, _) => finishedBy(kill, version) }.mkString)

    everyBatchOnce(spiked, kills = Seq(1, 0, 0))
    val taken = takenWhenKilled(kill, published = false)
    // One version taken more than batches acknowledged: the batch in flight was a commit.
    kill.acknowledged shouldBe CommitsBeforeTheKill
    kill.entriesOfProcess shouldBe kill.acknowledged + 1
    // It is in the table at that version, as the commit of the process that died. The process
    // started in its place repeated the batch, found it there, and wrote nothing.
    val (killed, _) = runsOfVictim(spiked)
    commitOfBatch(spiked.run, Victim, kill.batchInFlight) shouldBe
      Some(BatchCommit(taken, Some(killed)))
  }

  test("a writer killed after its publish made a commit, which others acknowledge", StressTest) {
    val (spiked, kill) = killedAt(
      FaultPoint.AfterPublish,
      "killed-after-publish",
      "Spike GREEN — PostgresLogStore, a writer killed after the publish",
      "between the publication of its commit and the acknowledgement of its entry"
    )(kill => kill.seen.collect { case InFlight(version, _) => finishedBy(kill, version) }.mkString)

    everyBatchOnce(spiked, kills = Seq(1, 0, 0))
    val taken = takenWhenKilled(kill, published = true)
    kill.acknowledged shouldBe CommitsBeforeTheKill
    kill.entriesOfProcess shouldBe kill.acknowledged + 1
    val (killed, _) = runsOfVictim(spiked)
    commitOfBatch(spiked.run, Victim, kill.batchInFlight) shouldBe
      Some(BatchCommit(taken, Some(killed)))
  }

  /**
   * On which side of its `INSERT` a process died, as the registry tells after the run: the
   * commit of the batch that was in flight is put down to the run of the dead process, which
   * had then taken its version, or to the run of a process started later.
   */
  private def sideOfTheInsert(run: StressRun, kill: Killed): Side =
    commitOfBatch(run, kill.writer, kill.batchInFlight) match {
      case Some(BatchCommit(version, by)) if by.nonEmpty && by == kill.run =>
        Side.After(kill.batchInFlight, version)
      case Some(BatchCommit(version, _)) => Side.Before(kill.batchInFlight, version)
      case None => Side.NoBatch
    }

  test(
    "writers killed at random moments of their commits: every batch is in the table once",
    StressTest
  ) {
    val seed = System.nanoTime()
    val random = new Random(seed)
    val commits = 50
    val kills = 6
    // A writer is not chosen with fewer batches left: it would be done before it is killed.
    val batchesLeft = 8
    // Longer than a batch takes, from its Spark job to its acknowledgement.
    val anywhereWithin = 2.seconds
    val killed = mutable.ArrayBuffer.empty[Killed]
    var stressRun = Option.empty[StressRun]

    val spiked = spikeRun(
      "killed-at-random",
      "Spike GREEN — PostgresLogStore, writers killed at random moments",
      coordinated(Writers, commits),
      chaos = (run, chaos) =>
        Using.resource(killerOf(run)) { killer =>
          stressRun = Some(run)
          (1 to kills).foreach { _ =>
            val candidates = (0 until Writers).filter { writer =>
              chaos.isAlive(writer) && chaos.acknowledged(writer).size + batchesLeft <= commits
            }
            if (candidates.nonEmpty) {
              val writer = candidates(random.nextInt(candidates.size))
              // The process is committing, not starting: it acknowledges two or three batches
              // more, of which only the first can be one that it found in the table.
              chaos.awaitAcknowledged(
                writer,
                chaos.acknowledged(writer).size + 2 + random.nextInt(2)
              )
              Thread.sleep(random.nextInt(anywhereWithin.toMillis.toInt).toLong)
              killed += killer.kill(chaos, writer)
              chaos.restart(writer)
            }
          }
        }
    ) { (_, _) =>
      val run = stressRun.getOrElse(fail("the run did not reach its writers"))
      Seq(
        s"No fault hook: three writers, $commits batches each, and $kills kills with " +
          s"`kill -9`, each followed by a process started in the writer's place. Each kill " +
          "takes a writer at random, waits until its process has acknowledged two or three " +
          "batches more, so that it is committing and not starting, and then for a time drawn " +
          s"at random under ${anywhereWithin.toMillis} ms, which is longer than a batch takes " +
          s"(seed $seed).",
        "A kill that lands inside the few milliseconds of a commit is rare; the runs that " +
          "kill at a chosen point are the ones that put a death there. What this run adds is " +
          "deaths anywhere in a batch: in the Spark job that writes the data file, in Delta's " +
          "preparation of the commit, between two batches.",
        "What each process left is what the registry puts down to it, read right after it " +
          "was gone. The side of the `INSERT` it died on is read after the run, from the run " +
          "that the commit of its batch in flight is put down to."
      ) ++ killed.map { kill =>
        s"- `${StressTable.writerId(kill.writer)}`, process ${kill.pid}, after " +
          s"${kill.acknowledged} batches acknowledged and ${kill.entriesOfProcess} versions " +
          s"taken by that process: ${kill.left}. ${sideOfTheInsert(run, kill).describe}"
      }
    }

    val sides = killed.map(sideOfTheInsert(spiked.run, _))
    info(
      s"seed $seed; ${sides.count(_.isInstanceOf[Side.After])} of ${killed.size} processes " +
        s"died after their INSERT; they left: ${killed.map(_.left).mkString("; ")}"
    )
    killed should have size kills.toLong
    // Every kill fell on a process that was committing: it had taken versions of its own,
    // and had a batch in flight, which is in the table.
    killed.foreach(_.entriesOfProcess should be >= 1)
    sides should not contain Side.NoBatch
    val killsOf = (0 until Writers).map(writer => killed.count(_.writer == writer))
    everyBatchOnce(spiked, killsOf)
  }
}

object SpikeGreenKillSpec {

  /** On which side of its `INSERT` a killed process died, for the batch that was in flight. */
  sealed private trait Side {
    def describe: String
  }

  private object Side {

    final case class Before(batch: Long, version: Long) extends Side {
      override def describe: String =
        s"It died before its `INSERT`: batch $batch is version $version, a commit of a " +
          "process started later."
    }

    final case class After(batch: Long, version: Long) extends Side {
      override def describe: String =
        s"It died after its `INSERT`: batch $batch is version $version, a commit of the " +
          "dead process, and the process started in its place wrote nothing for it."
    }

    case object NoBatch extends Side {
      override def describe: String = "It had no batch in flight."
    }
  }
}
