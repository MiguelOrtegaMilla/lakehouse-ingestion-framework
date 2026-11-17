package lakehouse.ingestion.it.stress

import scala.concurrent.duration._

import lakehouse.ingestion.it.stress.CoordinatedStressSuite.{Look, Outage}
import lakehouse.ingestion.it.stress.GreenSpikeSuite.Spiked
import lakehouse.ingestion.it.stress.StressHarness.{Settings, WriterOutcome}
import lakehouse.ingestion.it.support.StressTest
import lakehouse.ingestion.logstore.coordination.CoordinationSettings
import lakehouse.ingestion.logstore.{FaultPoint, RetryPolicy}
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Spike GREEN, the coordination database that dies (ADR-0004 §7.2): PostgreSQL is killed in the
 * middle of a run, as `docker kill` does, and started again some seconds later.
 *
 * While it is down no version can be taken, so no commit can be made: every attempt fails
 * (fail closed, F5). What a writer does then is the engine's part, and here the writer does it:
 * it repeats the batch with its `txnVersion` until it is in the table. A failed attempt says
 * nothing of whether the commit was made, since the kill may have cut the reply to an `INSERT`
 * that was done, and Delta skips a batch that is in the table.
 *
 * PostgreSQL is killed, not stopped. It gets no time to shut down or to write a checkpoint, and
 * recovers from its write-ahead log when it is started. What it had confirmed before the kill
 * must be there afterwards: a version that a writer was told it had taken, and that the
 * database then forgot, would be taken again by another writer.
 *
 * Two runs:
 *
 *   - '''in a natural run''': the kill finds the writers wherever they are;
 *   - '''with a version taken and not published''': every writer is held after its `INSERT`,
 *     and PostgreSQL is killed when the registry shows a version whose commit is not in the
 *     log. That commit exists in the coordination database alone, which is the one that dies.
 *
 * Each run is held to the criterion of the spike, with every batch in the table exactly once.
 * It must also show that the outage was in the way of every writer: each repeated a batch, and
 * none before the kill. No version of the registry may be gone after the restart.
 *
 * '''The outage is longer than the LogStore waits.''' A writer waits for a connection for the
 * connect timeout, five seconds, and the LogStore tries the `INSERT` that takes a version three
 * times. An outage shorter than that is waited out inside a commit and no batch fails: with
 * ten seconds down, the writer that held the version saw no failure at all. Thirty seconds are
 * longer than that wait on every path.
 */
final class SpikeGreenOutageSpec extends AnyFunSuite with Matchers with GreenSpikeSuite {

  private val Writers = 3

  /**
   * From the kill to the moment PostgreSQL is started again: longer than the LogStore waits
   * for the database on any path, so that every writer sees a batch fail.
   */
  private val Down = 30.seconds

  /** Far longer than the outage: a writer that gives a batch up has not recovered. */
  private val RepeatFor = 2.minutes

  /** The delay of the widened window: a writer holds its version for this long. */
  private val Held = 1500.millis

  private def repeating(commits: Int, alsoWith: Seq[String] = Nil): Settings =
    coordinated(Writers, commits, alsoWith).copy(repeatFor = RepeatFor)

  private def seconds(duration: FiniteDuration): String = f"${duration.toMillis / 1000.0}%.1f s"

  /** What an outage was, for the report of its run. */
  private def told(outage: Outage, outcomes: Seq[WriterOutcome]): Seq[String] = {
    val repeated = outcomes.map(_.journal.repeated)
    val errors = repeated.flatten
      .groupMapReduce(_.error.takeWhile(_ != ':'))(_ => 1)(_ + _)
      .toSeq
      .sortBy { case (error, count) => (-count, error) }
      .map { case (error, count) => s"`$error` $count times" }
    val gone =
      if (outage.gone.isEmpty) "none was gone"
      else s"versions ${outage.gone.mkString(", ")} WERE GONE"
    Seq(
      "PostgreSQL ran with " +
        outage.durability.map { case (name, value) => s"`$name = $value`" }.mkString(", ") +
        ". It was killed as `docker kill` does, with no shutdown and no checkpoint, and " +
        s"started again ${Down.toSeconds} s later, where it recovered from its write-ahead " +
        s"log: ${seconds(outage.down)} from the kill to the first connection it accepted.",
      s"At the last look before the kill the registry had ${outage.before.describe}. At the " +
        s"first look after the restart it had ${outage.after.describe}. Of the versions that " +
        s"had an entry before the kill, $gone.",
      s"The writers had acknowledged ${outage.acknowledgedAtKill.mkString(", ")} batches at " +
        s"the kill, and ${outage.acknowledgedWhileDown.mkString(", ")} more while PostgreSQL " +
        "was down. A writer whose version was taken before the kill needs no database to " +
        "publish its commit. Whether its batch is acknowledged then depends on what Delta " +
        "reads after a commit, so the number is told and not required.",
      s"Attempts that failed and were repeated, each with the `txnVersion` of its batch: " +
        s"${repeated.map(_.size).mkString(", ")}. Their errors: ${errors.mkString(", ")}.",
      s"A writer waits up to ${CoordinationSettings.DefaultConnectTimeout.toSeconds} s for a " +
        "connection to the coordination database, and the LogStore tries the `INSERT` that " +
        s"takes a version up to ${RetryPolicy.Default.attempts} times. An outage shorter " +
        "than that is waited out inside a commit, and no batch fails. This one is longer: it " +
        "reaches the writer as a batch that failed, after that wait each time, and it is " +
        "the writer that repeats the batch."
    )
  }

  /**
   * The batch whose commit was `version`, taken before the kill, and what its writer saw of it.
   * An attempt at that batch that failed did so after the commit was made: the writer was
   * holding the version when PostgreSQL died.
   */
  private def fateOf(run: StressRun, version: Long, outcomes: Seq[WriterOutcome]): String =
    LogReader
      .logView(spark, run.tableDir)
      .commits
      .find(_.version == version)
      .toSeq
      .flatMap(_.txns) match {
      case Seq(LogModel.Txn(writer, batch)) =>
        val failed = outcomes
          .filter(outcome => StressTable.writerId(outcome.writer) == writer)
          .flatMap(_.journal.repeated)
          .count(_.txnVersion == batch)
        if (failed > 0)
          s"Version $version is batch $batch of `$writer`. Its writer saw $failed attempts at " +
            "that batch fail, the first of them after the commit was made, and repeated " +
            "the batch: it is in the table once, at the version taken before the kill."
        else
          s"Version $version is batch $batch of `$writer`, which saw no attempt at that " +
            "batch fail: it published the commit without the database."
      case other => s"Version $version is not the commit of one batch: $other."
    }

  /** What a run with an outage must leave. */
  private def survived(spiked: Spiked, outage: Outage, commits: Int): Assertion = {
    isGreen(spiked.report)
    spiked.outcomes.map(_.exitCode) shouldBe Seq.fill(Writers)(StressWriter.ExitCode.Ok)
    spiked.outcomes.map(_.journal.acked.size) shouldBe Seq.fill(Writers)(commits)
    spiked.report.summary.writerCommitsInLog shouldBe Writers * commits
    // PostgreSQL forgot nothing that it had confirmed.
    outage.gone shouldBe empty
    // The outage was in the way of every writer: each repeated a batch, and none of them a
    // batch from before the kill.
    spiked.outcomes.zip(outage.acknowledgedAtKill).foreach { case (outcome, atKill) =>
      withClue(s"${StressTable.writerId(outcome.writer)}: ") {
        outcome.journal.repeated should not be empty
        outcome.journal.repeated.map(_.txnVersion).min should be > atKill.toLong
      }
    }
    succeed
  }

  test("PostgreSQL killed in the middle of a run: every batch is in the table once", StressTest) {
    val commits = 30
    val batchesBeforeTheKill = 5
    var outage = Option.empty[Outage]

    val spiked = spikeRun(
      "postgres-killed",
      "Spike GREEN — PostgresLogStore, PostgreSQL killed in the middle of a run",
      repeating(commits),
      chaos = (run, chaos) => {
        (0 until Writers).foreach(chaos.awaitAcknowledged(_, batchesBeforeTheKill))
        outage = Some(killPostgres(run, chaos, Writers, Down, look(run)))
      }
    ) { (outcomes, _) =>
      Seq(
        s"No fault hook: three writers, $commits batches each. PostgreSQL was killed once " +
          s"every writer had acknowledged $batchesBeforeTheKill batches, wherever each of " +
          "them was by then. A writer repeats a batch that fails, with its `txnVersion`, " +
          "as the engine is to repeat a batch whose outcome it does not know."
      ) ++ told(outage.getOrElse(fail("PostgreSQL was not killed")), outcomes)
    }

    survived(spiked, outage.getOrElse(fail("PostgreSQL was not killed")), commits)
  }

  test(
    "PostgreSQL killed with a version taken and not published: the commit is in the table",
    StressTest
  ) {
    val commits = 20
    val batchesBeforeTheKill = 3
    var seen = Option.empty[Look]
    var outage = Option.empty[Outage]
    var stressRun = Option.empty[StressRun]

    val spiked = spikeRun(
      "postgres-killed-version-taken",
      "Spike GREEN — PostgresLogStore, PostgreSQL killed with a version taken and not published",
      repeating(commits, alsoWith = Seq(delayAt(FaultPoint.AfterInsert, Held))),
      chaos = (run, chaos) => {
        (0 until Writers).foreach(chaos.awaitAcknowledged(_, batchesBeforeTheKill))
        val taken = awaitTakenAndNotPublished(run, within = 1.minute)
        seen = Some(taken)
        stressRun = Some(run)
        outage = Some(killPostgres(run, chaos, Writers, Down, taken))
      }
    ) { (outcomes, _) =>
      val taken =
        seen.getOrElse(fail("no version was seen taken")).unfinished.filterNot(_.published)
      Seq(
        s"Every writer runs with the fault hook `afterInsert` set to ${Held.toMillis} ms, " +
          "which holds it between the `INSERT` that takes its version and the publication " +
          s"of its commit. PostgreSQL was killed when the registry showed " +
          s"${taken.map(e => s"version ${e.version}").mkString(", ")} taken and its commit " +
          "not in the log: a commit that existed in the coordination database alone, and " +
          "that database is the one that died."
      ) ++ told(outage.getOrElse(fail("PostgreSQL was not killed")), outcomes) ++
        stressRun.toSeq.flatMap(run => taken.map(e => fateOf(run, e.version, outcomes)))
    }

    val found = outage.getOrElse(fail("PostgreSQL was not killed"))
    survived(spiked, found, commits)
    // The version that only the database knew of is known to it after its recovery. Its commit
    // is then in the log and its entry complete, as the criterion says of every entry.
    val taken = seen.toSeq.flatMap(_.unfinished).filterNot(_.published).map(_.version)
    taken should not be empty
    found.after.versions should contain allElementsOf taken
    taken.foreach(version => info(fateOf(spiked.run, version, spiked.outcomes)))
  }
}
