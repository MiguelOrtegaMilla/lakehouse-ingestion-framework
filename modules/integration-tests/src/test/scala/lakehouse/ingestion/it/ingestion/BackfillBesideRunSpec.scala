package lakehouse.ingestion.it.ingestion

import java.nio.file.Path
import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.Using

import io.delta.tables.DeltaTable
import lakehouse.ingestion.cli.Main
import lakehouse.ingestion.core.config.{EngineColumns, RootUri, Source}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.control.{Backfills, Leases}
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.{CommandProcess, DeploymentFiles}
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A backfill beside the incremental load of its source, each in a process of its own, on the
 * same table of Bronze at the same time (discovery §6.2, the second type of concurrency; ADR-0021
 * §5.7, P7.8): the command `run` and the command `backfill`, started together as an orchestrator
 * and an operator would start them.
 *
 * The table of the source does not change while they run: what is asked is what two sequences
 * do to one table of Bronze, and a table that changes is P6's. Its rows are stamped every twelve
 * hours, from before the range of the backfill to the present. The regular sequence starts at
 * `S`, a window of a day a batch; the backfill reads `(F, T]`, a day a chunk, with `F < S < T`.
 * So each row is in Bronze once for each sequence that reads it, and for no other: once from the
 * backfill if it is in `(F, T]`, once from the regular sequence if it is after `S`. The row at
 * `F` is in neither, the one at `S` is the backfill's alone, and the one at `T` is in both.
 *
 * '''The two invocations are to have been running at one time''', by the clock of the control
 * plane, as in `FanInSpec`: a round whose invocations were not is made again, on a source of its
 * own, up to three times, and is held to everything else all the same. Whether their commits met
 * is said, and not asked.
 *
 * '''The process of the backfill is killed with -9''' in the middle of a chunk, in the second
 * test, and the backfill is run again by the means a deployment has: at once, and skipped, for
 * the run that died holds its lease; and once that lease has run out, by itself, to the end. A
 * kill that comes between two chunks leaves no lease held, and its round is made again. What the
 * killed run had done of its chunk is the timing's: it is said, and the faults of the engine one
 * at a time are P7.9's.
 *
 * Each test ends with the ledger of both sequences, read from the lake of the deployment.
 */
final class BackfillBesideRunSpec extends AnyFunSuite with Matchers with JdbcIngestionSuite {

  import BackfillBesideRunSpec._

  // The lake that the suite reads is the one that the processes write.
  private lazy val files = new DeploymentFiles(pg, workDir)
  override protected lazy val lakeRoot: RootUri = RootUri.parse(files.lake.toUri.toString).value
  override protected lazy val landingDirectory: Path = files.landing

  /**
   * The profile of the processes of the test that kills one: the lease of a run that died is
   * waited out in `MortalTtl`, and not in the two minutes of a deployment. A lease is to outlast
   * three renewals that take as long as they may, each a connection to the control plane and an
   * answer (`Profile.rules`): the connection is waited for a second, and not five.
   */
  private lazy val mortal: Path = files.profileFile(
    control = files.connection(Engine, pg.password(Engine), connect = "1s"),
    leases = Some(s"${MortalTtl.toSeconds}s")
  )

  /** The moments of a round, by the clock of the database (`Moments`). */
  private def moments(): Moments = {
    val start = databaseNow().truncatedTo(ChronoUnit.DAYS).minus(10, ChronoUnit.DAYS)
    Moments(start.minus(20, ChronoUnit.DAYS), start, start.plus(5, ChronoUnit.DAYS))
  }

  /**
   * A source of a table of its own, its rows stamped every twelve hours from a day before `F`
   * to two hours ago: at `F`, `S` and `T` exactly among them.
   *
   * @return the source, and the moment of each of its rows by its id
   */
  private def history(at: Moments): (Source, Map[Int, Instant]) = {
    val id = newSourceId()
    val table = id.value.replace('.', '_')
    val until = databaseNow().minus(2, ChronoUnit.HOURS)
    val stamps = Iterator
      .iterate(at.from.minus(1, ChronoUnit.DAYS))(_.plus(12, ChronoUnit.HOURS))
      .takeWhile(!_.isAfter(until))
      .zipWithIndex
      .map { case (stamp, i) => (i + 1) -> stamp }
      .toMap
    asOwner { c =>
      execute(c, s"CREATE TABLE $table (id int PRIMARY KEY, updated_at timestamptz NOT NULL)")
      Using.resource(c.prepareStatement(s"INSERT INTO $table VALUES (?, ?)")) { statement =>
        stamps.foreach { case (row, stamp) =>
          statement.setInt(1, row)
          statement.setObject(2, OffsetDateTime.ofInstant(stamp, ZoneOffset.UTC))
          statement.executeUpdate(): Unit
        }
      }
    }
    val source = files.tableSource(
      id,
      s"public.$table",
      files.connection("sales_ro", pg.password("sales_ro"), url = pg.jdbcUrl("sales")),
      load = "{ mode: incremental, watermark: { column: updated_at, safety_margin: 1h, " +
        s"""start: "${at.start}" }, max_batch_range: 1d }"""
    )
    (source, stamps)
  }

  private def regular(of: Source, profile: Path = files.validProfile): CommandProcess =
    CommandProcess.start(files.arguments(of, profile), files.newLog(), Heap)

  private def backfill(
      of: Source,
      at: Moments,
      profile: Path = files.validProfile
  ): CommandProcess =
    CommandProcess.start(
      Seq(
        "backfill",
        "--profile",
        profile.toString,
        "--sources",
        files.sources.toString,
        "--source",
        of.id.value,
        "--backfill",
        History.value,
        "--from",
        at.from.toString,
        "--to",
        at.to.toString,
        "--chunk",
        "1d",
        "--reason",
        "the history before start"
      ),
      files.newLog(),
      Heap
    )

  /** Each run of a source, with the sequence that it acted on. */
  private def runsOf(of: Source): Seq[RunRecord] = asSuperuser { c =>
    query(c, "SELECT run_id::text FROM control.ingestion_run WHERE source_id = ?", of.id.value)
      .map(row => runOf(RunId.parse(row.head).toOption.get))
  }

  private def sequenceOf(run: RunRecord): String =
    if (run.start.backfill.nonEmpty) Backfill else Regular

  /** From the first start to the last end of the runs of one sequence. */
  private def spanOf(runs: Seq[RunRecord]): (Instant, Instant) =
    (runs.map(_.startedAt).min, runs.flatMap(_.finishedAt).max)

  /** The sequences that have each row in Bronze, by its id, each copy told by its run. */
  private def copiesOf(of: Source): Map[Int, Seq[String]] = {
    val sequences = runsOf(of).map(run => run.start.id.toString -> sequenceOf(run)).toMap
    spark.read
      .format("delta")
      .load(bronze(of).uri)
      .select("id", EngineColumns.RunId)
      .collect()
      .toSeq
      .groupMap(_.getInt(0))(row => sequences(row.getString(1)))
      .view
      .mapValues(_.sorted)
      .toMap
  }

  /** Each row is in Bronze once for each sequence that reads it, and for no other. */
  private def assertCopies(of: Source, at: Moments, stamps: Map[Int, Instant]): Unit = {
    val expected = stamps.collect {
      case (row, stamp) if at.readers(stamp).nonEmpty => row -> at.readers(stamp).toSeq.sorted
    }
    copiesOf(of) shouldBe expected
    expected.values.count(_.size == 2) should be > 0: Unit
  }

  /** How many rows of Bronze a run wrote, by the column of their run. */
  private def writtenBy(of: Source, run: RunId): Long =
    spark.read
      .format("delta")
      .load(bronze(of).uri)
      .filter(col(EngineColumns.RunId) === run.toString)
      .count()

  /** How many times, in the log of the table, a commit of one sequence follows one of the other. */
  private def turnsOf(of: Source): Int = {
    val app = TxnAppId.backfill(of.id, Layer.Bronze, History).value
    val inTurn = DeltaTable
      .forPath(spark, bronze(of).uri)
      .history()
      .select("version", "userMetadata")
      .collect()
      .toSeq
      .sortBy(_.getLong(0))
      .map(row => Option(row.getString(1)).exists(_.contains(app)))
    inTurn.zip(inTurn.drop(1)).count { case (one, next) => one != next }
  }

  private def recordOf(of: Source): Option[BackfillRecord] =
    database.run(Backfills.find(_, of.id, Layer.Bronze, History))

  /** The backfill is complete, with its progress at its last chunk. */
  private def assertComplete(of: Source, chunks: Long): Unit = {
    val record = recordOf(of).value
    record.completedAt should not be empty
    record.progress.value.lastCommitted.value shouldBe chunks: Unit
  }

  private def backfillLease(of: Source): LeaseKey =
    LeaseKey.backfill(of.id, Layer.Bronze, History)

  private def holderOfBackfill(of: Source): Option[Leases.Holder] =
    database.run(Leases.find(_, backfillLease(of))).flatMap(_.holder)

  /** Whether the lease of the backfill has run out, by the clock of the control plane. */
  private def leaseExpired(of: Source): Boolean = asSuperuser { c =>
    query(
      c,
      "SELECT (expires_at < now())::text FROM control.source_lease WHERE lease_key = ?",
      backfillLease(of).value
    )
  } == Seq(Seq("true"))

  @tailrec
  private def poll(deadline: Deadline, what: String)(condition: => Boolean): Unit =
    if (!condition) {
      if (deadline.isOverdue()) fail(s"$what did not come in time")
      Thread.sleep(50)
      poll(deadline, what)(condition)
    }

  /**
   * Makes rounds until one says that it showed what it is for.
   *
   * @param round one round, by its number: whether it showed it
   * @param otherwise what none of the rounds showed
   */
  private def rounds(round: Int => Boolean, otherwise: String): Unit = {
    @tailrec
    def from(number: Int): Unit =
      if (!round(number)) {
        if (number < Tries) from(number + 1)
        else fail(s"in none of $Tries rounds $otherwise")
      }
    from(1)
  }

  /**
   * One round: a source of its own, and `run` and `backfill` of it started together, both before
   * either is awaited. Both end in 0, and everything holds that holds however they interleave.
   *
   * @return whether the two invocations were running at one time
   */
  private def round(number: Int): Boolean = {
    val at = moments()
    val (of, stamps) = history(at)

    val started = Seq(regular(of), backfill(of, at))
    val ended = started.map(_.await(5.minutes))

    ended.foreach(process => withClue(process.log)(process.exit shouldBe Main.Exit.Ok))
    ended(1).log should include(s"The backfill ${History.value} of ${of.id} is complete")

    assertCopies(of, at, stamps)

    // The state of the source is the regular sequence's, and the backfill is complete.
    val runs = runsOf(of)
    runs.map(_.status).distinct shouldBe Seq(RunStatus.Succeeded)
    sequenceOf(runOf(stateOf(of).value.updatedBy)) shouldBe Regular
    assertComplete(of, at.chunks)
    assertLedger(of)

    val (regularSpan, backfillSpan) = (
      spanOf(runs.filter(sequenceOf(_) == Regular)),
      spanOf(runs.filter(sequenceOf(_) == Backfill))
    )
    val together = regularSpan._1.isBefore(backfillSpan._2) &&
      backfillSpan._1.isBefore(regularSpan._2)
    info(
      s"round $number: ${runs.count(sequenceOf(_) == Regular)} regular runs and " +
        s"${runs.count(sequenceOf(_) == Backfill)} chunks; a commit of one sequence followed " +
        s"one of the other ${turnsOf(of)} times in the log of the table; " +
        s"${runs.flatMap(_.counts.commitContention).sum} commits met another" +
        (if (together) "" else "; the two invocations were not running at one time")
    )
    together
  }

  /**
   * One round of the kill: `run` and `backfill` of a source of its own started together, and the
   * process of the backfill killed with -9 once two chunks are committed and a run holds the
   * lease of the backfill. The invocation started at once is skipped; the one started once the
   * lease has run out takes it over, ends the run that died as `ABANDONED`, recovers what it
   * left, and reads the rest. The regular sequence goes on all the while, and ends in 0.
   *
   * @return whether the kill came in the middle of a run of the backfill
   */
  private def killRound(number: Int): Boolean = {
    val at = moments()
    val (of, stamps) = history(at)

    val going = regular(of, mortal)
    val killed = backfill(of, at, mortal)
    poll(3.minutes.fromNow, "a run of the backfill after two chunks") {
      if (!killed.isAlive) fail(s"the backfill ended before it was killed:\n${killed.await().log}")
      recordOf(of).flatMap(_.progress).exists(_.lastCommitted.value >= 2) &&
      holderOfBackfill(of).nonEmpty
    }
    killed.signal("KILL")
    val regularAtTheKill = going.isAlive
    killed.await(30.seconds).exit shouldBe KilledBySignal
    val killedAt = databaseNow()
    val committedAtTheKill = recordOf(of).flatMap(_.progress).fold(0L)(_.lastCommitted.value)

    holderOfBackfill(of) match {
      case None =>
        val regularEnded = going.await(5.minutes)
        withClue(regularEnded.log)(regularEnded.exit shouldBe Main.Exit.Ok)
        info(s"round $number: the kill came between two chunks, and left the lease free")
        false

      case Some(dead) =>
        runOf(dead.run).status shouldBe RunStatus.Running

        // At once: the lease is the dead run's until it runs out, and the invocation is skipped.
        val skipped = backfill(of, at, mortal).await(2.minutes)
        withClue(skipped.log)(skipped.exit shouldBe Main.Exit.SkippedLocked)
        skipped.log should include(
          s"the run ${dead.run} holds the lease of the backfill ${History.value}"
        )
        holderOfBackfill(of).map(_.run) shouldBe Some(dead.run)

        // Once it has run out, by itself: the next invocation takes it over, and ends the backfill.
        poll((MortalTtl + 30.seconds).fromNow, "the end of the lease of the run that died") {
          leaseExpired(of)
        }
        val resumed = backfill(of, at, mortal).await(5.minutes)
        withClue(resumed.log)(resumed.exit shouldBe Main.Exit.Ok)
        resumed.log should include(s"The backfill ${History.value} of ${of.id} is complete")
        val regularEnded = going.await(5.minutes)
        withClue(regularEnded.log)(regularEnded.exit shouldBe Main.Exit.Ok)

        assertCopies(of, at, stamps)

        // The run that died is ended by the one that took its lease over, the invocation started
        // at once was skipped, and every other run succeeded.
        val runs = runsOf(of)
        val (backfillRuns, regularRuns) = runs.partition(sequenceOf(_) == Backfill)
        regularRuns.map(_.status).distinct shouldBe Seq(RunStatus.Succeeded)
        val died = runOf(dead.run)
        died.status shouldBe RunStatus.Abandoned
        backfillRuns
          .filter(run => run.start.id != dead.run && run.status != RunStatus.Succeeded)
          .map(_.status) shouldBe Seq(RunStatus.SkippedLocked)
        sequenceOf(runOf(stateOf(of).value.updatedBy)) shouldBe Regular
        assertComplete(of, at.chunks)
        assertLedger(of)

        val left = died.batch.fold("had planned no chunk") { seq =>
          val chunk = batchOf(BatchId(TxnAppId.backfill(of.id, Layer.Bronze, History), seq))
          val by = chunk.committedBy.fold("nobody")(_.toString)
          if (writtenBy(of, dead.run) > 0)
            s"had written the chunk ${seq.value} and not committed it: the run $by rolled it forward"
          else
            s"had planned the chunk ${seq.value} and had nothing of it in the table: the run " +
              s"$by read its window again"
        }
        val skippedRun = backfillRuns.find(_.status == RunStatus.SkippedLocked).value
        val margin = Duration.between(skippedRun.startedAt, dead.expiresAt)
        val besides = regularRuns.count { run =>
          run.startedAt.isAfter(killedAt) && died.finishedAt.exists(run.startedAt.isBefore)
        }
        info(
          s"round $number: the run ${dead.run} was killed with $committedAtTheKill chunks " +
            s"committed, and $left; the invocation started at once was skipped " +
            s"${margin.toMillis / 1000.0}s before the lease ran out; $besides regular runs " +
            "started between the kill and the takeover" +
            (if (regularAtTheKill) "" else "; the regular invocation had ended before the kill")
        )
        true
    }
  }

  test(
    "a backfill and the incremental load of its source, in two processes at once: each row once a sequence"
  ) {
    rounds(round, "were the backfill and the regular runs running at one time")
  }

  test(
    "a backfill killed with -9 beside the incremental load: its lease is waited out, and the next invocation ends it"
  ) {
    rounds(killRound, "did the kill come in the middle of a run of the backfill")
  }
}

object BackfillBesideRunSpec {

  /**
   * The moments of a round, by the clock of the database: `S` ten days before today, `F` twenty
   * days before `S`, `T` five days after it.
   */
  final case class Moments(from: Instant, start: Instant, to: Instant) {

    /** The sequences that are to have a row stamped `at`: the backfill, the regular one. */
    def readers(at: Instant): Set[String] =
      Set(
        Option.when(at.isAfter(from) && !at.isAfter(to))(Backfill),
        Option.when(at.isAfter(start))(Regular)
      ).flatten

    /** How many chunks of a day the backfill has. */
    def chunks: Long = Duration.between(from, to).toDays
  }

  /** A heap as the processes of `FanInSpec` have: two of them, beside the JVM of the tests. */
  private val Heap = "512m"

  /** How many rounds are made before the test says that none showed what it is for. */
  private val Tries = 3

  /**
   * The time to live of a lease, in the test that kills a process: the shortest that the rules
   * of a profile let a control plane of 1 s and 10 s have (33 s), and a renewal every 12 s. The
   * run that dies leaves 24 s of it at least, for the invocation started at once to be skipped.
   */
  private val MortalTtl = 36.seconds

  /** What a JVM that SIGKILL ended exits with: 128 + 9. */
  private val KilledBySignal = 137

  private val History = BackfillId.parse("history").toOption.get

  private val Regular = "regular"
  private val Backfill = "backfill"
}
