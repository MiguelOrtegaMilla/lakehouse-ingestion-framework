package lakehouse.ingestion.it.ingestion

import java.nio.file.Path
import java.time.temporal.ChronoUnit
import java.time.{Instant, OffsetDateTime, ZoneOffset}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.Using

import io.delta.tables.DeltaTable
import lakehouse.ingestion.cli.Main
import lakehouse.ingestion.core.config.{EngineColumns, RootUri, Source}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.control.Backfills
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.{CommandProcess, DeploymentFiles}
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
 * Each test ends with the ledger of both sequences, read from the lake of the deployment.
 */
final class BackfillBesideRunSpec extends AnyFunSuite with Matchers with JdbcIngestionSuite {

  import BackfillBesideRunSpec._

  // The lake that the suite reads is the one that the processes write.
  private lazy val files = new DeploymentFiles(pg, workDir)
  override protected lazy val lakeRoot: RootUri = RootUri.parse(files.lake.toUri.toString).value
  override protected lazy val landingDirectory: Path = files.landing

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

  private def regular(of: Source): CommandProcess =
    CommandProcess.start(files.arguments(of), files.newLog(), Heap)

  private def backfill(of: Source, at: Moments): CommandProcess =
    CommandProcess.start(
      Seq(
        "backfill",
        "--profile",
        files.validProfile.toString,
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

    // Each row once for each sequence that reads it, and for no other.
    val expected = stamps.collect {
      case (row, stamp) if at.readers(stamp).nonEmpty => row -> at.readers(stamp).toSeq.sorted
    }
    copiesOf(of) shouldBe expected
    expected.values.count(_.size == 2) should be > 0

    // The state of the source is the regular sequence's, and the backfill is complete.
    val runs = runsOf(of)
    runs.map(_.status).distinct shouldBe Seq(RunStatus.Succeeded)
    val updatedBy = runOf(stateOf(of).value.updatedBy)
    sequenceOf(updatedBy) shouldBe Regular
    val record = database.run(Backfills.find(_, of.id, Layer.Bronze, History)).value
    record.completedAt should not be empty
    record.progress.value.lastCommitted.value shouldBe
      runs.count(run => sequenceOf(run) == Backfill).toLong
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

  test(
    "a backfill and the incremental load of its source, in two processes at once: each row once a sequence"
  ) {
    @tailrec
    def rounds(number: Int): Unit =
      if (!round(number)) {
        if (number < Tries) rounds(number + 1)
        else
          fail(
            s"in none of $Tries rounds were the backfill and the regular runs running at one time"
          )
      }
    rounds(1)
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
  }

  /** A heap as the processes of `FanInSpec` have: two of them, beside the JVM of the tests. */
  private val Heap = "512m"

  /** How many rounds are made before the test says that the two were never at one time. */
  private val Tries = 3

  private val History = BackfillId.parse("history").toOption.get

  private val Regular = "regular"
  private val Backfill = "backfill"
}
