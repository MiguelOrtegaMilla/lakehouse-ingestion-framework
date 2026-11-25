package lakehouse.ingestion.it.ingestion

import java.time.temporal.ChronoUnit
import java.time.{Clock, Instant, OffsetDateTime, ZoneOffset}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.util.Using

import lakehouse.ingestion.core.config.{Source, SourceId}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.core.watermark.BackfillRange
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.control.Leases
import lakehouse.ingestion.engine.{FaultHooks, FaultPoint, IngestionService}
import lakehouse.ingestion.it.ingestion.IngestionServiceSpec.Died
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The matrix of faults in one JVM (discovery §17, P7.9): each fault point of the engine, for
 * each kind of batch that a run takes, with a fault there, and the run after it. A cell is a
 * kind, a point and a fault, and a test of its own, on a source of its own; each ends with the
 * ledger of the source, which finds what the cell does not ask.
 *
 * '''The kinds''' are those of the sequences that a batch is in: new files, a table read whole,
 * a window of a table, the files of a committed batch read again, and a chunk of a backfill.
 * Each is read by its real connector, and a point that a kind never reaches is left out of its
 * column: a table has no record rejected, and so no commit to a quarantine. That a kind
 * reaches the points of its column, and no other, is a test of its own.
 *
 * '''What a cell expects''' is said once, from the protocol (ADR-0005, ADR-0021), and not in
 * each test (`afterDeathAt`): the table of the data says what the run after one that died
 * does. A batch whose rows are in it is rolled forward, and one whose rows are not is run again
 * under its number, as it was planned; a quarantine that has the records of the batch changes
 * nothing of that.
 *
 * '''The lease of a run that died is made to run out in the database''' (`runOut`), at once, and
 * is not waited for: what a cell asks is what the next run does with what the dead one left,
 * and a wait of a time to live in each cell would make the matrix minutes longer for nothing. A
 * lease that runs out by itself, with the process that held it killed, is P7.8b's test, in
 * `BackfillBesideRunSpec`.
 */
final class FaultMatrixSpec extends AnyFunSuite with Matchers with JdbcIngestionSuite {

  import FaultMatrixSpec._

  private val asked = OperatorRequest.of("ops.oncall", "the matrix of faults").toOption.get

  /** The service, as an application puts it together, with both connectors and these hooks. */
  private def engine(hooks: FaultHooks = FaultHooks.Off): IngestionService =
    new IngestionService(
      database,
      connectors,
      spark,
      lakeRoot,
      timing,
      Clock.fixed(Now, ZoneOffset.UTC),
      hooks
    )

  // ---------------------------------------------------------------------------------------------
  // A cell: a source of one kind, made ready for the run that the cell is about
  // ---------------------------------------------------------------------------------------------

  /**
   * A source of one kind, with the run that a cell is about still to come.
   *
   * @param batch the batch that the run plans
   * @param lease the lease of its sequence, that it and the run after it ask for
   * @param attempt the run, by a service
   * @param after the run after it, of its sequence: as an orchestrator or an operator starts it
   */
  final private class Cell(
      val source: Source,
      val batch: BatchId,
      val lease: LeaseKey,
      attempt: IngestionService => Outcome,
      after: IngestionService => Outcome
  ) {

    /** The run of the cell, by a service with these hooks. */
    def run(hooks: FaultHooks): Outcome = attempt(engine(hooks))

    /** The run after it, by a service with nothing done to it. */
    def next(): Outcome = after(engine())
  }

  /** A source of the kind, made ready: its first batch, or its first after one committed. */
  private def cellOf(kind: Kind): Cell = kind match {
    case Kind.Files =>
      val of = fileSource(rejected = Some("0.5"))
      drop(of, "a.csv", RejectingCsv)
      regular(of, seq = 1)

    case Kind.Full =>
      val id = newSourceId()
      val of = jdbcSource(
        tableOf(id, Day.plus(1, ChronoUnit.HOURS), Day.plus(26, ChronoUnit.HOURS)),
        id = id.value
      )
      regular(of, seq = 1)

    case Kind.Incremental =>
      val id = newSourceId()
      val table = tableOf(
        id,
        Day.plus(1, ChronoUnit.HOURS),
        Day.plus(2, ChronoUnit.HOURS),
        Day.plus(26, ChronoUnit.HOURS)
      )
      // Its first window is the first day of 2026, and the row after it is the next one's.
      val of = jdbcSource(table, load = s"load: $WindowOfADay", id = id.value)
      regular(of, seq = 1)

    case Kind.Reprocess =>
      val of = fileSource(rejected = Some("0.5"))
      drop(of, "a.csv", RejectingCsv)
      ingested(engine().run(of, versionOf(of))).batch shouldBe batchId(of, 1)
      // The next run of the sequence is a regular one, which finds the reprocess planned.
      new Cell(
        of,
        batchId(of, 2),
        LeaseKey.of(of.id, Layer.Bronze),
        _.reprocess(of, versionOf(of), BatchSeq.of(1).toOption.get, asked),
        _.run(of, versionOf(of))
      )

    case Kind.Chunk =>
      val id = newSourceId()
      val table = tableOf(
        id,
        History.plus(12, ChronoUnit.HOURS),
        History.plus(1, ChronoUnit.DAYS),
        History.plus(36, ChronoUnit.HOURS)
      )
      val of = jdbcSource(table, load = s"load: $WindowOfADay", id = id.value)
      // The first chunk of (H, H + 2d], which the run creates the backfill to read.
      val request = BackfillRequest(
        Backfilled,
        BackfillRange.of(History, History.plus(2, ChronoUnit.DAYS)).toOption.get,
        Some(1.day)
      )
      val backfill: IngestionService => Outcome = _.backfill(of, versionOf(of), request, asked)
      new Cell(
        of,
        BatchId(TxnAppId.backfill(of.id, Layer.Bronze, Backfilled), BatchSeq.of(1).toOption.get),
        LeaseKey.backfill(of.id, Layer.Bronze, Backfilled),
        backfill,
        backfill
      )
  }

  /** A cell of the regular sequence of a source, whose runs are `run`. */
  private def regular(of: Source, seq: Long): Cell = {
    val run: IngestionService => Outcome = _.run(of, versionOf(of))
    new Cell(of, batchId(of, seq), LeaseKey.of(of.id, Layer.Bronze), run, run)
  }

  /** A table of its own for a source, a row stamped at each moment given. */
  private def tableOf(id: SourceId, stamps: Instant*): String = {
    val table = id.value.replace('.', '_')
    asOwner { c =>
      execute(c, s"CREATE TABLE $table (id int PRIMARY KEY, updated_at timestamptz NOT NULL)")
      Using.resource(c.prepareStatement(s"INSERT INTO $table VALUES (?, ?)")) { statement =>
        stamps.zipWithIndex.foreach { case (stamp, i) =>
          statement.setInt(1, i + 1)
          statement.setObject(2, OffsetDateTime.ofInstant(stamp, ZoneOffset.UTC))
          statement.executeUpdate(): Unit
        }
      }
    }
    s"public.$table"
  }

  // ---------------------------------------------------------------------------------------------
  // What a cell asks of the control plane
  // ---------------------------------------------------------------------------------------------

  private def holderOf(lease: LeaseKey): Option[RunId] =
    database.run(Leases.find(_, lease)).flatMap(_.holder).map(_.run)

  /**
   * The lease runs out in the database now, whatever its holder believes of it: that of a run
   * that died is not waited out in a cell (see the suite).
   */
  private def runOut(lease: LeaseKey): Unit = asSuperuser { c =>
    update(
      c,
      "UPDATE control.source_lease SET expires_at = now() - interval '1 second' WHERE lease_key = ?",
      lease.value
    ) shouldBe 1: Unit
  }

  /** The batches of a sequence, each by its number with how it stands. */
  private def sequenceOf(batch: BatchId): Seq[(Long, String)] = asSuperuser { c =>
    query(
      c,
      "SELECT batch_seq::text, status FROM control.ingestion_batch WHERE txn_app_id = ? ORDER BY 1",
      batch.app.value
    ).map(row => (row(0).toLong, row(1)))
  }

  private def rolledForward(outcome: Outcome): Outcome.RolledForward = outcome match {
    case done: Outcome.RolledForward => done
    case other => fail(s"no batch was rolled forward: $other")
  }

  // ---------------------------------------------------------------------------------------------
  // The matrix
  // ---------------------------------------------------------------------------------------------

  Kind.All.foreach { kind =>
    test(
      s"${kind.name}: a run reaches the ${kind.points.size} fault points of its column, in order"
    ) {
      val cell = cellOf(kind)
      val seen = ArrayBuffer.empty[FaultPoint]
      val hooks: FaultHooks = (point, _) => { seen += point; () }

      ingested(cell.run(hooks)).batch shouldBe cell.batch

      seen.toSeq shouldBe kind.points
      assertLedger(cell.source)
    }
  }

  // A run that dies at the point: the next takes its lease over, and finishes its batch.
  for (kind <- Kind.All; point <- kind.points) {
    val recovery = afterDeathAt(point)
    test(s"${kind.name}: a run that dies at $point ${recovery.saying}") {
      val cell = cellOf(kind)

      intercept[Died](cell.run(hookAt(point)(_ => throw new Died)))

      // As a process that died leaves it: its run running, its lease held, its batch planned.
      val dead = holderOf(cell.lease).value
      runOf(dead).status shouldBe RunStatus.Running
      batchOf(cell.batch).status shouldBe BatchStatus.Planned
      batchOf(cell.batch).plannedBy shouldBe dead

      runOut(cell.lease)
      val next = cell.next()

      val finished = recovery match {
        case Recovery.Replay => ingested(next).batch
        case Recovery.RollForward => rolledForward(next).batch
      }
      finished shouldBe cell.batch
      runOf(dead).status shouldBe RunStatus.Abandoned
      runOf(next.run).status shouldBe RunStatus.Succeeded
      val committed = batchOf(cell.batch)
      committed.plannedBy shouldBe dead
      committed.committedBy shouldBe Some(next.run)
      // Under its number, and no other: the sequence has no batch after it, nor one given up.
      sequenceOf(cell.batch) shouldBe (1L to cell.batch.seq.value).map(_ -> "COMMITTED")
      holderOf(cell.lease) shouldBe None
      assertLedger(cell.source)
    }
  }
}

object FaultMatrixSpec {

  /** The kinds of batch of the matrix, each with the fault points that its run reaches. */
  sealed abstract class Kind(val name: String, val points: Seq[FaultPoint])

  object Kind {

    case object Files extends Kind("files", FaultPoint.All)

    case object Full
        extends Kind("full load", FaultPoint.All.filterNot(_ == FaultPoint.AfterQuarantineWrite))

    case object Incremental
        extends Kind(
          "incremental load",
          FaultPoint.All.filterNot(_ == FaultPoint.AfterQuarantineWrite)
        )

    case object Reprocess extends Kind("reprocess", FaultPoint.All)

    case object Chunk
        extends Kind(
          "backfill chunk",
          FaultPoint.All.filterNot(_ == FaultPoint.AfterQuarantineWrite)
        )

    val All: Seq[Kind] = Seq(Files, Full, Incremental, Reprocess, Chunk)
  }

  /** What the run after one that died does with the batch that it left planned. */
  sealed abstract class Recovery(val saying: String)

  object Recovery {

    /** Its plan is run again, under its number: nothing of it is in the table of its data. */
    case object Replay extends Recovery("leaves its batch to be run again under its number")

    /** It is committed as it was written: its rows are in the table of its data. */
    case object RollForward extends Recovery("leaves its batch to be rolled forward")
  }

  /**
   * The oracle of the column of a run that dies (ADR-0005 §4): the table of the data of the
   * batch decides. Its rows are in it from `AfterBronzeCommit` on; at `AfterQuarantineWrite`
   * the quarantine has the rejected records and the data does not.
   */
  def afterDeathAt(point: FaultPoint): Recovery = point match {
    case FaultPoint.AfterBronzeCommit | FaultPoint.BeforeStateCommit => Recovery.RollForward
    case _ => Recovery.Replay
  }

  /** A file with a record of three that cannot be parsed, which its source lets be rejected. */
  private val RejectingCsv = "id,amount\n1,2\n3\n4,5\n"

  /** The first day of 2026: the first window of a source that reads a day at a time from it. */
  private val Day = Instant.parse("2026-01-01T00:00:00Z")

  /** How a source of a table reads it by windows of a day, from `Day`. */
  private val WindowOfADay =
    "{ mode: incremental, watermark: { column: updated_at, safety_margin: 1h, " +
      s"""start: "$Day" }, max_batch_range: 1d }"""

  /** The start of the range of the backfill of a chunk: long before `Day`. */
  private val History = Instant.parse("2025-06-01T00:00:00Z")

  private val Backfilled = BackfillId.parse("matrix").toOption.get
}
