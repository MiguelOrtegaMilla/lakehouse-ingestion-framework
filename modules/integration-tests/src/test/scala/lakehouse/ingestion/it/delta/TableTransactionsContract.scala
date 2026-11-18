package lakehouse.ingestion.it.delta

import lakehouse.ingestion.core.config.{SourceId, TableLocation}
import lakehouse.ingestion.core.control._
import org.scalatest.{EitherValues, OptionValues}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What the engine may assume of anything that answers for a table (`TableTransactions`,
 * ADR-0005 §3): which batch of a sequence the table got to, and at which version of the table
 * each batch is.
 *
 * Two suites extend this one. The Delta that the control plane was built against in P4 is a
 * simulation (`SimulatedDelta`), and the one that the engine runs on is real, on the
 * coordinated LogStore. Both pass the same tests, so what was shown of recovery against the
 * first holds on the second, and the simulation cannot drift from what Delta does.
 *
 * Every test works on tables of its own.
 */
abstract class TableTransactionsContract
    extends AnyFunSuite
    with Matchers
    with OptionValues
    with EitherValues {

  /** What is under test. */
  protected def tables: TableTransactions

  /** A table that no test has written to, and that is not there yet. */
  protected def newTable(name: String): TableLocation

  /**
   * Writes the rows of a batch to a table, as the engine writes them: under the name of the
   * sequence of the batch and its number.
   *
   * @return the version of the table at which they were committed; none if the table had the
   *   batch already, or one beyond it, and the write was skipped
   */
  protected def write(
      table: TableLocation,
      source: SourceId,
      batch: BatchId,
      rows: Seq[String]
  ): Option[Long]

  /** Every row of a table, sorted. */
  protected def rowsOf(table: TableLocation): Seq[String]

  protected val returns: SourceId = SourceId.parse("shop.returns").value
  protected val orders: SourceId = SourceId.parse("shop.orders").value

  protected def batch(source: SourceId, seq: Long, layer: Layer = Layer.Bronze): BatchId =
    BatchId(TxnAppId.of(source, layer), BatchSeq.of(seq).value)

  private def app(source: SourceId): TxnAppId = TxnAppId.of(source, Layer.Bronze)

  private def rows(source: SourceId, seq: Long): Seq[String] =
    (1 to 3).map(i => s"$source-$seq-$i")

  /** Writes a batch of a source, with rows that say which batch they are of. */
  protected def writeBatch(table: TableLocation, source: SourceId, seq: Long): Option[Long] =
    write(table, source, batch(source, seq), rows(source, seq))

  test("a table that is not there has no batch of any sequence") {
    val table = newTable("absent")

    tables.lastVersion(table, app(returns)) shouldBe None
    tables.committedAt(table, batch(returns, 1)) shouldBe None
  }

  test(
    "a batch that is written is the last of its sequence, and is at the version it was committed at"
  ) {
    val table = newTable("written")

    val versions = (1L to 3L).map { seq =>
      val version = writeBatch(table, returns, seq).value
      tables.lastVersion(table, app(returns)) shouldBe Some(seq)
      version
    }

    versions shouldBe Seq(0L, 1L, 2L)
    (1L to 3L).map(seq => tables.committedAt(table, batch(returns, seq)).value) shouldBe versions
    rowsOf(table) shouldBe (1L to 3L).flatMap(rows(returns, _)).sorted
  }

  test("a batch that is written again is skipped, and is where it was") {
    val table = newTable("replayed")
    val versions = (1L to 3L).map(seq => writeBatch(table, returns, seq).value)

    // The last one, as a run that died after its write and before it said so; and one before
    // it, as a zombie that wakes up with an old batch.
    writeBatch(table, returns, 3) shouldBe None
    writeBatch(table, returns, 1) shouldBe None

    tables.lastVersion(table, app(returns)) shouldBe Some(3)
    (1L to 3L).map(seq => tables.committedAt(table, batch(returns, seq)).value) shouldBe versions
    rowsOf(table) shouldBe (1L to 3L).flatMap(rows(returns, _)).sorted
  }

  test("each sequence of a table has numbers of its own") {
    val table = newTable("fan-in")

    // Two sources of one table, each with its batches 1 and 2 (ADR-0005 §2).
    val written = Seq(returns -> 1L, orders -> 1L, orders -> 2L, returns -> 2L).map {
      case (source, seq) => (source, seq) -> writeBatch(table, source, seq).value
    }

    written.map(_._2) shouldBe Seq(0L, 1L, 2L, 3L)
    written.foreach { case ((source, seq), version) =>
      withClue(s"$source, batch $seq: ") {
        tables.committedAt(table, batch(source, seq)) shouldBe Some(version)
      }
    }
    tables.lastVersion(table, app(returns)) shouldBe Some(2)
    tables.lastVersion(table, app(orders)) shouldBe Some(2)
    // A source that never wrote to it, and the same source in another layer.
    val other = SourceId.parse("shop.refunds").value
    tables.lastVersion(table, app(other)) shouldBe None
    tables.committedAt(table, batch(other, 1)) shouldBe None
    tables.lastVersion(table, TxnAppId.of(returns, Layer.Silver)) shouldBe None
    tables.committedAt(table, batch(returns, 1, Layer.Silver)) shouldBe None
  }

  test("a batch that a table has not got to is at no version of it") {
    val table = newTable("ahead")
    writeBatch(table, returns, 1).value shouldBe 0
    writeBatch(table, returns, 2).value shouldBe 1

    tables.committedAt(table, batch(returns, 3)) shouldBe None
    tables.committedAt(table, batch(returns, 300)) shouldBe None
  }

  test("a number that a sequence passed over is in no commit, and is skipped if it comes late") {
    val table = newTable("gap")
    writeBatch(table, returns, 1).value shouldBe 0
    writeBatch(table, returns, 3).value shouldBe 1

    tables.lastVersion(table, app(returns)) shouldBe Some(3)
    tables.committedAt(table, batch(returns, 2)) shouldBe None

    // Delta skips what is not beyond the last: the rows of batch 2 never get in.
    writeBatch(table, returns, 2) shouldBe None
    tables.committedAt(table, batch(returns, 2)) shouldBe None
    rowsOf(table) shouldBe (rows(returns, 1) ++ rows(returns, 3)).sorted
  }

  test("one sequence in two tables is at a batch of its own in each") {
    // As the quarantine of a source and its data: the quarantine is written first, and a run
    // that dies between the two leaves them apart (ADR-0005 §3, step 5).
    val quarantine = newTable("quarantine")
    val data = newTable("data")
    writeBatch(quarantine, returns, 1).value shouldBe 0
    writeBatch(data, returns, 1).value shouldBe 0
    writeBatch(quarantine, returns, 2).value shouldBe 1

    tables.lastVersion(quarantine, app(returns)) shouldBe Some(2)
    tables.lastVersion(data, app(returns)) shouldBe Some(1)
    tables.committedAt(quarantine, batch(returns, 2)) shouldBe Some(1)
    tables.committedAt(data, batch(returns, 2)) shouldBe None
    Recovery.of(
      Some(
        PlannedBatch(
          batch(returns, 2),
          io.circe.Json.obj(),
          RunId.generate(),
          java.time.Instant.EPOCH
        )
      ),
      data,
      tables
    ) shouldBe a[Recovery.Replay]
  }
}
