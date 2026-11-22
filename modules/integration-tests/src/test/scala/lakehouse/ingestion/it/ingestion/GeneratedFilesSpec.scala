package lakehouse.ingestion.it.ingestion

import lakehouse.ingestion.core.config.{EngineColumns, Source}
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.it.support.FileGenerator._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Files as a producer leaves them, made from a seed (`FileGenerator`), through a run of their
 * source: files with a column more and a column less, records that cannot be parsed, empty
 * files and files sent twice, all in one delivery.
 *
 * What a run is to leave is said by the generator, record by record, and not read back from
 * the lake: every record that parses is in Bronze once for each file that has it, as it was
 * written and with the name of its file; every record that does not is in the quarantine of
 * the source, as it was written; and the control plane counts both.
 *
 * What the engine does with a column that a table does not have yet is the policy of a schema
 * (P9). Until then a batch of JSON has the columns of all its files, and a file that lacks a
 * column of its table is taken, with nothing in it: the deliveries here are those that P5 takes.
 */
final class GeneratedFilesSpec extends AnyFunSuite with Matchers with GeneratedDeliveries {

  /** One run of a delivery that is all new: what it counted is what the delivery has. */
  private def take(of: Source, delivery: Delivery): Outcome.Ingested = {
    val done = ingested(launch(of))
    done.counts.rowsRead shouldBe Some(delivery.parsed + delivery.unparsed)
    done.counts.rowsWritten shouldBe Some(delivery.parsed)
    done.counts.rowsRejected shouldBe Some(delivery.unparsed)
    done.counts.bytesRead shouldBe Some(bytes(delivery))
    done
  }

  private val Everything =
    Mix(files = 4, added = 2, missing = 2, corrupt = 5, empty = 2, repeated = 2)

  test("JSON: a delivery with a file of every kind is in Bronze and in the quarantine as written") {
    for (seed <- Seq(11L, 12L, 13L)) withClue(s"seed $seed: ") {
      val orders = fileSource(format = "json", rejected = Some("0.5"))
      val delivery = deliver(seed, Format.Json, Everything)
      leave(orders, delivery)

      take(orders, delivery)

      // The columns of every file of the batch, and a file without one has nothing in it.
      columnsOf(orders) shouldBe (Columns :+ Added).toSet ++ EngineColumns.All
      written(orders, delivery.columns) shouldBe expected(delivery.columns, delivery)
      quarantinedOf(orders) shouldBe setApart(1, delivery)
      // Every file is taken, those with no record among them, and none is taken again.
      takenOf(orders).map(_._1) shouldBe delivery.files.map(_.name).sorted
      val version = tableVersion(orders)
      launch(orders) shouldBe a[Outcome.NothingNew]
      tableVersion(orders) shouldBe version
      assertLedger(orders)
    }
  }

  test("CSV: a delivery is in Bronze as written, and a later one without a column is taken") {
    val orders = fileSource(rejected = Some("0.5"))
    val first =
      deliver(21, Format.Csv, Mix(files = 5, corrupt = 5, empty = 3, repeated = 2), number = 1)
    // The delivery has both kinds of empty file: of no bytes, and of a header alone.
    first.of(_ == Kind.Empty).map(_.text.isEmpty).distinct should have size 2
    leave(orders, first)

    take(orders, first)

    columnsOf(orders) shouldBe Columns.toSet ++ EngineColumns.All
    written(orders, Columns) shouldBe expected(Columns, first)
    quarantinedOf(orders) shouldBe setApart(1, first)
    takenOf(orders).map(_._1) shouldBe first.files.map(_.name).sorted

    // The producer stops writing one of its columns.
    val second = deliver(21, Format.Csv, Mix(files = 0, missing = 3, corrupt = 2), number = 2)
    val Kind.Without(gone) = second.files.head.kind: @unchecked
    leave(orders, second)

    take(orders, second)

    columnsOf(orders) shouldBe Columns.toSet ++ EngineColumns.All
    val all = written(orders, Columns)
    all shouldBe expected(Columns, first, second)
    // The rows of the second delivery have nothing under the column, and those of the first do.
    val at = Columns.indexOf(gone)
    all.filter(_._1.contains("-d02-")).map(_._2(at)).distinct shouldBe Seq(None)
    all.filter(_._1.contains("-d01-")).map(_._2(at)) should contain noneOf (None, Some(""))
    quarantinedOf(orders) shouldBe (setApart(1, first) ++ setApart(2, second)).sorted
    launch(orders) shouldBe a[Outcome.NothingNew]
    assertLedger(orders)
  }

  test("a file sent twice is in Bronze twice, each time under its own name") {
    val orders = fileSource(format = "json")
    val delivery = deliver(31, Format.Json, Mix(files = 2, repeated = 1))
    val again = delivery.of(_.isInstanceOf[Kind.Again]).head
    val Kind.Again(original) = again.kind: @unchecked
    leave(orders, delivery)

    take(orders, delivery)

    // Bronze is what was received: telling the two apart, and keeping one, is Silver's (P10).
    val rows = written(orders, Seq("id"))
    rows.filter(_._1 == again.name).map(_._2) shouldBe rows.filter(_._1 == original).map(_._2)
    rows.filter(_._1 == again.name) should have size again.records.size.toLong
    assertLedger(orders)
  }
}
