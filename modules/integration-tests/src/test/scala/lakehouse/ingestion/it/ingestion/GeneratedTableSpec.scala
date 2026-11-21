package lakehouse.ingestion.it.ingestion

import java.sql.{Connection, Timestamp}
import java.time.temporal.ChronoUnit
import java.time.{Clock, Instant, ZoneOffset}

import scala.util.Using

import lakehouse.ingestion.core.config.{EngineColumns, SecretResolver, Source}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.core.watermark.{StoredWatermark, WatermarkPlan}
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.connector.ConnectorRegistry
import lakehouse.ingestion.engine.jdbc.JdbcSourceConnector
import lakehouse.ingestion.engine.{FaultHooks, IngestionService}
import lakehouse.ingestion.it.support.TableGenerator
import lakehouse.ingestion.it.support.TableGenerator._
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A table of a database changed by its applications, made from a seed (`TableGenerator`), and
 * read by a run of its source after each round of changes: rows inserted, updated and deleted,
 * rows that share a moment, rows changed twice before they are read, and transactions left
 * open while a batch is read.
 *
 * What each batch is to have is said by the generator, and not read back from the lake: the
 * rows that a reader sees when the batch is read, those of its window for a load by a
 * watermark, all of them for a full load. Each batch is compared with that, row by row and
 * value by value; and each thing that a load is to get right, or is known to get wrong, is
 * said of the rows that the generator made for it, so that a failure names it.
 *
 * The windows are decided by the configuration alone, a `start` and a range that are months
 * behind any clock, so nothing here depends on the time: the moment of every row is the
 * generator's. What the margin of a load does against the clock of a source is in
 * `JdbcIngestionSpec`.
 */
final class GeneratedTableSpec extends AnyFunSuite with Matchers with IngestionSuite {

  private val Start = Instant.parse("2026-01-01T00:00:00Z")

  /** The window of the `n`th batch of a source of `IncrementalLoad`: the `n`th day of 2026. */
  private def day(n: Int): Window =
    Window(Start.plus((n - 1).toLong, ChronoUnit.DAYS), Start.plus(n.toLong, ChronoUnit.DAYS))

  private val IncrementalLoad =
    s"""load:
       |  mode: incremental
       |  watermark: { column: updated_at, safety_margin: 1m, start: "$Start" }
       |  max_batch_range: 1d""".stripMargin

  private val FullLoad = "load: { mode: full }"

  private val Everything =
    Mix(
      inserts = 6,
      updates = 3,
      deletes = 2,
      ties = 3,
      replaced = 2,
      moved = 2,
      lost = 2,
      early = 2
    )

  /** The rounds of each seed: a batch is read after each, and one more after the last. */
  private val Rounds = 4

  // ---------------------------------------------------------------------------------------------
  // The source, and what a run left
  // ---------------------------------------------------------------------------------------------

  private def asOwner[T](body: Connection => T): T =
    Using.resource(pg.connect("sales_owner", "sales"))(body)

  private def ownersConnection: () => Connection = () => pg.connect("sales_owner", "sales")

  private def ordersSource(table: String, load: String): Source = {
    val id = newSourceId().value
    Source
      .load(
        s"$id.yaml",
        s"""id: $id
           |connector:
           |  type: jdbc
           |  connection:
           |    url: "${pg.jdbcUrl("sales")}"
           |    user: sales_ro
           |    password: { secretRef: "env:SALES_RO_PASSWORD" }
           |    tls: { mode: disable }
           |  table: public.$table
           |$load
           |targets: { bronze: { table: bronze/${id.replace('.', '/')} } }
           |""".stripMargin
      )
      .value
      .value
  }

  private def reading: IngestionService =
    new IngestionService(
      database,
      ConnectorRegistry(
        new JdbcSourceConnector(
          SecretResolver(Map("SALES_RO_PASSWORD" -> pg.password("sales_ro")).get)
        )
      ),
      spark,
      lakeRoot,
      timing,
      Clock.fixed(Now, ZoneOffset.UTC),
      FaultHooks.Off
    )

  /** A run that commits a batch of `expected` rows, and the count of the batch as it said. */
  private def readBy(of: Source, expected: Vector[Row]): Outcome.Ingested = {
    val done = ingested(launch(of, reading))
    val count = Some(expected.size.toLong)
    (done.counts.rowsRead, done.counts.rowsWritten, done.counts.rowsRejected) shouldBe
      ((count, count, Some(0L)))
    // A batch with no row writes nothing.
    done.outputs.isEmpty shouldBe expected.isEmpty
    done
  }

  private def watermarkOf(of: Source): Option[Instant] =
    stateOf(of).flatMap(_.watermark).map(StoredWatermark.fromJson(_).value.value)

  /** Each row of Bronze, as the source had it, with the batch that wrote it. */
  private def images(of: Source): Vector[(Long, Row)] =
    spark.read
      .format("delta")
      .load(bronze(of).uri)
      .select(
        col("order_id"),
        col("customer_email"),
        col("amount"),
        col("status"),
        col("updated_at"),
        col(EngineColumns.BatchSeq)
      )
      .collect()
      .map { row =>
        val moment = row.get(4) match {
          case at: Timestamp => at.toInstant
          case at: Instant => at
          case other => fail(s"updated_at read as $other")
        }
        row.getLong(5) -> Row(
          row.getLong(0),
          Option(row.getString(1)),
          BigDecimal(row.getDecimal(2)),
          row.getString(3),
          moment
        )
      }
      .toVector
      .sortBy { case (batch, row) => (batch, row.orderId) }

  /** The rows of Bronze by the batch that wrote them, each batch by key. */
  private def batches(of: Source): Map[Long, Vector[Row]] =
    images(of).groupBy(_._1).map { case (batch, rows) => batch -> rows.map(_._2) }

  /** The batches of a row, by its key and its moment: one image of it. */
  private def batchesOfImage(of: Vector[(Long, Row)], row: Row): Vector[Long] =
    of.collect {
      case (batch, image) if image.orderId == row.orderId && image.updatedAt == row.updatedAt =>
        batch
    }

  // ---------------------------------------------------------------------------------------------
  // The generator, against a database
  // ---------------------------------------------------------------------------------------------

  test("the generator writes what it says, and what it holds is seen once it is committed") {
    asOwner(TableGenerator.create(_, "generated_itself"))
    val first = round(5, 1, day(1), Table.Empty, Everything.copy(updates = 0, deletes = 0))

    val held = write(first, "generated_itself", ownersConnection)
    try {
      // Another connection, as a reader: what is held is not there yet.
      asOwner(readBack(_, "generated_itself")) shouldBe
        first.whenRead(Table.Empty).rows.values.toVector
      held.commit()
    } finally held.close()
    val table = first.afterwards(Table.Empty)
    asOwner(readBack(_, "generated_itself")) shouldBe table.rows.values.toVector

    val second = round(5, 2, day(2), table, Everything)
    write(second, "generated_itself", ownersConnection).close()
    // The held transaction was never committed: it is as if it had not been.
    asOwner(readBack(_, "generated_itself")) shouldBe
      second.whenRead(table).rows.values.toVector
  }

  // ---------------------------------------------------------------------------------------------
  // A load by a watermark
  // ---------------------------------------------------------------------------------------------

  test("a watermark: each batch has the rows of its window committed when it was read") {
    for (seed <- Seq(21L, 22L, 23L)) withClue(s"seed $seed: ") {
      val name = s"generated_by_window_$seed"
      asOwner(TableGenerator.create(_, name))
      val orders = ordersSource(name, IncrementalLoad)

      // Round by round: the changes, the batch of their window, and then what was held.
      val (table, rounds, expected) =
        (1 to Rounds + 1).foldLeft(
          (Table.Empty, Vector.empty[Round], Map.empty[Long, Vector[Row]])
        ) { case ((before, done, expected), n) =>
          val made =
            if (n > Rounds) None
            else {
              val mix = if (n == 1) Everything.copy(updates = 0, deletes = 0) else Everything
              Some(round(seed, n, day(n), before, mix))
            }
          val held = made.map(write(_, name, ownersConnection))
          try {
            val read = made.fold(before)(_.whenRead(before))
            val rows = read.visibleIn(day(n))

            val batch = readBy(orders, rows)

            // The window is the one that the configuration says, and the watermark its end.
            val window = WatermarkPlan.fromJson(batchOf(batch.batch).plan).value
            (window.lower, window.upper) shouldBe (Some(day(n).lower) -> day(n).upper)
            watermarkOf(orders) shouldBe Some(day(n).upper)
            held.foreach(_.commit())
            (made.fold(before)(_.afterwards(before)), done ++ made, expected + (n.toLong -> rows))
          } finally held.foreach(_.close())
        }

      // Each batch is what the source had committed in its window when it was read, and Bronze
      // has nothing else.
      batches(orders) shouldBe expected.filter(_._2.nonEmpty)
      val all = images(orders)

      rounds.foreach { made =>
        val k = made.number.toLong
        withClue(s"round $k: ") {
          // Rows that share a moment are in one batch, those at a bound as well: at the upper
          // bound of a window, in its batch; a microsecond after its lower bound, in its batch,
          // and not in the one before.
          made.ties.foreach { case (at, keys) =>
            all.filter(_._2.updatedAt == at).map(_._1).distinct shouldBe Vector(k)
            all.filter(_._2.updatedAt == at).map(_._2.orderId) should contain allElementsOf keys
          }
          // A row replaced before its window was read is there in its second image alone.
          made.replaced.foreach { case (first, second) =>
            batchesOfImage(all, first) shouldBe empty
            batchesOfImage(all, second) shouldBe Vector(k)
          }
          // A row moved to the next window is read once, in the batch of that window.
          made.moved.foreach { case (first, second) =>
            batchesOfImage(all, first) shouldBe empty
            batchesOfImage(all, second) shouldBe Vector(k + 1)
          }
          // A row whose transaction was open while its window was read is lost: the source has
          // it, behind the watermark, and no batch read it. It is the known limit of a load by
          // a column (ADR-0020), pinned here.
          made.lost.foreach { row =>
            all.filter(_._2.orderId == row.orderId) shouldBe empty
            table.rows.get(row.orderId) shouldBe Some(row)
            watermarkOf(orders).value.isAfter(row.updatedAt) shouldBe true
          }
          // A row whose transaction committed before its window was read is in its batch.
          made.early.foreach(row => batchesOfImage(all, row) shouldBe Vector(k + 1))
          // A row that was deleted is still in Bronze, as it was last read, and nothing says
          // that it is gone: a load by a watermark sees no delete (ADR-0009).
          made.deleted.foreach { row =>
            all.filter(_._2.orderId == row.orderId).map(_._2).lastOption shouldBe Some(row)
            table.rows.contains(row.orderId) shouldBe false
          }
        }
      }
      // What the source has and Bronze does not is what was lost.
      val inBronze = all.map(_._2).toSet
      table.rows.values.filterNot(inBronze).toSet shouldBe rounds.flatMap(_.lost).toSet
    }
  }

  // ---------------------------------------------------------------------------------------------
  // A full load
  // ---------------------------------------------------------------------------------------------

  test("a full load: each batch is the table as it was committed when it was read") {
    val name = "generated_whole"
    asOwner(TableGenerator.create(_, name))
    val orders = ordersSource(name, FullLoad)

    val (table, rounds, expected) =
      (1 until Rounds).foldLeft((Table.Empty, Vector.empty[Round], Map.empty[Long, Vector[Row]])) {
        case ((before, done, expected), n) =>
          val mix = if (n == 1) Everything.copy(updates = 0, deletes = 0) else Everything
          val made = round(31, n, day(n), before, mix)
          val held = write(made, name, ownersConnection)
          try {
            val rows = made.whenRead(before).rows.values.toVector
            readBy(orders, rows).batch shouldBe batchId(orders, n.toLong)
            held.commit()
            (made.afterwards(before), done :+ made, expected + (n.toLong -> rows))
          } finally held.close()
      }

    // Each batch is the whole table as it was committed when it was read: what a transaction
    // held then is not in it.
    val snapshots = batches(orders)
    snapshots shouldBe expected
    rounds.foreach { made =>
      val k = made.number.toLong
      withClue(s"round $k: ") {
        // What a transaction held while the table was read is in the snapshot after.
        (made.lost ++ made.early).foreach { row =>
          snapshots(k).map(_.orderId) should not contain row.orderId
          if (k + 1 < Rounds) snapshots(k + 1) should contain(row)
        }
        // A row that was deleted is in the snapshot before, and missing from its own.
        made.deleted.foreach { row =>
          snapshots(k - 1).map(_.orderId) should contain(row.orderId)
          snapshots(k).map(_.orderId) should not contain row.orderId
        }
      }
    }

    // The table is emptied. Its batch writes nothing, and is committed with no output: the last
    // snapshot in Bronze is still the one before. Nothing in Bronze says that its rows are gone;
    // Silver is to learn of it from the control plane (discovery §20, P10).
    val emptied = emptying(Rounds, day(Rounds), table)
    write(emptied, name, ownersConnection).close()
    asOwner(readBack(_, name)) shouldBe empty

    val last = readBy(orders, Vector.empty)

    last.batch shouldBe batchId(orders, Rounds.toLong)
    batchOf(last.batch).status shouldBe BatchStatus.Committed
    batches(orders) shouldBe expected
  }
}
