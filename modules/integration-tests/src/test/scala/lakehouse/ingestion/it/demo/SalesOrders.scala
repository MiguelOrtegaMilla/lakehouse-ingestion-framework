package lakehouse.ingestion.it.demo

import java.sql.{Connection, DriverManager, SQLException}
import java.time.Instant
import java.time.temporal.ChronoUnit

import scala.collection.immutable.SortedMap
import scala.util.Using

import lakehouse.ingestion.it.support.TableGenerator
import lakehouse.ingestion.it.support.TableGenerator.{Change, Mix, Round, Table, Window}

/**
 * The table of the example source `sales.orders` (`conf/sources/sales.orders.yaml`), in the
 * database `sales` of the local PostgreSQL (`docker/docker-compose.yml`): made, filled and
 * changed by `TableGenerator`, as the applications of the source would (README, "Running a
 * source").
 *
 * It is test code, run with `integration-tests/Test/runMain`, and is in no artifact of the
 * application: the engine never writes to a source. It connects as `sales_owner`, the role of
 * the source system, with the password in `SALES_OWNER_PASSWORD`.
 *
 *  - `--create` makes `public.orders` and fills it, a round a week from the `start` of the
 *    example up to two minutes ago by the clock of the database, so that the first run of the
 *    source reads every row of it.
 *  - `--changes` changes the table as it is, at moments of the last minute by that clock:
 *    rows inserted, updated and deleted. A run reads them once the clock is a minute past them,
 *    the `safety_margin` of the example.
 *
 * Neither leaves a transaction open, nor a row behind its window: what a run is to read of the
 * table is then every row of it, and the generator's tests (`GeneratedTableSpec`) are where
 * what a load loses is shown.
 */
object SalesOrders {

  val Orders = "public.orders"

  /** The `load.watermark.start` of the example: no row before it is the source's. */
  val Start: Instant = Instant.parse("2026-01-01T00:00:00Z")

  /** A fill and its changes are the same on any machine and any day, but for their moments. */
  private val Seed = 20260101L

  private val Week = 7L * 24 * 3600

  /** A week of orders: some new, some changed, a few deleted, and moments that rows share. */
  private val Weekly = Mix(inserts = 40, updates = 12, deletes = 3, ties = 2, replaced = 2)

  /** The changes of a minute. */
  private val Changes = Mix(inserts = 5, updates = 5, deletes = 2, ties = 1, replaced = 1)

  /** What `create` made: how many rows the table has, in how many rounds, and up to when. */
  final case class Made(rows: Int, rounds: Int, through: Instant)

  /**
   * Makes the table, and fills it a round a week from `Start` up to `until`.
   *
   * @param connect a connection as the owner of the table, each time it is called
   */
  def create(connect: () => Connection, until: Instant): Made = {
    require(until.isAfter(Start.plusSeconds(1)), s"nothing to fill between $Start and $until")
    Using.resource(connect())(TableGenerator.create(_, Orders))
    val windows = Iterator
      .iterate(Start)(_.plusSeconds(Week))
      .takeWhile(_.isBefore(until))
      .map(lower => Window(lower, Seq(lower.plusSeconds(Week), until).min))
      .toVector
    val filled = windows.zipWithIndex.foldLeft(TableGenerator.Table.Empty) {
      case (before, (window, index)) =>
        // The first week has nothing yet to change.
        val mix = if (before.rows.size >= Weekly.updates + Weekly.deletes) Weekly else Weekly.copy(
          updates = 0,
          deletes = 0
        )
        write(TableGenerator.round(Seed, index + 1, window, before, mix), before, connect)
    }
    Made(filled.rows.size, windows.size, until)
  }

  /**
   * Changes the table as it is now, at moments of `window`.
   *
   * @return the changes, as they were committed
   */
  def change(connect: () => Connection, window: Window): Round = {
    val rows = Using.resource(connect())(TableGenerator.readBack(_, Orders))
    require(rows.nonEmpty, s"$Orders has no rows: make it with --create first")
    val before =
      TableGenerator.Table(SortedMap.from(rows.map(row => row.orderId -> row)), rows.map(_.orderId).max, Set.empty)
    // The seed is spread over the rows that are there: the changes of one table are one series.
    val round = TableGenerator.round(Seed ^ before.lastKey, 1, window, before, Changes)
    write(round, before, connect)
    round
  }

  private def write(round: Round, before: Table, connect: () => Connection): Table = {
    Using.resource(TableGenerator.write(round, Orders, connect))(_ => ())
    round.whenRead(before)
  }

  /** The time of the database, to the second, which is the clock that a run reads it by. */
  def databaseNow(c: Connection): Instant =
    Using.resource(c.createStatement()) { statement =>
      val result = statement.executeQuery("SELECT now()")
      result.next()
      result.getTimestamp(1).toInstant.truncatedTo(ChronoUnit.SECONDS)
    }

  private val Usage =
    """usage: SalesOrders --create | --changes [--url <jdbc url of the database sales>]
      |  --create   makes public.orders, filled from 2026-01-01 up to two minutes ago
      |  --changes  inserts, updates and deletes rows of it, at moments of the last minute
      |The password of sales_owner is read from SALES_OWNER_PASSWORD.""".stripMargin

  def main(args: Array[String]): Unit = {
    val (command, url) = args.toList match {
      case command :: Nil => command -> "jdbc:postgresql://localhost:5432/sales"
      case command :: "--url" :: url :: Nil => command -> url
      case _ => fail(Usage)
    }
    val password =
      sys.env.get("SALES_OWNER_PASSWORD").filter(_.nonEmpty).getOrElse {
        fail(
          "SALES_OWNER_PASSWORD is not set: " +
            "export SALES_OWNER_PASSWORD=\"$(cat docker/secrets/sales_owner_password)\""
        )
      }
    val connect = () => DriverManager.getConnection(url, "sales_owner", password)
    val now = Using.resource(connect())(databaseNow)
    command match {
      case "--create" =>
        val made =
          try create(connect, now.minusSeconds(120))
          catch {
            case there: SQLException if there.getSQLState == "42P07" =>
              fail(
                s"$Orders is there already. It is changed with --changes; to make it again, " +
                  s"drop it first, as sales_owner"
              )
          }
        println(
          s"$Orders was made, with ${made.rows} rows changed in ${made.rounds} rounds of a " +
            s"week, from $Start to ${made.through}."
        )
      case "--changes" =>
        val window = Window(now.minusSeconds(60), now)
        val round = change(connect, window)
        def count(kind: Change => Boolean) = round.committed.count(kind)
        println(
          s"$Orders was changed at moments after ${window.lower} and up to ${window.upper}: " +
            s"${count(_.isInstanceOf[Change.Insert])} rows inserted, " +
            s"${count(_.isInstanceOf[Change.Update])} updated and " +
            s"${count(_.isInstanceOf[Change.Delete])} deleted. A run reads them once the " +
            s"clock of the database is a minute past ${window.upper}."
        )
      case _ => fail(Usage)
    }
  }

  private def fail(message: String): Nothing = {
    System.err.println(message)
    sys.exit(64)
  }
}
