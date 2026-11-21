package lakehouse.ingestion.it.support

import java.sql.{Connection, Types}
import java.time.temporal.ChronoUnit
import java.time.{Instant, OffsetDateTime, ZoneOffset}

import scala.collection.immutable.SortedMap
import scala.util.Using

/**
 * A table of a database as its own applications change it, made from a seed: the same seed
 * gives the same changes, to the microsecond, on any machine (`java.util.Random` is specified
 * to).
 *
 * The table has the columns of the orders of the demo source (`conf/sources/sales.orders.yaml`):
 * a key, an email that may be null, an amount, a status, and `updated_at`, the moment of the
 * last change of the row, which a load by a watermark goes by.
 *
 * A '''round''' is the changes that the applications make while a batch is to come, all
 * stamped inside one window of time `(lower, upper]`, as ADR-0020 has a window; the one batch
 * that reads that window is run after them. The time of a round is the generator's, and not a
 * clock's: a row says the moment it was given, whatever the time is when it is written. What a
 * round has is said by a `Mix`, in numbers; the seed decides the rest. Besides rows that are
 * inserted, updated and deleted, a round has one kind of row for each thing that a load by a
 * watermark is to get right, or is known to get wrong:
 *
 *  - '''Ties.''' Rows that share one moment, in three groups: one a microsecond after the lower
 *    bound, one at the upper bound itself, and one inside the window.
 *  - '''Replaced.''' A row inserted and updated again before its window is read: the first of
 *    its images is never there to be read.
 *  - '''Moved.''' A row inserted in the window and updated again with a moment of the next one:
 *    its first image has left the window before the window is read.
 *  - '''Lost.''' A row inserted in a transaction that is still open when its window is read, and
 *    committed after: it comes into view behind the watermark, and no window holds it again.
 *  - '''Early.''' A row inserted with a moment of the next window, in a transaction that commits
 *    before that window is read.
 *
 * The rows of the last two are written in one transaction that a test commits when it says
 * (`Held`). The rows of the last three are never changed again by a later round, so that what
 * a test says of them stays true.
 *
 * With the changes comes what the table is (`Table`): what a reader sees when a batch is read,
 * and so what each batch is to have. A test compares Bronze with that, and not with what it
 * reads back from the source.
 */
object TableGenerator {

  /** The table, made by its owner, as the applications of the source have it. */
  def create(c: Connection, table: String): Unit =
    Using.resource(c.createStatement()) { statement =>
      statement.execute(
        s"""CREATE TABLE $table (
           |  order_id bigint PRIMARY KEY,
           |  customer_email text,
           |  amount numeric(12,2) NOT NULL,
           |  status text NOT NULL,
           |  updated_at timestamptz NOT NULL
           |)""".stripMargin
      ): Unit
    }

  /** A row of the table, with every value as the database keeps it. */
  final case class Row(
      orderId: Long,
      customerEmail: Option[String],
      amount: BigDecimal,
      status: String,
      updatedAt: Instant
  )

  sealed trait Change { def orderId: Long }
  object Change {
    final case class Insert(row: Row) extends Change { def orderId: Long = row.orderId }

    /** Another image of a row that is there: every value but the key may change. */
    final case class Update(row: Row) extends Change { def orderId: Long = row.orderId }
    final case class Delete(orderId: Long) extends Change
  }

  /** A stretch of time that a batch reads: after `lower`, up to `upper` and with it. */
  final case class Window(lower: Instant, upper: Instant) {
    require(lower.isBefore(upper), "a window ends after it starts")
    def holds(moment: Instant): Boolean = moment.isAfter(lower) && !moment.isAfter(upper)
  }

  /**
   * What a round has. Every number is of rows; `ties` is of each of its three groups.
   *
   * @param updates rows that are there, given another image
   * @param deletes rows that are there, deleted
   */
  final case class Mix(
      inserts: Int = 0,
      updates: Int = 0,
      deletes: Int = 0,
      ties: Int = 0,
      replaced: Int = 0,
      moved: Int = 0,
      lost: Int = 0,
      early: Int = 0
  ) {
    require(
      Seq(inserts, updates, deletes, ties, replaced, moved, lost, early).forall(_ >= 0),
      "no negative"
    )
  }

  /**
   * The table as a reader sees it: the rows that are committed. The rows of the kinds that are
   * never changed again are `spared`, and a round chooses the rows that it updates or deletes
   * among the others.
   */
  final case class Table(rows: SortedMap[Long, Row], lastKey: Long, spared: Set[Long]) {

    def after(changes: Seq[Change]): Table =
      changes.foldLeft(this) {
        case (table, Change.Insert(row)) =>
          require(!table.rows.contains(row.orderId), s"${row.orderId} is there already")
          table.copy(
            rows = table.rows.updated(row.orderId, row),
            lastKey = table.lastKey max row.orderId
          )
        case (table, Change.Update(row)) =>
          require(table.rows.contains(row.orderId), s"${row.orderId} is not there")
          table.copy(rows = table.rows.updated(row.orderId, row))
        case (table, Change.Delete(key)) =>
          require(table.rows.contains(key), s"$key is not there")
          table.copy(rows = table.rows.removed(key))
      }

    /** The rows that a batch of `window` reads, by their key. */
    def visibleIn(window: Window): Vector[Row] =
      rows.valuesIterator.filter(row => window.holds(row.updatedAt)).toVector
  }

  object Table {
    val Empty: Table = Table(SortedMap.empty, 0L, Set.empty)
  }

  /**
   * The changes of a round, in the order they are written, and what each of its kinds is.
   *
   * @param committed the changes that are committed before the batch of the window is read
   * @param held the changes of a transaction that is open while it is read: the rows `lost`
   *   and `early`
   * @param ties each moment that rows share, with the keys of the rows that the round gave it
   * @param replaced the two images of each row that was replaced
   * @param moved the two images of each row that was moved
   * @param deleted the last image of each row that was deleted
   */
  final case class Round(
      number: Int,
      window: Window,
      committed: Vector[Change],
      held: Vector[Change],
      ties: Vector[(Instant, Vector[Long])],
      replaced: Vector[(Row, Row)],
      moved: Vector[(Row, Row)],
      lost: Vector[Row],
      early: Vector[Row],
      deleted: Vector[Row]
  ) {

    /** The table when the batch of the window reads it: what was committed, and not what is held. */
    def whenRead(before: Table): Table =
      before
        .after(committed)
        .copy(spared = before.spared ++ (moved.map(_._1) ++ lost ++ early).map(_.orderId))

    /** The table once the held transaction is committed as well. */
    def afterwards(before: Table): Table = whenRead(before).after(held)

    /** Every change of the round, the held ones last, one to a line: what a seed means. */
    def describe: String =
      (committed.map("committed " + _) ++ held.map("held " + _)).mkString("\n")
  }

  /**
   * The round of a seed.
   *
   * @param number which round it is, from 1: two rounds of one seed are apart
   * @param window the window whose batch is read after it; its length is a whole number of
   *   seconds, and the next window is as long and starts where it ends
   * @param before the table before the round, as the rounds before it left it
   */
  def round(seed: Long, number: Int, window: Window, before: Table, mix: Mix): Round = {
    require(number >= 1, "a round from 1")
    val seconds = ChronoUnit.SECONDS.between(window.lower, window.upper)
    require(
      window.lower.plusSeconds(seconds) == window.upper && seconds <= Int.MaxValue,
      "a window of whole seconds"
    )
    // The number is spread over every bit of the seed, as FileGenerator does with a delivery.
    val random = new java.util.Random(seed ^ (number * 0x9e3779b97f4a7c15L))
    val next = Window(window.upper, window.upper.plusSeconds(seconds))
    val keys = Iterator.from(1).map(before.lastKey + _)

    /** A moment of a window, after its lower bound and up to its upper bound, to the microsecond. */
    def momentIn(of: Window): Instant =
      of.lower.plus(
        1L + random.nextInt(seconds.toInt) * 1000000L + random.nextInt(1000000),
        ChronoUnit.MICROS
      )
    def row(key: Long, at: Instant): Row =
      Row(key, email(random), amount(random), status(random), at)
    def inserted(at: => Instant): Row = row(keys.next(), at)

    // The rows that are there and may be changed, in an order that the seed decides.
    val there = shuffled(random, before.rows.keys.filterNot(before.spared).toVector)
    require(
      there.size >= mix.updates + mix.deletes,
      s"${there.size} rows to change, and the mix has ${mix.updates + mix.deletes}"
    )
    val deleted = there.take(mix.deletes).map(before.rows)
    val updated =
      there.slice(mix.deletes, mix.deletes + mix.updates).map(key => row(key, momentIn(window)))

    val inserts = Vector.fill(mix.inserts)(inserted(momentIn(window)))
    val middle = Iterator
      .continually(momentIn(window))
      .find(at => at != window.lower.plus(1, ChronoUnit.MICROS) && at != window.upper)
      .get
    val tieMoments = Vector(window.lower.plus(1, ChronoUnit.MICROS), middle, window.upper)
    val tied = tieMoments.map(at => at -> Vector.fill(mix.ties)(inserted(at)))
    val replaced = Vector.fill(mix.replaced) {
      val first = inserted(momentIn(window))
      first -> row(first.orderId, momentIn(window))
    }
    val moved = Vector.fill(mix.moved) {
      val first = inserted(momentIn(window))
      first -> row(first.orderId, momentIn(next))
    }
    val lost = Vector.fill(mix.lost)(inserted(momentIn(window)))
    val early = Vector.fill(mix.early)(inserted(momentIn(next)))

    Round(
      number,
      window,
      committed = deleted.map(r => Change.Delete(r.orderId)) ++
        updated.map(Change.Update) ++
        (inserts ++ tied.flatMap(_._2) ++ replaced.map(_._1) ++ moved.map(_._1))
          .map(Change.Insert) ++
        (replaced.map(_._2) ++ moved.map(_._2)).map(Change.Update),
      held = (lost ++ early).map(Change.Insert),
      ties = tied.map { case (at, rows) => at -> rows.map(_.orderId) },
      replaced = replaced,
      moved = moved,
      lost = lost,
      early = early,
      deleted = deleted
    )
  }

  /** A round that deletes every row of the table, in one transaction. */
  def emptying(number: Int, window: Window, before: Table): Round =
    Round(
      number,
      window,
      committed = before.rows.keys.toVector.map(Change.Delete),
      held = Vector.empty,
      ties = Vector.empty,
      replaced = Vector.empty,
      moved = Vector.empty,
      lost = Vector.empty,
      early = Vector.empty,
      deleted = before.rows.values.toVector
    )

  // -----------------------------------------------------------------------------------------------
  // The changes, written
  // -----------------------------------------------------------------------------------------------

  /** A transaction left open, with the changes that it holds; it is committed once, or never. */
  final class Held private[TableGenerator] (connection: Connection) extends AutoCloseable {
    def commit(): Unit = connection.commit()

    /** Ends the transaction with nothing committed, if it was not; and the connection. */
    def close(): Unit = connection.close()
  }

  /**
   * Writes a round to `table`: its committed changes in one transaction, and its held ones in
   * another, left open on a connection of its own.
   *
   * @param connect a connection of the owner of the table, each time it is called
   */
  def write(round: Round, table: String, connect: () => Connection): Held = {
    Using.resource(connect())(c => inOneTransaction(c, table, round.committed)(_.commit()))
    val held = connect()
    try {
      inOneTransaction(held, table, round.held)(_ => ())
      new Held(held)
    } catch {
      case failure: Throwable =>
        held.close()
        throw failure
    }
  }

  /** The rows of `table`, as a reader sees them, by their key. */
  def readBack(c: Connection, table: String): Vector[Row] =
    Using.resource(c.createStatement()) { statement =>
      val rows = statement.executeQuery(
        s"SELECT order_id, customer_email, amount, status, updated_at FROM $table ORDER BY 1"
      )
      Iterator
        .continually(rows.next())
        .takeWhile(identity)
        .map { _ =>
          Row(
            rows.getLong(1),
            Option(rows.getString(2)),
            BigDecimal(rows.getBigDecimal(3)),
            rows.getString(4),
            rows.getObject(5, classOf[OffsetDateTime]).toInstant
          )
        }
        .toVector
    }

  private def inOneTransaction(c: Connection, table: String, changes: Seq[Change])(
      end: Connection => Unit
  ): Unit = {
    c.setAutoCommit(false)
    changes.foreach {
      case Change.Insert(row) =>
        statement(
          c,
          s"INSERT INTO $table (customer_email, amount, status, updated_at, order_id) " +
            "VALUES (?, ?, ?, ?, ?)",
          row
        )
      case Change.Update(row) =>
        statement(
          c,
          s"UPDATE $table SET customer_email = ?, amount = ?, status = ?, updated_at = ? " +
            "WHERE order_id = ?",
          row
        )
      case Change.Delete(key) =>
        Using.resource(c.prepareStatement(s"DELETE FROM $table WHERE order_id = ?")) { delete =>
          delete.setLong(1, key)
          require(delete.executeUpdate() == 1, s"$key was not there to delete")
        }
    }
    end(c)
  }

  /** A statement whose parameters are the values of a row, its key last. */
  private def statement(c: Connection, sql: String, row: Row): Unit =
    Using.resource(c.prepareStatement(sql)) { statement =>
      row.customerEmail match {
        case Some(email) => statement.setString(1, email)
        case None => statement.setNull(1, Types.VARCHAR)
      }
      statement.setBigDecimal(2, row.amount.bigDecimal)
      statement.setString(3, row.status)
      statement.setObject(4, OffsetDateTime.ofInstant(row.updatedAt, ZoneOffset.UTC))
      statement.setLong(5, row.orderId)
      require(statement.executeUpdate() == 1, s"${row.orderId} was not written")
    }

  // -----------------------------------------------------------------------------------------------
  // Values, as a source has them
  // -----------------------------------------------------------------------------------------------

  private val Emails = Vector(
    Some("ana.ruiz@example.com"),
    Some("o'brien@example.com"),
    Some("núñez.maría@example.es"),
    Some("\"quoted\"@example.com"),
    Some("李雷@example.cn"),
    None
  )
  private val Statuses =
    Vector("placed", "paid", "shipped", "cancelled", "en reparto, \"urgente\"", "a\\b")

  private def email(random: java.util.Random): Option[String] = Emails(random.nextInt(Emails.size))
  private def status(random: java.util.Random): String = Statuses(random.nextInt(Statuses.size))

  /** Up to 99 999.99, always with its two places, as `numeric(12,2)` keeps it. */
  private def amount(random: java.util.Random): BigDecimal =
    BigDecimal(BigInt(random.nextInt(10000000)), 2)

  /** Fisher and Yates, here and not the library's, so that a seed means one order for good. */
  private def shuffled[A](random: java.util.Random, items: Vector[A]): Vector[A] = {
    val array = items.toBuffer
    for (i <- (array.size - 1) to 1 by -1) {
      val j = random.nextInt(i + 1)
      val kept = array(i)
      array(i) = array(j)
      array(j) = kept
    }
    array.toVector
  }
}
