package lakehouse.ingestion.it.support

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.time.Instant
import java.time.temporal.ChronoUnit

import lakehouse.ingestion.it.support.TableGenerator._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The generator of a table, by itself: that a seed means one series of changes, that a round
 * has what its mix says, each row with a moment of the window it is said to be in, and that
 * what it says the table is follows from its changes. What it writes to a database is held to
 * what it says by `GeneratedTableSpec`, which has one.
 */
final class TableGeneratorSpec extends AnyFunSuite with Matchers {

  private val Start = Instant.parse("2026-01-01T00:00:00Z")
  private def day(n: Int): Window =
    Window(Start.plus((n - 1).toLong, ChronoUnit.DAYS), Start.plus(n.toLong, ChronoUnit.DAYS))

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

  /** The rounds of a seed, each from the table that the one before it left, once committed. */
  private def rounds(seed: Long, count: Int, mix: Mix = Everything): Vector[(Table, Round)] =
    (1 to count)
      .foldLeft((Table.Empty, Vector.empty[(Table, Round)])) { case ((table, done), n) =>
        // The first round has nothing to change yet.
        val of = if (n == 1) mix.copy(updates = 0, deletes = 0) else mix
        val round = TableGenerator.round(seed, n, day(n), table, of)
        (round.afterwards(table), done :+ (table -> round))
      }
      ._2

  private def digest(of: Seq[Round]): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(of.map(_.describe).mkString("\n--\n").getBytes(UTF_8))
      .map(b => f"$b%02x")
      .mkString

  test("a seed gives the same changes every time, and another seed gives others") {
    rounds(7, 4) shouldBe rounds(7, 4)
    rounds(7, 4).map(_._2.describe) should not be rounds(8, 4).map(_._2.describe)
  }

  test("a seed means the same changes on every machine, and for good") {
    // Whoever changes what the generator makes changes this on purpose: the data of every test
    // that names a seed changes with it.
    digest(rounds(1, 4).map(_._2)) shouldBe
      "717982aa5b869123dee73761ef2d7722c7389116a82fb7df4f95d3c98f0ea839"
  }

  test("a round has what its mix says, each row with a moment of its window") {
    for (seed <- 1L to 30L; (before, round) <- rounds(seed, 5)) withClue(s"seed $seed: ") {
      val mix = if (round.number == 1) Everything.copy(updates = 0, deletes = 0) else Everything
      val window = round.window
      val next = Window(window.upper, window.upper.plus(1, ChronoUnit.DAYS))

      round.deleted.size shouldBe mix.deletes
      round.committed.collect { case u: Change.Update => u } should have size
        (mix.updates + mix.replaced + mix.moved).toLong
      round.committed.collect { case i: Change.Insert => i } should have size
        (mix.inserts + 3 * mix.ties + mix.replaced + mix.moved).toLong
      round.held should have size (mix.lost + mix.early).toLong
      round.held.forall(_.isInstanceOf[Change.Insert]) shouldBe true

      // Every row has a moment of its window, but the second image of a row that was moved and
      // the rows that are early, which have one of the next window.
      val elsewhere = (round.moved.map(_._2) ++ round.early).toSet
      val written = (round.committed ++ round.held).collect {
        case Change.Insert(row) => row
        case Change.Update(row) => row
      }
      written.filterNot(elsewhere).foreach(row => window.holds(row.updatedAt) shouldBe true)
      elsewhere.foreach(row => next.holds(row.updatedAt) shouldBe true)

      // Three groups of ties: a microsecond after the lower bound, inside, at the upper bound.
      round.ties.map(_._1).head shouldBe window.lower.plus(1, ChronoUnit.MICROS)
      round.ties.map(_._1).last shouldBe window.upper
      round.ties.map(_._1).distinct should have size 3
      round.ties.foreach(_._2 should have size mix.ties.toLong)

      // A row is changed by one transaction alone, and no spared row is changed again.
      val committedKeys = round.committed.map(_.orderId).toSet
      round.held.map(_.orderId).toSet.intersect(committedKeys) shouldBe empty
      (round.committed.collect {
        case Change.Update(row) => row.orderId
        case Change.Delete(deleted) => deleted
      }.toSet intersect before.spared) shouldBe empty
      // Every key that is inserted is new, and only that row's.
      val inserted = (round.committed ++ round.held).collect { case Change.Insert(row) =>
        row.orderId
      }
      inserted.distinct should have size inserted.size.toLong
      inserted.foreach(before.rows.contains(_) shouldBe false)
    }
  }

  test("the table when a window is read has what was committed, and not what is held") {
    for (seed <- 1L to 30L; (before, round) <- rounds(seed, 5)) withClue(s"seed $seed: ") {
      val read = round.whenRead(before)
      val after = round.afterwards(before)

      round.deleted.foreach(row => read.rows.contains(row.orderId) shouldBe false)
      round.lost.foreach(row => read.rows.contains(row.orderId) shouldBe false)
      round.lost.foreach(row => after.rows.get(row.orderId) shouldBe Some(row))
      // A row replaced or moved is there in its second image alone.
      (round.replaced ++ round.moved).foreach { case (_, second) =>
        read.rows.get(second.orderId) shouldBe Some(second)
      }
      // What the batch of the window reads: the rows of the window, with each tie whole.
      val visible = read.visibleIn(round.window).map(_.orderId).toSet
      round.ties.flatMap(_._2).foreach(visible should contain(_))
      round.lost.map(_.orderId).foreach(visible should not contain _)
      round.moved.map(_._1.orderId).foreach(visible should not contain _)
      round.early.map(_.orderId).foreach(visible should not contain _)
      // The rows held back, and the moved ones, are never changed by a round after.
      after.spared should contain allElementsOf
        (round.lost ++ round.early ++ round.moved.map(_._1)).map(_.orderId)
    }
  }

  test("a round that empties the table deletes every row of it") {
    val (before, last) = rounds(3, 3).last
    val table = last.afterwards(before)
    val emptied = emptying(4, day(4), table)

    emptied.whenRead(table).rows shouldBe empty
    emptied.deleted shouldBe table.rows.values.toVector
    emptied.held shouldBe empty
  }
}
