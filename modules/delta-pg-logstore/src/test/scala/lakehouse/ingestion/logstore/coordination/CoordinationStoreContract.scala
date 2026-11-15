package lakehouse.ingestion.logstore.coordination

import java.io.IOException
import java.util.UUID
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

import lakehouse.ingestion.logstore.coordination.RegisterResult.{AlreadyRegistered, Registered}
import lakehouse.ingestion.logstore.coordination.TakeResult.{NotRegistered, Taken, TakenBy}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What the commit protocol may assume of any `CoordinationStore`.
 *
 * Each implementation has a suite that extends this one. The in-memory store used by the
 * protocol's unit tests and the PostgreSQL store used in production therefore pass the same
 * tests, and a behaviour the protocol relies on cannot hold in one and not in the other.
 *
 * Every test works on tables of its own, so the store under test may be shared between tests,
 * and need not be empty.
 */
abstract class CoordinationStoreContract extends AnyFunSuite with Matchers {

  import CoordinationStoreContract._

  /** The store under test. */
  protected def store: CoordinationStore

  test("a table is not coordinated until it is registered") {
    val table = newTable()

    store.registration(table) shouldBe None

    store.register(table, Registration.NotYetCreated, deltaTableId = None) shouldBe Registered
    store.registration(table) shouldBe Some(
      Registration(table, Registration.NotYetCreated, deltaTableId = None, taintReason = None)
    )
  }

  test("registering a table again keeps the first registration") {
    val table = newTable()
    store.register(table, 41L, Some("id-of-the-adopted-table")) shouldBe Registered
    val first = Registration(table, 41L, Some("id-of-the-adopted-table"), taintReason = None)

    store.register(table, 99L, Some("another-id")) shouldBe AlreadyRegistered(first)
    store.register(table, 41L, Some("id-of-the-adopted-table")) shouldBe AlreadyRegistered(first)

    store.registration(table) shouldBe Some(first)
  }

  test("a registration needs a version of -1 or above") {
    an[IllegalArgumentException] should be thrownBy store.register(newTable(), -2L, None)
  }

  test("no version can be taken on a table that is not registered") {
    val table = newTable()

    store.take(claim(table, 0L)) shouldBe NotRegistered

    store.entry(table, 0L) shouldBe None
    store.latestEntry(table) shouldBe None
  }

  test("the first claim takes the version, and a later one is told who owns it") {
    val table = registered()
    val run = UUID.randomUUID()
    val first = Claim(table, 3L, ".tmp/3.json.first", "host-a:1:app", Some(run))
    val owned =
      CommitEntry(table, 3L, ".tmp/3.json.first", complete = false, "host-a:1:app", Some(run))

    store.take(first) shouldBe Taken
    store.entry(table, 3L) shouldBe Some(owned)

    store.take(Claim(table, 3L, ".tmp/3.json.second", "host-b:2:app", None)) shouldBe TakenBy(owned)
    store.entry(table, 3L) shouldBe Some(owned)
  }

  test("a claim that is repeated gets the same answer") {
    val table = registered()
    val winner = claim(table, 5L)
    val loser = claim(table, 5L)

    store.take(winner) shouldBe Taken
    val lost = store.take(loser)
    lost shouldBe a[TakenBy]

    // The winner whose reply was lost learns that the version is its own, not somebody else's.
    store.take(winner) shouldBe Taken
    store.take(loser) shouldBe lost
    store.entry(table, 5L).map(_.tempPath) shouldBe Some(winner.tempPath)
  }

  test("the latest entry is the one with the highest version, as a number") {
    val table = registered()
    store.latestEntry(table) shouldBe None

    // Neither the order of arrival nor the order of the versions as text.
    Seq(9L, 100L, 10L, 2L).foreach(version => store.take(claim(table, version)) shouldBe Taken)

    store.latestEntry(table).map(_.version) shouldBe Some(100L)
  }

  test("an entry is completed through its own temporary path, and stays complete") {
    val table = registered()
    val owner = claim(table, 0L)
    store.take(owner) shouldBe Taken

    // Not the entry of this version: nothing changes.
    store.complete(table, 0L, ".tmp/0.json.somebody-else") shouldBe false
    store.complete(table, 1L, owner.tempPath) shouldBe false
    store.entry(table, 0L).map(_.complete) shouldBe Some(false)

    store.complete(table, 0L, owner.tempPath) shouldBe true
    store.entry(table, 0L).map(_.complete) shouldBe Some(true)

    // A recoverer and the writer may both acknowledge the same commit.
    store.complete(table, 0L, owner.tempPath) shouldBe true
    store.entry(table, 0L).map(_.complete) shouldBe Some(true)
    // Completion does not free the version.
    store.take(claim(table, 0L)) shouldBe a[TakenBy]
  }

  test("a taint shows in the registration, and its first reason stays") {
    val table = registered()

    store.taint(table, "commit 7 has no entry")
    store.taint(table, "commit 9 differs from its entry")

    val registration = store.registration(table).value
    registration.isTainted shouldBe true
    registration.taintReason shouldBe Some("commit 7 has no entry")
  }

  test("a table that is not registered cannot be tainted") {
    val table = newTable()

    an[IOException] should be thrownBy store.taint(table, "commit 7 has no entry")

    store.registration(table) shouldBe None
  }

  test("tables do not see each other's entries or taints") {
    val one = registered()
    val other = registered()
    store.take(claim(one, 0L)) shouldBe Taken
    store.taint(one, "commit 7 has no entry")

    store.entry(other, 0L) shouldBe None
    store.latestEntry(other) shouldBe None
    store.registration(other).value.isTainted shouldBe false
    store.take(claim(other, 0L)) shouldBe Taken
  }

  test("of many writers that claim one version at once, exactly one takes it") {
    val table = registered()
    val claims = (1 to Writers).map(_ => claim(table, 0L))

    val results = atOnce(claims)(store.take)

    val winners = claims.zip(results).collect { case (claim, Taken) => claim }
    winners should have size 1
    val owner = store.entry(table, 0L).value
    owner.tempPath shouldBe winners.head.tempPath
    // Every other writer is told the same owner.
    results.filterNot(_ == Taken).distinct shouldBe Seq(TakenBy(owner))
  }

  test("of many writers that register one table at once, exactly one registers it") {
    val table = newTable()
    val versions = (1 to Writers).map(_.toLong)

    val results = atOnce(versions)(version => store.register(table, version, None))

    val winners = versions.zip(results).collect { case (version, Registered) => version }
    winners should have size 1
    val kept = store.registration(table).value
    kept.registeredVersion shouldBe winners.head
    results.filterNot(_ == Registered).distinct shouldBe Seq(AlreadyRegistered(kept))
  }

  private def registered(): TableUri = {
    val table = newTable()
    store.register(table, Registration.NotYetCreated, deltaTableId = None) shouldBe Registered
    table
  }

  /** Runs `call` for every input on its own thread, all released at the same moment. */
  private def atOnce[A, B](inputs: Seq[A])(call: A => B): Seq[B] = {
    val pool = Executors.newFixedThreadPool(inputs.size)
    implicit val threads: ExecutionContext = ExecutionContext.fromExecutor(pool)
    val start = new CountDownLatch(1)
    try {
      val calls = inputs.map { input =>
        Future {
          start.await(WaitSeconds, TimeUnit.SECONDS)
          call(input)
        }
      }
      start.countDown()
      Await.result(Future.sequence(calls), WaitSeconds.seconds)
    } finally pool.shutdownNow(): Unit
  }

  implicit private class Defined[A](option: Option[A]) {

    /** The value, or a failed test. */
    def value: A = option.getOrElse(fail("expected a value, found None"))
  }
}

object CoordinationStoreContract {

  private val Writers = 16
  private val WaitSeconds = 30L

  /**
   * A table no other test uses. A store never looks at what a key says, so these tests need no
   * directory behind it; the keys of real paths are `TableIdentitySpec`'s subject.
   */
  def newTable(): TableUri = new TableUri(s"file:/contract/${UUID.randomUUID()}")

  /** A claim on `version` with a temporary path of its own, as each write attempt has. */
  def claim(table: TableUri, version: Long): Claim =
    Claim(
      table,
      version,
      tempPath = f".tmp/$version%020d.json.${UUID.randomUUID()}",
      writerInstance = "host:1:app",
      runId = None
    )
}
