package lakehouse.ingestion.logstore.coordination

import java.io.IOException
import java.util.UUID

import lakehouse.ingestion.logstore.Concurrently
import lakehouse.ingestion.logstore.coordination.RegisterResult.{AlreadyRegistered, Registered}
import lakehouse.ingestion.logstore.coordination.TakeResult.{
  EpochNotInForce,
  NotRegistered,
  RegistrationChanged,
  Taken,
  TakenBy
}
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

    val made = registrationBy(store.register(table, Registration.NotYetCreated, None))
    made shouldBe Registration(table, Registration.NotYetCreated, None, None, made.generation)
    store.registration(table) shouldBe Some(made)
  }

  test("every registration has a generation of its own") {
    val generations = (1 to 3).map { _ =>
      registrationBy(store.register(newTable(), Registration.NotYetCreated, None)).generation
    }

    generations.distinct shouldBe generations
  }

  test("registering a table again keeps the first registration") {
    val table = newTable()
    val first = registrationBy(store.register(table, 41L, Some("id-of-the-adopted-table")))
    (first.registeredVersion, first.deltaTableId) shouldBe
      ((41L, Some("id-of-the-adopted-table")))

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

  test("a claim under a generation that the registration does not have takes nothing") {
    val table = registered()
    val current = generationOf(table)
    // What a writer holds that read the registry of an earlier registration of the table.
    val stale = claim(table, 0L).copy(generation = current - 1)

    store.take(stale) shouldBe RegistrationChanged

    store.entry(table, 0L) shouldBe None
    // The version is free for a writer that read the registration as it is.
    store.take(stale.copy(generation = current)) shouldBe Taken
  }

  test("the first claim takes the version, and a later one is told who owns it") {
    val table = registered()
    val run = UUID.randomUUID()
    val generation = generationOf(table)
    val first = Claim(table, 3L, ".tmp/3.json.first", "host-a:1:app", Some(run), generation)
    val owned =
      CommitEntry(table, 3L, ".tmp/3.json.first", complete = false, "host-a:1:app", Some(run))

    store.take(first) shouldBe Taken
    store.entry(table, 3L) shouldBe Some(owned)

    val second = Claim(table, 3L, ".tmp/3.json.second", "host-b:2:app", None, generation)
    store.take(second) shouldBe TakenBy(owned)
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

    val results = Concurrently.atOnce(claims)(store.take)

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

    val results = Concurrently.atOnce(versions)(version => store.register(table, version, None))

    val winners = versions.zip(results).collect { case (version, _: Registered) => version }
    winners should have size 1
    val kept = store.registration(table).value
    kept.registeredVersion shouldBe winners.head
    results.distinct should contain theSameElementsAs
      Seq(Registered(kept), AlreadyRegistered(kept))
  }

  // ---------------------------------------------------------------------------------------------
  // The epoch that a writer commits under (ADR-0019)
  // ---------------------------------------------------------------------------------------------

  test("the epoch in force under a key is the latest that was announced, and only grows") {
    val key = newKey()

    store.announce(WriterEpoch(key, 3L)) shouldBe 3L
    // Again, as a writer whose reply was lost asks: the same answer, and nothing changed.
    store.announce(WriterEpoch(key, 3L)) shouldBe 3L
    store.announce(WriterEpoch(key, 5L)) shouldBe 5L
    // An earlier one changes nothing, and whoever announced it is told which is in force.
    store.announce(WriterEpoch(key, 4L)) shouldBe 5L
    store.announce(WriterEpoch(key, 1L)) shouldBe 5L
    // Another key has its own.
    store.announce(WriterEpoch(newKey(), 1L)) shouldBe 1L
  }

  test("an epoch is 1 or more, and is of a key") {
    an[IllegalArgumentException] should be thrownBy WriterEpoch(newKey(), 0L)
    an[IllegalArgumentException] should be thrownBy WriterEpoch("  ", 1L)
  }

  test("a claim under the epoch in force takes its version") {
    val table = registered()
    val turn = WriterEpoch(newKey(), 2L)
    store.announce(turn) shouldBe 2L

    store.take(claim(table, 0L).copy(epoch = Some(turn))) shouldBe Taken
    store.take(claim(table, 1L).copy(epoch = Some(turn))) shouldBe Taken

    store.latestEntry(table).map(_.version) shouldBe Some(1L)
  }

  test("a claim under an epoch that a later one replaced takes nothing, then or afterwards") {
    val table = registered()
    val key = newKey()
    val lost = WriterEpoch(key, 2L)
    store.announce(lost) shouldBe 2L
    store.take(claim(table, 0L).copy(epoch = Some(lost))) shouldBe Taken

    store.announce(WriterEpoch(key, 3L)) shouldBe 3L

    val late = claim(table, 1L).copy(epoch = Some(lost))
    store.take(late) shouldBe EpochNotInForce(Some(3L))
    // However often it asks, and at whatever version.
    store.take(late) shouldBe EpochNotInForce(Some(3L))
    store.take(claim(table, 2L).copy(epoch = Some(lost))) shouldBe EpochNotInForce(Some(3L))
    store.entry(table, 1L) shouldBe None
    // The version is free for the writer that has the turn.
    store.take(claim(table, 1L).copy(epoch = Some(WriterEpoch(key, 3L)))) shouldBe Taken
  }

  test("a claim under an epoch that nobody announced takes nothing") {
    val table = registered()
    val key = newKey()

    // No epoch of the key at all, and then one that is ahead of the one in force: a writer
    // announces its epoch before it commits under it.
    store.take(claim(table, 0L).copy(epoch = Some(WriterEpoch(key, 1L)))) shouldBe
      EpochNotInForce(None)
    store.announce(WriterEpoch(key, 1L)) shouldBe 1L
    store.take(claim(table, 0L).copy(epoch = Some(WriterEpoch(key, 2L)))) shouldBe
      EpochNotInForce(Some(1L))

    store.entry(table, 0L) shouldBe None
  }

  test("a claim that names no epoch is held to none, whatever was announced") {
    val table = registered()
    val key = newKey()
    store.announce(WriterEpoch(key, 7L)) shouldBe 7L

    // A writer that no lease numbers: an operator's OPTIMIZE, a table that nobody leases.
    store.take(claim(table, 0L)) shouldBe Taken
    store.take(claim(table, 1L).copy(epoch = Some(WriterEpoch(key, 7L)))) shouldBe Taken
    store.take(claim(table, 2L)) shouldBe Taken
  }

  test("a version that a claim took in its turn is its own, also once its turn is over") {
    val table = registered()
    val key = newKey()
    val turn = WriterEpoch(key, 1L)
    store.announce(turn) shouldBe 1L
    val mine = claim(table, 0L).copy(epoch = Some(turn))
    store.take(mine) shouldBe Taken

    store.announce(WriterEpoch(key, 2L)) shouldBe 2L

    // The writer whose reply was lost asks again: the commit exists, and it is told so. Were
    // it told that it had no version, it would throw away the bytes of a commit that is made.
    store.take(mine) shouldBe Taken
    store.entry(table, 0L).map(_.tempPath) shouldBe Some(mine.tempPath)
  }

  test("a claim out of its turn is told so, whoever has the version") {
    val table = registered()
    val key = newKey()
    store.announce(WriterEpoch(key, 2L)) shouldBe 2L
    store.take(claim(table, 0L).copy(epoch = Some(WriterEpoch(key, 2L)))) shouldBe Taken

    // Told that the version is taken, the writer would try the next one, to be refused there.
    store.take(claim(table, 0L).copy(epoch = Some(WriterEpoch(key, 1L)))) shouldBe
      EpochNotInForce(Some(2L))
  }

  test("keys do not see each other's epochs") {
    val table = registered()
    val (one, other) = (newKey(), newKey())
    store.announce(WriterEpoch(one, 1L)) shouldBe 1L
    store.announce(WriterEpoch(other, 1L)) shouldBe 1L

    store.announce(WriterEpoch(one, 9L)) shouldBe 9L

    // Two sources that write one table, each under its own lease.
    store.take(claim(table, 0L).copy(epoch = Some(WriterEpoch(other, 1L)))) shouldBe Taken
    store.take(claim(table, 1L).copy(epoch = Some(WriterEpoch(one, 1L)))) shouldBe
      EpochNotInForce(Some(9L))
  }

  test("once a later epoch is announced, no claim under the earlier one takes a version") {
    (1 to Rounds).foreach { _ =>
      val table = registered()
      val key = newKey()
      val earlier = WriterEpoch(key, 1L)
      store.announce(earlier) shouldBe 1L
      val claims = (0 until Writers).map(v => claim(table, v.toLong).copy(epoch = Some(earlier)))

      // The claims and the announcement of the next epoch, all at once. What the registry has
      // of the table is read as soon as the announcement returns, as the writer that takes the
      // work over reads it.
      val calls = claims.map(Left(_)) :+ Right(WriterEpoch(key, 2L))
      val results = Concurrently.atOnce(calls) {
        case Left(one) => Left(store.take(one))
        case Right(next) =>
          store.announce(next) shouldBe 2L
          Right(claims.filter(one => store.entry(table, one.version).nonEmpty).map(_.version))
      }

      val seenByTheNext = results.collectFirst { case Right(versions) => versions.toSet }.value
      val taken = claims.zip(results).collect { case (one, Left(Taken)) => one.version }.toSet
      // Every claim was answered one of the two, and the ones that took a version are exactly
      // the ones that the next writer found: none got in behind its back.
      val answers = results.collect { case Left(result) => result }.toSet
      answers.subsetOf(Set[TakeResult](Taken, EpochNotInForce(Some(2L)))) shouldBe true
      taken shouldBe seenByTheNext
    }
  }

  private def registered(): TableUri = {
    val table = newTable()
    store.register(table, Registration.NotYetCreated, deltaTableId = None) shouldBe a[Registered]
    table
  }

  /** The registration that a call made, or a failed test if it made none. */
  private def registrationBy(result: RegisterResult): Registration = result match {
    case Registered(registration) => registration
    case other => fail(s"expected a new registration, got $other")
  }

  private def generationOf(table: TableUri): Long = store.registration(table).value.generation

  /** A claim by a writer that read the registration of `table` just now. */
  private def claim(table: TableUri, version: Long): Claim =
    CoordinationStoreContract.claim(store, table, version)

  implicit private class Defined[A](option: Option[A]) {

    /** The value, or a failed test. */
    def value: A = option.getOrElse(fail("expected a value, found None"))
  }
}

object CoordinationStoreContract {

  private val Writers = 16

  /** How many times the race of claims and an announcement is run. */
  private val Rounds = 20

  /** A key of epochs that no other test uses. */
  def newKey(): String = s"contract.${UUID.randomUUID()}/bronze"

  /**
   * A table no other test uses. A store never looks at what a key says, so these tests need no
   * directory behind it; the keys of real paths are `TableIdentitySpec`'s subject.
   */
  def newTable(): TableUri = new TableUri(s"file:/contract/${UUID.randomUUID()}")

  /** A generation that no registration has: the one of a claim on a table that has none. */
  private val NoGeneration = 0L

  /**
   * A claim on `version` with a temporary path of its own, as each write attempt has, by a
   * writer that read the registration of `table` in `store` just now.
   */
  def claim(store: CoordinationStore, table: TableUri, version: Long): Claim =
    Claim(
      table,
      version,
      tempPath = f".tmp/$version%020d.json.${UUID.randomUUID()}",
      writerInstance = "host:1:app",
      runId = None,
      generation = store.registration(table).map(_.generation).getOrElse(NoGeneration)
    )
}
