package lakehouse.ingestion.logstore.coordination

import lakehouse.ingestion.logstore.coordination.CoordinationStoreContract.newTable
import lakehouse.ingestion.logstore.coordination.RegisterResult.Registered
import lakehouse.ingestion.logstore.coordination.RegistryChange.{
  Applied,
  EntriesChanged,
  NotRegistered
}
import lakehouse.ingestion.logstore.coordination.TakeResult.{RegistrationChanged, Taken}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What an operator's tools may assume of any `RegistryMaintenance`, on the registry that a
 * `CoordinationStore` of the commit path works in. The in-memory registry and PostgreSQL pass
 * the same tests.
 *
 * Every test works on tables of its own, so the registry may be shared between tests.
 */
abstract class RegistryMaintenanceContract extends AnyFunSuite with Matchers {

  /** The commit path's view of the registry under test. */
  protected def store: CoordinationStore

  /** The operator's view of the same registry. */
  protected def maintenance: RegistryMaintenance

  /** A table with complete entries for `versions`, created through the commit path. */
  private def tableWith(versions: Long*): TableUri = {
    val table = newTable()
    store.register(table, Registration.NotYetCreated, Some("the-first-id")) shouldBe a[Registered]
    versions.foreach { version =>
      val taken = claim(table, version)
      store.take(taken) shouldBe Taken
      store.complete(table, version, taken.tempPath) shouldBe true
    }
    table
  }

  /** A claim by a writer that read the registration of `table` just now. */
  protected def claim(table: TableUri, version: Long): Claim =
    CoordinationStoreContract.claim(store, table, version)

  /** Adopts `table` again as an operator who has just examined the entries its log contradicts. */
  private def readopt(
      table: TableUri,
      version: Long,
      deltaTableId: Option[String]
  ): RegistryChange[Readoption] =
    maintenance.readopt(table, version, deltaTableId, maintenance.contradicted(table, version))

  /** Forgets `table` as an operator who has just examined its unfinished entries. */
  private def forget(table: TableUri): RegistryChange[Unit] =
    maintenance.forget(table, maintenance.unfinished(table))

  /** What the registration says, without the generation the registry gave it. */
  private def saidBy(
      registration: Option[Registration]
  ): Option[(Long, Option[String], Option[String])] =
    registration.map(r => (r.registeredVersion, r.deltaTableId, r.taintReason))

  test("a table that was never coordinated is adopted at the version given, once") {
    val table = newTable()

    maintenance.adopt(table, 41L, Some("its-id")) shouldBe true
    val adopted = maintenance.registration(table)
    saidBy(adopted) shouldBe Some((41L, Some("its-id"), None))

    // A table that is registered is not adopted: its registration is not an operator's slip away.
    maintenance.adopt(table, 99L, Some("another-id")) shouldBe false
    store.registration(table) shouldBe adopted

    // From then on its commits are coordinated.
    store.take(claim(table, 42L)) shouldBe Taken
  }

  test("adopting a table again moves its version, clears its taint and removes what contradicts") {
    val table = tableWith(0L, 1L, 2L)
    val unfinished = claim(table, 3L)
    val above = claim(table, 4L)
    Seq(unfinished, above).foreach(store.take(_) shouldBe Taken)
    store.complete(table, 4L, above.tempPath) shouldBe true
    store.taint(table, "version 3 is not the commit that took it")

    val readoption = readopt(table, 3L, deltaTableId = None) match {
      case Applied(done) => done
      case other => fail(s"the table was not adopted again: $other")
    }

    readoption.clearedTaint shouldBe Some("version 3 is not the commit that took it")
    // The entry that is not complete, and the one above the version the log is declared to be at.
    readoption.removedEntries.map(entry => entry.version -> entry.tempPath) shouldBe
      Seq(3L -> unfinished.tempPath, 4L -> above.tempPath)
    // The id it had stays, since no other was given.
    saidBy(store.registration(table)) shouldBe Some((3L, Some("the-first-id"), None))

    // The commits the log agrees with keep their entries.
    (0L to 2L).map(store.entry(table, _).map(_.complete)) shouldBe Seq.fill(3)(Some(true))
    Seq(3L, 4L).foreach(store.entry(table, _) shouldBe None)
    store.latestEntry(table).map(_.version) shouldBe Some(2L)
    // And the next version can be taken again.
    store.take(claim(table, 4L)) shouldBe Taken
  }

  test("adopting a table again records a new id only when one is given") {
    val table = tableWith(0L)

    readopt(table, 0L, Some("another-id")) shouldBe Applied(Readoption(None, Nil))
    store.registration(table).value.deltaTableId shouldBe Some("another-id")

    readopt(table, 0L, None) shouldBe Applied(Readoption(None, Nil))
    store.registration(table).value.deltaTableId shouldBe Some("another-id")
  }

  test("a table that is not registered cannot be adopted again") {
    val table = newTable()

    readopt(table, 5L, Some("an-id")) shouldBe NotRegistered

    store.registration(table) shouldBe None
  }

  test("a table is adopted at a version it has: not below zero") {
    an[IllegalArgumentException] should be thrownBy maintenance.adopt(newTable(), -1L, None)
    an[IllegalArgumentException] should be thrownBy
      maintenance.readopt(tableWith(0L), -1L, None, Nil)
  }

  // ---------------------------------------------------------------------------------------------
  // A writer that was not stopped (ADR-0004 §3.5)
  // ---------------------------------------------------------------------------------------------

  test("a table adopted again has a new generation: a claim made before it takes nothing") {
    val table = tableWith(0L, 1L)
    val before = store.registration(table).value.generation
    // A writer read the registration, and has not taken its version yet.
    val stalled = claim(table, 2L)

    readopt(table, 1L, None) shouldBe Applied(Readoption(None, Nil))

    store.registration(table).value.generation should not be before
    store.take(stalled) shouldBe RegistrationChanged
    store.entry(table, 2L) shouldBe None
    // The entries that stayed are the table's, under its new registration: it is written on.
    (0L to 1L).map(store.entry(table, _).map(_.complete)) shouldBe Seq.fill(2)(Some(true))
    store.take(claim(table, 2L)) shouldBe Taken
  }

  test("a table forgotten and registered again is another table to a claim made before") {
    val table = tableWith(0L, 1L)
    val stalled = claim(table, 2L)

    forget(table) shouldBe Applied(())
    store.take(stalled) shouldBe TakeResult.NotRegistered

    // A table is created at the same path. The writer of the old one gets no version of it.
    store.register(table, Registration.NotYetCreated, Some("the-second-id")) shouldBe a[Registered]
    store.take(stalled) shouldBe RegistrationChanged
    store.latestEntry(table) shouldBe None
    store.take(claim(table, 0L)) shouldBe Taken
  }

  test("a writer that owns its version is told so again after the table is adopted again") {
    val table = tableWith(0L)
    val mine = claim(table, 1L)
    store.take(mine) shouldBe Taken
    store.complete(table, 1L, mine.tempPath) shouldBe true

    readopt(table, 1L, None) shouldBe Applied(Readoption(None, Nil))

    // The reply to its claim was lost, and it asks again with the generation it had read.
    store.take(mine) shouldBe Taken
  }

  test("a table is not adopted again if its entries are not the ones that were examined") {
    val table = tableWith(0L, 1L)
    store.taint(table, "version 2 has no entry")
    val before = store.registration(table)
    val examined = maintenance.contradicted(table, 1L)
    examined shouldBe empty
    // After the operator has looked, a writer that was not stopped takes version 2.
    val taken = claim(table, 2L)
    store.take(taken) shouldBe Taken

    val called = maintenance.readopt(table, 1L, None, examined)

    called shouldBe EntriesChanged(store.entry(table, 2L).toSeq)
    // Nothing was changed: not the registration, its generation or its taint, nor the entry.
    store.registration(table) shouldBe before
    store.complete(table, 2L, taken.tempPath) shouldBe true
  }

  test("a table is not forgotten if its unfinished entries are not the ones that were examined") {
    val table = tableWith(0L)
    val before = store.registration(table)
    val examined = maintenance.unfinished(table)
    val taken = claim(table, 1L)
    store.take(taken) shouldBe Taken

    maintenance.forget(table, examined) shouldBe EntriesChanged(store.entry(table, 1L).toSeq)

    store.registration(table) shouldBe before
    store.entry(table, 0L).map(_.complete) shouldBe Some(true)
    store.complete(table, 1L, taken.tempPath) shouldBe true
  }

  test("the entries a log contradicts are the ones above its version and the unfinished ones") {
    val table = tableWith(0L, 1L, 3L)
    val unfinished = claim(table, 2L)
    store.take(unfinished) shouldBe Taken

    maintenance.contradicted(table, 3L).map(_.version) shouldBe Seq(2L)
    maintenance.contradicted(table, 1L).map(_.version) shouldBe Seq(2L, 3L)
    maintenance.contradicted(table, 0L).map(_.version) shouldBe Seq(1L, 2L, 3L)
    maintenance.contradicted(tableWith(0L, 1L), 1L) shouldBe empty
  }

  test("the unfinished entries of a table are the ones that are not complete, lowest first") {
    val table = tableWith(0L)
    maintenance.unfinished(table) shouldBe empty

    val later = claim(table, 2L)
    val earlier = claim(table, 1L)
    Seq(later, earlier).foreach(store.take(_) shouldBe Taken)

    maintenance.unfinished(table).map(_.tempPath) shouldBe Seq(earlier.tempPath, later.tempPath)
    maintenance.entry(table, 1L) shouldBe store.entry(table, 1L)
  }

  test("forgetting a table removes its taint, its entries and its registration, and no other's") {
    val table = tableWith(0L, 1L)
    store.take(claim(table, 2L)) shouldBe Taken
    store.taint(table, "version 2 has no entry")
    val other = tableWith(0L)

    forget(table) shouldBe Applied(())

    store.registration(table) shouldBe None
    (0L to 2L).foreach(store.entry(table, _) shouldBe None)
    maintenance.unfinished(table) shouldBe empty
    store.entry(other, 0L).map(_.complete) shouldBe Some(true)

    // Nothing is left to forget, and a table can be created at the path as if it were new.
    forget(table) shouldBe NotRegistered
    store.register(table, Registration.NotYetCreated, None) shouldBe a[Registered]
    store.take(claim(table, 0L)) shouldBe Taken
  }

  implicit private class Defined[A](option: Option[A]) {

    /** The value, or a failed test. */
    def value: A = option.getOrElse(fail("expected a value, found None"))
  }
}
