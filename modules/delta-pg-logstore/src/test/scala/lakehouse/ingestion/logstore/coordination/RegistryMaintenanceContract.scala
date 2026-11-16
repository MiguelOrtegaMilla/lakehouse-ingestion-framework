package lakehouse.ingestion.logstore.coordination

import lakehouse.ingestion.logstore.coordination.CoordinationStoreContract.{claim, newTable}
import lakehouse.ingestion.logstore.coordination.RegisterResult.Registered
import lakehouse.ingestion.logstore.coordination.TakeResult.Taken
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
    store.register(table, Registration.NotYetCreated, Some("the-first-id")) shouldBe Registered
    versions.foreach { version =>
      val taken = claim(table, version)
      store.take(taken) shouldBe Taken
      store.complete(table, version, taken.tempPath) shouldBe true
    }
    table
  }

  test("a table that was never coordinated is adopted at the version given, once") {
    val table = newTable()

    maintenance.adopt(table, 41L, Some("its-id")) shouldBe true
    val adopted = Registration(table, 41L, Some("its-id"), taintReason = None)
    maintenance.registration(table) shouldBe Some(adopted)

    // A table that is registered is not adopted: its registration is not an operator's slip away.
    maintenance.adopt(table, 99L, Some("another-id")) shouldBe false
    store.registration(table) shouldBe Some(adopted)

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

    val readoption = maintenance.readopt(table, 3L, deltaTableId = None).value

    readoption.clearedTaint shouldBe Some("version 3 is not the commit that took it")
    // The entry that is not complete, and the one above the version the log is declared to be at.
    readoption.removedEntries.map(entry => entry.version -> entry.tempPath) shouldBe
      Seq(3L -> unfinished.tempPath, 4L -> above.tempPath)
    // The id it had stays, since no other was given.
    store.registration(table) shouldBe
      Some(Registration(table, 3L, Some("the-first-id"), taintReason = None))

    // The commits the log agrees with keep their entries.
    (0L to 2L).map(store.entry(table, _).map(_.complete)) shouldBe Seq.fill(3)(Some(true))
    Seq(3L, 4L).foreach(store.entry(table, _) shouldBe None)
    store.latestEntry(table).map(_.version) shouldBe Some(2L)
    // And the next version can be taken again.
    store.take(claim(table, 4L)) shouldBe Taken
  }

  test("adopting a table again records a new id only when one is given") {
    val table = tableWith(0L)

    maintenance.readopt(table, 0L, Some("another-id")) shouldBe Some(Readoption(None, Nil))
    store.registration(table).value.deltaTableId shouldBe Some("another-id")

    maintenance.readopt(table, 0L, None) shouldBe Some(Readoption(None, Nil))
    store.registration(table).value.deltaTableId shouldBe Some("another-id")
  }

  test("a table that is not registered cannot be adopted again") {
    val table = newTable()

    maintenance.readopt(table, 5L, Some("an-id")) shouldBe None

    store.registration(table) shouldBe None
  }

  test("a table is adopted at a version it has: not below zero") {
    an[IllegalArgumentException] should be thrownBy maintenance.adopt(newTable(), -1L, None)
    an[IllegalArgumentException] should be thrownBy maintenance.readopt(tableWith(0L), -1L, None)
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

    maintenance.forget(table) shouldBe true

    store.registration(table) shouldBe None
    (0L to 2L).foreach(store.entry(table, _) shouldBe None)
    maintenance.unfinished(table) shouldBe empty
    store.entry(other, 0L).map(_.complete) shouldBe Some(true)

    // Nothing is left to forget, and a table can be created at the path as if it were new.
    maintenance.forget(table) shouldBe false
    store.register(table, Registration.NotYetCreated, None) shouldBe Registered
    store.take(claim(table, 0L)) shouldBe Taken
  }

  implicit private class Defined[A](option: Option[A]) {

    /** The value, or a failed test. */
    def value: A = option.getOrElse(fail("expected a value, found None"))
  }
}
