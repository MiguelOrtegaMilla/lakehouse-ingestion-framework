package lakehouse.ingestion.it.logstore

import java.sql.Connection

import scala.util.Using

import lakehouse.ingestion.it.support.{BootstrappedPostgres, CoordinationSchema, CoordinationStores}
import lakehouse.ingestion.logstore.coordination.CoordinationStoreContract.{claim, newTable}
import lakehouse.ingestion.logstore.coordination.{
  PostgresCoordinationStore,
  PostgresRegistryMaintenance,
  Readoption,
  RegistryChange,
  TableUri,
  TakeResult
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The coordination schema upgraded under a registry that is in use: tables and entries made by
 * version 2 of the schema, then migration V3 (the generation of a registration, ADR-0004 §3.5).
 * `CoordinationSchemaSpec` has the schema as the migrations build it on an empty database.
 */
final class CoordinationSchemaUpgradeSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private lazy val pg = BootstrappedPostgres.start()

  override def afterAll(): Unit = pg.stop()

  private def asCommitPath[T](body: Connection => T): T =
    Using.resource(pg.connect(CoordinationSchema.CommitPathRole, CoordinationSchema.Database))(body)

  private def update(c: Connection, sql: String, parameters: AnyRef*): Int =
    Using.resource(c.prepareStatement(sql)) { statement =>
      parameters.zipWithIndex.foreach { case (value, i) => statement.setObject(i + 1, value) }
      statement.executeUpdate()
    }

  /** A table with complete entries for versions 0 and 1 and a taken version 2, as V2 stored it. */
  private def tableOfVersion2(c: Connection): TableUri = {
    val table = newTable()
    update(
      c,
      "INSERT INTO delta_coordination.coordinated_table (table_uri, registered_version) " +
        "VALUES (?, -1)",
      table.value
    ) shouldBe 1
    (0L to 2L).foreach { version =>
      update(
        c,
        "INSERT INTO delta_coordination.commit_entry " +
          "(table_uri, version, temp_path, writer_instance) VALUES (?, ?, ?, 'host:1:app')",
        table.value,
        Long.box(version),
        s".tmp/$version.json.of-version-2"
      ) shouldBe 1
    }
    update(
      c,
      "UPDATE delta_coordination.commit_entry SET complete = true, completed_at = now() " +
        "WHERE table_uri = ? AND version < 2",
      table.value
    ) shouldBe 2
    table
  }

  test("V3 gives the registrations and entries of version 2 their generations, and they go on") {
    CoordinationSchema.migrate(pg, upTo = "2").migrationsExecuted shouldBe 2
    val (one, other) = asCommitPath(c => (tableOfVersion2(c), tableOfVersion2(c)))

    CoordinationSchema.migrate(pg).migrationsExecuted shouldBe 1

    val commitPath = CoordinationStores.settings(pg.jdbcUrl(CoordinationSchema.Database), pg)
    val operator = CoordinationStores.settings(
      pg.jdbcUrl(CoordinationSchema.Database),
      pg,
      role = CoordinationSchema.MaintenanceRole
    )
    Using.resources(
      new PostgresCoordinationStore(commitPath),
      new PostgresRegistryMaintenance(operator)
    ) { (store, maintenance) =>
      // Each registration has a generation of its own, and its entries were taken under it.
      val generations = Seq(one, other).map(store.registration(_).map(_.generation))
      generations.flatten.distinct should have size 2
      Seq(one, other).foreach { table =>
        store.latestEntry(table).map(entry => (entry.version, entry.complete)) shouldBe
          Some((2L, false))
        // The writer that had taken version 2 finishes it; the next version is taken and
        // finished by a writer that reads the generation the migration gave the table.
        store.complete(table, 2L, ".tmp/2.json.of-version-2") shouldBe true
        val next = claim(store, table, 3L)
        store.take(next) shouldBe TakeResult.Taken
        store.complete(table, 3L, next.tempPath) shouldBe true
      }

      // An operator's change holds a writer of that registry to what it read, like any other.
      val stalled = claim(store, one, 4L)
      maintenance.readopt(one, 3L, None, maintenance.contradicted(one, 3L)) shouldBe
        RegistryChange.Applied(Readoption(None, Nil))
      store.take(stalled) shouldBe TakeResult.RegistrationChanged
      store.take(claim(store, other, 4L)) shouldBe TakeResult.Taken
    }
  }
}
