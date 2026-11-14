package lakehouse.ingestion.it.logstore

import scala.util.Using

import lakehouse.ingestion.it.support.{BootstrappedPostgres, CoordinationSchema, CoordinationStores}
import lakehouse.ingestion.logstore.coordination.{
  CoordinationSchemaException,
  CoordinationStoreContract,
  PostgresCoordinationStore
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Scenario F13 (ADR-0004 §7.1): a coordination schema that is missing or at another version
 * stops the store before it reads or writes anything. The data path never creates or alters the
 * schema (deviation 10); it only refuses to run on one it does not know.
 */
final class CoordinationSchemaCheckSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private lazy val pg = BootstrappedPostgres.start()
  private lazy val settings =
    CoordinationStores.settings(pg.jdbcUrl(CoordinationSchema.Database), pg)

  override def beforeAll(): Unit = { pg; () }

  override def afterAll(): Unit = pg.stop()

  private def setVersion(version: Int): Unit =
    Using.resource(pg.connect(CoordinationSchema.Migrator, CoordinationSchema.Database)) { c =>
      Using.resource(
        c.prepareStatement("UPDATE delta_coordination.schema_version SET version = ?")
      ) { statement =>
        statement.setInt(1, version)
        statement.executeUpdate() shouldBe 1: Unit
      }
    }

  test("F13: without the coordination schema, the store refuses to work and says what to do") {
    CoordinationSchema.drop(pg)
    val table = CoordinationStoreContract.newTable()

    Using.resource(new PostgresCoordinationStore(settings)) { store =>
      val failure = the[CoordinationSchemaException] thrownBy store.registration(table)
      failure.getMessage should include("migrations")

      // Not remembered: once the migrator has run, the same store works.
      CoordinationSchema.migrate(pg)
      store.registration(table) shouldBe None
    }
  }

  test("F13: with the schema at another version, the store refuses to work") {
    CoordinationSchema.migrate(pg)
    val table = CoordinationStoreContract.newTable()
    val newer = PostgresCoordinationStore.SchemaVersion + 1

    setVersion(newer)
    try
      Using.resource(new PostgresCoordinationStore(settings)) { store =>
        val failure = the[CoordinationSchemaException] thrownBy store.take(
          CoordinationStoreContract.claim(table, 0L)
        )
        failure.getMessage should include(s"version $newer")
        failure.getMessage should include(
          s"needs version ${PostgresCoordinationStore.SchemaVersion}"
        )
      }
    finally setVersion(PostgresCoordinationStore.SchemaVersion)

    Using.resource(new PostgresCoordinationStore(settings)) { store =>
      store.registration(table) shouldBe None
    }
  }
}
