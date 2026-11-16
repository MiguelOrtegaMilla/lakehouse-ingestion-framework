package lakehouse.ingestion.it.logstore

import java.io.IOException

import scala.concurrent.duration._
import scala.util.Using

import lakehouse.ingestion.it.support.{BootstrappedPostgres, CoordinationSchema, CoordinationStores}
import lakehouse.ingestion.logstore.coordination.CoordinationStoreContract.{claim, newTable}
import lakehouse.ingestion.logstore.coordination._
import org.scalatest.BeforeAndAfterAll

/**
 * `PostgresRegistryMaintenance` keeps the contract of every `RegistryMaintenance`, against the
 * migrated schema of a real PostgreSQL: the commit path connects as its own role, and the
 * operator's tools as `ingest_maintenance` (ADR-0013 §3). Then what the database itself adds:
 * the commit path's role cannot do an operator's work.
 */
final class PostgresRegistryMaintenanceSpec
    extends RegistryMaintenanceContract
    with BeforeAndAfterAll {

  private lazy val pg = BootstrappedPostgres.start()
  private lazy val commitPath =
    CoordinationStores.settings(pg.jdbcUrl(CoordinationSchema.Database), pg)
  private lazy val operator = CoordinationStores.settings(
    pg.jdbcUrl(CoordinationSchema.Database),
    pg,
    role = CoordinationSchema.MaintenanceRole
  )

  private lazy val postgresStore = new PostgresCoordinationStore(commitPath)
  private lazy val postgresMaintenance = new PostgresRegistryMaintenance(operator)

  override protected def store: CoordinationStore = postgresStore
  override protected def maintenance: RegistryMaintenance = postgresMaintenance

  override def beforeAll(): Unit = CoordinationSchema.migrate(pg): Unit

  override def afterAll(): Unit =
    try postgresMaintenance.close()
    finally
      try postgresStore.close()
      finally pg.stop()

  test("the commit path's role cannot adopt a table again, nor forget one") {
    val table = newTable()
    store.register(table, Registration.NotYetCreated, Some("its-id"))
    store.take(claim(table, 0L)) shouldBe TakeResult.Taken
    store.taint(table, "version 0 is not the commit that took it")
    val before = (store.registration(table), store.entry(table, 0L))

    // The same code, connected as the role of the commit path: the database refuses it.
    Using.resource(new PostgresRegistryMaintenance(commitPath)) { asCommitPath =>
      an[IOException] should be thrownBy asCommitPath.readopt(table, 0L, None)
      an[IOException] should be thrownBy asCommitPath.forget(table)
    }

    // And nothing of what it tried was done, not even in part.
    (store.registration(table), store.entry(table, 0L)) shouldBe before
  }

  test("an operator's change is whole: if a part of it fails, none of it was made") {
    val table = newTable()
    store.register(table, Registration.NotYetCreated, None)
    store.take(claim(table, 0L)) shouldBe TakeResult.Taken
    store.taint(table, "version 0 has no entry")

    // Forgetting deletes the taint, then the entries, then the registration. A session that
    // holds the entry makes the second step wait until the statement is cancelled.
    Using.resource(pg.connect(CoordinationSchema.CommitPathRole, CoordinationSchema.Database)) {
      holder =>
        holder.setAutoCommit(false)
        Using.resource(
          holder.prepareStatement(
            "SELECT 1 FROM delta_coordination.commit_entry WHERE table_uri = ? FOR SHARE"
          )
        ) { statement =>
          statement.setString(1, table.value)
          statement.executeQuery().close()
        }
        val impatient =
          operator.copy(statementTimeout = 1.second)
        Using.resource(new PostgresRegistryMaintenance(impatient)) { blocked =>
          an[IOException] should be thrownBy blocked.forget(table)
        }
        holder.rollback()
    }

    // The taint, deleted first, is back with everything else.
    store.registration(table).flatMap(_.taintReason) shouldBe Some("version 0 has no entry")
    store.entry(table, 0L).isDefined shouldBe true
  }
}
