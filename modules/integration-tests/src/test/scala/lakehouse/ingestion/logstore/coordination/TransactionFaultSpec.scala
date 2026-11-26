package lakehouse.ingestion.logstore.coordination

import java.io.IOException
import java.sql.SQLException

import scala.concurrent.duration._
import scala.util.Using

import lakehouse.ingestion.it.support.{CoordinationSchema, CoordinationStores, ProxiedPostgres}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The transactions of the coordination store, which the operations of maintenance run in, when
 * the connection is lost in the middle of one, for real: a proxy between the store and
 * PostgreSQL is cut while the transaction is open. What broke the transaction is what the store
 * reports, and what its rollback and its return to auto-commit then said is under it.
 *
 * It is in the package of the store, whose transactions are its own (`transaction` is not part
 * of `CoordinationStore`), and beside the integration tests, where the proxy is.
 */
final class TransactionFaultSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private lazy val pg = ProxiedPostgres.start()

  override def beforeAll(): Unit = CoordinationSchema.migrate(pg.postgres): Unit

  override def afterAll(): Unit = pg.stop()

  private def store(): PostgresCoordinationStore =
    new PostgresCoordinationStore(
      CoordinationStores.settings(
        pg.faultyJdbcUrl(CoordinationSchema.Database),
        pg.postgres,
        connectTimeout = 2.seconds,
        statementTimeout = 2.seconds,
        role = CoordinationSchema.MaintenanceRole
      )
    )

  test("a transaction whose connection is lost reports what lost it, and not its rollback") {
    Using.resource(store()) { store =>
      val failure =
        try
          intercept[IOException] {
            store.transaction("read the registry") { connection =>
              // The transaction is open, and the connection under it goes.
              pg.faulty.disable()
              Using.resource(connection.createStatement()) {
                _.execute("SELECT count(*) FROM delta_coordination.coordinated_table")
              }
            }
          }
        finally pg.faulty.enable()

      // The statement is what failed, and what it said is what the store says.
      failure.getMessage should startWith("coordination store: cannot read the registry: ")
      val first = failure.getCause
      first shouldBe a[SQLException]
      failure.getMessage should include(first.getMessage)
      // What the driver says of a connection that went in the middle of a statement.
      first.getMessage should include("An I/O error occurred while sending to the backend")
      // The rollback and the return to auto-commit failed on the same lost connection, and are
      // said with it.
      val suppressed = first.getSuppressed.toSeq
      suppressed should have size 2
      all(suppressed.map(_.getMessage)) should include("closed")
    }
  }
}
