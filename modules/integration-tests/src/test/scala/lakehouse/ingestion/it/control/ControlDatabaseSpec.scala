package lakehouse.ingestion.it.control

import java.net.ServerSocket
import java.sql.SQLException

import scala.concurrent.duration._
import scala.util.Using

import lakehouse.ingestion.cli.Main
import lakehouse.ingestion.core.config.{DatabaseConnection, PostgresUrl, SecretResolver, Timeouts}
import lakehouse.ingestion.engine.control.{ControlDatabase, DatabaseFailure}
import lakehouse.ingestion.it.support.{BootstrappedPostgres, ControlPlaneSchema}
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import org.scalatest.{BeforeAndAfterAll, EitherValues}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The connection of the engine to its control plane, opened from a profile against a real
 * PostgreSQL: what is found when it opens, and what a connection of it does.
 *
 * The first test is on the database as the bootstrap leaves it, before it is migrated, and the
 * rest after.
 */
final class ControlDatabaseSpec
    extends AnyFunSuite
    with Matchers
    with EitherValues
    with BeforeAndAfterAll {

  private lazy val pg = BootstrappedPostgres.start()
  private lazy val control: DatabaseConnection = loaded(pg).databases.control
  private lazy val secrets: SecretResolver = SecretResolver(environment(pg).get)

  override def beforeAll(): Unit = pg.jdbcUrl(Database): Unit

  override def afterAll(): Unit = pg.stop()

  private def opened[T](connection: DatabaseConnection = control)(body: ControlDatabase => T): T =
    Using.resource(ControlDatabase.open(connection, secrets).value)(body)

  private def one(database: ControlDatabase, sql: String): String =
    database.run(c => query(c, sql).head.head)

  private def snapshots(database: ControlDatabase): Int =
    one(database, "SELECT count(*) FROM control.config_snapshot").toInt

  private def insertSnapshot(hash: Char): String =
    s"INSERT INTO control.config_snapshot (config_hash, source_id, content) " +
      s"VALUES ('${hash.toString * 64}', 'test.source', '{}'::jsonb)"

  test("before the schema is migrated, the control plane does not open, and says what to do") {
    val failure = ControlDatabase.open(control, secrets).left.value

    failure shouldBe a[DatabaseFailure.Refused]
    failure.message shouldBe
      s"the control plane (${pg.jdbcUrl(Database)}, as ingest_control): the schema control has " +
      "no version that this role can read: apply the migrations (the command migrate) before " +
      "the application runs"
  }

  test("it opens on a schema at its version, as the role and with the timeouts of the profile") {
    ControlPlaneSchema.migrate(pg).exit shouldBe Main.Exit.Ok

    opened() { database =>
      one(database, "SELECT current_user") shouldBe "ingest_control"
      one(database, "SELECT current_setting('application_name')") shouldBe "lakehouse-ingestion"
      one(database, "SHOW statement_timeout") shouldBe "10s"
      // The example profile has no TLS: its database is on the machine.
      one(database, "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()") shouldBe "f"
    }
  }

  test("a transaction is all of it or none, and what is run outside one is there at once") {
    opened() { database =>
      val before = snapshots(database)

      val failure = intercept[SQLException] {
        database.transaction { c =>
          update(c, insertSnapshot('a')) shouldBe 1
          // A hash that is not one: refused by the table, after the first row went in.
          update(c, insertSnapshot('Z'))
        }
      }
      failure.getSQLState shouldBe "23514"
      snapshots(database) shouldBe before

      database.transaction { c =>
        update(c, insertSnapshot('a')) shouldBe 1
        update(c, insertSnapshot('b')) shouldBe 1
        // Not seen from another connection until the transaction ends.
        snapshots(database) shouldBe before
      }
      snapshots(database) shouldBe before + 2

      database.run(c => update(c, insertSnapshot('c'))) shouldBe 1
      snapshots(database) shouldBe before + 3
      // A connection that was in a transaction is given back as one that commits by itself.
      (1 to 4).foreach(_ => database.run(_.getAutoCommit) shouldBe true)
    }
  }

  test("a failure that is no SQL's undoes the transaction as well") {
    opened() { database =>
      val before = snapshots(database)
      intercept[IllegalStateException] {
        database.transaction { c =>
          update(c, insertSnapshot('d')): Unit
          throw new IllegalStateException("the engine's own")
        }
      }.getMessage shouldBe "the engine's own"
      snapshots(database) shouldBe before
    }
  }

  test("a transaction that ends in an interrupt, or in an error of the JVM, commits nothing") {
    // Neither is an exception that it is safe to catch, and both leave a transaction half done.
    // A connection given back with auto-commit turned on commits what is pending on it: what
    // was written must be taken back first, whatever the transaction ended with.
    opened() { database =>
      val before = snapshots(database)

      intercept[InterruptedException] {
        database.transaction { c =>
          update(c, insertSnapshot('e')): Unit
          throw new InterruptedException("the thread was interrupted")
        }
      }
      snapshots(database) shouldBe before

      intercept[LinkageError] {
        database.transaction { c =>
          update(c, insertSnapshot('f')): Unit
          throw new LinkageError("a class that could not be loaded")
        }
      }
      snapshots(database) shouldBe before
      // And the connection went back to the pool as one that commits each statement by itself.
      (1 to 4).foreach(_ => database.run(_.getAutoCommit) shouldBe true)
    }
  }

  test("a connection that breaks in a transaction is reported as what broke it, and may pass") {
    opened() { database =>
      val before = snapshots(database)

      val failure = intercept[SQLException] {
        database.transaction { c =>
          update(c, insertSnapshot('9')): Unit
          // The server closes this very connection, as when it is restarted.
          val backend = query(c, "SELECT pg_backend_pid()").head.head.toInt
          Using.resource(pg.connectAsSuperuser(Database)) { s =>
            query(s, s"SELECT pg_terminate_backend($backend)"): Unit
          }
          Thread.sleep(200)
          update(c, insertSnapshot('8'))
        }
      }

      // What the server said, and not what the rollback of a closed connection said after it.
      failure.getSQLState should (startWith("57P") or startWith("08"))
      failure.getMessage should not be "Connection is closed"
      failure.getSuppressed should not be empty
      DatabaseFailure.mayPass(failure) shouldBe true
      // Nothing of it stayed, and the pool gives a connection that works.
      snapshots(database) shouldBe before
      database.transaction(c => update(c, insertSnapshot('9'))) shouldBe 1
    }
  }

  test("a statement that runs past its timeout is cancelled by the server, and the pool goes on") {
    opened(control.copy(timeouts = Timeouts(connect = 5.seconds, statement = 1.second))) {
      database =>
        val started = System.nanoTime()
        val failure = intercept[SQLException](one(database, "SELECT pg_sleep(30)"))
        val took = (System.nanoTime() - started).nanos

        failure.getSQLState shouldBe "57014"
        took should be < 5.seconds
        // It may pass: the lock it waited on is released, the database is less busy.
        DatabaseFailure.mayPass(failure) shouldBe true
        one(database, "SELECT 1") shouldBe "1"
    }
  }

  test("a password that is wrong, or not where the profile says, is refused and never said") {
    val wrong = "not-the-password-7c1e"
    val refused = ControlDatabase
      .open(control, SecretResolver(Map("INGEST_CONTROL_PASSWORD" -> wrong).get))
      .left
      .value
    refused shouldBe a[DatabaseFailure.Refused]
    refused.message should startWith(
      s"the control plane (${pg.jdbcUrl(Database)}, as ingest_control) could not be opened: "
    )
    refused.message should include("password authentication failed")
    refused.message should include("(SQLState 28P01)")
    refused.message should not include wrong

    ControlDatabase.open(control, SecretResolver(_ => None)).left.value shouldBe
      DatabaseFailure.Refused(
        "the control plane: cannot read the secret env:INGEST_CONTROL_PASSWORD: the variable is not set"
      )
  }

  test("a database that is not there is unreachable, and found so within the connect timeout") {
    val closed = Using.resource(new ServerSocket(0))(_.getLocalPort)
    val nowhere = control.copy(
      url = PostgresUrl.parse(s"jdbc:postgresql://localhost:$closed/ingestion_control").value,
      timeouts = Timeouts(connect = 1.second, statement = 10.seconds)
    )

    val started = System.nanoTime()
    val failure = ControlDatabase.open(nowhere, secrets).left.value
    val took = (System.nanoTime() - started).nanos

    failure shouldBe a[DatabaseFailure.Unreachable]
    failure.message should include("(SQLState 08001)")
    took should be < 5.seconds
  }

  test("a schema that is not at the version of the application is refused when it opens") {
    def setVersion(version: Int): Unit =
      Using.resource(pg.connect(Migrator, Database)) { c =>
        update(c, s"UPDATE control.schema_version SET version = $version") shouldBe 1: Unit
      }

    setVersion(2)
    try {
      val failure = ControlDatabase.open(control, secrets).left.value
      failure shouldBe a[DatabaseFailure.Refused]
      failure.message should include(
        "is at version 2, and this application was built for version 1"
      )
    } finally setVersion(1)
    opened()(one(_, "SELECT 1") shouldBe "1")
  }

  test("a connection that was closed gives no more connections, and holds none") {
    val database = ControlDatabase.open(control, secrets).value
    one(database, "SELECT 1") shouldBe "1"
    database.close()

    intercept[SQLException](one(database, "SELECT 1"))
    Using.resource(pg.connectAsSuperuser(Database)) { c =>
      query(c, "SELECT count(*) FROM pg_stat_activity WHERE usename = 'ingest_control'") shouldBe
        Seq(Seq("0"))
    }
  }
}
