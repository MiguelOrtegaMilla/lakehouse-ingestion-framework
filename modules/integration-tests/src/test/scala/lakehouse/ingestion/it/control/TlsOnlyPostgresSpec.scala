package lakehouse.ingestion.it.control

import java.sql.{DriverManager, SQLException}

import scala.util.Using

import lakehouse.ingestion.cli.Main
import lakehouse.ingestion.core.config.{DatabaseConnection, PostgresUrl, SecretResolver, Tls}
import lakehouse.ingestion.engine.control.{ControlDatabase, DatabaseFailure, LogStoreSettings}
import lakehouse.ingestion.it.support.{BootstrappedPostgres, ControlPlaneSchema}
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.logstore.coordination.{
  CoordinationSettings,
  CoordinationStoreContract,
  PostgresCoordinationStore
}
import org.scalatest.{BeforeAndAfterAll, EitherValues}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A PostgreSQL that takes TLS and nothing else (ADR-0013 §5): everything of the application
 * that connects to it does so with `verify-full`, and nothing does without.
 *
 * The server is the one of every other test, with a certificate of its own, signed by an
 * authority that is made with it, and with no line in `pg_hba.conf` that lets a connection in
 * over TCP without TLS. What connects is what a deployment runs: the command `migrate`, the
 * connection of the engine to its control plane, and the LogStore with the settings that are
 * made for it from the profile.
 */
final class TlsOnlyPostgresSpec
    extends AnyFunSuite
    with Matchers
    with EitherValues
    with BeforeAndAfterAll {

  private lazy val pg = BootstrappedPostgres.start(tlsOnly = true)

  // The profile of a deployment whose databases are reached with verify-full.
  private lazy val profile = loaded(pg, rootCertificate = Some(pg.rootCertificate))
  private lazy val control: DatabaseConnection = profile.databases.control
  private lazy val coordination: DatabaseConnection = profile.databases.coordination
  private lazy val secrets: SecretResolver = SecretResolver(environment(pg).get)

  // Migrated over TLS, as a deployment is: if that fails, the suite aborts once, with the error.
  override def beforeAll(): Unit = {
    val ran = ControlPlaneSchema.migrate(pg, rootCertificate = Some(pg.rootCertificate))
    require(ran.exit == Main.Exit.Ok, ran.err)
  }

  override def afterAll(): Unit = pg.stop()

  /** Whether the sessions of `role` that are open now are encrypted, one answer for each. */
  private def sessionsOf(role: String): Seq[String] =
    Using.resource(pg.connectAsSuperuser(Database)) { c =>
      query(
        c,
        "SELECT s.ssl FROM pg_stat_ssl s JOIN pg_stat_activity a USING (pid) WHERE a.usename = ?",
        role
      ).map(_.head)
    }

  private def logStore(connection: DatabaseConnection): PostgresCoordinationStore = {
    val settings = CoordinationSettings.from(LogStoreSettings.hadoop(connection).get _).value
    new PostgresCoordinationStore(settings, environment(pg).get)
  }

  test("the server lets nobody in without TLS, whatever the password") {
    val failure = intercept[SQLException] {
      DriverManager
        .getConnection(s"${pg.jdbcUrl(Database)}?sslmode=disable", Engine, pg.password(Engine))
        .close()
    }

    failure.getSQLState shouldBe "28000"
    failure.getMessage should include("no encryption")
  }

  test("migrate reaches it with verify-full, and is refused without TLS") {
    val again = ControlPlaneSchema.migrate(pg, rootCertificate = Some(pg.rootCertificate))
    withClue(again.err)(again.exit shouldBe Main.Exit.Ok)
    again.lines.map(_.takeWhile(_ != '(').trim) shouldBe Seq(
      "control: version 4, nothing to apply",
      "delta_coordination: version 4, nothing to apply"
    )

    // The example profile as it is: TLS off, for a database on the machine.
    val without = ControlPlaneSchema.migrate(pg)
    without.exit shouldBe Main.Exit.Permanent
    without.err should include("no encryption")
    without.out shouldBe empty
  }

  test("the control plane opens with verify-full, on a session that is encrypted") {
    Using.resource(ControlDatabase.open(control, secrets).value) { database =>
      database.run { c =>
        query(c, "SELECT ssl, version FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
      } shouldBe Seq(Seq("t", "TLSv1.3"))
      sessionsOf(Engine) shouldBe Seq("t")
    }
  }

  test("a profile with TLS off is refused by the server: running again will not do") {
    val failure = ControlDatabase.open(control.copy(tls = Tls.Disabled), secrets).left.value

    failure shouldBe a[DatabaseFailure.Refused]
    failure.message should include("no encryption")
    failure.message should include("(SQLState 28000)")
  }

  test("a server whose certificate another authority signed is not trusted, and not tried again") {
    val other = Tls.VerifyFull(pg.anotherAuthority().toString)

    val failure = ControlDatabase.open(control.copy(tls = other), secrets).left.value

    // The driver calls it a connection that failed. It is a certificate that will not change.
    withClue(failure.message)(failure shouldBe a[DatabaseFailure.Refused])
    failure.message should include("(SQLState 08006)")
    failure.message should not include pg.password(Engine)
  }

  test(
    "a certificate that is for another name is refused: the server is verified, not only encrypted"
  ) {
    // The certificate is for the name the tests reach the server by, and for no address.
    assume(pg.host == "localhost", "the Docker host is not this machine")
    val byAddress =
      PostgresUrl.parse(pg.jdbcUrl(Database).replace("//localhost:", "//127.0.0.1:")).value

    val failure = ControlDatabase.open(control.copy(url = byAddress), secrets).left.value

    withClue(failure.message)(failure shouldBe a[DatabaseFailure.Refused])
    failure.message should include("127.0.0.1 could not be verified")
  }

  test("the LogStore, with the settings made for it from the profile, works over TLS") {
    val table = CoordinationStoreContract.newTable()

    Using.resource(logStore(coordination)) { store =>
      // Its first operation opens its pool, reads its password and checks its schema.
      store.registration(table) shouldBe None
      sessionsOf("ingest_coordination") shouldBe Seq("t")
    }
  }

  test("the LogStore with TLS off in the profile commits nothing") {
    val table = CoordinationStoreContract.newTable()

    Using.resource(logStore(coordination.copy(tls = Tls.Disabled))) { store =>
      val failure = intercept[Exception](store.registration(table))
      val said =
        Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).map(_.getMessage)
      said.mkString(" / ") should include("no encryption")
    }
    sessionsOf("ingest_coordination") shouldBe empty
  }
}
