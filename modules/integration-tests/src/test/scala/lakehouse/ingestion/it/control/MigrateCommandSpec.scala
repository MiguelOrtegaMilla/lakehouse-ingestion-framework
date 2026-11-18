package lakehouse.ingestion.it.control

import java.net.ServerSocket
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.sql.Connection

import scala.util.Using

import lakehouse.ingestion.cli.{Main, Migrations}
import lakehouse.ingestion.core.config.{ConfigLoader, Secret, SqlIdentifier}
import lakehouse.ingestion.engine.control.{ControlSchema, DatabaseFailure}
import lakehouse.ingestion.it.support.{BootstrappedPostgres, ControlPlaneSchema}
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.logstore.coordination.PostgresCoordinationStore
import org.scalatest.{BeforeAndAfterAll, EitherValues}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The command `migrate` against a database as the bootstrap leaves it: schemas that are there
 * and empty, and the roles of ADR-0013 §3, each with its own password.
 *
 * The tests are the life of one database, in the order they are written: before it is
 * migrated, migrated, migrated again, and at a version that is not the application's.
 */
final class MigrateCommandSpec
    extends AnyFunSuite
    with Matchers
    with EitherValues
    with BeforeAndAfterAll {

  private lazy val pg = BootstrappedPostgres.start()

  override def beforeAll(): Unit = pg.jdbcUrl(Database): Unit

  override def afterAll(): Unit = pg.stop()

  private def as[T](role: String)(body: Connection => T): T =
    Using.resource(pg.connect(role, Database))(body)

  private def tables(schema: String): Seq[String] = as(BootstrappedPostgres.Superuser) { c =>
    query(c, "SELECT tablename FROM pg_tables WHERE schemaname = ? ORDER BY 1", schema).map(_.head)
  }

  private def migrateAs(user: String, password: String): Ran =
    command(Map(PasswordVariable -> password))(
      "migrate",
      "--profile",
      profile(pg).toString,
      "--password",
      s"env:$PasswordVariable",
      "--user",
      user
    )

  test("before it is migrated, the engine finds no schema it can work on, and says what to do") {
    as(Engine) { c =>
      ControlSchema.verify(c).left.value shouldBe
        "the schema control has no version that this role can read: apply the migrations " +
        "(the command migrate) before the application runs"
    }
  }

  test("a password that is not the migrator's ends in 4, and nothing is made") {
    val ran = migrateAs(Migrator, "not-the-password-0f3a")

    ran.exit shouldBe Main.Exit.Permanent
    ran.errors.head should startWith("error: the schema control was not migrated: ")
    ran.err should include("password authentication failed")
    ran.err should include("28P01")
    (ran.out + ran.err) should not include "not-the-password-0f3a"
    ran.out shouldBe empty
    tables("control") shouldBe empty
    tables("delta_coordination") shouldBe empty
  }

  test("a role that does not own the schemas migrates nothing") {
    // The engine's own role, with its own password: it is let in, and may define nothing.
    val ran = migrateAs(Engine, pg.password(Engine))

    ran.exit shouldBe Main.Exit.Permanent
    ran.errors.head should startWith("error: the schema control was not migrated: ")
    ran.err should include("42501")
    tables("control") shouldBe empty
  }

  test("a database that is not there ends in 3: running again may do") {
    val closed = Using.resource(new ServerSocket(0))(_.getLocalPort)
    val nowhere = profile(pg, url = Some(s"jdbc:postgresql://localhost:$closed/ingestion_control"))

    val ran = command(Map(PasswordVariable -> pg.password(Migrator)))(
      "migrate",
      "--profile",
      nowhere.toString,
      "--password",
      s"env:$PasswordVariable"
    )

    ran.exit shouldBe Main.Exit.Transient
    ran.errors.head should startWith("error: the schema control was not migrated: ")
    ran.err should include("08001")
    ran.out shouldBe empty
  }

  test("migrate brings both schemas to the versions that the application needs") {
    val ran = ControlPlaneSchema.migrate(pg)

    withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
    ran.lines shouldBe Seq(
      s"control: version 2, 2 migrations applied (${pg.jdbcUrl(Database)})",
      s"delta_coordination: version 3, 3 migrations applied (${pg.jdbcUrl(Database)})"
    )
    (ran.out + ran.err) should not include pg.password(Migrator)

    tables("control") shouldBe Seq(
      "config_snapshot",
      "flyway_schema_history",
      "ingestion_batch",
      "ingestion_run",
      "processed_file",
      "schema_version",
      "source_lease",
      "source_state"
    )
    // What the engine checks, and what the LogStore checks, each as its own role.
    as(Engine)(c => ControlSchema.verify(c) shouldBe Right(()))
    as("ingest_coordination") { c =>
      query(c, "SELECT version FROM delta_coordination.schema_version") shouldBe
        Seq(Seq(PostgresCoordinationStore.SchemaVersion.toString))
    }
    ControlSchema.Version shouldBe 2
  }

  test("migrating again changes nothing, with the password in a file as a deployment has it") {
    val secret = Files.createTempFile("migrator-", ".secret")
    secret.toFile.deleteOnExit()
    // As `echo` leaves it: with a line break that is not the password's.
    Files.write(secret, (pg.password(Migrator) + "\n").getBytes(UTF_8))

    val ran =
      command(Map.empty)(
        "migrate",
        "--profile",
        profile(pg).toString,
        "--password",
        s"file:$secret"
      )

    withClue(ran.err)(ran.exit shouldBe Main.Exit.Ok)
    ran.lines shouldBe Seq(
      s"control: version 2, nothing to apply (${pg.jdbcUrl(Database)})",
      s"delta_coordination: version 3, nothing to apply (${pg.jdbcUrl(Database)})"
    )
  }

  test("the engine refuses a schema that is behind it, and one that is ahead of it") {
    def at(version: Int): Either[String, Unit] = {
      as(Migrator)(c =>
        update(c, s"UPDATE control.schema_version SET version = $version")
      ) shouldBe 1
      as(Engine)(ControlSchema.verify)
    }

    try {
      at(1).left.value shouldBe
        "the schema control is at version 1, and this application needs version 2: apply the " +
        "migrations (the command migrate) before it runs"
      at(3).left.value shouldBe
        "the schema control is at version 3, and this application was built for version 2: it " +
        "is older than the database, and must be upgraded before it runs"
    } finally (at(ControlSchema.Version) shouldBe Right(())): Unit
  }

  test("migrations that do not make the version the application needs are refused") {
    // An application put together from a LogStore and an engine of two different builds.
    val loaded = ConfigLoader.loadProfile(profile(pg)).value.value
    val control = Migrations.targets(loaded).head.copy(version = 3)

    val refused = Migrations
      .migrate(control, SqlIdentifier.parse(Migrator).value, new Secret(pg.password(Migrator)))

    refused.left.value shouldBe DatabaseFailure.Refused(
      "the schema control is at version '2' with every migration applied, and this application " +
        "needs version 3: it was not built whole"
    )
  }

  test("the schemas of the profile are the two of a deployment, each in its database") {
    val loaded = ConfigLoader.loadProfile(profile(pg)).value.value

    Migrations.targets(loaded).map(target => (target.schema, target.version)) shouldBe
      Seq(
        "control" -> ControlSchema.Version,
        "delta_coordination" -> PostgresCoordinationStore.SchemaVersion
      )
    Migrations.targets(loaded).map(_.connection) shouldBe
      Seq(loaded.databases.control, loaded.databases.coordination)
  }
}
