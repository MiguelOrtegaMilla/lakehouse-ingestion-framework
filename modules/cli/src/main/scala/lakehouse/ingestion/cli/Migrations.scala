package lakehouse.ingestion.cli

import java.sql.SQLException

import lakehouse.ingestion.core.config.{DatabaseConnection, Profile, Secret, SqlIdentifier}
import lakehouse.ingestion.engine.control.{ControlSchema, PostgresJdbc}
import lakehouse.ingestion.logstore.coordination.PostgresCoordinationStore
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.FlywayException
import org.postgresql.ds.PGSimpleDataSource

/**
 * Applies the migrations of the two schemas of a deployment: the control plane's, which the
 * engine ships, and the commit coordination's, which the LogStore ships (ADR-0003, ADR-0004).
 *
 * It is the one place that runs DDL, and it runs it as the role that owns the schemas, which no
 * other part of the application connects as (ADR-0013 §3). The engine and the LogStore only
 * check the version they find, and refuse to work on another.
 *
 * Each schema has its own history, kept by Flyway in the schema itself, so the two are migrated
 * apart, whether they are in one database or in two.
 */
object Migrations {

  /**
   * A schema to migrate.
   *
   * @param migrations where its migrations are, on the classpath
   * @param version the version that this application needs it at: that of its last migration
   * @param connection where it is. The role and the password of the profile are not used
   */
  final case class Target(
      schema: String,
      migrations: String,
      version: Int,
      connection: DatabaseConnection
  )

  /** The schemas of a deployment, in the databases that its profile says. */
  def targets(profile: Profile): Seq[Target] = Seq(
    Target(
      ControlSchema.Name,
      ControlSchema.Migrations,
      ControlSchema.Version,
      profile.databases.control
    ),
    Target(
      "delta_coordination",
      "classpath:db/migration/delta_coordination",
      PostgresCoordinationStore.SchemaVersion,
      profile.databases.coordination
    )
  )

  /**
   * @param applied how many migrations were applied now; none, if the schema was up to date
   * @param version the version the schema is at
   */
  final case class Migrated(schema: String, applied: Int, version: String)

  sealed trait Failure {
    def message: String
  }

  /** The database was not there to be asked: running again may do. */
  final case class Unreachable(message: String) extends Failure

  /** The database said no, or the migrations are not what it has: running again will not do. */
  final case class Refused(message: String) extends Failure

  /**
   * Brings a schema to the version of this application.
   *
   * A migration is applied whole or not at all: PostgreSQL runs DDL in a transaction, and Flyway
   * gives each migration one. Two runs at once do no harm either, since Flyway takes a lock of
   * the database for the time it works.
   *
   * @param user the role that owns the schema
   */
  def migrate(target: Target, user: SqlIdentifier, password: Secret): Either[Failure, Migrated] = {
    val source = new PGSimpleDataSource()
    source.setUrl(target.connection.url.value)
    PostgresJdbc.forMigrations(target.connection).foreach { case (name, value) =>
      source.setProperty(name, value)
    }
    source.setUser(user.value)
    source.setPassword(password.reveal)

    try {
      val result = Flyway
        .configure(getClass.getClassLoader)
        .dataSource(source)
        .locations(target.migrations)
        .defaultSchema(target.schema)
        // The schema is the bootstrap's, with its owner. One that is not there is a deployment
        // that was not bootstrapped, and is not to be made here with whatever owner this is.
        .createSchemas(false)
        .failOnMissingLocations(true)
        // The command is run again when it fails; it does not wait on its own.
        .connectRetries(0)
        .load()
        .migrate()

      val version = Option(result.targetSchemaVersion)
        .orElse(Option(result.initialSchemaVersion))
        .getOrElse("")
      if (version == target.version.toString)
        Right(Migrated(target.schema, result.migrationsExecuted, version))
      else
        Left(
          Refused(
            s"the schema ${target.schema} is at version '$version' with every migration applied, " +
              s"and this application needs version ${target.version}: it was not built whole"
          )
        )
    } catch {
      case e: FlywayException => Left(failure(target, e))
    }
  }

  /**
   * The classification of errors is its own work (P8). This is the least of it: a database that
   * is not there, or is starting or stopping, from one that answered and said no.
   */
  private def failure(target: Target, e: FlywayException): Failure = {
    val state = Iterator
      .iterate[Throwable](e)(_.getCause)
      .takeWhile(_ != null)
      .take(16)
      .collectFirst { case sql: SQLException if sql.getSQLState != null => sql.getSQLState }

    // Flyway's message has the database's own, the state, and the statement of a migration that
    // failed. Its rules of dashes are for a terminal that this is not.
    val said = Option(e.getMessage)
      .getOrElse(e.getClass.getSimpleName)
      .linesIterator
      .map(_.trim)
      .filter(line => line.nonEmpty && !line.forall(_ == '-'))
      .mkString("\n  ")
    val message = s"the schema ${target.schema} was not migrated: $said"

    if (state.exists(code => code.startsWith("08") || code.startsWith("57P"))) Unreachable(message)
    else Refused(message)
  }
}
