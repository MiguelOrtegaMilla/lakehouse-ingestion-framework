package lakehouse.ingestion.it.support

import org.flywaydb.core.Flyway
import org.flywaydb.core.api.configuration.FluentConfiguration
import org.flywaydb.core.api.output.MigrateResult

/**
 * The LogStore's coordination schema (ADR-0004 §3.1), applied the way a deployment applies it:
 * by Flyway, as `ingest_migrator`, from the migrations `delta-pg-logstore` ships.
 */
object CoordinationSchema {

  val Database = "ingestion_control"
  val Schema = "delta_coordination"

  /** The only role that runs DDL (ADR-0013 §3). */
  val Migrator = "ingest_migrator"

  /** Where `delta-pg-logstore` keeps its migrations on the classpath. */
  val Location = "classpath:db/migration/delta_coordination"

  /**
   * Brings the schema of `pg` to the latest version.
   *
   * Flyway keeps its history table in the schema it migrates, so each schema of the database has
   * its own history. It must not try to create the schema: the bootstrap did, and the migrator
   * could not.
   */
  def migrate(pg: BootstrappedPostgres): MigrateResult = flyway(pg).migrate()

  /** Brings the schema of `pg` to version `upTo` and no further: a deployment not yet upgraded. */
  def migrate(pg: BootstrappedPostgres, upTo: String): MigrateResult =
    flyway(pg, _.target(upTo)).migrate()

  /** Drops everything the migrations created, history included; the schema itself stays. */
  def drop(pg: BootstrappedPostgres): Unit = flyway(pg).clean(): Unit

  private def flyway(
      pg: BootstrappedPostgres,
      also: FluentConfiguration => FluentConfiguration = identity
  ): Flyway =
    also(
      Flyway
        .configure()
        .dataSource(pg.jdbcUrl(Database), Migrator, pg.password(Migrator))
        .locations(Location)
        .defaultSchema(Schema)
        .createSchemas(false)
        // Dropping is for tests only; Flyway refuses it unless asked.
        .cleanDisabled(false)
    ).load()

  /** The role the commit path connects as (ADR-0013 §3). */
  val CommitPathRole = "ingest_coordination"

  /** The role an operator's tools connect as (ADR-0013 §3). */
  val MaintenanceRole = "ingest_maintenance"
}
