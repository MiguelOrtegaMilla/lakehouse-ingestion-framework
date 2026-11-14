package lakehouse.ingestion.it.support

import org.flywaydb.core.Flyway
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
  def migrate(pg: BootstrappedPostgres): MigrateResult =
    Flyway
      .configure()
      .dataSource(pg.jdbcUrl(Database), Migrator, pg.password(Migrator))
      .locations(Location)
      .defaultSchema(Schema)
      .createSchemas(false)
      .load()
      .migrate()
}
