package lakehouse.ingestion.engine.control

import java.sql.{Connection, SQLException}

import scala.util.Using

/**
 * The schema `control`, as the engine expects to find it (ADR-0003).
 *
 * The engine does not make its tables and does not change them: that is the migrations', applied
 * by a role that the engine never is (ADR-0013 §3). It checks which version is there before it
 * touches anything, and refuses to run on any other. A database that is behind the application,
 * or ahead of it, is a deployment that is half made, and what would happen to its rows is
 * nothing that was tested.
 */
object ControlSchema {

  val Name = "control"

  /** The version of the schema that this application was built for: its last migration. */
  val Version = 4

  /** Where the migrations of the schema are, on the classpath of whatever applies them. */
  val Migrations = "classpath:db/migration/control"

  private val VersionQuery = "SELECT version FROM control.schema_version"

  // undefined_table, invalid_schema_name, insufficient_privilege: no schema that this role can use.
  private val NotThere = Set("42P01", "3F000", "42501")

  /**
   * Whether the schema that `connection` reaches is the one this application was built for.
   *
   * @return what is wrong, and what mends it
   * @throws java.sql.SQLException if the database could not be asked at all
   */
  @throws[SQLException]
  def verify(connection: Connection): Either[String, Unit] = {
    val found =
      try
        Using.resource(connection.createStatement()) { statement =>
          Using.resource(statement.executeQuery(VersionQuery)) { rows =>
            Option.when(rows.next())(rows.getInt(1))
          }
        }
      catch {
        case e: SQLException if NotThere(e.getSQLState) => None
      }

    found match {
      case Some(Version) => Right(())
      case Some(other) if other < Version =>
        Left(
          s"the schema $Name is at version $other, and this application needs version $Version: " +
            "apply the migrations (the command migrate) before it runs"
        )
      case Some(other) =>
        Left(
          s"the schema $Name is at version $other, and this application was built for version " +
            s"$Version: it is older than the database, and must be upgraded before it runs"
        )
      case None =>
        Left(
          s"the schema $Name has no version that this role can read: apply the migrations (the " +
            "command migrate) before the application runs"
        )
    }
  }
}
