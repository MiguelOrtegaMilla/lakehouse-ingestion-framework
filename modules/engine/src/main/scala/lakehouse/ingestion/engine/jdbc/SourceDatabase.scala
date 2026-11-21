package lakehouse.ingestion.engine.jdbc

import java.sql.{Connection, DriverManager, ResultSetMetaData}
import java.time.{Instant, OffsetDateTime}
import java.util.Properties

import scala.util.Using

import lakehouse.ingestion.core.config.{DatabaseConnection, QualifiedName, Secret}
import lakehouse.ingestion.engine.control.PostgresJdbc

/**
 * A connection to the database of a source, for what the connector asks of it besides its
 * rows: what the columns of a table are, and what time it is there. The rows are read by Spark, on connections of its own
 * that are opened the same way (`PostgresJdbc.ofASource`).
 *
 * It is one connection, opened for a question and closed after it: a run asks a source a
 * handful of things, and a pool would outlive them all.
 *
 * Every statement here is a constant, or is made of names alone (`JdbcSql`). What goes wrong
 * is a `SQLException`, as the driver throws it: what it means for a run is said by the
 * connector (`JdbcFailures`).
 */
final class SourceDatabase private (jdbc: Connection) extends AutoCloseable {

  /**
   * The columns of a table, in its order, as a statement over it would give them. A table
   * without a schema is found as that statement finds it, by the search path of the role.
   *
   * @throws java.sql.SQLException for a table that is not there (`42P01`), or that the role
   *   may not read (`42501`), as for anything else that the source says
   */
  def columns(table: QualifiedName): Seq[SourceColumn] =
    Using.resource(jdbc.createStatement()) { statement =>
      Using.resource(statement.executeQuery(JdbcSql.noRows(table))) { rows =>
        val metadata = rows.getMetaData
        (1 to metadata.getColumnCount).map { index =>
          SourceColumn(
            name = metadata.getColumnName(index),
            typeName = metadata.getColumnTypeName(index),
            precision = metadata.getPrecision(index),
            scale = metadata.getScale(index),
            // What is not known to be never null may be.
            nullable = metadata.isNullable(index) != ResultSetMetaData.columnNoNulls
          )
        }
      }
    }

  /**
   * What the source says the time is, to the microsecond, which is what PostgreSQL keeps: the
   * clock that a batch of a watermark is bounded by (ADR-0020). It is the time at which the
   * statement started, on a connection that has no transaction open.
   *
   * @throws java.sql.SQLException for whatever the source says in place of it
   */
  def clock(): Instant =
    Using.resource(jdbc.createStatement()) { statement =>
      Using.resource(statement.executeQuery(JdbcSql.Clock)) { rows =>
        rows.next(): Unit
        rows.getObject(1, classOf[OffsetDateTime]).toInstant
      }
    }

  override def close(): Unit = jdbc.close()
}

object SourceDatabase {

  /**
   * @param password of the role of the connection. It goes to the driver, and nowhere else
   * @throws java.sql.SQLException if the database cannot be reached, or refuses the connection
   */
  def open(connection: DatabaseConnection, password: Secret): SourceDatabase = {
    val properties = new Properties
    PostgresJdbc.ofASource(connection).foreach { case (name, value) =>
      properties.setProperty(name, value)
    }
    properties.setProperty("user", connection.user.value)
    properties.setProperty("password", password.reveal)
    val jdbc = DriverManager.getConnection(connection.url.value, properties)
    try {
      // Nothing here writes, and the session is told so.
      jdbc.setReadOnly(true)
      new SourceDatabase(jdbc)
    } catch {
      case e: Throwable =>
        jdbc.close()
        throw e
    }
  }
}
