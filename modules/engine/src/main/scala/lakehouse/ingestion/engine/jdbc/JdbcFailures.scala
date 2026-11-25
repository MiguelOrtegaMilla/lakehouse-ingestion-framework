package lakehouse.ingestion.engine.jdbc

import java.io.IOException
import java.sql.SQLException

import lakehouse.ingestion.core.config.{DatabaseConnection, Durations}
import lakehouse.ingestion.engine.failure.RunFailedException
import lakehouse.ingestion.engine.control.DatabaseFailure
import org.apache.spark.SparkThrowable

/**
 * What a failure of a read of a source is for its run (discovery §14): one that may pass, and
 * is worth running again, or one that will not until somebody changes something.
 *
 * '''A failure is the source's only if the source said it.''' The rows of a table are read
 * while they are written, so a failure of the read comes up inside the write of Delta, where
 * other things fail as well: the LogStore talks to a PostgreSQL of its own. What is taken for
 * the source's is a failure of SQL that nothing of I/O wraps: the LogStore gives every failure
 * of its database as an `IOException` around it, and a task of Spark gives a failure of its
 * read as it was thrown, with what the socket said under it and not over it.
 *
 * '''The class is the database's own word''', its SQLState, read as the control plane reads
 * one (`DatabaseFailure`): a connection that failed or was lost, a server that is starting or
 * stopping and a statement that was cut for running too long may pass; a password, a table
 * that is not there, a permission and a certificate will not.
 */
object JdbcFailures {

  /**
   * What the message of a row that the connector refuses starts with (`raise_error`): the
   * failure is then one that the connector raised, with what it has to say after this.
   */
  val Raised = "lakehouse-ingestion, the JDBC connector: "

  /** The SQLState of a statement that the server cancelled. */
  private val Cancelled = "57014"

  /** The SQLState of a value that does not fit the type it is read into. */
  private val OutOfRange = "22003"

  /** What the server says of a statement that ran past `statement_timeout`. */
  private val StatementTimeout = "statement timeout"

  /** Spark's condition for an error that a query raised itself. */
  private val UserRaised = "USER_RAISED_EXCEPTION"

  /**
   * @param where the source, for the start of a message: its database and its role
   * @return the failure of the run, if `error` is one of the source's; none, if it is of
   *   something else
   */
  def of(
      where: String,
      connection: DatabaseConnection,
      error: Throwable
  ): Option[RunFailedException] = {
    val causes = Iterator.iterate(error)(_.getCause).takeWhile(_ != null).take(MaxCauses).toVector
    raised(causes).map(RunFailedException.permanent(_, error)).orElse {
      ofTheSource(causes).map { said =>
        val state = causes.reverseIterator.collectFirst {
          case sql: SQLException if Option(sql.getSQLState).exists(_.nonEmpty) => sql.getSQLState
        }
        val told = DatabaseFailure.said(said)
        if (
          state.contains(Cancelled) && Option(said.getMessage).exists(_.contains(StatementTimeout))
        )
          RunFailedException.transient(
            s"$where took longer over one statement than " +
              s"${Durations.render(connection.timeouts.statement)}, and the statement was cut " +
              s"($told). A batch is read with a cursor, and that is how long one fetch of it " +
              "may take: raise connector.connection.timeouts.statement, or, for a load by a " +
              "watermark, have a batch read less, with load.watermark.start and " +
              "load.max_batch_range. Nothing of the batch was written",
            error
          )
        else if (state.contains(OutOfRange))
          RunFailedException.permanent(
            s"$where has a value that does not fit the type that its column is read into, " +
              s"and nothing of the batch was written: $told. A value that the type has not, " +
              "as NaN in a numeric with a bound, is kept if its column is read as text: add " +
              "the column to connector.text_columns",
            error
          )
        else
          DatabaseFailure.of(where, said, told) match {
            case DatabaseFailure.Unreachable(message) =>
              RunFailedException.transient(message, error)
            case DatabaseFailure.Refused(message) => RunFailedException.permanent(message, error)
          }
      }
    }
  }

  /** What the connector raised for a row, if that is what failed. */
  private def raised(causes: Seq[Throwable]): Option[String] =
    causes
      .collectFirst {
        case spark: SparkThrowable if spark.getCondition == UserRaised =>
          Option(spark.getMessageParameters).flatMap(parameters =>
            Option(parameters.get("errorMessage"))
          )
      }
      .flatten
      .filter(_.startsWith(Raised))
      .map(_.stripPrefix(Raised))

  /**
   * The failure of SQL that the source gave, if the failure is one: the first of SQL in the
   * causes, unless a failure of I/O comes before it, which is then what wrapped it.
   */
  private def ofTheSource(causes: Seq[Throwable]): Option[SQLException] =
    causes.collectFirst {
      case sql: SQLException => Some(sql)
      case _: IOException => None
    }.flatten

  private val MaxCauses = 16
}
