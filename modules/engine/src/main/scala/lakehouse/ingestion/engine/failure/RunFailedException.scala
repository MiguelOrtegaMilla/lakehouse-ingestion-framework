package lakehouse.ingestion.engine.failure

import lakehouse.ingestion.core.control.FailureClass

/**
 * A failure of a run that whoever raised it knew the class of (discovery §14): what the run is
 * recorded to have failed by, and what its exit code says to whoever started it (§14.1).
 *
 * It is raised where a failure can be told for what it is, and by nothing that only guesses.
 * Telling the errors of a database, of a file system and of Delta apart is the classification
 * of errors (P8); an exception that is not one of these is not classified yet.
 *
 * @param message what went wrong, for a person. Never a secret, nor a value of a row
 */
final class RunFailedException(
    val failure: FailureClass,
    message: String,
    cause: Throwable = null
) extends RuntimeException(message, cause)

object RunFailedException {

  /** What may pass by itself: something that the run needs did not answer. */
  def transient(message: String, cause: Throwable = null): RunFailedException =
    new RunFailedException(FailureClass.Transient, message, cause)

  /** What running again does not mend, until somebody has changed something. */
  def permanent(message: String, cause: Throwable = null): RunFailedException =
    new RunFailedException(FailureClass.Permanent, message, cause)

  /** Another writer changed a table under a write, more often than the write tries again. */
  def conflict(message: String, cause: Throwable = null): RunFailedException =
    new RunFailedException(FailureClass.Conflict, message, cause)
}
