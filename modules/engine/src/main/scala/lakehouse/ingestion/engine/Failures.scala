package lakehouse.ingestion.engine

import java.io.IOException
import java.sql.SQLException

import scala.util.control.NonFatal

import io.delta.exceptions.{
  ConcurrentAppendException,
  ConcurrentDeleteDeleteException,
  ConcurrentDeleteReadException,
  ConcurrentTransactionException,
  ConcurrentWriteException,
  MetadataChangedException,
  ProtocolChangedException
}
import lakehouse.ingestion.engine.control.DatabaseFailure
import lakehouse.ingestion.engine.failure.RunFailedException
import lakehouse.ingestion.logstore.{
  CommitMissingException,
  CommitPublicationPendingException,
  RegistrationChangedException,
  StaleRegistrationException,
  TableNotRegisteredException,
  TableTaintedException,
  UncoordinatedWriteException,
  UnpublishableCommitException
}
import lakehouse.ingestion.logstore.coordination.CoordinationSchemaException

/**
 * What a failure that reached the end of a run is, if anything knows (ADR-0022 §2): the one
 * place where what was raised without a class, from inside a library, is given one.
 *
 * What knows the class of a failure where it raises it says so there (`RunFailedException`).
 * What comes from a database, from the LogStore or from Delta is raised by code that does not
 * know what it means for a run, and arrives wrapped: in a failure of I/O, in a job of Spark, in
 * whatever called it. So the chain of causes is read, to a bounded depth, in this order:
 *
 *  1. a failure that has its class, whatever it is wrapped in;
 *  2. the LogStore, which says what it refused and why. It refuses a table for what it knows of
 *     the table, or of itself, and running again does not mend that: a table that is tainted,
 *     is not registered, has a registry that its log contradicts or a commit that cannot be
 *     published, or was written without it; a coordination schema of another version. Two of
 *     its failures pass once the transaction starts again from the table as it is now: a
 *     registration that an operator changed under the commit, and a commit that took its
 *     version and whose publication is pending, which the next look at the table publishes;
 *  3. the conflicts of Delta, as ADR-0010 §1 has them;
 *  4. an error of a database, by its SQLState (`DatabaseFailure`). The LogStore gives every
 *     failure of its own database as a failure of I/O around it, so one that a failure of I/O
 *     wraps is the coordination database's, and one that nothing wraps is the control plane's.
 *
 * What none of these is has no class: a mistake of the application, or a failure that nothing
 * has shown the class of yet. It is not guessed: a class is added with the test that shows it.
 *
 * '''The connector of a source is not asked here.''' It says what a failure of its source is
 * where its source is read (`SourceConnector.failure`), and the JDBC connector takes a failure
 * of SQL that nothing of I/O wraps for its database's: here, that would be the control plane's.
 */
object Failures {

  /**
   * @return the failure with its class and a message for a person, which says nothing that the
   *   failure did not say of itself (no secret, no value of a row); none, if nothing knows what
   *   it is
   */
  def classify(error: Throwable): Option[RunFailedException] = {
    val causes = chain(error)
    causes
      .collectFirst { case classed: RunFailedException => classed }
      .orElse(causes.iterator.map(ofTheLogStore(_, error)).collectFirst { case Some(c) => c })
      .orElse(causes.iterator.map(ofDelta(_, error)).collectFirst { case Some(c) => c })
      .orElse(ofADatabase(causes, error))
  }

  /**
   * What is said of a failure that nothing classified, where a person reads it: the audit trail
   * of its run, and the error of its process (ADR-0022 §3). Its type and where it was thrown,
   * and not what it says: its message was written for nobody in particular, and may hold what
   * it was handed, a value of a row among it. The failure is in the log of the instance, whole.
   */
  def unclassified(error: Throwable): String = {
    val where = error.getStackTrace.headOption.fold("no frame") { frame =>
      val file = Option(frame.getFileName).fold("")(name => s"($name:${frame.getLineNumber})")
      s"${frame.getClassName}.${frame.getMethodName}$file"
    }
    s"nothing classified the failure, a mistake of the application: ${error.getClass.getName} " +
      s"at $where. The log of the instance has it whole"
  }

  /**
   * A failure that something knows the class of, as a pattern of a `catch`: one that says its
   * class, or one that the classifier knows. A fatal error of the JVM is never one.
   */
  object Known {
    def unapply(error: Throwable): Option[RunFailedException] =
      if (NonFatal(error)) classify(error) else None
  }

  private def ofTheLogStore(cause: Throwable, error: Throwable): Option[RunFailedException] = {
    def refused(more: String = ""): Option[RunFailedException] =
      Some(
        RunFailedException.permanent(
          s"the LogStore refused a table of the run, and running again does not mend it: " +
            s"${firstLine(cause.getMessage)}$more. Nothing was committed to it",
          error
        )
      )
    cause match {
      case _: TableTaintedException | _: TableNotRegisteredException |
          _: StaleRegistrationException | _: CommitMissingException |
          _: UnpublishableCommitException =>
        refused()
      case _: UncoordinatedWriteException =>
        refused(
          ". The table is tainted by it, and refuses every commit until an operator has looked " +
            "at its log and adopted it again (ADR-0004 §3.5)"
        )
      case _: CoordinationSchemaException =>
        refused(". The schema is brought to its version by the command migrate")
      case _: RegistrationChangedException | _: CommitPublicationPendingException =>
        Some(
          RunFailedException.transient(
            "a commit of the run is to be looked at again, from the table as it is now: " +
              firstLine(cause.getMessage),
            error
          )
        )
      case _ => None
    }
  }

  private def ofDelta(cause: Throwable, error: Throwable): Option[RunFailedException] =
    cause match {
      case _: ConcurrentAppendException | _: ConcurrentDeleteReadException |
          _: ConcurrentDeleteDeleteException | _: ConcurrentWriteException =>
        Some(
          RunFailedException.conflict(
            s"another writer changed what a write of the run read: ${firstLine(cause.getMessage)}",
            error
          )
        )
      // The write of a batch tries once more on the table as it is now (`BatchAppend`): one
      // that comes here was not tried again, and a change of schema is the policy's (P9).
      case _: MetadataChangedException | _: ProtocolChangedException =>
        Some(
          RunFailedException.permanent(
            "another writer changed the schema or the protocol of a table under a write of " +
              s"the run: ${firstLine(cause.getMessage)}",
            error
          )
        )
      // Two writers of one sequence: what the lease of a source is there to prevent. The write
      // of a batch asks its table whether the other wrote the batch (`BatchAppend`).
      case _: ConcurrentTransactionException =>
        Some(
          RunFailedException.permanent(
            "another writer committed to a table under the name of the sequence of the run, " +
              s"while the run wrote to it: ${firstLine(cause.getMessage)}",
            error
          )
        )
      case _ => None
    }

  private def ofADatabase(causes: Seq[Throwable], error: Throwable): Option[RunFailedException] = {
    val sql = causes.indexWhere(_.isInstanceOf[SQLException])
    Option.when(sql >= 0) {
      val what =
        if (causes.take(sql).exists(_.isInstanceOf[IOException]))
          "the LogStore could not have a commit of the run coordinated by its database"
        else "the run could not go on with its control plane"
      DatabaseFailure.of(what, error, DatabaseFailure.said(error)) match {
        case DatabaseFailure.Unreachable(message) => RunFailedException.transient(message, error)
        case DatabaseFailure.Refused(message) => RunFailedException.permanent(message, error)
      }
    }
  }

  /** The failure and its causes, outermost first, each once and no more than `MaxCauses`. */
  private def chain(error: Throwable): Vector[Throwable] =
    Iterator
      .iterate(Option(error))(_.flatMap(e => Option(e.getCause).filter(_ ne e)))
      .takeWhile(_.nonEmpty)
      .flatten
      .take(MaxCauses)
      .toVector
      .distinct

  /** As deep as `DatabaseFailure` reads a failure of a database. */
  private val MaxCauses = 16

  private def firstLine(message: String): String =
    Option(message).flatMap(_.linesIterator.map(_.trim).find(_.nonEmpty)).getOrElse("no message")
}
