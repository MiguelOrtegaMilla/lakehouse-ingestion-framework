package lakehouse.ingestion.engine

import java.io.{FileNotFoundException, IOException}
import java.nio.file.Files
import java.sql.{SQLException, SQLTransientConnectionException}

import io.delta.exceptions.{
  ConcurrentAppendException,
  ConcurrentDeleteDeleteException,
  ConcurrentDeleteReadException,
  ConcurrentTransactionException,
  ConcurrentWriteException,
  MetadataChangedException,
  ProtocolChangedException
}
import lakehouse.ingestion.core.control.FailureClass
import lakehouse.ingestion.engine.failure.RunFailedException
import lakehouse.ingestion.logstore.coordination.{CoordinationSchemaException, TableUri}
import lakehouse.ingestion.logstore.{
  CommitMissingException,
  CommitPublicationPendingException,
  RegistrationChangedException,
  StaleRegistrationException,
  TableIdentity,
  TableNotRegisteredException,
  TableTaintedException,
  UncoordinatedWriteException,
  UnpublishableCommitException
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkException
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What the classifier makes of a failure that reaches the end of a run, by the failures
 * themselves: the types of the LogStore and of Delta, and failures of SQL as the driver gives
 * them, wrapped as the LogStore and Spark wrap them. That a real table, a real Delta and a real
 * database give them so is held apart (`FailureClassesSpec`).
 */
final class FailuresSpec extends AnyFunSuite with Matchers with OptionValues {

  private val table: TableUri = {
    val directory = Files.createTempDirectory("failures-spec")
    directory.toFile.deleteOnExit()
    TableIdentity.of(new Path(directory.toUri), FileSystem.getLocal(new Configuration()))
  }

  /** A failure of a job of Spark, as a write gives one. */
  private def inAJob(cause: Throwable): Throwable =
    new SparkException("Job aborted due to stage failure", cause)

  /** What the LogStore makes of a failure of its own database. */
  private def ofTheCoordinationStore(cause: SQLException): IOException =
    new IOException(s"coordination store: cannot claim a version: ${cause.getMessage}", cause)

  private def classOf(error: Throwable): FailureClass = Failures.classify(error).value.failure

  test("a failure that says its class keeps it, whatever it is wrapped in") {
    val classed = RunFailedException.permanent("the plan is not one that this application reads")
    Failures.classify(classed).value shouldBe theSameInstanceAs(classed)
    Failures.classify(inAJob(new RuntimeException("wrapped", classed))).value shouldBe
      theSameInstanceAs(classed)
  }

  test("a table that the LogStore refuses is a permanent failure that says the LogStore's words") {
    Seq(
      new TableTaintedException(table, "an uncoordinated commit at version 3"),
      new TableNotRegisteredException(table),
      new StaleRegistrationException(table, "the registry has version 7"),
      new UncoordinatedWriteException(table, 4L, "a commit that this protocol did not make"),
      new CommitMissingException(table, 2L),
      new UnpublishableCommitException(table, 5L, "/lake/_delta_log/_tmp/5.json.x"),
      new CoordinationSchemaException("the coordination schema is at version 2, not 4")
    ).foreach { refusal =>
      withClue(refusal.getClass.getSimpleName) {
        Seq(refusal, inAJob(refusal)).foreach { error =>
          val classed = Failures.classify(error).value
          classed.failure shouldBe FailureClass.Permanent
          classed.getMessage should startWith("the LogStore refused a table of the run")
          classed.getMessage should include(refusal.getMessage.linesIterator.next())
          classed.getCause shouldBe theSameInstanceAs(error)
        }
      }
    }
  }

  test("what the LogStore says of a write that it did not coordinate, and of its own schema") {
    Failures
      .classify(new UncoordinatedWriteException(table, 4L, "found"))
      .value
      .getMessage should include("tainted by it")
    Failures
      .classify(new CoordinationSchemaException("the schema is missing"))
      .value
      .getMessage should include("the command migrate")
  }

  test("what the LogStore says passes once the transaction starts again, passes") {
    Seq(
      new RegistrationChangedException(table),
      new CommitPublicationPendingException(table, 6L, new IOException("the link failed"))
    ).foreach { error =>
      withClue(error.getClass.getSimpleName) {
        classOf(error) shouldBe FailureClass.Transient
        classOf(inAJob(error)) shouldBe FailureClass.Transient
      }
    }
  }

  test("the LogStore is looked at before the failure of SQL that it wraps") {
    val wrapped = new CoordinationSchemaException(
      "the coordination schema cannot be read",
      new SQLException("connection refused", "08001")
    )
    classOf(wrapped) shouldBe FailureClass.Permanent
  }

  test("the conflicts of Delta are of the classes of ADR-0010") {
    Seq(
      new ConcurrentAppendException("files were added") -> FailureClass.Conflict,
      new ConcurrentDeleteReadException("files read were removed") -> FailureClass.Conflict,
      new ConcurrentDeleteDeleteException("files were removed twice") -> FailureClass.Conflict,
      new ConcurrentWriteException("another write") -> FailureClass.Conflict,
      new MetadataChangedException("the metadata changed") -> FailureClass.Permanent,
      new ProtocolChangedException("the protocol changed") -> FailureClass.Permanent,
      new ConcurrentTransactionException("the same appId") -> FailureClass.Permanent
    ).foreach { case (conflict, expected) =>
      withClue(conflict.getClass.getSimpleName) {
        classOf(conflict) shouldBe expected
        classOf(inAJob(conflict)) shouldBe expected
        Failures.classify(conflict).value.getMessage should include(conflict.getMessage)
      }
    }
  }

  test("a failure of SQL that nothing wraps is the control plane's, by its state") {
    Seq(
      "08006" -> FailureClass.Transient,
      "57P01" -> FailureClass.Transient,
      "40001" -> FailureClass.Transient,
      "53300" -> FailureClass.Transient,
      "28P01" -> FailureClass.Permanent,
      "42501" -> FailureClass.Permanent,
      "42P01" -> FailureClass.Permanent
    ).foreach { case (state, expected) =>
      withClue(state) {
        val classed = Failures.classify(new SQLException("the server said so", state)).value
        classed.failure shouldBe expected
        classed.getMessage shouldBe
          s"the run could not go on with its control plane: the server said so (SQLState $state)"
      }
    }
  }

  test("a failure of SQL inside a failure of I/O is the coordination database's") {
    val cut = new SQLException("An I/O error occurred while sending to the backend.", "08006")
    Seq(ofTheCoordinationStore(cut), inAJob(ofTheCoordinationStore(cut))).foreach { error =>
      val classed = Failures.classify(error).value
      classed.failure shouldBe FailureClass.Transient
      classed.getMessage should startWith(
        "the LogStore could not have a commit of the run coordinated by its database"
      )
      classed.getMessage should include("(SQLState 08006)")
    }
    classOf(ofTheCoordinationStore(new SQLException("permission denied", "42501"))) shouldBe
      FailureClass.Permanent
  }

  test("a pool that timed out is classified by what the database said to its attempt") {
    val refused = new SQLException("password authentication failed for user", "28P01")
    classOf(new SQLTransientConnectionException("pool timed out", "08001", refused)) shouldBe
      FailureClass.Permanent
  }

  test("what nothing knows has no class: a mistake, or a failure of I/O of its own") {
    Seq(
      new IllegalStateException("a mistake of the application"),
      new IOException("No space left on device"),
      new FileNotFoundException("/lake/bronze/part-0001.parquet"),
      inAJob(new IOException("Stream closed")),
      new OutOfMemoryError("Java heap space")
    ).foreach { error =>
      withClue(error.toString) {
        Failures.classify(error) shouldBe None
      }
    }
  }

  test("a fatal error of the JVM is never known, even around a failure that has a class") {
    val fatal = new OutOfMemoryError("Java heap space")
    fatal.initCause(new SQLException("connection lost", "08006"))
    Failures.Known.unapply(fatal) shouldBe None
    Failures.Known.unapply(new SQLException("connection lost", "08006")).value.failure shouldBe
      FailureClass.Transient
  }

  test("a chain of causes that loops, or that is very deep, is read to its bound") {
    val first = new IOException("first")
    val second = new IOException("second", first)
    first.initCause(second)
    Failures.classify(first) shouldBe None

    // Deeper than the bound, the failure of SQL is not seen.
    val deep = (1 to 20).foldLeft[Throwable](new SQLException("lost", "08006")) { (cause, n) =>
      new RuntimeException(s"level $n", cause)
    }
    Failures.classify(deep) shouldBe None
    val shallow = (1 to 5).foldLeft[Throwable](new SQLException("lost", "08006")) { (cause, n) =>
      new RuntimeException(s"level $n", cause)
    }
    classOf(shallow) shouldBe FailureClass.Transient
  }
}
