package lakehouse.ingestion.engine.jdbc

import java.io.IOException
import java.net.SocketException
import java.sql.SQLException

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import lakehouse.ingestion.core.config.{DatabaseConnection, Source, Timeouts}
import lakehouse.ingestion.core.control.FailureClass
import org.apache.spark.{SparkException, SparkThrowable}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, OptionValues}

/**
 * What a failure of a read is for its run, by the failures themselves, made as Spark and the
 * driver make them. That they are made so is held to a real server apart
 * (`JdbcIngestionSpec`).
 */
final class JdbcFailuresSpec extends AnyFunSuite with Matchers with EitherValues with OptionValues {

  import JdbcFailuresSpec._

  private val Where = "the source sales.orders (jdbc:postgresql://db:5432/sales, as sales_ro)"

  private val connection: DatabaseConnection = Source
    .load(
      "sales.orders.yaml",
      """id: sales.orders
        |connector:
        |  type: jdbc
        |  connection:
        |    url: jdbc:postgresql://db:5432/sales
        |    user: sales_ro
        |    password: { secretRef: "env:SALES_RO_PASSWORD" }
        |    tls: { mode: disable }
        |    timeouts: { statement: 90s }
        |  table: public.orders
        |load: { mode: full }
        |targets: { bronze: { table: bronze/sales/orders } }
        |""".stripMargin
    )
    .value
    .value
    .connector
    .asInstanceOf[lakehouse.ingestion.core.config.JdbcConnectorConfig]
    .connection

  /** A failure of a task of Spark, as a write of Delta gives one. */
  private def inATask(cause: Throwable): Throwable =
    new SparkException("[TASK_WRITE_FAILED] Task failed while writing rows", cause)

  private def of(error: Throwable) = JdbcFailures.of(Where, connection, error)

  test("the configuration of the test is read as it is written") {
    connection.timeouts shouldBe Timeouts(5.seconds, 90.seconds)
  }

  test("a failure of SQL is of the class that its state says") {
    Seq(
      "08006" -> FailureClass.Transient,
      "08001" -> FailureClass.Transient,
      "57P01" -> FailureClass.Transient,
      "57P03" -> FailureClass.Transient,
      "28P01" -> FailureClass.Permanent,
      "3D000" -> FailureClass.Permanent,
      "42P01" -> FailureClass.Permanent,
      "42501" -> FailureClass.Permanent,
      "42703" -> FailureClass.Permanent
    ).foreach { case (state, expected) =>
      withClue(state) {
        val failed = of(inATask(new SQLException("the server said so", state))).value
        failed.failure shouldBe expected
        failed.getMessage shouldBe s"$Where: the server said so (SQLState $state)"
      }
    }
  }

  test("the state is the innermost one, and what the socket said is under the failure of SQL") {
    val cut = new SQLException(
      "An I/O error occurred while sending to the backend.",
      "08006",
      new SocketException("Connection reset")
    )
    val failed = of(new SparkException("Job aborted due to stage failure", inATask(cut))).value

    failed.failure shouldBe FailureClass.Transient
    failed.getCause shouldBe a[SparkException]
  }

  test("a statement that ran too long may pass, and says which settings to change") {
    val failed = of(
      inATask(new SQLException("ERROR: canceling statement due to statement timeout", "57014"))
    ).value

    failed.failure shouldBe FailureClass.Transient
    failed.getMessage shouldBe
      s"$Where took longer over one statement than 90s, and the statement was cut (ERROR: " +
      "canceling statement due to statement timeout (SQLState 57014)). A batch is read with " +
      "a cursor, and that is how long one fetch of it may take: raise " +
      "connector.connection.timeouts.statement, or, for a load by a watermark, have a batch " +
      "read less, with load.watermark.start and load.max_batch_range. Nothing of the batch " +
      "was written"
    // A statement that was cancelled otherwise may pass as well, and has no setting to blame.
    val cancelled = of(
      inATask(new SQLException("ERROR: canceling statement due to user request", "57014"))
    ).value
    cancelled.failure shouldBe FailureClass.Transient
    cancelled.getMessage should startWith(s"$Where: ERROR: canceling statement due to user")
  }

  test("a value that does not fit its type will not pass") {
    val failed =
      of(inATask(new SQLException("Bad value for type BigDecimal : NaN", "22003"))).value

    failed.failure shouldBe FailureClass.Permanent
    failed.getMessage should startWith(
      s"$Where has a value that does not fit the type that its column is read into"
    )
    failed.getMessage should include("Bad value for type BigDecimal : NaN (SQLState 22003)")
  }

  test("a failure of SQL under one of I/O is not the source's: the LogStore has a database too") {
    // As `PostgresCoordinationStore` gives every failure of its database.
    val coordination = new IOException(
      "coordination store: cannot claim a version: the connection was lost",
      new SQLException("the connection was lost", "08006")
    )

    of(coordination) shouldBe None
    of(new SparkException("the commit failed", coordination)) shouldBe None
    of(new java.io.UncheckedIOException(coordination)) shouldBe None
  }

  test("a failure with nothing of SQL in it is not the source's") {
    of(new IllegalStateException("something else")) shouldBe None
    of(inATask(new IOException("No space left on device"))) shouldBe None
    of(new SparkException("Job 3 cancelled")) shouldBe None
  }

  test("a row that the connector refused fails for good, with what the connector said of it") {
    def raised(message: String): Throwable = inATask(new Raised(message))

    val failed = of(raised(JdbcFailures.Raised + "the column valid_to has infinity")).value
    failed.failure shouldBe FailureClass.Permanent
    failed.getMessage shouldBe "the column valid_to has infinity"
    // What a query of someone else's raised is not the connector's to explain.
    of(raised("something that a function of the source raised")) shouldBe None
  }
}

object JdbcFailuresSpec {

  /** An error that a query raised itself, as Spark gives one: its message is a parameter. */
  final class Raised(message: String)
      extends RuntimeException(s"[USER_RAISED_EXCEPTION] $message SQLSTATE: P0001")
      with SparkThrowable {
    override def getCondition: String = "USER_RAISED_EXCEPTION"
    override def getMessageParameters: java.util.Map[String, String] =
      Map("errorMessage" -> message).asJava
  }
}
