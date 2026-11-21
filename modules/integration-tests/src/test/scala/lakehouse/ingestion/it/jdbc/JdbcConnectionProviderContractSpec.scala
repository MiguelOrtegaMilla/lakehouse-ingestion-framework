package lakehouse.ingestion.it.jdbc

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import lakehouse.ingestion.core.config.{SecretRef, SecretResolver}
import lakehouse.ingestion.engine.jdbc.SecretRefConnectionProvider
import lakehouse.ingestion.it.support.{BootstrappedPostgres, CapturedLogs, LocalSparkSuite}
import org.apache.logging.log4j.Level
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * How Spark 4.1 opens the connection of a read, and what the engine's provider of a connection
 * stands on of it (`SecretRefConnectionProvider`): the one place where the engine uses Spark's
 * `JdbcConnectionProvider`, a developer API, and `JDBCOptions`, which is no API at all.
 *
 * As `DeltaSetTransactionContractSpec` does for Delta, each test pins one fact, so that a Spark
 * that changes it fails here, with the name of what it breaks, before a read hands a password
 * to a log (ADR-0014, upgrade procedure). The first pins why the provider is there: Spark's own
 * logs the password of every connection that it opens, at `DEBUG`.
 */
final class JdbcConnectionProviderContractSpec
    extends AnyFunSuite
    with Matchers
    with LocalSparkSuite {

  private lazy val pg = BootstrappedPostgres.start()

  override def afterAll(): Unit =
    try pg.stop()
    finally super.afterAll()

  /** Where the logs of Spark's JDBC source are, a connection's provider among them. */
  private val SparkJdbc = "org.apache.spark.sql.execution.datasources.jdbc"

  private val Settings =
    "(SELECT current_user AS role, current_setting('application_name') AS application) AS s"

  private def password = pg.password("sales_ro")

  private def secrets: SecretResolver =
    SecretResolver(Map("SALES_RO_PASSWORD" -> password).get)

  /** A read of `Settings` whose connection is opened as the options say. */
  private def read(options: Map[String, String]): Row =
    spark.read
      .format("jdbc")
      .options(
        Map(
          "url" -> pg.jdbcUrl("sales"),
          "user" -> "sales_ro",
          "dbtable" -> Settings,
          "ApplicationName" -> "contract"
        ) ++ options
      )
      .load()
      .head()

  private def byTheEngine(resolver: String = SecretRefConnectionProvider.register(secrets)) =
    SecretRefConnectionProvider.options(SecretRef.Env("SALES_RO_PASSWORD"), resolver)

  private def withPassword(logged: Seq[CapturedLogs.Logged]): Seq[String] =
    logged.map(_.message).filter(_.contains(password))

  test("Spark's own provider logs every property of a connection at DEBUG, the password too") {
    val (row, logged) =
      CapturedLogs.under(SparkJdbc, Level.DEBUG)(read(Map("password" -> password)))

    row shouldBe Row("sales_ro", "contract")
    // When a Spark stops doing so, this fails, and the provider may be looked at again.
    withPassword(logged) should not be empty
    logged.map(_.message).filter(_.contains(password)).head should include(
      "JDBC connection initiated with URL"
    )
  }

  test("Spark finds the engine's provider, and a read that names it is opened with no password") {
    val (row, logged) = CapturedLogs.under(SparkJdbc, Level.DEBUG)(read(byTheEngine()))

    // The connection is made as the role, with what JDBCOptions makes properties of: an option
    // of the driver keeps the case of its name.
    row shouldBe Row("sales_ro", "contract")
    withPassword(logged) shouldBe empty
  }

  test("a read that two providers can open is refused, unless it names one of them") {
    val ofBoth = byTheEngine() - "connectionProvider"

    val refused = intercept[Exception](read(ofBoth))

    refused.getMessage should include("more than one connection provider")
  }

  test("a read given its password as an option is refused by the engine's provider") {
    val refused = intercept[Exception](read(byTheEngine() + ("password" -> password)))

    Iterator
      .iterate[Throwable](refused)(_.getCause)
      .takeWhile(_ != null)
      .map(_.getMessage)
      .mkString(" / ") should include("was given its password as an option")
    refused.getMessage should not include password
  }

  test("a password that cannot be read fails the connection, and says where it was to be") {
    val nothing = SecretRefConnectionProvider.register(SecretResolver(_ => None))

    val refused = intercept[Exception](read(byTheEngine(nothing)))

    Iterator
      .iterate[Throwable](refused)(_.getCause)
      .takeWhile(_ != null)
      .map(_.getMessage)
      .mkString(" / ") should include("the password at env:SALES_RO_PASSWORD could not be read")
  }

  test("a JVM that has not the resolver of a read reads the reference from its own files") {
    // As an executor on another machine would: the reference is read where it points.
    val file = Files.createTempFile(workDir, "sales_ro", ".secret")
    Files.write(file, password.getBytes(UTF_8))
    val options = SecretRefConnectionProvider.options(SecretRef.File(file.toString), "not-here")

    read(options) shouldBe Row("sales_ro", "contract")
  }
}
