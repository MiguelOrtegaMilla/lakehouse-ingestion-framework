package lakehouse.ingestion.it.cli

import java.nio.charset.StandardCharsets.{ISO_8859_1, UTF_8}
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._
import scala.util.Using

import lakehouse.ingestion.cli.Main
import lakehouse.ingestion.core.config.Source
import lakehouse.ingestion.it.control.ControlPlaneSuite
import lakehouse.ingestion.it.support.ControlPlaneSchema.Database
import lakehouse.ingestion.it.support.{CommandProcess, DeploymentFiles}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The password of a source, from end to end (ADR-0013, discovery §17, P6.9): `run` in a process
 * of its own, as a deployment starts it, reads a table of a database once with its password and
 * once with one that the database refuses. Neither is anywhere that the run left something:
 * what the process printed, the files of the lake, the configuration of the deployment, the
 * rows of the control plane and of the coordination of Delta, and the log of the server.
 *
 * The process logs at the level of a deployment, and at `DEBUG` for the application and for
 * Spark's reads of a database: what an operator does to look into a run that went wrong. Its
 * password is in a file of secrets, as a deployment mounts it, which is the one place where it
 * is to be.
 *
 * What Spark itself shows of a read, its plans and the events of its execution, is pinned by
 * `SparkJdbcContractSpec`; a password that the source refuses is in no message of the
 * connector, by `JdbcSourceConnectorSpec`.
 */
final class SourcePasswordSpec extends AnyFunSuite with Matchers with ControlPlaneSuite {

  private lazy val work = Files.createTempDirectory("source-password-")
  private lazy val files = new DeploymentFiles(pg, work)

  override def afterAll(): Unit =
    try files.close()
    finally super.afterAll()

  /** A logging of a deployment, at `DEBUG` where a read of a source is made. */
  private lazy val logging: Path = {
    val file = work.resolve("log4j2.properties")
    Files.write(
      file,
      """rootLogger.level = info
        |rootLogger.appenderRef.console.ref = console
        |appender.console.type = Console
        |appender.console.name = console
        |appender.console.layout.type = PatternLayout
        |appender.console.layout.pattern = %d{HH:mm:ss.SSS} %-5level %c - %msg%n
        |logger.application.name = lakehouse
        |logger.application.level = debug
        |logger.sparkJdbc.name = org.apache.spark.sql.execution.datasources.jdbc
        |logger.sparkJdbc.level = debug
        |""".stripMargin.getBytes(UTF_8)
    )
    file
  }

  private def run(of: Source): CommandProcess.Ended = {
    val log = files.newLog()
    val ended = CommandProcess
      .start(files.arguments(of), log, jvm = Seq(s"-Dlog4j2.configurationFile=$logging"))
      .await()
    ended.copy(log = new String(Files.readAllBytes(log), UTF_8))
  }

  /**
   * Every file under `root`, but the secrets of the deployment, which the password is in, and
   * what the processes printed, which is looked at line by line.
   */
  private def filesUnder(root: Path): Seq[Path] =
    Using.resource(Files.walk(root))(
      _.iterator.asScala
        .filter(Files.isRegularFile(_))
        .filterNot(_.startsWith(work.resolve("secrets")))
        .filterNot(_.getFileName.toString.startsWith("process-"))
        .toVector
    )

  /** The files where `text` is, byte for byte. */
  private def filesWith(text: String, among: Seq[Path]): Seq[Path] =
    among.filter(file => new String(Files.readAllBytes(file), ISO_8859_1).contains(text))

  /** The lines of `log` that hold `secret`, with it hidden: where a failure is to look. */
  private def linesWith(secret: String, log: String): Seq[String] =
    log.linesIterator
      .filter(_.contains(secret))
      .map(_.replace(secret, "<secret>").split(" - ").head.drop(13).take(160))
      .toSeq
      .distinct

  /** The rows of the control plane and of the coordination of Delta, as text. */
  private def databaseDump(): String =
    pg.exec("pg_dump", "-U", "postgres", "--data-only", Database)

  test("the password of a source is in no log, no file and no row that a run leaves") {
    Using.resource(pg.connect("sales_owner", "sales")) { c =>
      Using.resource(c.createStatement()) { statement =>
        statement.execute("CREATE TABLE secrecy (id int, note text)")
        statement.execute("INSERT INTO secrecy VALUES (1, 'a'), (2, 'b')"): Unit
      }
    }
    val password = pg.password("sales_ro")
    // Of the same form as the right one, which no line of a log can hold by chance.
    val refused = password.reverse
    val read = files.tableSource(
      newSourceId(),
      "public.secrecy",
      files.connection("sales_ro", password, url = pg.jdbcUrl("sales"))
    )
    val notLetIn = files.tableSource(
      newSourceId(),
      "public.secrecy",
      files.connection("sales_ro", refused, url = pg.jdbcUrl("sales"))
    )

    val done = run(read)
    val failed = run(notLetIn)

    withClue(done.log.takeRight(3000))(done.exit shouldBe Main.Exit.Ok)
    withClue(failed.log.takeRight(3000))(failed.exit shouldBe Main.Exit.Permanent)
    // The processes logged at DEBUG, and wrote what a run writes.
    done.log should include(" DEBUG ")
    failed.log should include("password")
    val left = filesUnder(work)
    left.exists(_.toString.contains("_delta_log")) shouldBe true
    val dump = databaseDump()
    dump should include(read.id.value)
    dump should include(notLetIn.id.value)

    // Where each password is, of everything that the runs left: nowhere.
    val found = for {
      (what, secret) <- Seq("the password" -> password, "the refused password" -> refused)
      (where, lines) <- Seq(
        "the log of the run that read" -> linesWith(secret, done.log),
        "the log of the run that was refused" -> linesWith(secret, failed.log),
        "the files of the deployment" -> filesWith(secret, left).map(_.toString),
        "the control plane" -> linesWith(secret, dump),
        "the log of the server" -> linesWith(secret, pg.serverLog())
      )
      line <- lines
    } yield s"$what, in $where: $line"
    found shouldBe empty
  }
}
