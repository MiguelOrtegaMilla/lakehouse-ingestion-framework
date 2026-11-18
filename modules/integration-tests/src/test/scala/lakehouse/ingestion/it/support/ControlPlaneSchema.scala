package lakehouse.ingestion.it.support

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.sql.{Connection, SQLException}

import scala.util.Using

import lakehouse.ingestion.cli.Main

/**
 * The schemas of a deployment, applied the way a deployment applies them: by the command
 * `migrate`, as `ingest_migrator`, with the example profile of the repository pointed at the
 * database of the test.
 */
object ControlPlaneSchema {

  val Database = "ingestion_control"

  /** The only role that runs DDL (ADR-0013 §3). */
  val Migrator = "ingest_migrator"

  /** The role the engine connects to the control plane as (ADR-0013 §3). */
  val Engine = "ingest_control"

  /** The variable that the tests give the password of the migrator in. */
  val PasswordVariable = "INGEST_MIGRATOR_PASSWORD"

  private val ExampleUrl = "jdbc:postgresql://localhost:5432/ingestion_control"

  /** What a command printed, and how it ended. */
  final case class Ran(exit: Int, out: String, err: String) {
    def lines: Seq[String] = out.linesIterator.toSeq
    def errors: Seq[String] = err.linesIterator.toSeq
  }

  /**
   * The example profile, in a file, with both of its databases at `url`.
   *
   * @param url where the databases are; that of `pg`, unless a test wants one that is not there
   */
  def profile(pg: BootstrappedPostgres, url: Option[String] = None): Path = {
    val example = new String(
      Files.readAllBytes(BootstrappedPostgres.repoRoot().resolve("conf/app/local.example.yaml")),
      UTF_8
    )
    require(example.split(java.util.regex.Pattern.quote(ExampleUrl), -1).length == 3)
    val file = Files.createTempFile("profile-", ".yaml")
    file.toFile.deleteOnExit()
    Files.write(
      file,
      example.replace(ExampleUrl, url.getOrElse(pg.jdbcUrl(Database))).getBytes(UTF_8)
    )
  }

  /** Runs `migrate` on the database of `pg`, as the migrator and with its password. */
  def migrate(pg: BootstrappedPostgres): Ran =
    command(Map(PasswordVariable -> pg.password(Migrator)))(
      "migrate",
      "--profile",
      profile(pg).toString,
      "--password",
      s"env:$PasswordVariable"
    )

  /** Runs a command line with an environment of the test's own. */
  def command(environment: Map[String, String])(args: String*): Ran = {
    val (out, err) = (new ByteArrayOutputStream, new ByteArrayOutputStream)
    val exit = Main.run(
      args,
      new PrintStream(out, true, "UTF-8"),
      new PrintStream(err, true, "UTF-8"),
      environment.get
    )
    Ran(exit, out.toString("UTF-8"), err.toString("UTF-8"))
  }

  /** Runs `sql` with its parameters; the number of rows it changed. */
  @throws[SQLException]
  def update(c: Connection, sql: String, parameters: AnyRef*): Int =
    Using.resource(c.prepareStatement(sql)) { statement =>
      parameters.zipWithIndex.foreach { case (value, i) => statement.setObject(i + 1, value) }
      statement.executeUpdate()
    }

  /** The rows of a query, every column as text (booleans as PostgreSQL prints them: t, f). */
  @throws[SQLException]
  def query(c: Connection, sql: String, parameters: AnyRef*): Seq[Seq[String]] =
    Using.resource(c.prepareStatement(sql)) { statement =>
      parameters.zipWithIndex.foreach { case (value, i) => statement.setObject(i + 1, value) }
      Using.resource(statement.executeQuery()) { rows =>
        val columns = rows.getMetaData.getColumnCount
        Iterator
          .continually(rows)
          .takeWhile(_.next())
          .map(row => (1 to columns).map(row.getString))
          .toVector
      }
    }
}
