package lakehouse.ingestion.it.support

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.sql.{Connection, SQLException}

import scala.util.Using

import lakehouse.ingestion.cli.Main
import lakehouse.ingestion.core.config.{ConfigLoader, Loaded, Profile}
import lakehouse.ingestion.engine.control.ControlSchema
import org.flywaydb.core.Flyway

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

  private val ExampleTls = "tls: { mode: disable }"

  /**
   * The example profile, in a file, with both of its databases at `url`.
   *
   * @param url where the databases are; that of `pg`, unless a test wants one that is not there
   * @param rootCertificate with it, both databases are reached with `verify-full` against it;
   *   without it, with no TLS, as the example has them
   */
  def profile(
      pg: BootstrappedPostgres,
      url: Option[String] = None,
      rootCertificate: Option[Path] = None
  ): Path = {
    val example = new String(
      Files.readAllBytes(BootstrappedPostgres.repoRoot().resolve("conf/app/local.example.yaml")),
      UTF_8
    )
    def twice(text: String): Boolean =
      example.split(java.util.regex.Pattern.quote(text), -1).length == 3
    require(twice(ExampleUrl) && twice(ExampleTls), "the example profile is not as this expects it")
    val tls = rootCertificate.fold(ExampleTls) { certificate =>
      s"tls: { mode: verify-full, root_certificate: $certificate }"
    }
    val file = Files.createTempFile("profile-", ".yaml")
    file.toFile.deleteOnExit()
    Files.write(
      file,
      example
        .replace(ExampleUrl, url.getOrElse(pg.jdbcUrl(Database)))
        .replace(ExampleTls, tls)
        .getBytes(UTF_8)
    )
  }

  /** That profile, read as the application reads it. */
  def loaded(
      pg: BootstrappedPostgres,
      url: Option[String] = None,
      rootCertificate: Option[Path] = None
  ): Profile =
    ConfigLoader.loadProfile(profile(pg, url, rootCertificate)) match {
      case Right(Loaded(valid, _)) => valid
      case Left(errors) =>
        throw new IllegalStateException(errors.toChain.toList.map(_.message).mkString("; "))
    }

  /** The text of an example source of the repository, as `sales.orders`. */
  def exampleSource(id: String): String = new String(
    Files.readAllBytes(BootstrappedPostgres.repoRoot().resolve(s"conf/sources/$id.yaml")),
    UTF_8
  )

  /** The environment in which the references of the example profile point at the passwords of `pg`. */
  def environment(pg: BootstrappedPostgres): Map[String, String] = Map(
    "INGEST_CONTROL_PASSWORD" -> pg.password(Engine),
    "INGEST_COORDINATION_PASSWORD" -> pg.password("ingest_coordination")
  )

  /** Runs `migrate` on the database of `pg`, as the migrator and with its password. */
  def migrate(pg: BootstrappedPostgres, rootCertificate: Option[Path] = None): Ran =
    command(Map(PasswordVariable -> pg.password(Migrator)))(
      "migrate",
      "--profile",
      profile(pg, rootCertificate = rootCertificate).toString,
      "--password",
      s"env:$PasswordVariable"
    )

  /**
   * Brings the schema `control` of `pg` to version `upTo` and no further: a deployment that is
   * not upgraded yet. By Flyway itself, as the migrator: the command only goes to the last.
   *
   * @return how many migrations were applied
   */
  def migrateUpTo(pg: BootstrappedPostgres, upTo: String): Int =
    Flyway
      .configure()
      .dataSource(pg.jdbcUrl(Database), Migrator, pg.password(Migrator))
      .locations(ControlSchema.Migrations)
      .defaultSchema(ControlSchema.Name)
      .createSchemas(false)
      .target(upTo)
      .load()
      .migrate()
      .migrationsExecuted

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
