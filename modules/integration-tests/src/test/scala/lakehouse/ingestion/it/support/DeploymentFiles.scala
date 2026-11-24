package lakehouse.ingestion.it.support

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path}
import java.time.Instant

import lakehouse.ingestion.core.config.{Loaded, Source, SourceId}
import lakehouse.ingestion.it.support.ControlPlaneSchema.{Database, Engine}

/**
 * What the command `run` is started with, as a deployment has it: a profile and its sources in
 * files, the passwords of both databases in files of their own, and a lake and a landing zone
 * on disk. Both databases are in the PostgreSQL of the test, each reached as its own role.
 *
 * Everything is under one directory, which `close` deletes.
 *
 * @param pg the PostgreSQL that has the control plane and the coordination database
 * @param work the directory of the deployment, which is this object's to delete
 */
final class DeploymentFiles(pg: BootstrappedPostgres, work: Path) extends AutoCloseable {

  val lake: Path = Files.createDirectories(work.resolve("lake"))
  val landing: Path = Files.createDirectories(work.resolve("landing"))
  val sources: Path = Files.createDirectories(work.resolve("sources"))
  private val secrets = Files.createDirectories(work.resolve("secrets"))

  /** A file of the secrets of the test: a deployment has them mounted as files. */
  private def secret(name: String, value: String): String = {
    val file = secrets.resolve(name)
    Files.write(file, value.getBytes(UTF_8))
    s"file:$file"
  }

  /**
   * A connection to a database, as it is written under its name in a profile.
   *
   * @param password what the role is to give as its password, kept in a file of the secrets
   * @param url where the database is; in `pg`, unless a test wants one that is not there
   */
  def connection(
      role: String,
      password: String,
      url: String = pg.jdbcUrl(Database),
      connect: String = "5s"
  ): String =
    s"""    url: "$url"
       |    user: $role
       |    password: { secretRef: "${secret(s"$role-${password.hashCode}", password)}" }
       |    tls: { mode: disable }
       |    timeouts: { connect: $connect, statement: 10s }
       |""".stripMargin

  /**
   * A profile of the deployment, in a file: its lake and landing zone, and both databases, each
   * reached as its own role with its password in a file.
   *
   * @param control the connection to the control plane, as it is written under it
   * @param coordination that to the coordination database
   * @param retries for how long a step that fails with an error that passes is tried again, as
   *   `retries.budget` says; for as long as by default, unless it is given
   * @param leases for how long a run holds a lease that it does not renew, as `leases.ttl` says;
   *   as long as by default, unless it is given
   */
  def profileFile(
      control: String = connection(Engine, pg.password(Engine)),
      coordination: String = connection("ingest_coordination", pg.password("ingest_coordination")),
      lake: Path = lake,
      retries: Option[String] = None,
      leases: Option[String] = None
  ): Path = {
    val file = Files.createTempFile(work, "profile-", ".yaml")
    Files.write(
      file,
      s"""lake: { uri: "${lake.toUri}", deployment: single-machine }
         |landing: { root: "${landing.toUri}" }
         |databases:
         |  control:
         |$control  coordination:
         |$coordination
         |${retries.fold("")(budget =>
          s"retries: { budget: $budget, first_backoff: 200ms, max_backoff: 1s }"
        )}
         |${leases.fold("")(ttl => s"leases: { ttl: $ttl }")}
         |spark:
         |  master: "local[2]"
         |  settings:
         |    spark.ui.enabled: false
         |    spark.sql.shuffle.partitions: 2
         |    spark.default.parallelism: 2
         |    spark.databricks.delta.snapshotPartitions: 2
         |    spark.driver.host: 127.0.0.1
         |    spark.driver.bindAddress: 127.0.0.1
         |    spark.sql.warehouse.dir: "${work.resolve("warehouse")}"
         |""".stripMargin.getBytes(UTF_8)
    )
  }

  /** The profile with nothing wrong in it. */
  lazy val validProfile: Path = profileFile()

  /**
   * A source of files, written among the sources of the deployment, with its directory made in
   * the landing zone.
   *
   * @param rejected the share of the records of a batch that it lets be rejected; none, unless
   *   it says so
   * @param table its table of Bronze, under the lake; one of its own, unless it is given one,
   *   which another source may be given as well
   */
  def fileSource(
      id: SourceId,
      rejected: Option[String] = None,
      format: String = "csv",
      table: Option[String] = None,
      options: String = ""
  ): Source = {
    Files.createDirectories(directoryOf(id))
    val text =
      s"""id: $id
         |connector: { type: file, path: ${directoryOf(id).getFileName}, format: $format$options }
         |load: { mode: new_files }
         |targets: { bronze: { table: ${table.getOrElse(
          s"bronze/${id.value.replace('.', '/')}"
        )} } }
         |${rejected.fold("")(share => s"quality: { max_rejected_ratio: $share }")}
         |""".stripMargin
    Files.write(sources.resolve(s"$id.yaml"), text.getBytes(UTF_8))
    Source.load(s"$id.yaml", text) match {
      case Right(Loaded(source, _)) => source
      case Left(errors) =>
        throw new IllegalStateException(errors.toChain.toList.map(_.message).mkString("; "))
    }
  }

  /**
   * A source that reads a table of a database, written among the sources of the deployment.
   *
   * @param connection how its database is reached (`connection`)
   * @param load how, as it is written under `load`: whole, unless it is given
   * @param textColumns the columns that it reads as text
   */
  def tableSource(
      id: SourceId,
      table: String,
      connection: String,
      load: String = "{ mode: full }",
      textColumns: Seq[String] = Nil
  ): Source = {
    val text =
      s"""id: $id
         |connector:
         |  type: jdbc
         |  connection:
         |${connection.linesIterator.map("  " + _).mkString("\n")}
         |  table: $table
         |  text_columns: [${textColumns.mkString(", ")}]
         |load: $load
         |targets: { bronze: { table: bronze/${id.value.replace('.', '/')} } }
         |""".stripMargin
    Files.write(sources.resolve(s"$id.yaml"), text.getBytes(UTF_8))
    Source.load(s"$id.yaml", text) match {
      case Right(Loaded(source, _)) => source
      case Left(errors) =>
        throw new IllegalStateException(errors.toChain.toList.map(_.message).mkString("; "))
    }
  }

  /** The directory of a source in the landing zone. */
  def directoryOf(source: SourceId): Path = landing.resolve(source.value.replace('.', '_'))

  /** A file of the source, last changed an hour ago: complete for a run that starts now. */
  def drop(of: Source, name: String, text: String): Path = {
    val path = directoryOf(of.id).resolve(name)
    Files.write(path, text.getBytes(UTF_8))
    Files.setLastModifiedTime(path, FileTime.from(Instant.now().minusSeconds(3600)))
  }

  /** The command line of one run of a source of the deployment. */
  def arguments(of: Source, profile: Path = validProfile): Seq[String] = Seq(
    "run",
    "--profile",
    profile.toString,
    "--sources",
    sources.toString,
    "--source",
    of.id.value
  )

  /** A file of the deployment that nothing else of it is named as, for what a process prints. */
  def newLog(): Path = Files.createTempFile(work, "process-", ".log")

  override def close(): Unit = LocalSparkSuite.deleteRecursively(work)
}
