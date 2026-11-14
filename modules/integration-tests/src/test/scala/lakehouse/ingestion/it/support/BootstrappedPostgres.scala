package lakehouse.ingestion.it.support

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.security.SecureRandom
import java.sql.{Connection, DriverManager}

import scala.jdk.CollectionConverters._
import scala.util.Using

import com.github.dockerjava.api.command.CreateContainerCmd
import com.github.dockerjava.api.model.Capability
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.{DockerImageName, MountableFile}

/**
 * The local PostgreSQL service of `docker/docker-compose.yml`, started by Testcontainers.
 *
 * Same image (read from the compose file), same init scripts, same non-root user and the same
 * container hardening. Only the passwords differ: each instance generates its own. A test that
 * passes here therefore tests the bootstrap the developer runs locally, not a copy of it.
 */
final class BootstrappedPostgres private (
    container: BootstrappedPostgres.Container,
    passwords: Map[String, String]
) {

  def jdbcUrl(database: String): String =
    s"jdbc:postgresql://${container.getHost}:${container.getMappedPort(BootstrappedPostgres.Port)}/$database"

  /** Opens a connection as `role`, authenticating over TCP with the role's own secret. */
  def connect(role: String, database: String): Connection =
    connect(role, database, password = passwords(role))

  def connect(role: String, database: String, password: String): Connection =
    DriverManager.getConnection(jdbcUrl(database), role, password)

  def connectAsSuperuser(database: String): Connection =
    connect(BootstrappedPostgres.Superuser, database)

  /** The secret of `role` in this instance, for code that opens its own connections. */
  def password(role: String): String = passwords(role)

  /** Runs a command inside the container as the container user; returns trimmed stdout. */
  def exec(command: String*): String = container.execInContainer(command: _*).getStdout.trim

  def stop(): Unit = container.stop()
}

object BootstrappedPostgres {

  val Superuser = "postgres"

  /** The login roles created by `docker/postgres/init` (ADR-0013 §3). */
  val RuntimeRoles: Seq[String] =
    Seq(
      "ingest_migrator",
      "ingest_control",
      "ingest_coordination",
      "ingest_maintenance",
      "sales_ro"
    )

  private val Port = 5432
  private val InitDir = "/docker-entrypoint-initdb.d"
  private val SuperuserSecret = "pg_superuser"
  // Mode 0644: the container runs as uid 999, the files are copied in as root.
  private val ReadableByAll = Integer.parseInt("644", 8)

  final class Container(image: DockerImageName) extends GenericContainer[Container](image)

  def start(): BootstrappedPostgres = {
    val root = repoRoot()
    val passwords = (RuntimeRoles :+ Superuser).map(_ -> randomPassword()).toMap

    val container = new Container(imageFromCompose(composeFile))
      .withExposedPorts(Int.box(Port))
      .withEnv("POSTGRES_PASSWORD_FILE", secretPath(SuperuserSecret))
      // Mirrors the compose service: non-root, no capabilities, no privilege escalation.
      .withCreateContainerCmdModifier { (cmd: CreateContainerCmd) =>
        cmd.withUser("postgres")
        cmd.getHostConfig
          .withCapDrop(Capability.ALL)
          .withSecurityOpts(List("no-new-privileges:true").asJava)
        ()
      }
      // The temporary server used during init logs the same line once; wait for the real one.
      .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\s", 2))

    initScripts(root.resolve("docker/postgres/init")).foreach { script =>
      container.withCopyFileToContainer(
        MountableFile.forHostPath(script, ReadableByAll),
        s"$InitDir/${script.getFileName}"
      )
    }
    passwords.foreach { case (role, password) =>
      val secretName = if (role == Superuser) SuperuserSecret else role
      container.withCopyFileToContainer(
        MountableFile.forHostPath(writeTempSecret(password), ReadableByAll),
        secretPath(secretName)
      )
    }

    container.start()
    new BootstrappedPostgres(container, passwords)
  }

  /**
   * The `image:` of the postgres service, which must be pinned by tag and digest (ADR-0013).
   * Testcontainers receives `repository@digest`: the digest alone identifies the image.
   */
  def imageFromCompose(composeFile: Path): DockerImageName = {
    val pinned = """^\s*image:\s*(postgres):([\w.-]+)@(sha256:[0-9a-f]{64})\s*$""".r
    val lines = Files.readAllLines(composeFile, StandardCharsets.UTF_8).asScala
    lines.collectFirst { case pinned(repository, _, digest) => s"$repository@$digest" } match {
      case Some(reference) => DockerImageName.parse(reference)
      case None =>
        throw new IllegalStateException(
          s"$composeFile: the postgres image must be pinned as postgres:<tag>@sha256:<digest>"
        )
    }
  }

  private def secretPath(name: String): String = s"/run/secrets/${name}_password"

  private def initScripts(dir: Path): Seq[Path] =
    Using
      .resource(Files.list(dir))(_.iterator.asScala.filter(Files.isRegularFile(_)).toList)
      .sortBy(_.getFileName.toString)

  /** The compose file that defines the local service; the single source of the image. */
  def composeFile: Path = repoRoot().resolve("docker/docker-compose.yml")

  private def repoRoot(): Path =
    sys.props.get("repo.root").map(Paths.get(_)).getOrElse {
      throw new IllegalStateException("system property repo.root is not set (see build.sbt)")
    }

  private val random = new SecureRandom()

  private def randomPassword(): String = {
    val bytes = new Array[Byte](16)
    random.nextBytes(bytes)
    bytes.map(b => f"${b & 0xff}%02x").mkString
  }

  private def writeTempSecret(password: String): Path = {
    val file = Files.createTempFile("pg-secret-", ".txt")
    file.toFile.deleteOnExit()
    Files.write(file, password.getBytes(StandardCharsets.UTF_8))
  }
}
