package lakehouse.ingestion.it.support

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.security.SecureRandom
import java.sql.{Connection, DriverManager}

import scala.jdk.CollectionConverters._
import scala.util.Using

import com.github.dockerjava.api.command.CreateContainerCmd
import com.github.dockerjava.api.model.Capability
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.{GenericContainer, Network}
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

  /** The host the server is reached at from the tests: the one its certificate is for. */
  def host: String = container.getHost

  /**
   * The certificate of the authority that signed the server's, in a file, when the server was
   * started as one that takes TLS alone. It is what a client is given to trust.
   */
  lazy val rootCertificate: Path =
    BootstrappedPostgres.writeTempCertificate(exec("sh", "-c", "cat \"$PGDATA/ca.crt\""))

  /** The certificate of an authority that signed nothing here, in a file. */
  def anotherAuthority(): Path = BootstrappedPostgres.writeTempCertificate(
    exec(
      "sh",
      "-c",
      "openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj '/CN=Another authority' " +
        "-keyout /tmp/other.key -out /tmp/other.crt 2>/dev/null && cat /tmp/other.crt"
    )
  )

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

  /**
   * Freezes every process of the server, as `docker pause` does: connections stay open, and
   * nothing answers.
   */
  def pause(): Unit = docker.pauseContainerCmd(container.getContainerId).exec(): Unit

  def unpause(): Unit = docker.unpauseContainerCmd(container.getContainerId).exec(): Unit

  /**
   * Kills the server, as `docker kill` does: no shutdown and no checkpoint, every connection
   * cut. The container and its data stay, and the server recovers from its write-ahead log
   * when it is started again (`restart`).
   */
  def kill(): Unit = docker.killContainerCmd(container.getContainerId).exec(): Unit

  /** Shuts the server down, as `docker stop` does. The container and its data stay. */
  def halt(): Unit = docker.stopContainerCmd(container.getContainerId).exec(): Unit

  /**
   * Starts a halted server again. It returns before the server accepts connections, and the
   * port published on the host may have changed: `jdbcUrl` is only good until the first `halt`.
   */
  def restart(): Unit = docker.startContainerCmd(container.getContainerId).exec(): Unit

  private def docker = container.getDockerClient

  def stop(): Unit = container.stop()
}

object BootstrappedPostgres {

  val Superuser = "postgres"

  /**
   * The login roles created by `docker/postgres/init` (ADR-0013 §3): those of the application,
   * and the two of the demo source, its owner and the one that the JDBC connector reads it as.
   */
  val LoginRoles: Seq[String] =
    Seq(
      "ingest_migrator",
      "ingest_control",
      "ingest_coordination",
      "ingest_maintenance",
      "sales_owner",
      "sales_ro"
    )

  /** The server's port inside its container and on a Docker network. */
  val Port = 5432

  /** The server's host name on the Docker network it is started in, if any. */
  val NetworkAlias = "postgres"
  private val InitDir = "/docker-entrypoint-initdb.d"
  private val SuperuserSecret = "pg_superuser"
  // On the classpath of the tests: what makes the server one that takes TLS alone.
  private val TlsOnlyScript = "postgres/90-tls-only.sh"
  // Executable: the entrypoint runs such a script in a shell of its own, and sources the rest.
  private val Executable = Integer.parseInt("755", 8)
  // Mode 0644: the container runs as uid 999, the files are copied in as root.
  private val ReadableByAll = Integer.parseInt("644", 8)

  final class Container(image: DockerImageName) extends GenericContainer[Container](image)

  /**
   * @param network a Docker network to join, where other containers reach it as `NetworkAlias`
   * @param tlsOnly a server with a certificate of its own, that lets nobody in over TCP without
   *   TLS (ADR-0013 §5). Its certificate is for the host the tests reach it at and for
   *   `NetworkAlias`, signed by an authority that is made with it (`rootCertificate`)
   */
  def start(network: Option[Network] = None, tlsOnly: Boolean = false): BootstrappedPostgres = {
    val root = repoRoot()
    val passwords = (LoginRoles :+ Superuser).map(_ -> randomPassword()).toMap

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

    if (tlsOnly) {
      val host = DockerClientFactory.instance().dockerHostIpAddress()
      // An address is named in a certificate as one, and not as a name.
      val kind = if (host.matches("[0-9.]+") || host.contains(":")) "IP" else "DNS"
      container
        .withEnv("TLS_SUBJECT_ALT_NAMES", s"$kind:$host,DNS:$NetworkAlias")
        .withCopyFileToContainer(
          MountableFile.forClasspathResource(TlsOnlyScript, Executable),
          s"$InitDir/90-tls-only.sh"
        )
    }

    network.foreach(n => container.withNetwork(n).withNetworkAliases(NetworkAlias))
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

  private[support] def repoRoot(): Path =
    sys.props.get("repo.root").map(Paths.get(_)).getOrElse {
      throw new IllegalStateException("system property repo.root is not set (see build.sbt)")
    }

  private def writeTempCertificate(pem: String): Path = {
    require(pem.startsWith("-----BEGIN CERTIFICATE-----"), "the container gave no certificate")
    val file = Files.createTempFile("pg-authority-", ".crt")
    file.toFile.deleteOnExit()
    Files.write(file, (pem + "\n").getBytes(StandardCharsets.UTF_8))
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
