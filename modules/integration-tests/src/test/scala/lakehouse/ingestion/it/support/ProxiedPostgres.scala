package lakehouse.ingestion.it.support

import java.sql.{Connection, DriverManager, SQLException}

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

import com.github.dockerjava.api.command.CreateContainerCmd
import com.github.dockerjava.api.model.Capability
import eu.rekawek.toxiproxy.{Proxy, ToxiproxyClient}
import org.testcontainers.containers.Network
import org.testcontainers.toxiproxy.ToxiproxyContainer
import org.testcontainers.utility.{DockerImageName, TestcontainersConfiguration}

/**
 * The bootstrapped PostgreSQL behind a Toxiproxy, both on a private Docker network, so that a
 * test can break the connection for real: cut it, delay it, or stop or freeze the server.
 *
 * Two proxies lead to the same server:
 *
 *  - `faulty` is the one the code under test connects through, and the one a test breaks;
 *  - the observer is never touched, so the test itself can look at the database while the other
 *    path is broken.
 *
 * Both keep their address when the server is stopped and started again, which the port that
 * Docker publishes for the server itself does not.
 */
final class ProxiedPostgres private (
    val postgres: BootstrappedPostgres,
    toxiproxy: ToxiproxyContainer,
    val faulty: Proxy,
    network: Network
) {

  import ProxiedPostgres._

  /** The URL of `database` through the proxy that tests break. */
  def faultyJdbcUrl(database: String): String = url(FaultyPort, database)

  /** A connection as `role` that the faults of `faulty` do not affect. */
  def observe(role: String, database: String): Connection =
    DriverManager.getConnection(url(ObserverPort, database), role, postgres.password(role))

  /** Shuts PostgreSQL down, as `docker stop` does; its data stays. */
  def stopPostgres(): Unit = postgres.halt()

  /** Starts PostgreSQL again and waits until it accepts connections. */
  def startPostgres(): Unit = {
    postgres.restart()
    awaitConnection(System.nanoTime() + ReadyTimeoutNanos)
  }

  def stop(): Unit =
    try toxiproxy.stop()
    finally
      try postgres.stop()
      finally network.close()

  private def url(port: Int, database: String): String =
    s"jdbc:postgresql://${toxiproxy.getHost}:${toxiproxy.getMappedPort(port)}/$database"

  @tailrec
  private def awaitConnection(deadlineNanos: Long): Unit = {
    val connected =
      try {
        observe(BootstrappedPostgres.Superuser, "postgres").close()
        true
      } catch { case _: SQLException => false }
    if (!connected) {
      if (System.nanoTime() > deadlineNanos)
        throw new IllegalStateException("PostgreSQL did not accept connections after its restart")
      Thread.sleep(100)
      awaitConnection(deadlineNanos)
    }
  }
}

object ProxiedPostgres {

  /** The key in `testcontainers.properties` that pins the Toxiproxy image (ADR-0013 §5). */
  val ImageProperty = "toxiproxy.container.image"

  // The Testcontainers module publishes ports 8666 to 8697 of the proxy container.
  private val FaultyPort = 8666
  private val ObserverPort = 8667
  private val ReadyTimeoutNanos = 60L * 1000 * 1000 * 1000

  def start(): ProxiedPostgres = {
    val network = Network.newNetwork()
    val postgres = BootstrappedPostgres.start(Some(network))
    val toxiproxy = new ToxiproxyContainer(image)
      .withNetwork(network)
      // The same hardening as the PostgreSQL service: non-root, no capabilities, no escalation.
      .withCreateContainerCmdModifier { (cmd: CreateContainerCmd) =>
        cmd.withUser("65534:65534")
        cmd.getHostConfig
          .withCapDrop(Capability.ALL)
          .withSecurityOpts(List("no-new-privileges:true").asJava)
        ()
      }
    toxiproxy.start()

    val client = new ToxiproxyClient(toxiproxy.getHost, toxiproxy.getControlPort)
    val upstream = s"${BootstrappedPostgres.NetworkAlias}:${BootstrappedPostgres.Port}"
    val faulty = client.createProxy("postgres-faulty", s"0.0.0.0:$FaultyPort", upstream)
    client.createProxy("postgres-observer", s"0.0.0.0:$ObserverPort", upstream)
    new ProxiedPostgres(postgres, toxiproxy, faulty, network)
  }

  private val PinnedImage = """([a-z0-9._/-]+):[\w.-]+@(sha256:[0-9a-f]{64})""".r

  /**
   * The image pinned in `testcontainers.properties`; anything but tag and digest is an error.
   * Testcontainers receives `repository@digest`, as for PostgreSQL: the digest alone identifies
   * the image, and Testcontainers does not parse a tag next to it.
   */
  def image: DockerImageName =
    TestcontainersConfiguration.getInstance().getEnvVarOrProperty(ImageProperty, "") match {
      case PinnedImage(repository, digest) =>
        DockerImageName.parse(s"$repository@$digest").asCompatibleSubstituteFor("shopify/toxiproxy")
      case other =>
        throw new IllegalStateException(
          s"$ImageProperty must pin the image as name:tag@sha256:<digest>, found '$other'"
        )
    }
}
