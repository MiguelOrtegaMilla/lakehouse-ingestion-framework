package lakehouse.ingestion.it.ingestion

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Using

import lakehouse.ingestion.core.config.{PostgresUrl, SecretResolver, Timeouts}
import lakehouse.ingestion.engine.control.ControlDatabase
import lakehouse.ingestion.it.support.ControlPlaneSchema.{environment, Database}
import lakehouse.ingestion.it.support.{BootstrappedPostgres, ProxiedPostgres}
import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers

/**
 * An ingestion suite whose PostgreSQL is behind a proxy that a test cuts for real
 * (`ProxiedPostgres`). The suite reaches the database directly, and so sees what is in it
 * whatever the proxy does; a test reaches it through the proxy where it says so.
 */
trait ProxiedIngestionSuite extends IngestionSuite {
  this: Suite with Matchers =>

  protected lazy val proxied: ProxiedPostgres = ProxiedPostgres.start()
  override protected lazy val pg: BootstrappedPostgres = proxied.postgres
  override protected def stopServers(): Unit = proxied.stop()

  private val threads = Executors.newCachedThreadPool()
  implicit protected val onThreads: ExecutionContext = ExecutionContext.fromExecutor(threads)

  override def afterAll(): Unit =
    try threads.shutdownNow(): Unit
    finally super.afterAll()

  // Each cut of the proxy, so that what is to make it whole again is for that cut alone: a
  // thread of an earlier test that wakes up late leaves a later cut as it is.
  private val cuts = new AtomicInteger

  /** The proxy is cut now, until it is made whole. */
  protected def cut(): Unit = {
    cuts.incrementAndGet()
    proxied.faulty.disable()
  }

  /** The proxy is cut now, and whole again `after` that, in a thread of its own. */
  protected def cutFor(after: FiniteDuration): Unit = {
    cut()
    val thisCut = cuts.get
    Future {
      Thread.sleep(after.toMillis)
      if (cuts.get == thisCut) proxied.faulty.enable()
    }: Unit
  }

  /** The proxy whole again, whatever cut it. */
  protected def whole(): Unit = {
    cuts.incrementAndGet()
    proxied.faulty.enable()
  }

  /**
   * An instance that reaches the control plane through the proxy, with `timeouts`: by default it
   * gives up on a call to it within a second. The proxy is whole again afterwards.
   */
  protected def throughTheProxy[T](
      timeouts: Timeouts = Timeouts(connect = 1.second, statement = 1.second)
  )(body: ControlDatabase => T): T = {
    val control = ControlDatabase
      .open(
        profile.databases.control.copy(
          url = PostgresUrl.parse(proxied.faultyJdbcUrl(Database)).toOption.get,
          timeouts = timeouts
        ),
        SecretResolver(environment(pg).get)
      )
      .toOption
      .get
    try Using.resource(control)(body)
    finally whole()
  }
}
