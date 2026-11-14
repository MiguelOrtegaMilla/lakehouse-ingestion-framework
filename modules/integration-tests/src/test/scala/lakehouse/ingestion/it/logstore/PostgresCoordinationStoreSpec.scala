package lakehouse.ingestion.it.logstore

import java.io.IOException
import java.util.concurrent.{Executors, TimeUnit}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Using

import lakehouse.ingestion.it.support.{BootstrappedPostgres, CoordinationSchema, CoordinationStores}
import lakehouse.ingestion.logstore.coordination.{
  CoordinationSettings,
  CoordinationStore,
  CoordinationStoreContract,
  PostgresCoordinationStore,
  SecretRef
}
import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfterAll

/**
 * `PostgresCoordinationStore` keeps the contract of every `CoordinationStore`, against the
 * migrated schema of a real PostgreSQL and as the role the commit path runs as (ADR-0004 §3.1,
 * ADR-0013 §3). Then what is its own: one lazy pool, bounded in size, with the password by
 * reference (ADR-0004 §3.9).
 */
final class PostgresCoordinationStoreSpec extends CoordinationStoreContract with BeforeAndAfterAll {

  private lazy val pg = BootstrappedPostgres.start()
  private lazy val settings =
    CoordinationStores.settings(pg.jdbcUrl(CoordinationSchema.Database), pg)
  private lazy val postgresStore = new PostgresCoordinationStore(settings)

  override protected def store: CoordinationStore = postgresStore

  override def beforeAll(): Unit = CoordinationSchema.migrate(pg): Unit

  override def afterAll(): Unit =
    try postgresStore.close()
    finally pg.stop()

  test("a store opens no connection until it is used, and never more than its pool allows") {
    val before = sessions()
    Using.resource(new PostgresCoordinationStore(settings.copy(poolMaxSize = 2))) { limited =>
      sessions() shouldBe before

      val table = CoordinationStoreContract.newTable()
      limited.registration(table) shouldBe None
      sessions() shouldBe before + 1

      // Eight threads at once share the two connections; none of them fails.
      val pool = Executors.newFixedThreadPool(8)
      implicit val threads: ExecutionContext = ExecutionContext.fromExecutor(pool)
      try {
        val calls = (1 to 40).map(_ => Future(limited.registration(table)))
        Await.result(Future.sequence(calls), 30.seconds).distinct shouldBe Seq(None)
      } finally {
        pool.shutdown()
        pool.awaitTermination(10, TimeUnit.SECONDS): Unit
      }
      sessions() should (be > before and be <= before + 2)
    }
    sessions() shouldBe before
  }

  test("the password is read through its reference when the pool opens, and tried again") {
    var environment = Map.empty[String, String]
    val byVariable = settings.copy(password = SecretRef.Env("COORDINATION_PASSWORD"))
    Using.resource(new PostgresCoordinationStore(byVariable, name => environment.get(name))) {
      viaEnvironment =>
        val table = CoordinationStoreContract.newTable()

        // Fail closed, naming the reference: this JVM cannot resolve it yet.
        val failure = the[IOException] thrownBy viaEnvironment.registration(table)
        failure.getMessage should include("env:COORDINATION_PASSWORD")

        // Once it can, the same store works: a failed opening is not remembered.
        environment = Map("COORDINATION_PASSWORD" -> pg.password(CoordinationSchema.CommitPathRole))
        viaEnvironment.registration(table) shouldBe None
    }
  }

  test("every LogStore of a JVM with the same settings shares one store, and so one pool") {
    val shared = PostgresCoordinationStore.shared(settings)

    PostgresCoordinationStore.shared(settings.copy()) should be theSameInstanceAs shared
    PostgresCoordinationStore.shared(
      settings.copy(poolMaxSize = 3)
    ) should not be theSameInstanceAs(shared)
    // Asking for the shared store opens nothing; these were never used.
  }

  test("the settings are read from the Hadoop configuration Delta hands to a LogStore") {
    val conf = new Configuration(false)
    conf.set(CoordinationSettings.JdbcUrl, settings.jdbcUrl)
    conf.set(CoordinationSettings.User, settings.user)
    conf.set(CoordinationSettings.PasswordRef, settings.password.toString)

    CoordinationSettings.from(conf) shouldBe Right(settings)
    CoordinationSettings.from(new Configuration(false)).left.map(_.size) shouldBe Left(3)
  }

  /** The sessions that stores of this JVM hold in the database, seen by the superuser. */
  private def sessions(): Int =
    Using.resource(pg.connectAsSuperuser(CoordinationSchema.Database)) { c =>
      Using.resource(
        c.prepareStatement("SELECT count(*) FROM pg_stat_activity WHERE application_name = ?")
      ) { statement =>
        statement.setString(1, PostgresCoordinationStore.ApplicationName)
        Using.resource(statement.executeQuery()) { rows =>
          rows.next()
          rows.getInt(1)
        }
      }
    }
}
