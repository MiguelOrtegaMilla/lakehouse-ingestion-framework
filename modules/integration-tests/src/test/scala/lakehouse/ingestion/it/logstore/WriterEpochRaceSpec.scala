package lakehouse.ingestion.it.logstore

import java.sql.Connection
import java.util.concurrent.Executors

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Using

import lakehouse.ingestion.it.support.{BootstrappedPostgres, CoordinationSchema, CoordinationStores}
import lakehouse.ingestion.logstore.coordination.CoordinationStoreContract.{claim, newKey, newTable}
import lakehouse.ingestion.logstore.coordination.{
  PostgresCoordinationStore,
  RegisterResult,
  Registration,
  TableUri,
  TakeResult,
  WriterEpoch
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The claim of a version under an epoch against the announcement of the next epoch, when the
 * two meet in the database (ADR-0019).
 *
 * A run that takes the lease of a source over announces its epoch and then asks the tables what
 * the run before it wrote. What it is answered must hold: a commit of the run before either is
 * recorded when the announcement returns, where the question finds it, or is never taken. That
 * is so only if a claim and an announcement of one key exclude each other, in whichever order
 * they arrive.
 *
 * Both orders are forced here, and nothing is simulated: the statements are held where they
 * meet by real transactions that are left open, and what waits is read from
 * `pg_stat_activity`. With the lock taken out of the claim, the tests of the two orders fail.
 * The contract of the store has the same race as it comes, many times over
 * (`CoordinationStoreContract`).
 */
final class WriterEpochRaceSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private lazy val pg = BootstrappedPostgres.start()

  // A statement that waits for a lock here is let wait: the tests end the wait themselves.
  private lazy val store = new PostgresCoordinationStore(
    CoordinationStores.settings(
      pg.jdbcUrl(CoordinationSchema.Database),
      pg,
      statementTimeout = 30.seconds
    )
  )

  private val background = Executors.newCachedThreadPool()
  implicit private val threads: ExecutionContext = ExecutionContext.fromExecutor(background)

  private val Patience = 20.seconds

  override def beforeAll(): Unit = CoordinationSchema.migrate(pg): Unit

  override def afterAll(): Unit =
    try background.shutdownNow(): Unit
    finally
      try store.close()
      finally pg.stop()

  test("an announcement waits for a claim under the epoch before it, and then finds its commit") {
    val (table, key) = (registered(), newKey())
    store.announce(WriterEpoch(key, 1L)) shouldBe 1L
    val zombie = claim(store, table, 0L).copy(epoch = Some(WriterEpoch(key, 1L)))

    // The claim of the run that lost its lease is held in the middle: it has read its epoch,
    // which is still in force, and its INSERT waits for another writer's claim of the version.
    val (taking, announcing) = Using.resource(inFlightClaim(table, 0L, ".tmp/in-flight")) { other =>
      val taking = Future(store.take(zombie))
      eventually(waitingForLock() == 1)

      // The run that took the lease over announces its epoch, and reads the registry the
      // moment it is answered, as it asks its table.
      val announcing = Future {
        val inForce = store.announce(WriterEpoch(key, 2L))
        (inForce, store.latestEntry(table).map(_.tempPath))
      }
      eventually(waitingForLock() == 2)
      taking.isCompleted shouldBe false
      announcing.isCompleted shouldBe false

      // The other writer gives the version up, and the claim that waited for it takes it.
      other.rollback()
      (taking, announcing)
    }

    Await.result(taking, Patience) shouldBe TakeResult.Taken
    // The announcement returned after the commit was recorded: the run that asked found it.
    Await.result(announcing, Patience) shouldBe ((2L, Some(zombie.tempPath)))
    // And nothing more of the earlier epoch is taken.
    store.take(claim(store, table, 1L).copy(epoch = Some(WriterEpoch(key, 1L)))) shouldBe
      TakeResult.EpochNotInForce(Some(2L))
    epochOfEntry(table, 0L) shouldBe Some((key, 1L))
  }

  test("a claim that meets the announcement of the next epoch in flight waits, and is refused") {
    val (table, key) = (registered(), newKey())
    store.announce(WriterEpoch(key, 1L)) shouldBe 1L
    val zombie = claim(store, table, 0L).copy(epoch = Some(WriterEpoch(key, 1L)))

    val taking = Using.resource(inFlightAnnouncement(WriterEpoch(key, 2L))) { next =>
      // The epoch that the claim names may yet be the one in force: it can neither take its
      // version nor be refused.
      val taking = Future(store.take(zombie))
      eventually(waitingForLock() == 1)
      taking.isCompleted shouldBe false
      store.entry(table, 0L) shouldBe None

      next.commit()
      taking
    }

    Await.result(taking, Patience) shouldBe TakeResult.EpochNotInForce(Some(2L))
    store.entry(table, 0L) shouldBe None
    // The run that announced takes the version.
    store.take(claim(store, table, 0L).copy(epoch = Some(WriterEpoch(key, 2L)))) shouldBe
      TakeResult.Taken
  }

  test("a claim that waited for an announcement that is called off takes its version") {
    val (table, key) = (registered(), newKey())
    store.announce(WriterEpoch(key, 1L)) shouldBe 1L
    val mine = claim(store, table, 0L).copy(epoch = Some(WriterEpoch(key, 1L)))

    val taking = Using.resource(inFlightAnnouncement(WriterEpoch(key, 2L))) { next =>
      val taking = Future(store.take(mine))
      eventually(waitingForLock() == 1)

      next.rollback()
      taking
    }

    Await.result(taking, Patience) shouldBe TakeResult.Taken
    store.announce(WriterEpoch(key, 1L)) shouldBe 1L
  }

  test("claims under one epoch do not wait for each other, nor does a claim under none") {
    val (held, free, key) = (registered(), registered(), newKey())
    store.announce(WriterEpoch(key, 1L)) shouldBe 1L
    val turn = Some(WriterEpoch(key, 1L))

    // One claim of the key is in flight, with the row of its epoch locked.
    Using.resource(inFlightClaim(held, 0L, ".tmp/in-flight")) { other =>
      val waiting = Future(store.take(claim(store, held, 0L).copy(epoch = turn)))
      eventually(waitingForLock() == 1)

      // Two sources of one table, or the quarantine of a batch and its data: they go on.
      store.take(claim(store, free, 0L).copy(epoch = turn)) shouldBe TakeResult.Taken
      store.take(claim(store, free, 1L)) shouldBe TakeResult.Taken
      waiting.isCompleted shouldBe false

      other.commit()
      Await.result(waiting, Patience) shouldBe a[TakeResult.TakenBy]
    }
  }

  test("an announcement that changes nothing has waited for the claims in flight all the same") {
    val (table, key) = (registered(), newKey())
    store.announce(WriterEpoch(key, 3L)) shouldBe 3L
    val mine = claim(store, table, 0L).copy(epoch = Some(WriterEpoch(key, 3L)))

    val (taking, announcing) = Using.resource(inFlightClaim(table, 0L, ".tmp/in-flight")) { other =>
      val taking = Future(store.take(mine))
      eventually(waitingForLock() == 1)
      // A run that is told again what it announced, or an earlier epoch that comes late.
      val announcing = Future {
        val inForce = store.announce(WriterEpoch(key, 2L))
        (inForce, store.entry(table, 0L).map(_.tempPath))
      }
      eventually(waitingForLock() == 2)

      other.rollback()
      (taking, announcing)
    }

    Await.result(taking, Patience) shouldBe TakeResult.Taken
    Await.result(announcing, Patience) shouldBe ((3L, Some(mine.tempPath)))
  }

  // ---------------------------------------------------------------------------------------------

  private def registered(): TableUri = {
    val table = newTable()
    store.register(table, Registration.NotYetCreated, None) shouldBe a[RegisterResult.Registered]
    table
  }

  /**
   * A claim on `version` in a transaction that is still open: what another writer's INSERT looks
   * like for the instant between its execution and its commit. The caller commits or rolls back.
   */
  private def inFlightClaim(table: TableUri, version: Long, tempPath: String): Connection = {
    val other = pg.connect(CoordinationSchema.CommitPathRole, CoordinationSchema.Database)
    other.setAutoCommit(false)
    Using.resource(
      other.prepareStatement(
        """INSERT INTO delta_coordination.commit_entry
          |  (table_uri, version, temp_path, writer_instance, generation)
          |SELECT table_uri, ?, ?, 'another-writer', generation
          |FROM delta_coordination.coordinated_table WHERE table_uri = ?""".stripMargin
      )
    ) { statement =>
      statement.setLong(1, version)
      statement.setString(2, tempPath)
      statement.setString(3, table.value)
      statement.executeUpdate() shouldBe 1
    }
    other
  }

  /**
   * The announcement of an epoch in a transaction that is still open, by the statement that the
   * store announces with and as the role that it runs as: what an announcement looks like for
   * the instant between its execution and its commit. The caller commits or rolls back.
   */
  private def inFlightAnnouncement(epoch: WriterEpoch): Connection = {
    val announcer = pg.connect(CoordinationSchema.CommitPathRole, CoordinationSchema.Database)
    announcer.setAutoCommit(false)
    Using.resource(
      announcer.prepareStatement(
        """INSERT INTO delta_coordination.writer_epoch AS held (epoch_key, epoch)
          |VALUES (?, ?)
          |ON CONFLICT (epoch_key) DO UPDATE
          |   SET epoch = EXCLUDED.epoch, announced_at = now()
          | WHERE held.epoch < EXCLUDED.epoch""".stripMargin
      )
    ) { statement =>
      statement.setString(1, epoch.key)
      statement.setLong(2, epoch.epoch)
      statement.executeUpdate() shouldBe 1
    }
    announcer
  }

  /** The epoch that the entry of a version says it was taken under, as the audit trail has it. */
  private def epochOfEntry(table: TableUri, version: Long): Option[(String, Long)] =
    Using.resource(pg.connectAsSuperuser(CoordinationSchema.Database)) { c =>
      Using.resource(
        c.prepareStatement(
          "SELECT epoch_key, epoch FROM delta_coordination.commit_entry " +
            "WHERE table_uri = ? AND version = ? AND epoch_key IS NOT NULL"
        )
      ) { statement =>
        statement.setString(1, table.value)
        statement.setLong(2, version)
        Using.resource(statement.executeQuery()) { rows =>
          Option.when(rows.next())((rows.getString(1), rows.getLong(2)))
        }
      }
    }

  /** How many sessions of the store are waiting for a lock, seen by the superuser. */
  private def waitingForLock(): Int =
    Using.resource(pg.connectAsSuperuser(CoordinationSchema.Database)) { observer =>
      Using.resource(
        observer.prepareStatement(
          "SELECT count(*) FROM pg_stat_activity WHERE application_name = ? AND wait_event_type = 'Lock'"
        )
      ) { statement =>
        statement.setString(1, PostgresCoordinationStore.ApplicationName)
        Using.resource(statement.executeQuery()) { rows =>
          rows.next()
          rows.getInt(1)
        }
      }
    }

  private def eventually(condition: => Boolean): Unit = {
    @tailrec
    def poll(deadline: Deadline): Unit =
      if (!condition) {
        if (deadline.isOverdue()) fail("the condition did not become true in time")
        Thread.sleep(25)
        poll(deadline)
      }
    poll(Patience.fromNow)
  }
}
