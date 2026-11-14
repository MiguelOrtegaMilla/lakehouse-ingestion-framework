package lakehouse.ingestion.it.logstore

import java.io.IOException
import java.sql.{Connection, SQLException}
import java.util.concurrent.Executors

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Try, Using}

import eu.rekawek.toxiproxy.model.ToxicDirection
import lakehouse.ingestion.it.support.{CoordinationSchema, CoordinationStores, ProxiedPostgres}
import lakehouse.ingestion.logstore.coordination.CoordinationStoreContract.{claim, newTable}
import lakehouse.ingestion.logstore.coordination.{
  PostgresCoordinationStore,
  RegisterResult,
  Registration,
  TableUri,
  TakeResult
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * `PostgresCoordinationStore` when the database or the network fails for real (ADR-0004 §3.5,
 * fail closed): a reply that never arrives, a lock that is not released, a server that freezes,
 * a server that is stopped. Nothing is simulated in the store: the faults are made with a TCP
 * proxy between the store and PostgreSQL, and with `docker pause` and `docker stop`.
 *
 * Every case ends the same way: an `IOException` within a bounded time, never a hang and never
 * a wrong answer, and a store that works again once the fault is gone.
 *
 * This is the store's side of scenarios F5, F6, F7 and F15 (ADR-0004 §7.1). The scenarios
 * themselves are about the protocol, and are tested with it.
 */
final class PostgresCoordinationFaultSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private lazy val pg = ProxiedPostgres.start()

  private val background = Executors.newCachedThreadPool()
  implicit private val threads: ExecutionContext = ExecutionContext.fromExecutor(background)

  override def beforeAll(): Unit = CoordinationSchema.migrate(pg.postgres): Unit

  override def afterAll(): Unit =
    try background.shutdownNow(): Unit
    finally pg.stop()

  /** A store that reaches PostgreSQL through the proxy the tests break. */
  private def storeWith(statementTimeout: FiniteDuration, connectTimeout: FiniteDuration) =
    new PostgresCoordinationStore(
      CoordinationStores.settings(
        pg.faultyJdbcUrl(CoordinationSchema.Database),
        pg.postgres,
        connectTimeout = connectTimeout,
        statementTimeout = statementTimeout
      )
    )

  private def register(store: PostgresCoordinationStore): TableUri = {
    val table = newTable()
    store.register(table, Registration.NotYetCreated, None) shouldBe RegisterResult.Registered
    table
  }

  /** The temporary path of the entry, read on a path the faults do not touch. */
  private def entryOnServer(table: TableUri, version: Long): Option[String] =
    Using.resource(pg.observe(CoordinationSchema.CommitPathRole, CoordinationSchema.Database)) {
      observer =>
        Using.resource(
          observer.prepareStatement(
            "SELECT temp_path FROM delta_coordination.commit_entry WHERE table_uri = ? AND version = ?"
          )
        ) { statement =>
          statement.setString(1, table.value)
          statement.setLong(2, version)
          Using.resource(statement.executeQuery())(rows =>
            Option.when(rows.next())(rows.getString(1))
          )
        }
    }

  /**
   * The outcome of `call` and how long it took. The call runs under a deadline of the test's
   * own: a store that hangs fails the test, which then still removes the fault it made.
   */
  private def timed[T](call: => T): (Try[T], FiniteDuration) = {
    val start = System.nanoTime()
    val outcome = Await.result(Future(Try(call)), HangsAfter)
    (outcome, (System.nanoTime() - start).nanos)
  }

  @tailrec
  private def eventually(deadline: Deadline)(condition: => Boolean): Unit =
    if (!condition) {
      if (deadline.isOverdue()) fail("the condition did not become true in time")
      Thread.sleep(25)
      eventually(deadline)(condition)
    }

  test("a claim whose reply is lost has taken the version, and repeating it says so") {
    // Timeouts well above the delay below: here the connection is cut, it does not time out.
    Using.resource(storeWith(statementTimeout = 20.seconds, connectTimeout = 20.seconds)) { store =>
      val table = register(store)
      val mine = claim(table, 0L)

      // Replies are held back for three seconds: the INSERT reaches the server and commits long
      // before its answer could reach the store.
      val delay = pg.faulty.toxics().latency("slow-replies", ToxicDirection.DOWNSTREAM, 3000)
      val attempt =
        try {
          val call = Future(store.take(mine))
          eventually(15.seconds.fromNow)(entryOnServer(table, 0L).contains(mine.tempPath))
          // The entry is committed. Cut the connection before the reply gets through.
          pg.faulty.disable()
          Try(Await.result(call, 30.seconds))
        } finally {
          delay.remove()
          pg.faulty.enable()
        }

      // The writer only learns that the call failed: it cannot know whether it took the version.
      attempt shouldBe a[Failure[_]]
      attempt.failed.get shouldBe an[IOException]

      // Repeating the same claim settles it: the version is its own. Anybody else is told so.
      store.take(mine) shouldBe TakeResult.Taken
      store.take(claim(table, 0L)) shouldBe a[TakeResult.TakenBy]
      store.entry(table, 0L).map(_.tempPath) shouldBe Some(mine.tempPath)
    }
  }

  test("a claim waits for an in-flight claim of the same version, and learns who took it") {
    Using.resource(storeWith(statementTimeout = 20.seconds, connectTimeout = 5.seconds)) { store =>
      val table = register(store)
      Using.resource(inFlightClaim(table, 0L, ".tmp/in-flight")) { other =>
        val call = Future(store.take(claim(table, 0L)))

        // The store's INSERT can neither succeed nor give up: the version may yet be free.
        eventually(15.seconds.fromNow)(waitingForLock() == 1)
        call.isCompleted shouldBe false

        other.commit()
        val result = Await.result(call, 15.seconds)
        result shouldBe a[TakeResult.TakenBy]
        result.asInstanceOf[TakeResult.TakenBy].owner.tempPath shouldBe ".tmp/in-flight"
      }
    }
  }

  test(
    "a claim that waits longer than the statement timeout for a lock fails, and can be repeated"
  ) {
    val statementTimeout = 1.second
    Using.resource(storeWith(statementTimeout, connectTimeout = 5.seconds)) { store =>
      val table = register(store)
      val mine = claim(table, 0L)
      Using.resource(inFlightClaim(table, 0L, ".tmp/never-committed")) { other =>
        val (attempt, elapsed) = timed(store.take(mine))

        // The server itself gives the wait up; the store does not hang on somebody else's
        // unfinished transaction.
        attempt.failed.get shouldBe an[IOException]
        sqlStateOf(attempt.failed.get) shouldBe Some(QueryCanceled)
        elapsed should be >= statementTimeout
        elapsed should be < statementTimeout + 5.seconds

        other.rollback()
      }
      // The other claim never committed, so the version is free.
      store.take(mine) shouldBe TakeResult.Taken
    }
  }

  test(
    "while PostgreSQL does not answer, a call fails within its timeouts instead of hanging"
  ) {
    Using.resource(storeWith(statementTimeout = 1.second, connectTimeout = 2.seconds)) { store =>
      val table = register(store)

      pg.postgres.pause()
      val (attempt, elapsed) =
        try timed(store.latestEntry(table))
        finally pg.postgres.unpause()

      attempt.failed.get shouldBe an[IOException]
      // The socket gives up two seconds after the statement timeout; opening a new connection
      // instead would give up after the connect timeout.
      elapsed should be < 15.seconds

      // Fail closed is not fail for ever: with the server back, the same store answers.
      store.latestEntry(table) shouldBe None
    }
  }

  test("while PostgreSQL is stopped every call fails; when it is back the same store works") {
    Using.resource(storeWith(statementTimeout = 2.seconds, connectTimeout = 2.seconds)) { store =>
      val table = register(store)
      store.take(claim(table, 0L)) shouldBe TakeResult.Taken

      pg.stopPostgres()
      try {
        val (reading, readTime) = timed(store.latestEntry(table))
        val (writing, writeTime) = timed(store.take(claim(table, 1L)))

        // Never "no entry" for a read that could not be made, and nothing written unseen.
        reading.failed.get shouldBe an[IOException]
        writing.failed.get shouldBe an[IOException]
        readTime should be < 15.seconds
        writeTime should be < 15.seconds
      } finally pg.startPostgres()

      // The same store, with no restart of its own, and the entries are still there.
      store.latestEntry(table).map(_.version) shouldBe Some(0L)
      store.entry(table, 1L) shouldBe None
      store.take(claim(table, 1L)) shouldBe TakeResult.Taken
    }
  }

  /** Far longer than any timeout the stores of this suite are given. */
  private val HangsAfter = 45.seconds

  /** PostgreSQL cancelled the statement, here on its `statement_timeout`. */
  private val QueryCanceled = "57014"

  @tailrec
  private def sqlStateOf(error: Throwable): Option[String] = error match {
    case null => None
    case sql: SQLException => Option(sql.getSQLState)
    case other => sqlStateOf(other.getCause)
  }

  /**
   * A claim on `version` in a transaction that is still open: what another writer's INSERT looks
   * like for the instant between its execution and its commit. The caller commits or rolls back.
   */
  private def inFlightClaim(table: TableUri, version: Long, tempPath: String): Connection = {
    val other = pg.observe(CoordinationSchema.CommitPathRole, CoordinationSchema.Database)
    other.setAutoCommit(false)
    Using.resource(
      other.prepareStatement(
        """INSERT INTO delta_coordination.commit_entry (table_uri, version, temp_path, writer_instance)
          |VALUES (?, ?, ?, 'another-writer')""".stripMargin
      )
    ) { statement =>
      statement.setString(1, table.value)
      statement.setLong(2, version)
      statement.setString(3, tempPath)
      statement.executeUpdate() shouldBe 1
    }
    other
  }

  /** How many sessions of the store are waiting for a lock, seen by the superuser. */
  private def waitingForLock(): Int =
    Using.resource(pg.observe("postgres", CoordinationSchema.Database)) { observer =>
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
}
