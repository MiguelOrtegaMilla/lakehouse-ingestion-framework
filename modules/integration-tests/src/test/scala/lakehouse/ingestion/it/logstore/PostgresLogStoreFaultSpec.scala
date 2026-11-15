package lakehouse.ingestion.it.logstore

import java.io.{FileNotFoundException, IOException}
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{FileAlreadyExistsException, Files, Path => LocalPath}
import java.util.concurrent.Executors

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}

import eu.rekawek.toxiproxy.model.ToxicDirection
import lakehouse.ingestion.it.support.{CoordinationSchema, CoordinationStores, ProxiedPostgres}
import lakehouse.ingestion.logstore._
import lakehouse.ingestion.logstore.coordination.{
  CoordinationSettings,
  PostgresCoordinationStore,
  TableUri
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The write and read paths of `PostgresLogStore` against a real PostgreSQL, with the faults of
 * ADR-0004 §7.1 made for real where they can be: the connection is cut by a TCP proxy between the
 * LogStore and the database, a reply is held back until the connection is cut under it, and
 * the log directory is made to refuse a new name. Only a writer that dies is still a fault
 * hook that throws, since the test runs in the writer's JVM.
 *
 * What is on the server is read over a second route that the faults do not touch.
 * `PostgresLogStoreWriteSpec` and `PostgresLogStoreReadSpec` have every case of both paths,
 * with a store that fails on request; these are the scenarios themselves, on the database the
 * LogStore runs on.
 */
final class PostgresLogStoreFaultSpec
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with ScratchDirectories {

  import PostgresLogStoreFaultSpec._

  private lazy val pg = ProxiedPostgres.start()
  private val conf = new Configuration()
  private val fs = FileSystem.getLocal(conf)

  private val background = Executors.newCachedThreadPool()
  implicit private val threads: ExecutionContext = ExecutionContext.fromExecutor(background)

  override def beforeAll(): Unit = CoordinationSchema.migrate(pg.postgres): Unit

  override def afterAll(): Unit =
    try background.shutdownNow(): Unit
    finally pg.stop()

  /** Reaches PostgreSQL through the proxy the tests break, and gives up on it within seconds. */
  private def settings: CoordinationSettings =
    CoordinationStores.settings(
      pg.faultyJdbcUrl(CoordinationSchema.Database),
      pg.postgres,
      connectTimeout = 2.seconds,
      statementTimeout = 20.seconds
    )

  /** A table directory with an empty `_delta_log`, and a store of its own for its writers. */
  final private class Lake extends AutoCloseable {
    val store = new PostgresCoordinationStore(settings)
    val directory: LocalPath = Files.createDirectories(newDirectory().resolve("lake/orders"))
    val log: LocalPath = Files.createDirectory(directory.resolve("_delta_log"))
    val table: TableUri = TableIdentity.of(hadoop(directory), fs)

    def writer(hooks: Option[FaultHooks] = None, retry: RetryPolicy = Patient): PostgresLogStore =
      LogStores.over(store, hooks, retry)

    def commitFile(version: Long): LocalPath = log.resolve(f"$version%020d.json")

    def commit(version: Long, by: PostgresLogStore = writer())(actions: Seq[String]): Unit =
      by.write(hadoop(commitFile(version)), actions.iterator.asJava, false, conf)

    /** The names a reader is given when it lists the log from version `from` on. */
    def list(from: Long = 0, by: PostgresLogStore = writer()): Seq[String] =
      by.listFrom(hadoop(commitFile(from)), conf).asScala.map(_.getPath.getName).toSeq

    def committed(version: Long): Seq[String] =
      Files.readAllLines(commitFile(version)).asScala.toSeq

    /** A writer that reaches `point` while committing `version`, and dies there. */
    def crashAt(point: FaultPoint, version: Long): Unit =
      a[SimulatedCrash] should be thrownBy
        commit(version, writer(LogStores.crashAt(point)))(append(version)): Unit

    /** Hadoop's checksum sidecars anywhere under the log. */
    def sidecars: Seq[LocalPath] =
      Using.resource(Files.walk(log)) {
        _.iterator.asScala.filter(_.getFileName.toString.endsWith(".crc")).toList
      }

    def withCommits(last: Long): Lake = {
      commit(0)(Creation)
      (1L to last).foreach(version => commit(version)(append(version)))
      this
    }

    /** The entry of `version` as the server has it, read past the faults. */
    def onServer(version: Long): Option[EntryRow] = entryOnServer(table, version)

    override def close(): Unit = store.close()
  }

  private def withLake(test: Lake => Any): Unit = Using.resource(new Lake)(test(_): Unit)

  test("a LogStore configured the way Delta configures it commits through PostgreSQL") {
    withLake { lake =>
      // The constructor Delta calls, and the settings a deployment puts in the configuration.
      val configured = new Configuration(conf)
      configured.set(CoordinationSettings.JdbcUrl, settings.jdbcUrl)
      configured.set(CoordinationSettings.User, settings.user)
      configured.set(CoordinationSettings.PasswordRef, settings.password.toString)
      configured.set("spark.app.id", "local-1763200000000")
      val logStore = new PostgresLogStore(configured)

      lake.commit(0, logStore)(Creation)
      lake.commit(1, logStore)(append(1))

      registrationOnServer(lake.table) shouldBe Some((-1L, Some(TableId)))
      lake.committed(1) shouldBe append(1)
      Seq(0L, 1L).foreach { version =>
        val entry = lake.onServer(version).value
        entry.complete shouldBe true
        entry.writerInstance should endWith(":local-1763200000000")
        Files.isSameFile(lake.log.resolve(entry.tempPath), lake.commitFile(version)) shouldBe true
      }
    }
  }

  test("F1: a writer that dies before taking its version leaves nothing in the database") {
    withLake { lake =>
      lake.withCommits(last = 0)

      a[SimulatedCrash] should be thrownBy
        lake.commit(1, lake.writer(LogStores.crashAt(FaultPoint.BeforeInsert)))(append(1))

      lake.onServer(1) shouldBe None
      Files.exists(lake.commitFile(1)) shouldBe false

      lake.commit(1)(Seq("the next writer's commit"))
      lake.committed(1) shouldBe Seq("the next writer's commit")
      lake.onServer(1).value.complete shouldBe true
    }
  }

  test("F5: with PostgreSQL out of reach a commit fails in bounded time and writes nothing") {
    withLake { lake =>
      lake.withCommits(last = 0)
      val writer = lake.writer()

      pg.faulty.disable()
      val (attempt, elapsed) =
        try timed(lake.commit(1, writer)(append(1)))
        finally pg.faulty.enable()

      attempt.failed.get shouldBe an[IOException]
      attempt.failed.get should not be a[FileAlreadyExistsException]
      elapsed should be < 20.seconds
      Files.exists(lake.commitFile(1)) shouldBe false
      lake.onServer(1) shouldBe None

      // The same LogStore, with the database back.
      lake.commit(1, writer)(append(1))
      lake.onServer(1).value.complete shouldBe true
    }
  }

  test("F6: a connection cut after the INSERT does not fail the commit") {
    withLake { lake =>
      lake.withCommits(last = 0)
      val cutBeforeTheAcknowledgement = LogStores.at(FaultPoint.AfterPublish)(pg.faulty.disable())

      try lake.commit(1, lake.writer(cutBeforeTheAcknowledgement))(append(1))
      finally pg.faulty.enable()

      // It returned normally: the commit is visible. Its entry could not be marked complete.
      lake.committed(1) shouldBe append(1)
      lake.onServer(1).value.complete shouldBe false

      // The next writer acknowledges it before writing its own.
      lake.commit(2)(append(2))
      lake.onServer(1).value.complete shouldBe true
    }
  }

  test("F9: a table deleted and created again at its path is refused by its old entries") {
    withLake { lake =>
      lake.withCommits(last = 1)
      val before = Seq(0L, 1L).map(lake.onServer)
      Using.resource(Files.walk(lake.log)) {
        _.iterator.asScala.filter(Files.isRegularFile(_)).toList.foreach(Files.delete)
      }

      a[StaleRegistrationException] should be thrownBy lake.commit(0)(Creation)

      Files.exists(lake.commitFile(0)) shouldBe false
      Seq(0L, 1L).map(lake.onServer) shouldBe before
    }
  }

  test("F12: a commit to a table that is not registered is refused, and the database has no row") {
    withLake { lake =>
      Files.write(lake.commitFile(0), Creation.asJava)

      a[TableNotRegisteredException] should be thrownBy lake.commit(1)(append(1))

      Files.exists(lake.commitFile(1)) shouldBe false
      registrationOnServer(lake.table) shouldBe None
      lake.onServer(1) shouldBe None
    }
  }

  test("F15: the reply to the INSERT is lost with the connection; the repeated claim settles it") {
    withLake { lake =>
      lake.withCommits(last = 0)
      // From the INSERT on, replies are held back for three seconds: the row is committed on
      // the server long before the writer could hear of it.
      val slowReplies = LogStores.at(FaultPoint.BeforeInsert) {
        pg.faulty.toxics().latency(SlowReplies, ToxicDirection.DOWNSTREAM, 3000): Unit
      }

      val writing = Future(Try(lake.commit(1, lake.writer(slowReplies))(append(1))))
      try {
        eventually(15.seconds.fromNow)(lake.onServer(1).isDefined)
        // The entry is there. The connection is cut under the reply, and then works again.
        pg.faulty.disable()
      } finally {
        Try(pg.faulty.toxics().get(SlowReplies).remove())
        pg.faulty.enable()
      }

      // The writer repeated its claim on a new connection and learnt the version was its own.
      Await.result(writing, 60.seconds).get
      lake.committed(1) shouldBe append(1)
      val entry = lake.onServer(1).value
      entry.complete shouldBe true
      Files.isSameFile(lake.log.resolve(entry.tempPath), lake.commitFile(1)) shouldBe true
    }
  }

  test("F15: if PostgreSQL stays out of reach, the commit fails and a later writer publishes it") {
    withLake { lake =>
      lake.withCommits(last = 0)
      val slowReplies = LogStores.at(FaultPoint.BeforeInsert) {
        pg.faulty.toxics().latency(SlowReplies, ToxicDirection.DOWNSTREAM, 3000): Unit
      }

      val writing = Future(Try(lake.commit(1, lake.writer(slowReplies, Impatient))(append(1))))
      val attempt =
        try {
          eventually(15.seconds.fromNow)(lake.onServer(1).isDefined)
          pg.faulty.disable()
          Await.result(writing, 60.seconds)
        } finally {
          Try(pg.faulty.toxics().get(SlowReplies).remove())
          pg.faulty.enable()
        }

      // The writer never learnt it, but version 1 is its own and its bytes are kept.
      attempt.failed.get shouldBe an[IOException]
      attempt.failed.get should not be a[FileAlreadyExistsException]
      Files.exists(lake.commitFile(1)) shouldBe false
      val landed = lake.onServer(1).value
      landed.complete shouldBe false
      Files.readAllLines(lake.log.resolve(landed.tempPath)).asScala shouldBe append(1)

      lake.commit(2)(append(2))
      lake.committed(1) shouldBe append(1)
      lake.onServer(1).value.complete shouldBe true
    }
  }

  test("F16: a commit that cannot be published is reported as pending, and is in the database") {
    withLake { lake =>
      lake.withCommits(last = 0)
      Files.setPosixFilePermissions(lake.log, PosixFilePermissions.fromString("r-xr-xr-x"))
      val pending =
        try {
          assume(!Files.isWritable(lake.log), "this user may write to any directory")
          the[CommitPublicationPendingException] thrownBy
            lake.commit(1, lake.writer(retry = Impatient))(append(1))
        } finally
          Files.setPosixFilePermissions(
            lake.log,
            PosixFilePermissions.fromString("rwxr-xr-x")
          ): Unit

      pending.version shouldBe 1L
      Files.exists(lake.commitFile(1)) shouldBe false
      lake.onServer(1).value.complete shouldBe false

      lake.commit(2)(append(2))
      lake.committed(1) shouldBe append(1)
      lake.onServer(1).value.complete shouldBe true
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The read path and recovery
  // ---------------------------------------------------------------------------------------------

  test("F2: a commit taken and not published is published by the first reader of the log") {
    withLake { lake =>
      lake.withCommits(last = 0)
      lake.crashAt(FaultPoint.AfterInsert, version = 1)
      lake.onServer(1).value.complete shouldBe false
      Files.exists(lake.commitFile(1)) shouldBe false

      lake.list() shouldBe Seq(commit(0), commit(1))

      lake.committed(1) shouldBe append(1)
      lake.onServer(1).value.complete shouldBe true
    }
  }

  test("F3: wherever a writer dies, the log holds the whole commit or none of it, and no sidecar") {
    FaultPoint.All.foreach { point =>
      withClue(s"a writer that dies at $point: ") {
        withLake { lake =>
          lake.withCommits(last = 0)

          lake.crashAt(point, version = 1)

          val published = Files.exists(lake.commitFile(1))
          published shouldBe (point == FaultPoint.AfterPublish)
          if (published) lake.committed(1) shouldBe append(1)
          lake.sidecars shouldBe empty

          val committed = point != FaultPoint.BeforeInsert
          lake.list() shouldBe (if (committed) Seq(commit(0), commit(1)) else Seq(commit(0)))
          lake.onServer(1).map(_.complete) shouldBe Option.when(committed)(true)
          if (committed) lake.committed(1) shouldBe append(1)
          lake.sidecars shouldBe empty
        }
      }
    }
  }

  test("F4: a commit published and not acknowledged is acknowledged by a reader, and not touched") {
    withLake { lake =>
      lake.withCommits(last = 0)
      lake.crashAt(FaultPoint.AfterPublish, version = 1)
      lake.onServer(1).value.complete shouldBe false
      def onDisk =
        (
          Files.readAllBytes(lake.commitFile(1)).toSeq,
          Files.getLastModifiedTime(lake.commitFile(1))
        )
      val before = onDisk

      lake.list() shouldBe Seq(commit(0), commit(1))

      lake.onServer(1).value.complete shouldBe true
      onDisk shouldBe before
    }
  }

  test("F7: with PostgreSQL out of reach the log is not listed, and is again when it is back") {
    withLake { lake =>
      lake.withCommits(last = 1)
      val reader = lake.writer()

      pg.faulty.disable()
      val (attempt, elapsed) =
        try timed(lake.list(by = reader))
        finally pg.faulty.enable()

      attempt.failed.get shouldBe an[IOException]
      // Not the exception that tells Delta the table does not exist.
      attempt.failed.get should not be a[FileNotFoundException]
      elapsed should be < 20.seconds

      lake.list(by = reader) shouldBe Seq(commit(0), commit(1))
    }
  }

  test("F8: processes that find the same unfinished commit at once all get the log, with it") {
    withLake { lake =>
      lake.withCommits(last = 0)
      lake.crashAt(FaultPoint.AfterInsert, version = 1)
      // Each reader has a store, and so a connection pool, of its own: as separate processes.
      val stores = Seq.fill(Readers)(new PostgresCoordinationStore(settings))

      val listings =
        try Concurrently.atOnce(stores)(store => lake.list(by = LogStores.over(store)))
        finally stores.foreach(_.close())

      listings.distinct shouldBe Seq(Seq(commit(0), commit(1)))
      lake.committed(1) shouldBe append(1)
      val entry = lake.onServer(1).value
      entry.complete shouldBe true
      Files.isSameFile(lake.log.resolve(entry.tempPath), lake.commitFile(1)) shouldBe true
    }
  }

  test("F14: a latest commit that is recorded as published and is not in the log stops readers") {
    withLake { lake =>
      lake.withCommits(last = 2)
      Files.delete(lake.commitFile(2))

      val missing = the[CommitMissingException] thrownBy lake.list()

      missing.version shouldBe 2L
    }
  }

  test("F17: an unfinished commit whose temporary file is gone stops readers and writers") {
    withLake { lake =>
      lake.withCommits(last = 0)
      lake.crashAt(FaultPoint.AfterInsert, version = 1)
      val unfinished = lake.onServer(1).value
      Files.delete(lake.log.resolve(unfinished.tempPath))

      val reading = the[UnpublishableCommitException] thrownBy lake.list()
      val writing = the[UnpublishableCommitException] thrownBy lake.commit(2)(append(2))

      Seq(reading, writing).foreach { lost =>
        (lost.version, lost.tempPath) shouldBe ((1L, unfinished.tempPath))
      }
      Seq(1L, 2L).foreach(version => Files.exists(lake.commitFile(version)) shouldBe false)
      // The entry is left as it is, for an operator.
      lake.onServer(1) shouldBe Some(unfinished)
      lake.onServer(2) shouldBe None
    }
  }

  private def entryOnServer(table: TableUri, version: Long): Option[EntryRow] =
    Using.resource(pg.observe(CoordinationSchema.CommitPathRole, CoordinationSchema.Database)) {
      observer =>
        Using.resource(
          observer.prepareStatement(
            """SELECT temp_path, complete, writer_instance FROM delta_coordination.commit_entry
              |WHERE table_uri = ? AND version = ?""".stripMargin
          )
        ) { statement =>
          statement.setString(1, table.value)
          statement.setLong(2, version)
          Using.resource(statement.executeQuery()) { rows =>
            Option.when(rows.next())(
              EntryRow(rows.getString(1), rows.getBoolean(2), rows.getString(3))
            )
          }
        }
    }

  /** The version the table was registered at and its id, as the server has them. */
  private def registrationOnServer(table: TableUri): Option[(Long, Option[String])] =
    Using.resource(pg.observe(CoordinationSchema.CommitPathRole, CoordinationSchema.Database)) {
      observer =>
        Using.resource(
          observer.prepareStatement(
            """SELECT registered_version, delta_table_id FROM delta_coordination.coordinated_table
              |WHERE table_uri = ?""".stripMargin
          )
        ) { statement =>
          statement.setString(1, table.value)
          Using.resource(statement.executeQuery()) { rows =>
            Option.when(rows.next())((rows.getLong(1), Option(rows.getString(2))))
          }
        }
    }

  /** The outcome of `call` and how long it took, under a deadline of the test's own. */
  private def timed[T](call: => T): (Try[T], FiniteDuration) = {
    val start = System.nanoTime()
    val outcome = Await.result(Future(Try(call)), 60.seconds)
    (outcome, (System.nanoTime() - start).nanos)
  }

  @tailrec
  private def eventually(deadline: Deadline)(condition: => Boolean): Unit =
    if (!condition) {
      if (deadline.isOverdue()) fail("the condition did not become true in time")
      Thread.sleep(25)
      eventually(deadline)(condition)
    }

  implicit private class Defined[A](option: Option[A]) {

    /** The value, or a failed test. */
    def value: A = option.getOrElse(fail("expected a value, found None"))
  }
}

object PostgresLogStoreFaultSpec {

  final private case class EntryRow(tempPath: String, complete: Boolean, writerInstance: String)

  private val SlowReplies = "slow-replies"

  private val Readers = 8

  private def commit(version: Long): String = f"$version%020d.json"

  /** Attempts far enough apart for a test to mend the connection between two of them. */
  private val Patient = RetryPolicy(attempts = 5, firstPause = 250.millis)

  /** The default number of attempts, close together. */
  private val Impatient = RetryPolicy(attempts = 3, firstPause = 50.millis)

  private val TableId = "5a0d1a66-0bb5-4d1c-8a8b-6a1f6f5a3c11"

  private val Creation: Seq[String] = Seq(
    """{"commitInfo":{"timestamp":1763200000000,"operation":"CREATE TABLE"}}""",
    """{"metaData":{"id":"""" + TableId + """","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{},"createdTime":1763200000000}}""",
    """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"""
  )

  private def append(version: Long): Seq[String] =
    Seq(s"""{"add":{"path":"part-$version.parquet","dataChange":true}}""")

  private def hadoop(path: LocalPath): Path = new Path("file", null, path.toString)
}
