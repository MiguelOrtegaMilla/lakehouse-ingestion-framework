package lakehouse.ingestion.it.logstore

import java.io.FileNotFoundException
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.sql.Timestamp
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.Using

import io.delta.exceptions.MetadataChangedException
import io.delta.tables.DeltaTable
import lakehouse.ingestion.it.logstore.RecordingLogStore._
import lakehouse.ingestion.it.support.LocalSparkSuite
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What Delta 4.4 asks of a LogStore: the facts `PostgresLogStore` is built on (ADR-0004 §8,
 * discovery §20).
 *
 * These are characterisation tests. They test Delta, not this project: each one pins a
 * behaviour that the port relies on and that Delta does not document as a contract. The store
 * under Delta is `RecordingLogStore`, loaded the way `PostgresLogStore` will be. A Delta upgrade
 * that changes one of these facts fails here, with the name of the assumption it breaks, before
 * it can break the LogStore (ADR-0014, upgrade procedure).
 */
final class DeltaLogStoreContractSpec
    extends AnyFunSuite
    with Matchers
    with LocalSparkSuite
    with BeforeAndAfterEach {

  import DeltaLogStoreContractSpec._

  // One setting per route into the Hadoop configuration a LogStore is built with.
  override protected def sparkConf: Map[String, String] = Map(
    "spark.delta.logStore.file.impl" -> classOf[RecordingLogStore].getName,
    "spark.hadoop.lakehouse.probe.hadoop" -> "set-at-launch",
    "spark.lakehouse.probe.spark" -> "set-at-launch"
  )

  override def beforeEach(): Unit = {
    super.beforeEach()
    RecordingLogStore.reset()
  }

  override def afterEach(): Unit =
    try RecordingLogStore.reset()
    finally super.afterEach()

  // ---------------------------------------------------------------------------------------------
  // Which files Delta writes through the LogStore
  // ---------------------------------------------------------------------------------------------

  test("commit files are the only writes with overwrite = false") {
    val path = createTable("inventory") // version 0
    append(path, 10L, 20L) // 1
    merge(path, rows(5L, 25L)) // 2
    DeltaTable.forPath(spark, path).delete("id = 3") // 3
    DeltaTable.forPath(spark, path).optimize().executeCompaction(): Unit // 4
    checkpoint(path)
    commitsOnDisk(path) shouldBe (0L to 4L)

    // VACUUM is a writer too: it commits when it starts and when it ends.
    vacuum(path)
    commitsOnDisk(path) shouldBe (0L to 6L)
    Seq(5L, 6L).map(operationOf(path, _)) shouldBe Seq("VACUUM START", "VACUUM END")

    val written = writesUnder(path)
    // Everything Delta writes through the LogStore is a direct child of _delta_log.
    written.map(_.directory).distinct shouldBe Seq(logDir(path))
    written.map(w => w.kind -> w.overwrite).toSet shouldBe Set(
      FileKind.Commit -> false,
      FileKind.LastCheckpoint -> true,
      FileKind.LastVacuumInfo -> true
    )

    // Delta wrote these too, but not through the LogStore: its version checksums (through
    // Spark's CheckpointFileManager) and the checkpoint itself (a Spark write job).
    val onDisk = logFiles(path)
    onDisk.filter(VersionChecksum.matches) should not be empty
    onDisk.filter(_.endsWith(".checkpoint.parquet")) should not be empty
  }

  test("a V2 checkpoint in JSON is written with overwrite = !isPartialWriteVisible") {
    val path = tablePath("v2-checkpoint")
    rows(0L, 10L).write.format("delta").option("delta.checkpointPolicy", "v2").save(path)

    def checkpointWrites(partialWriteVisible: Boolean): Seq[(FileKind, Boolean)] = {
      append(path, 10L, 20L)
      RecordingLogStore.reset()
      RecordingLogStore.answerPartialWriteVisible(partialWriteVisible)
      checkpoint(path)
      writesUnder(path).map(w => w.kind -> w.overwrite)
    }

    withSessionConf("spark.databricks.delta.checkpointV2.topLevelFileFormat" -> "json") {
      // A file that is not a commit, written with overwrite = false: Delta asks for the
      // temporary-file-and-rename path because a direct write could be seen half done.
      checkpointWrites(partialWriteVisible = true) shouldBe Seq(
        FileKind.V2CheckpointJson -> false,
        FileKind.LastCheckpoint -> true
      )
      checkpointWrites(partialWriteVisible = false) shouldBe Seq(
        FileKind.V2CheckpointJson -> true,
        FileKind.LastCheckpoint -> true
      )
    }
  }

  // ---------------------------------------------------------------------------------------------
  // What Delta does when the write of a commit fails
  // ---------------------------------------------------------------------------------------------

  test("a taken version is signalled with java.nio's FileAlreadyExistsException; Delta retries") {
    val path = createTable("retry")
    val transaction = DeltaLog.forTable(spark, path).startTransaction(None) // reads version 0
    append(path, 10L, 20L) // another writer takes version 1
    RecordingLogStore.clearJournal()

    transaction.commit(Nil, DeltaOperations.ManualUpdate) shouldBe 2L

    commitAttempts(path) shouldBe Seq(
      1L -> Failed("java.nio.file.FileAlreadyExistsException"),
      2L -> Written
    )
    // Between the two attempts Delta looks for the commits that won, by listing the log: the
    // conflict check goes through LogStore.listFrom, on the committing thread.
    val betweenAttempts = callsOn(path)
      .dropWhile(!isCommitWrite(_))
      .drop(1)
      .takeWhile(!isCommitWrite(_))
    val committer = commitWrites(path).head.thread
    betweenAttempts.collect { case l: ListFrom => l.thread } should contain(committer)
  }

  test("a logical conflict with the winning commit fails the transaction instead of retrying") {
    val path = createTable("conflict")
    val transaction = DeltaLog.forTable(spark, path).startTransaction(None)
    spark.sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('lakehouse.owner' = 'x')"): Unit
    RecordingLogStore.clearJournal()

    intercept[MetadataChangedException] {
      transaction.commit(Nil, DeltaOperations.ManualUpdate)
    }

    commitAttempts(path) shouldBe Seq(1L -> Failed("java.nio.file.FileAlreadyExistsException"))
  }

  test("Hadoop's FileAlreadyExistsException is not a conflict signal: the commit just fails") {
    val path = createTable("hadoop-exception")
    RecordingLogStore.clearJournal()
    RecordingLogStore.onCommitWrite { _ =>
      Fail(() => new org.apache.hadoop.fs.FileAlreadyExistsException("probe"))
    }

    val failure = intercept[Exception](append(path, 10L, 20L))

    causes(failure).map(_.getClass.getName) should contain(
      "org.apache.hadoop.fs.FileAlreadyExistsException"
    )
    commitAttempts(path) shouldBe Seq(
      1L -> Failed("org.apache.hadoop.fs.FileAlreadyExistsException")
    )
    commitsOnDisk(path) shouldBe Seq(0L)
  }

  test("Delta reads the commit file's status as soon as write returns") {
    val path = createTable("status-after-write")
    RecordingLogStore.clearJournal()
    RecordingLogStore.onCommitWrite(_ => ReturnWithoutWriting)

    // A LogStore whose write returns before the commit file is visible does not get a
    // successful commit: the transaction fails on the missing file.
    val failure = intercept[Exception](append(path, 10L, 20L))

    causes(failure).exists(_.isInstanceOf[FileNotFoundException]) shouldBe true
    commitAttempts(path) shouldBe Seq(1L -> NotWritten)
    commitsOnDisk(path) shouldBe Seq(0L)
  }

  // ---------------------------------------------------------------------------------------------
  // The thread a commit is written on
  // ---------------------------------------------------------------------------------------------

  test("every commit is written on the thread that called the operation") {
    val path = createTable("caller-thread")
    val operations = Seq[(String, () => Unit)](
      "append with txnAppId" -> { () =>
        rows(10L, 20L).write
          .format("delta")
          .mode("append")
          .option("txnAppId", "contract.source/bronze")
          .option("txnVersion", 1L)
          .save(path)
      },
      "MERGE" -> (() => merge(path, rows(5L, 25L))),
      "DELETE" -> (() => DeltaTable.forPath(spark, path).delete("id = 3")),
      // Compaction runs its jobs on a thread pool; the commit still comes back to the caller.
      "OPTIMIZE" -> (() => DeltaTable.forPath(spark, path).optimize().executeCompaction(): Unit)
    )

    operations.foreach { case (name, operation) =>
      RecordingLogStore.clearJournal()
      val caller = onNewThread(s"caller of $name")(operation())
      withClue(s"$name: ") {
        val committers = commitWrites(path).map(_.thread)
        committers should not be empty
        committers.distinct shouldBe Seq(caller)
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // How LogStore instances are created and configured
  // ---------------------------------------------------------------------------------------------

  test("one LogStore instance serves a table until its DeltaLog leaves the cache") {
    val first = createTable("lifecycle-first")
    append(first, 10L, 20L)
    spark.read.format("delta").load(first).count() shouldBe 20L
    val second = createTable("lifecycle-second")

    val servingFirst = instancesServing(first)
    servingFirst should have size 1
    instancesServing(second) should have size 1
    instancesServing(second) should not be servingFirst

    // Delta evicts a DeltaLog after an hour without use; clearing the cache does the same now.
    DeltaLog.clearCache()
    RecordingLogStore.clearJournal()
    append(first, 20L, 30L)

    instancesServing(first) should have size 1
    instancesServing(first) should not be servingFirst
  }

  test("a LogStore is configured through the Hadoop configuration, at creation and per call") {
    val expected = Map(
      // spark.hadoop.* at launch: with the prefix stripped, as for any Hadoop client...
      "lakehouse.probe.hadoop" -> "set-at-launch",
      // ...and, like every Spark setting, at launch or set on the session, under its full name.
      "spark.hadoop.lakehouse.probe.hadoop" -> "set-at-launch",
      "spark.lakehouse.probe.spark" -> "set-at-launch",
      "spark.lakehouse.probe.session" -> "set-at-runtime"
    )

    withSessionConf("spark.lakehouse.probe.session" -> "set-at-runtime") {
      val path = createTable("settings")

      val serving = instancesServing(path)
      serving should have size 1
      val created = RecordingLogStore.events.collect {
        case c: Created if c.instance == serving.head => c
      }
      created.map(_.settings) shouldBe Seq(expected)
      commitWrites(path).map(_.settings) shouldBe Seq(expected)
    }
  }

  // ---------------------------------------------------------------------------------------------
  // LogStores created inside Spark tasks (on executors, outside local mode)
  // ---------------------------------------------------------------------------------------------

  test("VACUUM lists data directories from Spark tasks, and never _delta_log") {
    val path = createTable("vacuum")
    append(path, 10L, 20L)
    RecordingLogStore.clearJournal()

    vacuum(path)

    val inTasks = RecordingLogStore.events.filter(_.thread.isSparkTask)
    val created = inTasks.collect { case c: Created => c }
    created should not be empty
    // The task builds its LogStore from the configuration the driver sent with the job.
    created.map(_.settings.get("lakehouse.probe.hadoop")).distinct shouldBe Seq(
      Some("set-at-launch")
    )

    val listed = inTasks.collect { case l: ListFrom => l.directory }
    listed should contain(tableDir(path))
    listed.filter(_.contains("_delta_log")) shouldBe empty
    inTasks.collect { case w: Write => w } shouldBe empty
    inTasks.collect { case r: Read => r } shouldBe empty
  }

  test("DESCRIBE HISTORY reads commit files from Spark tasks, without listing") {
    val path = createTable("history")
    append(path, 10L, 20L)
    RecordingLogStore.clearJournal()

    DeltaTable.forPath(spark, path).history().collect() should have length 2

    val inTasks = RecordingLogStore.events.filter(_.thread.isSparkTask)
    inTasks.collect { case c: Created => c } should not be empty
    inTasks.collect { case r: Read => kindOf(r.path) }.distinct shouldBe Seq(FileKind.Commit)
    inTasks.collect { case l: ListFrom => l } shouldBe empty
    inTasks.collect { case w: Write => w } shouldBe empty
  }

  test("resolving a timestamp over a long history lists _delta_log from Spark tasks") {
    val path = createTable("timestamp-search") // 10 rows at version 0
    (1L to 4L).foreach(i => append(path, i * 10, i * 10 + 5)) // 5 more rows per version
    val asOfVersion2 = new Timestamp(
      Files.getLastModifiedTime(Paths.get(logDir(path), commitFileName(2L))).toMillis
    )

    // Delta searches in parallel when the history is more than twice this long (default 1000).
    // The history manager reads the setting when its DeltaLog is created.
    withSessionConf("spark.databricks.delta.history.maxKeysPerList" -> "1") {
      DeltaLog.clearCache()
      RecordingLogStore.clearJournal()
      try {
        val asOf = spark.read.format("delta").option("timestampAsOf", asOfVersion2.toString)
        asOf.load(path).count() shouldBe 20L
      } finally DeltaLog.clearCache()
    }

    // The one path found where a task needs the coordinated read of _delta_log, and so, with
    // PostgresLogStore, the coordination database.
    val listedInTasks = RecordingLogStore.events.collect {
      case l: ListFrom if l.thread.isSparkTask => l.directory
    }
    listedInTasks should not be empty
    listedInTasks.distinct shouldBe Seq(logDir(path))
  }

  // ---------------------------------------------------------------------------------------------
  // Delta's own commit lock
  // ---------------------------------------------------------------------------------------------

  test("with isPartialWriteVisible = true, Delta lets one thread at a time commit to a table") {
    val path = createTable("commit-lock")
    RecordingLogStore.answerPartialWriteVisible(true)

    // While one writer is inside LogStore.write, a second one in the same JVM waits for the
    // DeltaLog's lock: it cannot even reach the LogStore. Contention between threads of one JVM
    // therefore never reaches a LogStore that answers `true`.
    secondWriterWhileFirstIsInsideWrite(path) shouldBe WaitedForDeltaLock
    commitsOnDisk(path) shouldBe (0L to 2L)
  }

  test("with isPartialWriteVisible = false, two threads reach LogStore.write together") {
    val path = createTable("no-commit-lock")
    RecordingLogStore.answerPartialWriteVisible(false)

    secondWriterWhileFirstIsInsideWrite(path) shouldBe EnteredWrite
    commitsOnDisk(path) shouldBe (0L to 2L)
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private def rows(from: Long, until: Long): DataFrame =
    spark
      .range(from, until)
      .selectExpr("id", "cast(id % 2 as int) as part", "cast(id as string) as value")

  /** A table of 10 rows in two partitions, at version 0. */
  private def createTable(name: String): String = {
    val path = tablePath(name)
    rows(0L, 10L).write.format("delta").partitionBy("part").save(path)
    path
  }

  private def append(path: String, from: Long, until: Long): Unit =
    rows(from, until).write.format("delta").mode("append").save(path)

  private def merge(path: String, source: DataFrame): Unit =
    DeltaTable
      .forPath(spark, path)
      .as("t")
      .merge(source.as("s"), "t.id = s.id")
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute(): Unit

  private def checkpoint(path: String): Unit = {
    val log = DeltaLog.forTable(spark, path)
    log.checkpoint(log.update())
  }

  private def vacuum(path: String): Unit =
    withSessionConf("spark.databricks.delta.retentionDurationCheck.enabled" -> "false") {
      DeltaTable.forPath(spark, path).vacuum(0.0): Unit
    }

  private def withSessionConf[T](settings: (String, String)*)(body: => T): T = {
    val previous = settings.map { case (key, _) => key -> spark.conf.getOption(key) }
    settings.foreach { case (key, value) => spark.conf.set(key, value) }
    try body
    finally
      previous.foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
  }

  /** Runs `body` on a new thread and waits for it; a failure in `body` fails the test. */
  private def onNewThread(name: String)(body: => Unit): ThreadRef = {
    val failure = new AtomicReference[Throwable]
    val thread = new Thread(
      () =>
        try {
          SparkSession.setActiveSession(spark)
          body
        } catch { case e: Throwable => failure.set(e) },
      name
    )
    thread.start()
    thread.join()
    Option(failure.get).foreach(e => throw e)
    ThreadRef.of(thread)
  }

  private def tableDir(path: String): String = Paths.get(URI.create(path)).toString

  private def logDir(path: String): String = Paths.get(tableDir(path), "_delta_log").toString

  /** The visible files of `_delta_log`, without Hadoop's hidden `.crc` sidecars. */
  private def logFiles(path: String): Seq[String] =
    Using
      .resource(Files.list(Paths.get(logDir(path)))) {
        _.iterator.asScala.map((p: Path) => p.getFileName.toString).toList
      }
      .filterNot(_.startsWith("."))
      .sorted

  private def commitsOnDisk(path: String): Seq[Long] =
    logFiles(path).collect { case CommitFile(version) => version.toLong }

  /** The operation a commit recorded, read from the commit file itself. */
  private def operationOf(path: String, version: Long): String = {
    val commit = Files.readString(Paths.get(logDir(path), commitFileName(version)))
    Operation.findFirstMatchIn(commit).map(_.group(1)).getOrElse(fail(s"no operation in $version"))
  }

  private def calls: Seq[Call] = RecordingLogStore.events.collect { case c: Call => c }

  /** The calls on the table's `_delta_log`, in order. */
  private def callsOn(path: String): Seq[Call] = calls.filter(_.directory == logDir(path))

  /** Every write anywhere under the table's directory. */
  private def writesUnder(path: String): Seq[Write] =
    calls.collect { case w: Write if w.directory.startsWith(tableDir(path)) => w }

  private def commitWrites(path: String): Seq[Write] =
    callsOn(path).collect { case w: Write if w.kind == FileKind.Commit => w }

  private def commitAttempts(path: String): Seq[(Long, Outcome)] =
    commitWrites(path).map(w => commitVersion(w.path) -> w.outcome)

  private def instancesServing(path: String): Seq[Int] = callsOn(path).map(_.instance).distinct

  /**
   * Holds the first writer inside `LogStore.write` and starts a second one on the same table.
   * Reports how far the second one got while the first was still inside; then lets both finish.
   */
  private def secondWriterWhileFirstIsInsideWrite(path: String): SecondWriter = {
    val firstInside = new CountDownLatch(1)
    val secondInside = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val entered = new AtomicInteger
    RecordingLogStore.onCommitWrite { _ =>
      if (entered.incrementAndGet() == 1) {
        firstInside.countDown()
        release.await(WaitSeconds, TimeUnit.SECONDS): Unit
      } else secondInside.countDown()
      Delegate
    }

    val failures = new java.util.concurrent.ConcurrentLinkedQueue[Throwable]
    def writer(name: String, from: Long): Thread =
      new Thread(
        () =>
          try {
            SparkSession.setActiveSession(spark)
            append(path, from, from + 5)
          } catch { case e: Throwable => failures.add(e): Unit },
        name
      )
    val first = writer("first writer", 100L)
    val second = writer("second writer", 200L)

    @tailrec
    def observe(deadlineNanos: Long): SecondWriter =
      if (secondInside.getCount == 0) EnteredWrite
      else if (isWaitingForSnapshotLock(second)) WaitedForDeltaLock
      else if (System.nanoTime() > deadlineNanos)
        fail(s"the second writer neither committed nor waited for Delta's lock: ${second.getState}")
      else {
        Thread.sleep(20)
        observe(deadlineNanos)
      }

    first.start()
    val observed =
      try {
        firstInside.await(WaitSeconds, TimeUnit.SECONDS) shouldBe true
        second.start()
        observe(System.nanoTime() + TimeUnit.SECONDS.toNanos(WaitSeconds))
      } finally release.countDown()
    first.join()
    second.join()
    failures.asScala.toSeq shouldBe empty
    observed
  }
}

object DeltaLogStoreContractSpec {

  private val WaitSeconds = 60L

  private val CommitFile = """(\d{20})\.json""".r
  private val VersionChecksum = """\d{20}\.crc""".r
  private val Operation = """"operation":"([^"]+)"""".r

  private def commitFileName(version: Long): String = f"$version%020d.json"

  private def isCommitWrite(call: Call): Boolean = call match {
    case w: Write => w.kind == FileKind.Commit
    case _ => false
  }

  /** How far a second writer got while the first one was inside `LogStore.write`. */
  sealed private trait SecondWriter
  private case object EnteredWrite extends SecondWriter
  private case object WaitedForDeltaLock extends SecondWriter

  /**
   * True when `thread` is parked acquiring the lock of `SnapshotManagement`, which Delta also
   * takes around a commit when the LogStore says partial writes are visible
   * (`OptimisticTransaction.lockCommitIfEnabled`).
   */
  private def isWaitingForSnapshotLock(thread: Thread): Boolean = {
    val parked = thread.getState == Thread.State.WAITING
    val frames = thread.getStackTrace.toSeq
    parked && frames.sliding(2).exists {
      case Seq(lock, caller) =>
        lock.getClassName == classOf[ReentrantLock].getName &&
        lock.getMethodName == "lockInterruptibly" &&
        caller.getMethodName.startsWith("withSnapshotLockInterruptibly")
      case _ => false
    }
  }

  @tailrec
  private def causes(error: Throwable, seen: List[Throwable] = Nil): List[Throwable] =
    if (error == null || seen.contains(error)) seen.reverse
    else causes(error.getCause, error :: seen)
}
