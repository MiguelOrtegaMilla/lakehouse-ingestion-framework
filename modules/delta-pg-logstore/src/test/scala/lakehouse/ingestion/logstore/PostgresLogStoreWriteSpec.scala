package lakehouse.ingestion.logstore

import java.io.IOException
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{FileAlreadyExistsException, Files}

import scala.jdk.CollectionConverters._
import scala.util.Using

import lakehouse.ingestion.logstore.coordination.FaultyCoordinationStore._
import lakehouse.ingestion.logstore.coordination._
import lakehouse.ingestion.logstore.storage.{
  LocalLogStorage,
  LogStorage,
  LogStorageSpec,
  PublishResult
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The write path of `PostgresLogStore` (ADR-0004 §3.2), through the method Delta calls, on a
 * real table directory.
 *
 * The table and its coordination store are `LakeFixture`'s. The scenarios named F1 to F17 are
 * those of ADR-0004 §7.1; the integration tests repeat them against a real PostgreSQL whose
 * connection is cut.
 */
final class PostgresLogStoreWriteSpec extends AnyFunSuite with Matchers with LakeFixture {

  import LakeFixture._
  import PostgresLogStoreWriteSpec._

  // ---------------------------------------------------------------------------------------------
  // A commit without incident
  // ---------------------------------------------------------------------------------------------

  test("the first commit registers the table with its id, and leaves a complete entry") {
    val lake = new Lake

    lake.commit(0)(Creation)

    lake.memory.registration(lake.table) shouldBe Some(
      Registration(lake.table, Registration.NotYetCreated, Some(TableId), taintReason = None)
    )
    lake.committed(0) shouldBe Creation
    val entry = lake.entry(0).value
    entry.complete shouldBe true
    entry.tempPath should fullyMatch regex """\.tmp/00000000000000000000\.json\.[0-9a-f-]{36}"""
    // The commit is its temporary file, under a second name.
    Files.isSameFile(lake.tempOf(entry), lake.commitFile(0)) shouldBe true
  }

  test("later commits take their versions in the registered table") {
    val lake = new Lake().withCommits(last = 2)

    lake.committed(2) shouldBe append(2)
    (0L to 2L).map(lake.entry(_).value.complete) shouldBe Seq(true, true, true)
    lake.memory.registration(lake.table).value.deltaTableId shouldBe Some(TableId)
    // One registration, when the table was created; one claim and one acknowledgement a commit.
    lake.store.calls.count(_ == Register) shouldBe 1
    lake.store.calls.count(_ == Take) shouldBe 3
    lake.store.calls.count(_ == Complete) shouldBe 3
  }

  test("an entry says who wrote it: host, process and Spark application") {
    val lake = new Lake
    val inAnApplication = new Configuration(conf)
    inAnApplication.set("spark.app.id", "local-1763200000000")

    lake.commit(0)(Creation)
    lake.commit(1, LogStores.over(lake.store, conf = inAnApplication))(append(1))

    val pid = ProcessHandle.current().pid()
    lake.entry(0).value.writerInstance should fullyMatch regex s"[^:]+:$pid:no-spark-app"
    lake.entry(1).value.writerInstance should fullyMatch regex s"[^:]+:$pid:local-1763200000000"
  }

  test("the table's id is read only from a metaData action that begins with it") {
    DeltaTableId.in(Creation) shouldBe Some(TableId)
    DeltaTableId.in(append(1)) shouldBe None
    // Not Delta's layout: no guess is made.
    DeltaTableId.in(Seq(s"""{"metaData":{"name":"orders","id":"$TableId"}}""")) shouldBe None
    DeltaTableId.in(
      Seq(s"""{"add":{"path":"{\\"metaData\\":{\\"id\\":\\"$TableId\\"}}"}}""")
    ) shouldBe None
  }

  // ---------------------------------------------------------------------------------------------
  // A version that is taken
  // ---------------------------------------------------------------------------------------------

  test("a version whose file is in the log is taken, and nothing is asked or written") {
    val lake = new Lake().withCommits(last = 1)
    val callsBefore = lake.store.calls.size
    val tempsBefore = lake.temps

    val taken = the[FileAlreadyExistsException] thrownBy lake.commit(1)(append(99))

    // java.nio's, the only one Delta answers by trying the next version.
    taken.getClass shouldBe classOf[FileAlreadyExistsException]
    lake.committed(1) shouldBe append(1)
    lake.store.calls.size shouldBe callsBefore
    lake.temps shouldBe tempsBefore
  }

  test("a version whose entry is another writer's is taken, before its file appears") {
    val lake = new Lake().withCommits(last = 0)
    val another = Claim(lake.table, 1L, ".tmp/another-writer", "host-b:2:app", runId = None)
    lake.memory.take(another) shouldBe TakeResult.Taken
    val tempsBefore = lake.temps

    val taken = the[FileAlreadyExistsException] thrownBy lake.commit(1)(append(1))

    taken.getClass shouldBe classOf[FileAlreadyExistsException]
    lake.entry(1).value.tempPath shouldBe another.tempPath
    Files.exists(lake.commitFile(1)) shouldBe false
    // The temporary file of the writer that lost is of no use to anybody.
    lake.temps shouldBe tempsBefore
  }

  // ---------------------------------------------------------------------------------------------
  // What the registry refuses
  // ---------------------------------------------------------------------------------------------

  test("F12: a commit to a table that is not registered is refused, and leaves nothing") {
    val lake = new Lake
    // A table that was created without this LogStore.
    Files.write(lake.commitFile(0), Creation.asJava)

    val refused = the[TableNotRegisteredException] thrownBy lake.commit(1)(append(1))

    refused.table shouldBe lake.table
    refused shouldBe a[WriteRefusedException]
    Files.exists(lake.commitFile(1)) shouldBe false
    lake.temps shouldBe empty
    lake.memory.registration(lake.table) shouldBe None
    lake.entry(1) shouldBe None
  }

  test("a tainted table refuses writes, and says why") {
    val lake = new Lake().withCommits(last = 0)
    lake.memory.taint(lake.table, "commit 7 has no entry")
    val tempsBefore = lake.temps

    val refused = the[TableTaintedException] thrownBy lake.commit(1)(append(1))

    refused.reason shouldBe "commit 7 has no entry"
    Files.exists(lake.commitFile(1)) shouldBe false
    lake.temps shouldBe tempsBefore
    lake.entry(1) shouldBe None
  }

  test("F9: a table deleted and created again at its path is refused until its registry is reset") {
    val lake = new Lake().withCommits(last = 2)
    val entriesBefore = (0L to 2L).map(lake.entry)
    // The table is dropped by deleting its files. The registry knows nothing of it.
    (0L to 2L).foreach(version => Files.delete(lake.commitFile(version)))
    lake.temps.foreach(Files.delete)

    val refused = the[StaleRegistrationException] thrownBy lake.commit(0)(Creation)

    refused.getMessage should include("up to version 2")
    Files.exists(lake.commitFile(0)) shouldBe false
    lake.temps shouldBe empty
    // Entry 0 of the old table was not taken for the commit of the new one.
    (0L to 2L).map(lake.entry) shouldBe entriesBefore
  }

  test("a creation that another writer has in flight is a taken version, not an earlier table") {
    val lake = new Lake
    lake.memory.register(lake.table, Registration.NotYetCreated, None)
    lake.memory.take(Claim(lake.table, 0L, ".tmp/creator", "host-b:2:app", None))

    a[FileAlreadyExistsException] should be thrownBy lake.commit(0)(Creation)
  }

  test("a creation that another writer finishes while this one starts is a taken version too") {
    // The other creator does all of it, and more, after this one has looked at the log and
    // before it looks at the registry: once before the registration is read, once after.
    Seq[Operation](ReadRegistration, Register).foreach { between =>
      val lake = new Lake
      lake.store.afterNext(between) {
        lake.commit(0)(Creation)
        lake.commit(1)(append(1))
      }

      val taken = the[FileAlreadyExistsException] thrownBy lake.commit(0)(append(99))

      // Not a table that was dropped and created again: nothing for an operator to reset.
      taken.getClass shouldBe classOf[FileAlreadyExistsException]
      lake.committed(0) shouldBe Creation
      (0L to 1L).map(lake.entry(_).value.complete) shouldBe Seq(true, true)
      // The winner's two files, and nothing of the writer that lost.
      lake.temps should have size 2
    }
  }

  test("a table registered by a creator that died before its first commit can be created") {
    val lake = new Lake
    a[SimulatedCrash] should be thrownBy
      lake.commit(0, lake.logStore(LogStores.crashAt(FaultPoint.BeforeInsert)))(Creation)
    lake.memory.registration(lake.table).value.registeredVersion shouldBe Registration.NotYetCreated
    lake.entry(0) shouldBe None

    lake.commit(0)(Creation)

    lake.entry(0).value.complete shouldBe true
  }

  test("an adopted table is written from the version after the one it was adopted at") {
    val lake = new Lake
    lake.memory.register(lake.table, 41L, Some(TableId))
    Files.write(lake.commitFile(41), append(41).asJava)

    // Commit 41 was made before the table was coordinated: it has no entry, and needs none.
    lake.commit(42)(append(42))
    lake.entry(42).value.complete shouldBe true

    // At or below it, nothing is written: not version 0, not a hole in the old log.
    Seq(0L, 40L).foreach { version =>
      val refused = the[StaleRegistrationException] thrownBy lake.commit(version)(append(version))
      refused.getMessage should include("adopted at version 41")
      Files.exists(lake.commitFile(version)) shouldBe false
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The commit before: published before anything is written after it
  // ---------------------------------------------------------------------------------------------

  test("a commit that was taken and not published is published before the next version") {
    val lake = new Lake().withCommits(last = 0)
    val unfinished = lake.crashedAt(FaultPoint.AfterInsert, version = 1)
    unfinished.complete shouldBe false
    Files.exists(lake.commitFile(1)) shouldBe false

    lake.commit(2)(append(2))

    lake.committed(1) shouldBe append(1)
    lake.entry(1).value.complete shouldBe true
    lake.committed(2) shouldBe append(2)
  }

  test("a commit that was published and not acknowledged is acknowledged by the next writer") {
    val lake = new Lake().withCommits(last = 0)
    lake.crashedAt(FaultPoint.AfterPublish, version = 1).complete shouldBe false
    lake.committed(1) shouldBe append(1)

    lake.commit(2)(append(2))

    lake.entry(1).value.complete shouldBe true
    lake.committed(1) shouldBe append(1)
  }

  test("a commit that is neither in the log nor in the registry stops the next writer") {
    val lake = new Lake().withCommits(last = 0)

    val failure = the[IOException] thrownBy lake.commit(2)(append(2))

    failure.getMessage should include("version 1 is not in its log")
    lake.entry(2) shouldBe None
  }

  test("a taken commit whose temporary file is gone stops the next writer, by name") {
    val lake = new Lake().withCommits(last = 0)
    val unfinished = lake.crashedAt(FaultPoint.AfterInsert, version = 1)
    Files.delete(lake.tempOf(unfinished))

    val lost = the[UnpublishableCommitException] thrownBy lake.commit(2)(append(2))

    (lost.version, lost.tempPath) shouldBe ((1L, unfinished.tempPath))
    // Nothing is published in its place, and nothing after it.
    Seq(1L, 2L).foreach(version => Files.exists(lake.commitFile(version)) shouldBe false)
    lake.entry(1) shouldBe Some(unfinished)
    lake.entry(2) shouldBe None
  }

  // ---------------------------------------------------------------------------------------------
  // Writers that die, a store that fails, a storage that fails
  // ---------------------------------------------------------------------------------------------

  test("F1: a writer that dies before taking its version leaves only an orphan temporary file") {
    val lake = new Lake().withCommits(last = 0)
    val tempsBefore = lake.temps

    a[SimulatedCrash] should be thrownBy
      lake.commit(1, lake.logStore(LogStores.crashAt(FaultPoint.BeforeInsert)))(append(1))

    lake.entry(1) shouldBe None
    Files.exists(lake.commitFile(1)) shouldBe false
    val orphan = lake.temps.diff(tempsBefore)
    orphan should have size 1

    // The version is free: the next writer commits it.
    lake.commit(1)(Seq("the next writer's commit"))
    lake.committed(1) shouldBe Seq("the next writer's commit")
    lake.entry(1).value.complete shouldBe true
    // The orphan waits for the garbage collection, which is a maintenance operation.
    lake.temps should contain(orphan.head)
  }

  test("F5: while the store cannot be reached, a commit fails and writes nothing") {
    val lake = new Lake().withCommits(last = 0)
    val tempsBefore = lake.temps
    val writer = lake.logStore()

    lake.store.down()
    val failure = the[IOException] thrownBy lake.commit(1, writer)(append(1))

    failure should not be a[FileAlreadyExistsException]
    Files.exists(lake.commitFile(1)) shouldBe false
    lake.temps shouldBe tempsBefore
    lake.entry(1) shouldBe None

    // Failing closed is not failing for ever: the same writer commits when the store is back.
    lake.store.up()
    lake.commit(1, writer)(append(1))
    lake.entry(1).value.complete shouldBe true
  }

  test("F5: a store that fails at the moment of the INSERT fails the commit after a few attempts") {
    val lake = new Lake().withCommits(last = 0)
    val tempsBefore = lake.temps
    val takesBefore = lake.store.calls.count(_ == Take)
    val storeFailsNow = LogStores.at(FaultPoint.BeforeInsert)(lake.store.down())

    val failure = the[IOException] thrownBy lake.commit(1, lake.logStore(storeFailsNow))(append(1))

    failure should not be a[FileAlreadyExistsException]
    lake.store.calls.count(_ == Take) - takesBefore shouldBe LogStores.Impatient.attempts
    Files.exists(lake.commitFile(1)) shouldBe false
    lake.entry(1) shouldBe None
    // The writer cannot know that the INSERT did not land, so the file it would point at stays.
    lake.temps.diff(tempsBefore) should have size 1
  }

  test("F15: a version taken while the reply was lost is the writer's own, and it commits") {
    val lake = new Lake().withCommits(last = 0)
    lake.store.failNext(Take, ReplyLost)

    lake.commit(1)(append(1))

    lake.committed(1) shouldBe append(1)
    lake.entry(1).value.complete shouldBe true
    // The entry is the one the lost call made: the repeated claim found it, and made no other.
    Files.isSameFile(lake.tempOf(lake.entry(1).value), lake.commitFile(1)) shouldBe true
  }

  test("F15: a claim that never arrived, repeated after another writer's, is told so") {
    val lake = new Lake().withCommits(last = 0)
    val another = Claim(lake.table, 1L, ".tmp/another-writer", "host-b:2:app", runId = None)
    lake.store.failNext(Take, Preempted(by = another))
    val tempsBefore = lake.temps

    val taken = the[FileAlreadyExistsException] thrownBy lake.commit(1)(append(1))

    taken.getClass shouldBe classOf[FileAlreadyExistsException]
    lake.entry(1).value.tempPath shouldBe another.tempPath
    Files.exists(lake.commitFile(1)) shouldBe false
    lake.temps shouldBe tempsBefore
  }

  test("F15: if the store stays down after the INSERT landed, the commit fails and still exists") {
    val lake = new Lake().withCommits(last = 0)
    lake.store.failNext(Take, ReplyLost, Unreachable, Unreachable)

    val failure = the[IOException] thrownBy lake.commit(1)(append(1))

    // The writer does not know it, but version 1 is its own: the entry and its file are there.
    failure should not be a[FileAlreadyExistsException]
    val landed = lake.entry(1).value
    landed.complete shouldBe false
    Files.readAllLines(lake.tempOf(landed)).asScala shouldBe append(1)
    Files.exists(lake.commitFile(1)) shouldBe false

    // Whoever comes next publishes it.
    lake.commit(2)(append(2))
    lake.committed(1) shouldBe append(1)
    lake.entry(1).value.complete shouldBe true
  }

  test("F6: a store that fails after the INSERT does not fail the commit") {
    val lake = new Lake().withCommits(last = 0)
    lake.store.failNext(Complete, Unreachable)

    // No exception: the commit is visible, which is all Delta needs.
    lake.commit(1)(append(1))

    lake.committed(1) shouldBe append(1)
    lake.entry(1).value.complete shouldBe false

    // The acknowledgement is made by the next writer.
    lake.commit(2)(append(2))
    lake.entry(1).value.complete shouldBe true
  }

  test("F16: a publish that fails is tried again") {
    val lake = new Lake().withCommits(last = 0)
    val storage = new FailingPublishes(failures = 1)

    lake.commit(1, lake.logStore(storageOf = _ => storage))(append(1))

    storage.publishes shouldBe 2
    lake.committed(1) shouldBe append(1)
    lake.entry(1).value.complete shouldBe true
  }

  test("F16: a publish that keeps failing is reported as a commit that is pending, not done") {
    val lake = new Lake().withCommits(last = 0)
    // The log directory accepts no new name; its directory of temporary files still does.
    Files.setPosixFilePermissions(lake.log, PosixFilePermissions.fromString("r-xr-xr-x"))
    val pending =
      try {
        assume(!Files.isWritable(lake.log), "this user may write to any directory")
        the[CommitPublicationPendingException] thrownBy lake.commit(1)(append(1))
      } finally
        Files.setPosixFilePermissions(lake.log, PosixFilePermissions.fromString("rwxr-xr-x")): Unit

    (pending.table, pending.version) shouldBe ((lake.table, 1L))
    pending.getCause shouldBe an[IOException]
    Files.exists(lake.commitFile(1)) shouldBe false
    // The commit happened: the version is taken, and its bytes are kept.
    val taken = lake.entry(1).value
    taken.complete shouldBe false
    Files.readAllLines(lake.tempOf(taken)).asScala shouldBe append(1)

    // With the storage back, the next writer publishes it before its own.
    lake.commit(2)(append(2))
    lake.committed(1) shouldBe append(1)
    lake.entry(1).value.complete shouldBe true
  }

  // ---------------------------------------------------------------------------------------------
  // Files that are not commits, and what the LogStore refuses to touch
  // ---------------------------------------------------------------------------------------------

  test("only <digits>.json directly under _delta_log is a commit") {
    def version(path: String): Option[Long] = PostgresLogStore.commitVersion(new Path(path))

    version("file:/lake/orders/_delta_log/00000000000000000007.json") shouldBe Some(7L)
    // Delta reads any run of digits as a version, so this is the same commit.
    version("file:/lake/orders/_delta_log/7.json") shouldBe Some(7L)

    Seq(
      "file:/lake/orders/_delta_log/_last_checkpoint",
      "file:/lake/orders/_delta_log/00000000000000000007.crc",
      "file:/lake/orders/_delta_log/00000000000000000007.checkpoint.parquet",
      "file:/lake/orders/_delta_log/00000000000000000007.checkpoint.0a1b2c3d.json",
      "file:/lake/orders/_delta_log/_staged_commits/00000000000000000007.json",
      "file:/lake/orders/part-00000000000000000007.json",
      "file:/lake/orders/00000000000000000007.json",
      "file:/lake/orders/_delta_log/99999999999999999999999.json"
    ).foreach(path => withClue(s"$path: ")(version(path) shouldBe None))
  }

  test("a file that is not a commit is written whole and replaced whole, without the registry") {
    val lake = new Lake
    val pointer = lake.log.resolve("_last_checkpoint")
    val writer = lake.logStore()

    Seq("""{"version":10}""", """{"version":20}""").foreach { content =>
      writer.write(hadoop(pointer), Iterator(content).asJava, true, conf)
      Files.readAllLines(pointer).asScala shouldBe Seq(content)
    }

    lake.store.calls shouldBe empty
    lake.temps shouldBe empty
  }

  test("a file that is not a commit and must not be overwritten is written only if absent") {
    val lake = new Lake
    val checkpoint =
      lake.log.resolve("00000000000000000010.checkpoint.3a0d1b5e-2f5b-4d42-9d0e-5f6c1a2b3c4d.json")
    val writer = lake.logStore()

    writer.write(hadoop(checkpoint), Iterator("the checkpoint").asJava, false, conf)
    val again = the[FileAlreadyExistsException] thrownBy
      writer.write(hadoop(checkpoint), Iterator("another").asJava, false, conf)

    again.getClass shouldBe classOf[FileAlreadyExistsException]
    Files.readAllLines(checkpoint).asScala shouldBe Seq("the checkpoint")
    lake.store.calls shouldBe empty
    // Its content has its final name: the temporary one is gone, both times.
    lake.temps shouldBe empty
  }

  test("a commit is never overwritten") {
    val lake = new Lake().withCommits(last = 0)

    val refused = the[IOException] thrownBy
      lake.logStore().write(hadoop(lake.commitFile(0)), Iterator("rewritten").asJava, true, conf)

    refused.getMessage should include("never rewritten")
    lake.committed(0) shouldBe Creation
  }

  test("a location the LogStore has no storage for is refused before anything is asked") {
    val lake = new Lake
    // file: served by something that is not Hadoop's local file system.
    val replaced = new Configuration(conf)
    replaced.set("fs.file.impl", classOf[LogStorageSpec.RenamingElsewhere].getName)
    replaced.setBoolean("fs.file.impl.disable.cache", true)
    val writer = lake.logStore()

    Seq(lake.commitFile(0), lake.log.resolve("_last_checkpoint")).foreach { file =>
      an[UnsupportedTableLocationException] should be thrownBy
        writer.write(hadoop(file), Creation.iterator.asJava, false, replaced)
      an[UnsupportedTableLocationException] should be thrownBy
        writer.isPartialWriteVisible(hadoop(file), replaced)
    }

    lake.store.calls shouldBe empty
    Using.resource(Files.list(lake.log))(_.iterator.asScala.toList) shouldBe empty
  }

  test("on a local disk a file written in place can be read half written, and Delta is told") {
    val lake = new Lake
    lake.logStore().isPartialWriteVisible(hadoop(lake.commitFile(0)), conf) shouldBe true
  }

  test("the LogStore Delta creates needs its settings for a commit, and not before") {
    val lake = new Lake
    // The constructor Delta calls, with a configuration that has none of the settings.
    val unconfigured = new PostgresLogStore(new Configuration())

    // A file that needs no coordination needs no database.
    val pointer = lake.log.resolve("_last_checkpoint")
    unconfigured.write(hadoop(pointer), Iterator("""{"version":10}""").asJava, true, conf)
    Files.readAllLines(pointer).asScala shouldBe Seq("""{"version":10}""")

    val failure = the[IOException] thrownBy
      unconfigured.write(hadoop(lake.commitFile(0)), Creation.iterator.asJava, false, conf)
    failure.getMessage should include(CoordinationSettings.JdbcUrl)
    failure.getMessage should include(CoordinationSettings.PasswordRef)
    Files.exists(lake.commitFile(0)) shouldBe false
  }

  test("a delay configured at a fault point makes every commit wait there") {
    val lake = new Lake
    val delayed = new Configuration(conf)
    delayed.setLong(FaultHooks.delayKey(FaultPoint.AfterInsert), 150L)
    val writer = LogStores.over(lake.store, conf = delayed)

    val start = System.nanoTime()
    lake.commit(0, writer)(Creation)

    (System.nanoTime() - start) / 1000000 should be >= 140L
    lake.entry(0).value.complete shouldBe true
  }

}

object PostgresLogStoreWriteSpec {

  /** A storage whose first publishes fail, as one with a passing fault does. */
  final private class FailingPublishes(failures: Int) extends LogStorage {
    private val disk = LocalLogStorage.shared
    var publishes = 0

    override def partialWritesVisible: Boolean = disk.partialWritesVisible
    override def writeTemp(temp: Path, lines: Iterator[String]): Unit = disk.writeTemp(temp, lines)
    override def replace(temp: Path, target: Path): Unit = disk.replace(temp, target)
    override def discard(temp: Path): Unit = disk.discard(temp)

    override def publish(temp: Path, target: Path): PublishResult = {
      publishes += 1
      if (publishes <= failures) throw new IOException("injected fault: the storage failed")
      disk.publish(temp, target)
    }
  }
}
