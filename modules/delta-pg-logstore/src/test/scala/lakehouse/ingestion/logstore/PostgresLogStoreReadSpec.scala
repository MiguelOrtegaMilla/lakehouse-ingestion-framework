package lakehouse.ingestion.logstore

import java.io.{FileNotFoundException, IOException}
import java.nio.file.Files

import scala.jdk.CollectionConverters._
import scala.util.Using

import lakehouse.ingestion.logstore.coordination.FaultyCoordinationStore._
import lakehouse.ingestion.logstore.storage.LogStorageSpec
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The read path of `PostgresLogStore` and the recovery of unfinished commits (ADR-0004 §3.3,
 * §3.8), through the method Delta lists a log with, on a real table directory.
 *
 * The table and its coordination store are `LakeFixture`'s. A reader is a LogStore of its own,
 * as a process that never wrote the table has. The scenarios named F1 to F17 are those of
 * ADR-0004 §7.1; the integration tests repeat them against a real PostgreSQL.
 */
final class PostgresLogStoreReadSpec extends AnyFunSuite with Matchers with LakeFixture {

  import LakeFixture._
  import PostgresLogStoreReadSpec._

  // ---------------------------------------------------------------------------------------------
  // Listing
  // ---------------------------------------------------------------------------------------------

  test("a log that is up to date is listed as Hadoop lists it, from the name given") {
    val lake = new Lake().withCommits(last = 2)
    val pointer = hadoop(lake.log.resolve("_last_checkpoint"))
    lake.logStore().write(pointer, Iterator("""{"version":2}""").asJava, true, conf)

    lake.list() shouldBe Seq(commit(0), commit(1), commit(2), "_last_checkpoint")
    lake.list(from = 2) shouldBe Seq(commit(2), "_last_checkpoint")
    // The directory of temporary files sorts before every version: no listing shows it.
    lake.list(from = 3) shouldBe Seq("_last_checkpoint")
  }

  test("the registry is asked once for a listing of a log, and never for another directory") {
    val lake = new Lake().withCommits(last = 1)
    val reader = lake.logStore()
    val callsBefore = lake.store.calls.size

    lake.list(by = reader) shouldBe Seq(commit(0), commit(1))
    lake.store.calls.drop(callsBefore) shouldBe Seq(ReadLatestEntry)

    // VACUUM lists the table's data directories through the LogStore.
    Files.createFile(lake.directory.resolve("part-1.parquet"))
    val listed = reader.listFrom(hadoop(lake.directory.resolve("part-0")), conf).asScala
    listed.map(_.getPath.getName).toSeq shouldBe Seq("part-1.parquet")
    lake.store.calls.drop(callsBefore) shouldBe Seq(ReadLatestEntry)

    // So a LogStore that has no settings for the database can still list them.
    val unconfigured = new PostgresLogStore(new Configuration())
    unconfigured.listFrom(hadoop(lake.directory.resolve("part-0")), conf).asScala should have size 1
    an[IOException] should be thrownBy unconfigured.listFrom(hadoop(lake.commitFile(0)), conf)
  }

  test("a log that is not there is FileNotFoundException: for Delta, a table that does not exist") {
    val lake = new Lake
    Files.delete(lake.log)

    a[FileNotFoundException] should be thrownBy lake.list()
  }

  test("the log of a table that is not coordinated is listed as it is") {
    val lake = new Lake
    Seq(0L, 1L).foreach(version => Files.write(lake.commitFile(version), append(version).asJava))

    lake.list() shouldBe Seq(commit(0), commit(1))
    lake.memory.registration(lake.table) shouldBe None
  }

  test("a log on a location without a storage is not listed") {
    val lake = new Lake().withCommits(last = 0)
    val replaced = new Configuration(conf)
    replaced.set("fs.file.impl", classOf[LogStorageSpec.RenamingElsewhere].getName)
    replaced.setBoolean("fs.file.impl.disable.cache", true)
    val callsBefore = lake.store.calls.size

    an[UnsupportedTableLocationException] should be thrownBy
      lake.logStore().listFrom(hadoop(lake.commitFile(0)), replaced)

    lake.store.calls.size shouldBe callsBefore
  }

  // ---------------------------------------------------------------------------------------------
  // Recovery: whoever lists the log finishes the commit of a writer that stopped
  // ---------------------------------------------------------------------------------------------

  test("F2: a commit taken and not published is published by the first reader of the log") {
    val lake = new Lake().withCommits(last = 0)
    val unfinished = lake.crashedAt(FaultPoint.AfterInsert, version = 1)
    Files.exists(lake.commitFile(1)) shouldBe false

    lake.list() shouldBe Seq(commit(0), commit(1))

    lake.committed(1) shouldBe append(1)
    lake.entry(1).value shouldBe unfinished.copy(complete = true)
    Files.isSameFile(lake.tempOf(unfinished), lake.commitFile(1)) shouldBe true
  }

  test("F3: wherever a writer dies, the log holds the whole commit or none of it, and no sidecar") {
    FaultPoint.All.foreach { point =>
      withClue(s"a writer that dies at $point: ") {
        val lake = new Lake().withCommits(last = 0)
        val tempsBefore = lake.temps

        lake.crashAt(point, version = 1)

        // There is no state between not published and published: no half of a commit file.
        val published = Files.exists(lake.commitFile(1))
        published shouldBe (point == FaultPoint.AfterPublish)
        if (published) lake.committed(1) shouldBe append(1)
        // The one new file is the commit's temporary file, whole, and nothing has a sidecar.
        val temp = lake.temps.diff(tempsBefore)
        temp should have size 1
        Files.readAllLines(temp.head).asScala shouldBe append(1)
        allFiles(lake).filter(_.endsWith(".crc")) shouldBe empty

        // A reader then finds the commit, whole, unless its version was never taken.
        val committed = point != FaultPoint.BeforeInsert
        lake.list() shouldBe (if (committed) Seq(commit(0), commit(1)) else Seq(commit(0)))
        lake.entry(1).map(_.complete) shouldBe Option.when(committed)(true)
        if (committed) lake.committed(1) shouldBe append(1)
        allFiles(lake).filter(_.endsWith(".crc")) shouldBe empty
      }
    }
  }

  test("F4: a commit published and not acknowledged is acknowledged by a reader, and not touched") {
    val lake = new Lake().withCommits(last = 0)
    lake.crashedAt(FaultPoint.AfterPublish, version = 1).complete shouldBe false
    def onDisk = (
      Files.readAllBytes(lake.commitFile(1)).toSeq,
      Files.getLastModifiedTime(lake.commitFile(1)),
      Files.getAttribute(lake.commitFile(1), "unix:ino")
    )
    val before = onDisk

    lake.list() shouldBe Seq(commit(0), commit(1))

    lake.entry(1).value.complete shouldBe true
    onDisk shouldBe before
  }

  test("F7: while the store cannot be reached, the log is not listed") {
    val lake = new Lake().withCommits(last = 1)
    val reader = lake.logStore()

    lake.store.down()
    val failure = the[IOException] thrownBy lake.list(by = reader)

    // Not the exception that tells Delta the table does not exist.
    failure should not be a[FileNotFoundException]

    lake.store.up()
    lake.list(by = reader) shouldBe Seq(commit(0), commit(1))
  }

  test("F8: readers that find the same unfinished commit at once all get the log, with it") {
    val lake = new Lake().withCommits(last = 0)
    val unfinished = lake.crashedAt(FaultPoint.AfterInsert, version = 1)
    val tempsBefore = lake.temps
    val readers = Seq.fill(Readers)(lake.logStore())

    val listings = Concurrently.atOnce(readers)(reader => lake.list(by = reader))

    // No reader fails, and none is shown the log without the commit.
    listings.distinct shouldBe Seq(Seq(commit(0), commit(1)))
    lake.committed(1) shouldBe append(1)
    lake.entry(1).value shouldBe unfinished.copy(complete = true)
    Files.isSameFile(lake.tempOf(unfinished), lake.commitFile(1)) shouldBe true
    lake.temps shouldBe tempsBefore
  }

  test("F14: a latest commit that is recorded as published and is not in the log stops readers") {
    val lake = new Lake().withCommits(last = 2)
    Files.delete(lake.commitFile(2))

    val missing = the[CommitMissingException] thrownBy lake.list()

    (missing.table, missing.version) shouldBe ((lake.table, 2L))
    missing should not be a[FileNotFoundException]
  }

  test("an old commit that is gone from the log is the log's cleanup, and stops nobody") {
    val lake = new Lake().withCommits(last = 2)
    Files.delete(lake.commitFile(0))

    lake.list() shouldBe Seq(commit(1), commit(2))
  }

  test("F17: an unfinished commit whose temporary file is gone stops readers, by name") {
    val lake = new Lake().withCommits(last = 0)
    val unfinished = lake.crashedAt(FaultPoint.AfterInsert, version = 1)
    Files.delete(lake.tempOf(unfinished))

    // Every reader, every time: nothing can publish the commit, and nothing may skip it.
    (1 to 2).foreach { _ =>
      val lost = the[UnpublishableCommitException] thrownBy lake.list()
      (lost.table, lost.version, lost.tempPath) shouldBe
        ((lake.table, 1L, unfinished.tempPath))
    }

    Files.exists(lake.commitFile(1)) shouldBe false
    lake.entry(1) shouldBe Some(unfinished)
  }

  test("a commit that cannot be acknowledged is not finished: nothing is read or written past it") {
    val lake = new Lake().withCommits(last = 0)
    lake.crashedAt(FaultPoint.AfterPublish, version = 1)
    val attempts = LogStores.Impatient.attempts

    // The reader publishes nothing new here, yet it must leave the entry complete or fail:
    // unfinished work is only ever looked for at the latest entry.
    lake.store.failNext(Complete, Seq.fill(attempts)(Unreachable): _*)
    an[IOException] should be thrownBy lake.list()
    lake.entry(1).value.complete shouldBe false

    lake.store.failNext(Complete, Seq.fill(attempts)(Unreachable): _*)
    an[IOException] should be thrownBy lake.commit(2)(append(2))
    lake.entry(2) shouldBe None
    Files.exists(lake.commitFile(2)) shouldBe false

    // One lost reply is not a failure: the acknowledgement is repeated.
    lake.store.failNext(Complete, ReplyLost)
    lake.list() shouldBe Seq(commit(0), commit(1))
    lake.entry(1).value.complete shouldBe true
  }

  /** The names of every file under the table's log, temporary files and hidden ones included. */
  private def allFiles(lake: Lake): Seq[String] =
    Using.resource(Files.walk(lake.log)) {
      _.iterator.asScala.filter(Files.isRegularFile(_)).map(_.getFileName.toString).toList
    }
}

object PostgresLogStoreReadSpec {

  private val Readers = 16

  private def commit(version: Long): String = f"$version%020d.json"
}
