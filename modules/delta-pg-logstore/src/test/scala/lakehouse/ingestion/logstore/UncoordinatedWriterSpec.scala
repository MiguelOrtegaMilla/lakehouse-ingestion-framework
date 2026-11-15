package lakehouse.ingestion.logstore

import java.nio.file.{Files, Path => LocalPath}

import scala.jdk.CollectionConverters._

import io.delta.storage.HDFSLogStore
import lakehouse.ingestion.logstore.coordination.FaultyCoordinationStore._
import lakehouse.ingestion.logstore.coordination.Registration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A writer that commits to a coordinated table without this LogStore (ADR-0004 §3.5, layers 3
 * and 4): its commit is detected, the table is tainted, and nothing of anybody's is replaced.
 *
 * The foreign writer is a real one: `HDFSLogStore`, the LogStore Delta uses for `file://` when
 * none is configured, writing its commit the way Delta has it written, sidecar included.
 */
final class UncoordinatedWriterSpec extends AnyFunSuite with Matchers with LakeFixture {

  import LakeFixture._
  import UncoordinatedWriterSpec._

  /** Commits `version` as a process without this LogStore does. */
  private def foreignCommit(lake: Lake, version: Long): Unit =
    new HDFSLogStore(conf)
      .write(hadoop(lake.commitFile(version)), ForeignActions.iterator.asJava, false, conf)

  private def taintOf(lake: Lake): Option[String] =
    lake.memory.registration(lake.table).flatMap(_.taintReason)

  /** The foreign commit and Hadoop's sidecar for it, as they are on disk. */
  private def onDisk(file: LocalPath) = {
    val sidecar = file.resolveSibling(s".${file.getFileName}.crc")
    (
      Files.readAllBytes(file).toSeq,
      Files.readAllBytes(sidecar).toSeq,
      Files.getLastModifiedTime(file)
    )
  }

  test("F10: a commit made with Delta's default LogStore taints the table for the next reader") {
    val lake = new Lake().withCommits(last = 1)
    foreignCommit(lake, 2)

    // The reader is not stopped: the log can still be read, and somebody has to look at it.
    lake.list() shouldBe Seq(commit(0), commit(1), commit(2))

    val reason = taintOf(lake).value
    reason should include("version 2")
    reason should include("no coordination entry")

    // Our writes are refused from then on, with the reason.
    val refused = the[TableTaintedException] thrownBy lake.commit(3)(append(3))
    refused.reason shouldBe reason
    Files.exists(lake.commitFile(3)) shouldBe false
    lake.entry(3) shouldBe None

    // Readers go on, and a table that is tainted is not tainted again.
    lake.list() shouldBe Seq(commit(0), commit(1), commit(2))
    lake.store.calls.count(_ == Taint) shouldBe 1
  }

  test(
    "F10: a writer that meets the foreign commit before any reader stops, and taints the table"
  ) {
    val lake = new Lake().withCommits(last = 1)
    foreignCommit(lake, 2)
    val tempsBefore = lake.temps

    val detected = the[UncoordinatedWriteException] thrownBy lake.commit(3)(append(3))

    detected.version shouldBe 2L
    taintOf(lake).value should include("version 2")
    Files.exists(lake.commitFile(3)) shouldBe false
    lake.entry(3) shouldBe None
    lake.temps shouldBe tempsBefore

    a[TableTaintedException] should be thrownBy lake.commit(3)(append(3))
  }

  test(
    "F11: a foreign commit that lands between our INSERT and our publish is detected, not replaced"
  ) {
    val lake = new Lake().withCommits(last = 0)
    var asWritten = Option.empty[(Seq[Byte], Seq[Byte], java.nio.file.attribute.FileTime)]
    val foreignWriterWins = LogStores.at(FaultPoint.AfterInsert) {
      foreignCommit(lake, 1)
      asWritten = Some(onDisk(lake.commitFile(1)))
    }

    val detected =
      the[UncoordinatedWriteException] thrownBy
        lake.commit(1, lake.logStore(foreignWriterWins))(append(1))

    // Not a silent loss: the writer is told, and the table is tainted.
    detected.version shouldBe 1L
    val reason = taintOf(lake).value
    reason should include("version 1")
    // The foreign commit and its sidecar are left as found.
    lake.committed(1) shouldBe ForeignActions
    Some(onDisk(lake.commitFile(1))) shouldBe asWritten
    // Our commit is kept too: its entry, and its bytes, for whoever sorts this out.
    val ours = lake.entry(1).value
    ours.complete shouldBe false
    Files.readAllLines(lake.tempOf(ours)).asScala shouldBe append(1)

    // Writers are refused. Readers too: the registry and the log disagree on what version 1 is.
    a[TableTaintedException] should be thrownBy lake.commit(2)(append(2))
    an[UncoordinatedWriteException] should be thrownBy lake.list()
    taintOf(lake).value shouldBe reason
    Some(onDisk(lake.commitFile(1))) shouldBe asWritten
  }

  test("a foreign file under the name of an unfinished commit stops the writer that comes next") {
    val lake = new Lake().withCommits(last = 0)
    lake.crashedAt(FaultPoint.AfterInsert, version = 1)
    foreignCommit(lake, 1)

    val detected = the[UncoordinatedWriteException] thrownBy lake.commit(2)(append(2))

    detected.version shouldBe 1L
    taintOf(lake).value should include("version 1")
    lake.committed(1) shouldBe ForeignActions
    lake.entry(2) shouldBe None
    Files.exists(lake.commitFile(2)) shouldBe false
  }

  test("a commit that a coordinated writer makes while a reader lists is not taken for foreign") {
    val lake = new Lake().withCommits(last = 1)
    // Between the reader's look at the registry and its listing, another writer commits.
    lake.store.afterNext(ReadLatestEntry)(lake.commit(2)(append(2)))

    lake.list() shouldBe Seq(commit(0), commit(1), commit(2))

    taintOf(lake) shouldBe None
  }

  test("commits made before a table was adopted need no entry; the first one after it does") {
    val lake = new Lake
    // The table's life before it was coordinated, written by the default LogStore.
    Seq(40L, 41L).foreach(foreignCommit(lake, _))
    lake.memory.register(lake.table, 41L, None)

    lake.list(from = 40) shouldBe Seq(commit(40), commit(41))
    taintOf(lake) shouldBe None

    foreignCommit(lake, 42)
    lake.list(from = 40) shouldBe Seq(commit(40), commit(41), commit(42))
    taintOf(lake).value should include("version 42")
  }

  test("a foreign commit on a table that was registered and never created taints it") {
    val lake = new Lake
    // Its creator died before its first commit; then somebody created it without the LogStore.
    lake.crashAt(FaultPoint.BeforeInsert, version = 0)
    lake.memory.registration(lake.table).value.registeredVersion shouldBe Registration.NotYetCreated
    foreignCommit(lake, 0)

    lake.list() shouldBe Seq(commit(0))

    taintOf(lake).value should include("version 0")
  }

  test("a table that is not coordinated is never tainted: nothing is expected of its log") {
    val lake = new Lake
    Seq(0L, 1L).foreach(foreignCommit(lake, _))

    lake.list() shouldBe Seq(commit(0), commit(1))

    lake.memory.registration(lake.table) shouldBe None
    lake.store.calls should not contain Taint
  }

  test("when the taint cannot be recorded the detection stands, and the next one records it") {
    val lake = new Lake().withCommits(last = 1)
    foreignCommit(lake, 2)

    // A writer is stopped by what it found, whether or not the registry took the taint.
    lake.store.failNext(Taint, Unreachable)
    val detected = the[UncoordinatedWriteException] thrownBy lake.commit(3)(append(3))
    detected.getSuppressed should have length 1
    taintOf(lake) shouldBe None
    Files.exists(lake.commitFile(3)) shouldBe false

    // A reader goes on, as it does when the taint is recorded.
    lake.store.failNext(Taint, Unreachable)
    lake.list() shouldBe Seq(commit(0), commit(1), commit(2))
    taintOf(lake) shouldBe None

    lake.list() shouldBe Seq(commit(0), commit(1), commit(2))
    taintOf(lake).value should include("version 2")
  }
}

object UncoordinatedWriterSpec {

  private val ForeignActions = Seq(
    """{"commitInfo":{"operation":"WRITE","engineInfo":"another writer"}}"""
  )

  private def commit(version: Long): String = f"$version%020d.json"
}
