package lakehouse.ingestion.logstore.storage

import java.io.IOException
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{FileAlreadyExistsException, Files, Path => LocalPath}
import java.util
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Using

import lakehouse.ingestion.logstore.ScratchDirectories
import lakehouse.ingestion.logstore.storage.PublishResult._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ChecksumException, FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Log files on a local disk (ADR-0015): whole or absent, on disk before they are reported,
 * never replaced by a publish, and without the sidecars that make a checksum-verifying reader
 * refuse a file.
 *
 * Everything here happens on a real file system. The readers that race the writers are real
 * threads, and the two kinds of reader a table has are Hadoop's own file systems. Only "forced
 * to disk" is observed and not provoked: a test cannot cut the power.
 */
final class LocalLogStorageSpec extends AnyFunSuite with Matchers with ScratchDirectories {

  import LocalLogStorageSpec._

  private val storage = new LocalLogStorage

  /** Hadoop's local file system: it verifies a file against its `.crc` sidecar, if it has one. */
  private val checked = FileSystem.getLocal(new Configuration())

  /** A reader that verifies nothing, as on object storage. */
  private val unchecked = checked.getRawFileSystem

  /** The `_delta_log` of a table, as Delta leaves it before the first commit: empty. */
  private def newLog(): LocalPath =
    Files.createDirectories(newDirectory().resolve("orders/_delta_log"))

  /** A temporary path as the protocol names them: `.tmp/<file>.<unique>`. */
  private def tempFor(target: LocalPath): LocalPath =
    target.resolveSibling(s".tmp/${target.getFileName}.${UUID.randomUUID()}")

  private def writeTemp(target: LocalPath, lines: Seq[String]): LocalPath = {
    val temp = tempFor(target)
    storage.writeTemp(hadoop(temp), lines.iterator)
    temp
  }

  private def publish(temp: LocalPath, target: LocalPath): PublishResult =
    storage.publish(hadoop(temp), hadoop(target))

  private def linesOf(reader: FileSystem, file: LocalPath): Seq[String] =
    Using
      .resource(reader.open(hadoop(file)))(in => new String(in.readAllBytes(), UTF_8))
      .split('\n')
      .toSeq

  /** Writes `file` as a writer without this LogStore does: through Hadoop, with a sidecar. */
  private def writeThroughHadoop(file: LocalPath, content: String): LocalPath = {
    Using.resource(checked.create(hadoop(file)))(_.write(content.getBytes(UTF_8)))
    val sidecar = sidecarOf(file)
    Files.exists(sidecar) shouldBe true
    sidecar
  }

  test("a temporary file holds the lines given, each ended by a line break, and no sidecar") {
    val log = newLog()
    val temp = tempFor(log.resolve(commit(0)))

    // The directory of temporary files is made by the first one.
    storage.writeTemp(
      hadoop(temp),
      Iterator("""{"commitInfo":{}}""", """{"add":"ñandú.parquet"}""")
    )

    new String(Files.readAllBytes(temp), UTF_8) shouldBe
      "{\"commitInfo\":{}}\n{\"add\":\"ñandú.parquet\"}\n"
    filesUnder(log) shouldBe Seq(temp)
  }

  test("a temporary file and its name are on disk before the write returns") {
    val log = newLog()
    val spy = new ForcedToDisk
    val first = tempFor(log.resolve(commit(0)))
    val second = tempFor(log.resolve(commit(1)))

    spy.storage.writeTemp(hadoop(first), Iterator("a", "b"))
    spy.forced shouldBe Seq(
      Forced(first, "a\nb\n"),
      Forced(first.getParent, first.getFileName.toString),
      // The directory was new: its own name, in the directory above.
      Forced(log, ".tmp")
    )

    spy.forced.clear()
    spy.storage.writeTemp(hadoop(second), Iterator("c"))
    spy.forced shouldBe Seq(
      Forced(second, "c\n"),
      Forced(second.getParent, Seq(first, second).map(_.getFileName.toString).sorted.mkString(" "))
    )
  }

  test("a temporary file that is already there is an error, and not Delta's 'version taken'") {
    val log = newLog()
    val temp = writeTemp(log.resolve(commit(0)), Seq("the first attempt"))

    val failure = the[IOException] thrownBy storage.writeTemp(hadoop(temp), Iterator("another"))

    failure should not be a[FileAlreadyExistsException]
    failure.getMessage should include(temp.toString)
    Files.readAllLines(temp).asScala shouldBe Seq("the first attempt")
  }

  test("a write that fails leaves no file behind") {
    val log = newLog()
    val temp = tempFor(log.resolve(commit(0)))
    val lines =
      Iterator("one", "two") ++ Iterator.continually(throw new IllegalStateException("no more"))

    an[IllegalStateException] should be thrownBy storage.writeTemp(hadoop(temp), lines)

    filesUnder(log) shouldBe empty
  }

  test("publishing gives the commit its name: the same file, whole, under two names") {
    val log = newLog()
    val target = log.resolve(commit(0))
    val temp = writeTemp(target, Seq("add a", "add b"))

    publish(temp, target) shouldBe Published

    Files.isSameFile(temp, target) shouldBe true
    filesUnder(log) should contain theSameElementsAs Seq(temp, target)
    Seq(checked, unchecked).foreach(linesOf(_, target) shouldBe Seq("add a", "add b"))
  }

  test("publishing a commit that is already published changes nothing") {
    val log = newLog()
    val target = log.resolve(commit(0))
    val temp = writeTemp(target, Seq("add a"))
    publish(temp, target) shouldBe Published
    val modified = Files.getLastModifiedTime(target)

    // A second recoverer, or the writer after a recoverer.
    publish(temp, target) shouldBe AlreadyPublished

    Files.isSameFile(temp, target) shouldBe true
    Files.getLastModifiedTime(target) shouldBe modified

    // The same bytes in another file, as after a table is restored from a copy: still this commit.
    val restored = log.resolve(commit(1))
    val itsTemp = writeTemp(restored, Seq("add b"))
    Files.copy(itsTemp, restored)
    publish(itsTemp, restored) shouldBe AlreadyPublished
    Files.isSameFile(itsTemp, restored) shouldBe false
  }

  test("a foreign writer's file is never replaced: it and its sidecar are left as found") {
    val log = newLog()
    val target = log.resolve(commit(3))
    val sidecar = writeThroughHadoop(target, "a foreign commit\n")
    def found = (
      Files.readAllBytes(target).toSeq,
      Files.readAllBytes(sidecar).toSeq,
      Files.getLastModifiedTime(target)
    )
    val before = found
    val temp = writeTemp(target, Seq("our commit"))

    publish(temp, target) shouldBe ForeignFile

    found shouldBe before
    // Both commits can still be read, by the operator who has to tell what happened.
    linesOf(checked, target) shouldBe Seq("a foreign commit")
    Files.readAllLines(temp).asScala shouldBe Seq("our commit")
  }

  test("a temporary file that is gone cannot be published, and nothing is created") {
    val log = newLog()
    val target = log.resolve(commit(0))
    val temp = writeTemp(target, Seq("add a"))
    Files.delete(temp)

    publish(temp, target) shouldBe TempFileLost
    Files.exists(target) shouldBe false

    // Nor is a file that is already under the name taken for the lost one.
    Files.write(target, "somebody's commit\n".getBytes(UTF_8))
    publish(temp, target) shouldBe TempFileLost
  }

  test("a published name is on disk before the publish returns, whoever made it") {
    val log = newLog()
    val spy = new ForcedToDisk
    val target = log.resolve(commit(0))
    val temp = writeTemp(target, Seq("add a"))
    val named = Seq(Forced(log, s".tmp ${commit(0)}"))

    spy.storage.publish(hadoop(temp), hadoop(target)) shouldBe Published
    spy.forced shouldBe named

    // The process that made the link may have died before forcing it.
    spy.forced.clear()
    spy.storage.publish(hadoop(temp), hadoop(target)) shouldBe AlreadyPublished
    spy.forced shouldBe named

    spy.forced.clear()
    val other = writeTemp(target, Seq("add b"))
    spy.storage.publish(hadoop(other), hadoop(target)) shouldBe ForeignFile
    spy.storage.publish(hadoop(tempFor(target)), hadoop(target)) shouldBe TempFileLost
    spy.forced shouldBe empty
  }

  test("of many recoverers that publish one commit at once, one names it and the rest find it") {
    val log = newLog()
    val target = log.resolve(commit(0))
    val temp = writeTemp(target, Seq("add a"))

    val results = atOnce(Seq.fill(Contenders)(temp))(publish(_, target))

    results.count(_ == Published) shouldBe 1
    results.filterNot(_ == Published).distinct shouldBe Seq(AlreadyPublished)
    Files.isSameFile(temp, target) shouldBe true
  }

  test("of many files published under one name at once, one gets it and it is not replaced") {
    val log = newLog()
    val target = log.resolve(commit(0))
    val temps = (1 to Contenders).map(writer => writeTemp(target, Seq(s"the commit of $writer")))

    val results = atOnce(temps)(publish(_, target))

    val winners = temps.zip(results).collect { case (temp, Published) => temp }
    winners should have size 1
    results.filterNot(_ == Published).distinct shouldBe Seq(ForeignFile)
    Files.isSameFile(winners.head, target) shouldBe true
  }

  test("a reader never finds part of a commit: the name appears with the whole file") {
    val log = newLog()
    val lines = Seq.fill(40000)("x" * 99)
    val whole = lines.size * 100L
    val targets = (0L until 25L).map(version => log.resolve(commit(version)))

    val partial = whileReading {
      targets.collectFirst {
        case target if Files.exists(target) && Files.size(target) != whole => target
      }
    } {
      targets.foreach(target => publish(writeTemp(target, lines), target) shouldBe Published)
    }

    partial shouldBe None
  }

  test("replacing a file is one step: a reader finds the old file or the new one, whole") {
    val log = newLog()
    val target = log.resolve("_last_checkpoint")
    val versions = Seq(Seq.fill(20000)("a" * 49), Seq.fill(30000)("b" * 49))
    val expected = versions.map(lines => lines.mkString("", "\n", "\n").getBytes(UTF_8))
    storage.replace(hadoop(writeTemp(target, versions.head)), hadoop(target))

    val neither = whileReading {
      val found = Files.readAllBytes(target)
      Option.when(!expected.exists(util.Arrays.equals(_, found)))(s"${found.length} bytes")
    } {
      (1 to 40).foreach { round =>
        storage.replace(hadoop(writeTemp(target, versions(round % 2))), hadoop(target))
      }
    }

    neither shouldBe None
    // The temporary files were used up.
    filesUnder(log) shouldBe Seq(target)
  }

  test("a replaced name is on disk before the replace returns, also the first time") {
    val log = newLog()
    val spy = new ForcedToDisk
    val target = log.resolve("_last_checkpoint")

    Seq("version 10", "version 20").foreach { content =>
      val temp = writeTemp(target, Seq(content))
      spy.forced.clear()

      spy.storage.replace(hadoop(temp), hadoop(target))

      spy.forced shouldBe Seq(Forced(log, ".tmp _last_checkpoint"))
      Seq(checked, unchecked).foreach(linesOf(_, target) shouldBe Seq(content))
    }
  }

  test("replacing a file that Hadoop wrote takes its sidecar away: both readers read the new one") {
    val log = newLog()
    val content = Seq("old ", "new ").map(_ * 400)

    // Why: under the old sidecar, a reader that verifies it refuses the new content.
    val unreadable = log.resolve("_last_vacuum_info")
    writeThroughHadoop(unreadable, content.head)
    Files.write(unreadable, content.last.getBytes(UTF_8))
    a[ChecksumException] should be thrownBy linesOf(checked, unreadable)

    val target = log.resolve("_last_checkpoint")
    val sidecar = writeThroughHadoop(target, content.head)

    storage.replace(hadoop(writeTemp(target, Seq(content.last))), hadoop(target))

    Files.exists(sidecar) shouldBe false
    Seq(checked, unchecked).foreach(linesOf(_, target) shouldBe Seq(content.last))
  }

  test("nothing written here has a sidecar") {
    val log = newLog()
    val commitFile = log.resolve(commit(0))
    val pointer = log.resolve("_last_checkpoint")

    publish(writeTemp(commitFile, Seq("add a")), commitFile) shouldBe Published
    storage.replace(hadoop(writeTemp(pointer, Seq("version 0"))), hadoop(pointer))
    storage.replace(hadoop(writeTemp(pointer, Seq("version 10"))), hadoop(pointer))

    filesUnder(log).filter(_.getFileName.toString.endsWith(".crc")) shouldBe empty
  }

  test("a discarded temporary file is gone; the file it was published as stays") {
    val log = newLog()
    val target = log.resolve("00000000000000000010.checkpoint.json")
    val temp = writeTemp(target, Seq("the checkpoint"))
    publish(temp, target) shouldBe Published

    storage.discard(hadoop(temp))

    filesUnder(log) shouldBe Seq(target)
    Seq(checked, unchecked).foreach(linesOf(_, target) shouldBe Seq("the checkpoint"))
    // Discarding what is not there is not an error: two writers may clean up the same litter.
    storage.discard(hadoop(temp))
  }

  test("a file written in place on a local disk can be read before it is complete") {
    storage.partialWritesVisible shouldBe true
  }

  /**
   * Runs `write` while another thread calls `look` without pause.
   *
   * @return the first thing `look` reported, if it ever reported anything
   */
  private def whileReading[A](look: => Option[A])(write: => Unit): Option[A] = {
    val reader = Executors.newSingleThreadExecutor()
    val written = new AtomicBoolean(false)
    try {
      val reading = Future {
        var seen = Option.empty[A]
        var looks = 0L
        while (seen.isEmpty && !written.get) {
          seen = look
          looks += 1
        }
        (seen, looks)
      }(ExecutionContext.fromExecutor(reader))
      try write
      finally written.set(true)
      val (seen, looks) = Await.result(reading, WaitSeconds.seconds)
      // Otherwise the reader saw nothing because it never looked.
      looks should be > 100L
      seen
    } finally reader.shutdownNow(): Unit
  }

  /** Runs `call` for every input on its own thread, all released at the same moment. */
  private def atOnce[A, B](inputs: Seq[A])(call: A => B): Seq[B] = {
    val pool = Executors.newFixedThreadPool(inputs.size)
    implicit val threads: ExecutionContext = ExecutionContext.fromExecutor(pool)
    val start = new CountDownLatch(1)
    try {
      val calls = inputs.map { input =>
        Future {
          start.await(WaitSeconds, TimeUnit.SECONDS)
          call(input)
        }
      }
      start.countDown()
      Await.result(Future.sequence(calls), WaitSeconds.seconds)
    } finally pool.shutdownNow(): Unit
  }
}

object LocalLogStorageSpec {

  private val Contenders = 16
  private val WaitSeconds = 60L

  private def hadoop(path: LocalPath): Path = new Path(LocalLogStorage.Scheme, null, path.toString)

  private def commit(version: Long): String = f"$version%020d.json"

  private def sidecarOf(file: LocalPath): LocalPath =
    file.resolveSibling(s".${file.getFileName}.crc")

  /** Every file under `root`, sidecars and temporary files included. */
  private def filesUnder(root: LocalPath): Seq[LocalPath] =
    Using.resource(Files.walk(root))(_.iterator.asScala.filter(Files.isRegularFile(_)).toList)

  /**
   * @param held what was there when it was forced: a file's content, or the names in a
   *   directory
   */
  final private case class Forced(path: LocalPath, held: String)

  /** A storage that forces nothing, and records what it was asked to force and when. */
  final private class ForcedToDisk {
    val forced = mutable.ArrayBuffer.empty[Forced]

    val storage = new LocalLogStorage(path =>
      forced += Forced(
        path,
        if (Files.isDirectory(path))
          Using.resource(Files.list(path))(
            _.iterator.asScala.map(_.getFileName.toString).toList.sorted.mkString(" ")
          )
        else new String(Files.readAllBytes(path), UTF_8)
      )
    )
  }
}
