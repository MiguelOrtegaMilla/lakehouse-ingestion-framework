package lakehouse.ingestion.it.ingestion

import java.io.{FileInputStream, FileOutputStream, IOException}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.{BasicFileAttributeView, FileTime, PosixFilePermissions}
import java.nio.file.{Files, LinkOption, Path}
import java.time.Instant
import java.util.zip.GZIPOutputStream

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Using

import lakehouse.ingestion.core.config.{Profile, Source}
import lakehouse.ingestion.core.control.{
  BatchSeq,
  BatchStatus,
  FailureClass,
  OperatorRequest,
  RunStatus
}
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.connector.UnreadableFilesException.Unreadable
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A file that cannot be read stops its source, or is skipped and recorded, as the source says
 * (ADR-0023): on files broken for real, real Delta on the coordinated LogStore, and a real
 * control plane. A file of Parquet cut short, a file of CSV in gzip cut short and a file that the
 * process may not open, under each policy; a symbolic link out of the landing zone to a pipe,
 * which nothing may open; a file whose read fails once and then reads; a failure that is no
 * file's; and a reprocess, which skips nothing. Each test ends with the ledger of the source.
 *
 * A test of a file that may not be opened is cancelled for a user that may open any file (root).
 */
final class UnreadableFilesSpec extends AnyFunSuite with Matchers with IngestionSuite {

  private val Skip = ", on_unreadable_file: skip"

  /** Later than every file of a test: a file written again, or touched, then. */
  private val later = anHourAgo.plusSeconds(60)

  // ---------------------------------------------------------------------------------------------
  // Files, broken for real
  // ---------------------------------------------------------------------------------------------

  private def setTime(path: Path, at: Instant): Path = {
    Files.setLastModifiedTime(path, FileTime.from(at))
    path
  }

  /** A file of Parquet of the source, of ids as text, written by Spark as one file. */
  private def dropParquet(of: Source, name: String, ids: Range, at: Instant = anHourAgo): Path = {
    val session = spark
    import session.implicits._
    val written = workDir.resolve(s"written-${of.id.value}-$name-${at.toEpochMilli}")
    ids
      .map(i => (i.toString, s"name $i"))
      .toDF("id", "name")
      .coalesce(1)
      .write
      .parquet(written.toString)
    val part = Using.resource(Files.list(written)) {
      _.iterator().asScala.find(_.getFileName.toString.endsWith(".parquet")).get
    }
    val target = directoryOf(of).resolve(name)
    Files.deleteIfExists(target)
    setTime(Files.move(part, target), at)
  }

  /** A file of CSV of the source in gzip, of the ids given. */
  private def dropGzip(of: Source, name: String, ids: Range, at: Instant = anHourAgo): Path = {
    val path = directoryOf(of).resolve(name)
    Using.resource(new GZIPOutputStream(Files.newOutputStream(path))) {
      _.write(("id,name\n" + ids.map(i => s"$i,name $i").mkString("", "\n", "\n")).getBytes(UTF_8))
    }
    setTime(path, at)
  }

  /** The file with its second half gone, as a copy that stopped halfway leaves it. */
  private def cutShort(file: Path): Path = {
    val at = Files.getLastModifiedTime(file)
    val bytes = Files.readAllBytes(file)
    Files.write(file, bytes.take(bytes.length / 2))
    Files.setLastModifiedTime(file, at)
    file
  }

  /** `body` with the file one that this process may not open, and then as it was. */
  private def unopenable[A](file: Path)(body: => A): A = {
    val permissions = Files.getPosixFilePermissions(file)
    Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("---------"))
    try {
      assume(!Files.isReadable(file), "the tests run as a user that may open any file")
      body
    } finally Files.setPosixFilePermissions(file, permissions): Unit
  }

  /**
   * A file that cannot be read, beside a sound one, `a`, of the same format.
   *
   * @param broken makes it, and says how to make it one that reads: written again with another
   *   time, for a file that is cut short
   * @param closed whether it is one that may not be opened, for as long as a run reads it
   */
  final private class Broken(
      val what: String,
      val format: String,
      val name: String,
      val sound: Source => Unit,
      val broken: Source => Path,
      val writtenAgain: Source => Unit,
      val closed: Boolean = false
  )

  private val cases = Seq(
    new Broken(
      "a file of Parquet cut short",
      "parquet",
      "b.parquet",
      of => dropParquet(of, "a.parquet", 1 to 1): Unit,
      of => cutShort(dropParquet(of, "b.parquet", 2 to 1001)),
      of => dropParquet(of, "b.parquet", 2 to 1001, at = later): Unit
    ),
    new Broken(
      "a file of CSV in gzip cut short",
      "csv",
      "b.csv.gz",
      of => drop(of, "a.csv", "id,name\n1,name 1\n"): Unit,
      of => cutShort(dropGzip(of, "b.csv.gz", 2 to 2001)),
      of => dropGzip(of, "b.csv.gz", 2 to 2001, at = later): Unit
    ),
    new Broken(
      "a file that may not be opened",
      "csv",
      "b.csv",
      of => drop(of, "a.csv", "id,name\n1,name 1\n"): Unit,
      of => drop(of, "b.csv", "id,name\n2,name 2\n"),
      // Touched: its permissions are back once the run that may not open it has ended.
      of => setTime(directoryOf(of).resolve("b.csv"), later): Unit,
      closed = true
    )
  )

  /** A run of the source, with the file shut for its length if it is one that may not be opened. */
  private def launchWith(c: Broken, file: Path, of: Source): Outcome =
    if (c.closed) unopenable(file)(launch(of)) else launch(of)

  private def ids(of: Source): Seq[String] = rowsOf(of).map(_._1).distinct

  // ---------------------------------------------------------------------------------------------
  // Under fail, the default
  // ---------------------------------------------------------------------------------------------

  cases.foreach { c =>
    test(s"${c.what} fails its run as permanent, naming it, and the batch stays planned") {
      val of = fileSource(c.format)
      c.sound(of)
      val file = c.broken(of)

      launchWith(c, file, of) match {
        case Outcome.Failed(run, FailureClass.Permanent, message) =>
          message should include(s"has 1 file that cannot be read, and nothing of it was written")
          message should include(s"'${c.name}' unreadable: it failed each of 3 reads of it alone")
          message should include("connector.on_unreadable_file: fail")
          runOf(run).status shouldBe RunStatus.Failed
        case other => fail(s"the run did not fail as permanent: $other")
      }
      batchesOf(of) shouldBe Seq(1L -> "PLANNED")
      failedOf(of) shouldBe empty
      takenOf(of) shouldBe empty

      if (c.closed) {
        // It may be opened again: the next run reads the batch as it was planned.
        ingested(launch(of)).batch.seq shouldBe BatchSeq.First
      } else {
        // Written again, it is another file: the plan is given up (ADR-0018), and the next
        // takes it.
        c.writtenAgain(of)
        launch(of) shouldBe a[Outcome.GivenUp]
        ingested(launch(of)).batch.seq.value shouldBe 2L
      }
      ids(of) should contain allOf ("1", "2")
      failedOf(of) shouldBe empty
      assertLedger(of)
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Under skip
  // ---------------------------------------------------------------------------------------------

  cases.foreach { c =>
    test(s"${c.what} is skipped and recorded, the rest is ingested, and once written again taken") {
      val of = fileSource(c.format, options = Skip)
      c.sound(of)
      val file = c.broken(of)

      val (run, why) = launchWith(c, file, of) match {
        case Outcome.FilesSkipped(run, batch, Seq(Unreadable(item, why))) =>
          item shouldBe c.name
          why should startWith("unreadable: it failed each of 3 reads of it alone: ")
          batch.seq shouldBe BatchSeq.First
          run -> why
        case other => fail(s"no file was skipped: $other")
      }
      // The run did what its source says; the batch was given up for the file, which says why.
      runOf(run).status shouldBe RunStatus.Succeeded
      val givenUp = batchOf(batchId(of, 1))
      givenUp.status shouldBe BatchStatus.Abandoned
      givenUp.abandoned.value.by shouldBe Some(run)
      givenUp.abandoned.value.reason should include(s"'${c.name}' unreadable")
      failedOf(of) shouldBe Seq((c.name, 1L, why))

      // The next plan leaves it out, and takes the rest.
      ingested(launch(of)).batch.seq.value shouldBe 2L
      ids(of) shouldBe Seq("1")
      launch(of) shouldBe a[Outcome.NothingNew]
      assertLedger(of)

      // Written again, or touched, it is another file, and is taken; what failed stays recorded.
      c.writtenAgain(of)
      ingested(launch(of)).batch.seq.value shouldBe 3L
      ids(of) should contain allOf ("1", "2")
      failedOf(of) shouldBe Seq((c.name, 1L, why))
      takenOf(of).map(file => file._1 -> file._3) should contain(c.name -> 3L)
      assertLedger(of)
    }
  }

  /**
   * A named pipe out of the landing zone, and whether anything opened it. A reader of a pipe
   * waits for a writer, and a writer for a reader: the writer here is a thread that waits for a
   * reader again each time one comes, so that a reader of the engine's is let through, as many
   * times as it opens the pipe, and is seen. The test is the last reader, which ends the writer.
   */
  final private class Watched(val path: Path) {
    @volatile private var ours = false
    @volatile private var opened = 0
    private val writer = new Thread(() =>
      while (!ours) {
        try Using.resource(new FileOutputStream(path.toFile))(_ => ())
        catch { case _: IOException => () }
        if (!ours) opened += 1
      }
    )
    writer.setDaemon(true)
    writer.start()

    /** How many times anything but this opened the pipe; the writer ends, once asked. */
    def openings(): Int = {
      ours = true
      val reader = new Thread(() =>
        try Using.resource(new FileInputStream(path.toFile))(_ => ())
        catch { case _: IOException => () }
      )
      reader.setDaemon(true)
      reader.start()
      writer.join(10000)
      writer.isAlive shouldBe false
      opened
    }
  }

  test("a link out of the landing zone is planned, recorded, and never opened, nor read alone") {
    val of = fileSource(options = Skip)
    drop(of, "a.csv", "id\n1\n")
    cutShort(dropGzip(of, "c.csv.gz", 2 to 2001))
    val pipe = Files.createDirectories(workDir.resolve(s"outside-${of.id.value}")).resolve("pipe")
    new ProcessBuilder("mkfifo", pipe.toString).inheritIO().start().waitFor() shouldBe 0
    val watched = new Watched(pipe)
    val link = Files.createSymbolicLink(directoryOf(of).resolve("b.csv"), pipe)
    // Complete by its own time, the only one that the landing zone looks at.
    Files
      .getFileAttributeView(link, classOf[BasicFileAttributeView], LinkOption.NOFOLLOW_LINKS)
      .setTimes(FileTime.from(anHourAgo), null, null)

    // The plan has it, and the look before the read refuses it: nothing of the batch is read.
    launch(of) match {
      case Outcome.FilesSkipped(_, batch, skipped) =>
        batch.seq shouldBe BatchSeq.First
        skipped shouldBe Seq(
          Unreadable(
            "b.csv",
            "path_outside_root: it leads out of the landing zone, through a symbolic link, and " +
              "was not opened"
          )
        )
      case other => fail(s"the link was not skipped: $other")
    }
    // The next plan has no link, and its read fails for the file cut short, which is read alone.
    launch(of) match {
      case Outcome.FilesSkipped(_, batch, Seq(Unreadable("c.csv.gz", _))) =>
        batch.seq.value shouldBe 2L
      case other => fail(s"the file cut short was not skipped: $other")
    }
    ingested(launch(of)).batch.seq.value shouldBe 3L
    ids(of) shouldBe Seq("1")
    failedOf(of).map(file => file._1 -> file._2) shouldBe Seq("b.csv" -> 1L, "c.csv.gz" -> 2L)
    failedOf(of).head._3 should not include pipe.toString

    watched.openings() shouldBe 0
    assertLedger(of)
  }

  /**
   * A file that the process may not open until two seconds after its first read alone, as a
   * mount that takes a few seconds to come back: it is given back by a thread of its own, by the
   * time and not by a read. The moment of each read of it alone is kept.
   */
  final private class AwayForAWhile(file: Path) {
    private val permissions = Files.getPosixFilePermissions(file)
    @volatile var reads = Vector.empty[Long]
    private var back = Option.empty[Thread]

    def beforeAlone(name: String, n: Int): Unit =
      if (file.getFileName.toString == name) {
        reads :+= System.nanoTime()
        if (n == 1) {
          val thread = new Thread(() => {
            Thread.sleep(2000)
            Files.setPosixFilePermissions(file, permissions): Unit
          })
          thread.setDaemon(true)
          thread.start()
          back = Some(thread)
        }
      }

    def awaitBack(): Unit = back.foreach(_.join(10000))

    /** How far apart the reads alone were. */
    def gaps: Vector[FiniteDuration] =
      reads.zip(reads.drop(1)).map { case (a, b) => (b - a).nanos }
  }

  test("a file that may not be opened for two seconds is waited for, within the budget") {
    val of = fileSource(options = Skip)
    drop(of, "a.csv", "id\n1\n")
    val closed = drop(of, "b.csv", "id\n2\n")
    val away = new AwayForAWhile(closed)
    val by = service(
      tries =
        Profile.Retries(budget = 30.seconds, firstBackoff = 100.millis, maxBackoff = 100.millis),
      beforeAlone = away.beforeAlone
    )

    val done = unopenable(closed)(ingested(launch(of, by)))
    away.awaitBack()

    // Read alone, it failed twice, 1 s and 2 s apart, and then read: the run tried again.
    away.gaps should have size 2
    away.gaps(0) should be >= 1.second
    away.gaps(1) should be >= 2.seconds
    done.batch.seq shouldBe BatchSeq.First
    runOf(done.run).counts.retries shouldBe Some(1)
    ids(of) shouldBe Seq("1", "2")
    failedOf(of) shouldBe empty
    assertLedger(of)
  }

  test("a run with no budget left does not wait: a file away for two seconds is recorded") {
    // The known limit (ADR-0023): what is away for longer than the waits is taken for the file's.
    val of = fileSource(options = Skip)
    drop(of, "a.csv", "id\n1\n")
    val closed = drop(of, "b.csv", "id\n2\n")
    val away = new AwayForAWhile(closed)

    val skipped = unopenable(closed) {
      val outcome = launch(of, service(beforeAlone = away.beforeAlone))
      away.awaitBack()
      outcome
    }

    skipped match {
      case Outcome.FilesSkipped(_, _, Seq(Unreadable("b.csv", _))) => ()
      case other => fail(s"the file was not skipped: $other")
    }
    away.gaps.foreach(_ should be < 1.second)
    // Back by now, and the same file: it is not taken again until it is touched.
    ingested(launch(of)).batch.seq.value shouldBe 2L
    launch(of) shouldBe a[Outcome.NothingNew]
    setTime(closed, later)
    ingested(launch(of)).batch.seq.value shouldBe 3L
    ids(of) shouldBe Seq("1", "2")
    assertLedger(of)
  }

  test("files of Parquet with a column of two types fail the run as permanent, recording none") {
    val of = fileSource("parquet", options = Skip)
    dropParquet(of, "a.parquet", 1 to 1)
    // A column of two types: each file reads alone, and the two fail together, whatever the
    // policy of the source, as no file of them is one to skip.
    val session = spark
    import session.implicits._
    val written = workDir.resolve(s"written-${of.id.value}-numbers")
    Seq((2L, "name 2")).toDF("id", "name").coalesce(1).write.parquet(written.toString)
    val part = Using.resource(Files.list(written)) {
      _.iterator().asScala.find(_.getFileName.toString.endsWith(".parquet")).get
    }
    setTime(Files.move(part, directoryOf(of).resolve("b.parquet")), anHourAgo)

    launch(of) match {
      case Outcome.Failed(_, failure, message) =>
        failure shouldBe FailureClass.Permanent
        message should include("a column of one type in one file and of another in another")
      case other => fail(s"the run did not fail: $other")
    }
    batchesOf(of) shouldBe Seq(1L -> "PLANNED")
    failedOf(of) shouldBe empty

    // As the failure says: the file written again with the type of the other, the plan is
    // given up (ADR-0018), and the next takes both.
    dropParquet(of, "b.parquet", 2 to 2, at = later)
    launch(of) shouldBe a[Outcome.GivenUp]
    ingested(launch(of)).batch.seq.value shouldBe 2L
    ids(of) should contain allOf ("1", "2")
    failedOf(of) shouldBe empty
    assertLedger(of)
  }

  test("a reprocess skips nothing: a file of it that cannot be read fails it as permanent") {
    val of = fileSource(options = Skip)
    val a = drop(of, "a.csv", "id\n1\n")
    ingested(launch(of))
    val asked = OperatorRequest.of("ops.oncall", "the parser was fixed").toOption.get

    val refused = unopenable(a) {
      service().reprocess(of, versionOf(of), BatchSeq.First, asked)
    }
    refused match {
      case Outcome.Failed(_, FailureClass.Permanent, message) =>
        message should include("a reprocess skips none of them")
        message should include("'a.csv' unreadable")
      case other => fail(s"the reprocess did not fail as permanent: $other")
    }
    batchesOf(of) shouldBe Seq(1L -> "COMMITTED", 2L -> "PLANNED")
    failedOf(of) shouldBe empty

    // Once it may be read, the next run finishes the reprocess.
    ingested(launch(of)).batch.seq.value shouldBe 2L
    assertLedger(of)
  }
}
