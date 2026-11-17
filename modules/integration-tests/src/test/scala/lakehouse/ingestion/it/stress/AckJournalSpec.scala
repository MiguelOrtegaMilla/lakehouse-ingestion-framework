package lakehouse.ingestion.it.stress

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{FileAlreadyExistsException, Files, Path}

import scala.util.Using

import lakehouse.ingestion.it.support.LocalSparkSuite
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class AckJournalSpec extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  private var dir: Option[Path] = None

  override def beforeEach(): Unit = dir = Some(Files.createTempDirectory("ack-journal-"))

  override def afterEach(): Unit = dir.foreach(LocalSparkSuite.deleteRecursively)

  private def journalPath: Path = dir.get.resolve("writer-0.acks")

  private def withRaw(content: String): Path = Files.write(journalPath, content.getBytes(UTF_8))

  test("reads back acknowledgements in order, and the failure that ended the writer") {
    Using.resource(AckJournal.open(journalPath)) { journal =>
      journal.ack(1)
      journal.ack(2)
      journal.fail(3, new java.io.IOException("disk\nfull"))
    }
    AckJournal.read(journalPath) shouldBe AckJournal.Contents(
      Vector(1L, 2L),
      Some(AckJournal.Failure(3, "java.io.IOException: disk full"))
    )
  }

  test("an empty journal acknowledges nothing") {
    Using.resource(AckJournal.open(journalPath))(_ => ())
    AckJournal.read(journalPath) shouldBe AckJournal.Contents(Vector.empty, None)
  }

  test("ignores a trailing fragment, so a cut-short 'ACK 12' never reads as 'ACK 1'") {
    withRaw("ACK 11\nACK 1")
    AckJournal.read(journalPath).acked shouldBe Vector(11L)
  }

  test("a journal reopened after a kill goes on after its entries, and drops one cut short") {
    Using.resource(AckJournal.open(journalPath)) { journal =>
      journal.ack(1)
      journal.ack(2)
    }
    // The writer was killed while it wrote its third entry.
    Files.write(journalPath, "ACK".getBytes(UTF_8), java.nio.file.StandardOpenOption.APPEND)

    Using.resource(AckJournal.reopen(journalPath)) { journal =>
      journal.ack(3)
      journal.ack(4)
    }

    AckJournal.read(journalPath) shouldBe AckJournal.Contents(Vector(1L, 2L, 3L, 4L), None)
    new String(Files.readAllBytes(journalPath), UTF_8) shouldBe "ACK 1\nACK 2\nACK 3\nACK 4\n"
  }

  test("refuses to open a journal that already exists") {
    withRaw("")
    an[FileAlreadyExistsException] should be thrownBy AckJournal.open(journalPath)
  }

  test("rejects malformed entries instead of guessing") {
    Seq("ACK\n", "ACK x\n", "ACK 0\n", "DONE 1\n", "FAIL 2\n", "FAIL 1 e: m\nACK 2\n").foreach {
      content =>
        withRaw(content)
        withClue(s"${content.replace("\n", "\\n")}: ") {
          an[IllegalStateException] should be thrownBy AckJournal.read(journalPath)
        }
    }
  }
}
