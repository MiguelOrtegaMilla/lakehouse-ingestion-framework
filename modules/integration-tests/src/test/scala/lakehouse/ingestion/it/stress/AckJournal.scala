package lakehouse.ingestion.it.stress

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, StandardOpenOption}

/**
 * A writer's record of what it believes it committed: one line per `txnVersion` whose Delta
 * write returned without error, appended and forced to disk before the next commit starts.
 *
 * A lost commit leaves no trace in the Delta log: it is simply absent. Only the comparison
 * between this journal and the log can reveal it, so the journal is the evidence that the
 * writer "saw success" (ADR-0004 §7.3).
 *
 * Format, one entry per line:
 * {{{
 * ACK <txnVersion>
 * FAIL <txnVersion> <exception class>: <message>
 * }}}
 * A writer stops at its first failure, so `FAIL`, if present, is the last entry.
 */
final class AckJournal private (channel: FileChannel) extends AutoCloseable {

  def ack(txnVersion: Long): Unit = append(s"${AckJournal.Ack} $txnVersion")

  def fail(txnVersion: Long, error: Throwable): Unit =
    append(s"${AckJournal.Fail} $txnVersion ${AckJournal.describe(error)}")

  override def close(): Unit = channel.close()

  private def append(line: String): Unit = {
    val buffer = ByteBuffer.wrap(s"$line\n".getBytes(UTF_8))
    while (buffer.hasRemaining) channel.write(buffer): Unit
    // Durable before the writer moves on: a kill -9 (P2b) then never loses an entry it wrote.
    channel.force(false)
  }
}

object AckJournal {

  private val Ack = "ACK"
  private val Fail = "FAIL"
  private val MaxMessageLength = 500

  final case class Failure(txnVersion: Long, error: String)

  /** What a journal says: the acknowledged `txnVersion`s in order, and the failure, if any. */
  final case class Contents(acked: Vector[Long], failure: Option[Failure])

  /** Opens a new journal; fails if one already exists, so a run can never mix two writers. */
  def open(path: Path): AckJournal =
    new AckJournal(
      FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
    )

  /**
   * Opens the journal of a writer that was killed, for the process started in its place: what
   * it acknowledges is added after the entries that are there. A fragment at the end, of an
   * entry whose write the kill cut short, is removed first: left there, the next entry would
   * be read as a part of it.
   */
  def reopen(path: Path): AckJournal = {
    val channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)
    val bytes = Files.readAllBytes(path)
    val whole = bytes.lastIndexOf('\n'.toByte) + 1
    channel.truncate(whole.toLong)
    channel.position(whole.toLong)
    new AckJournal(channel)
  }

  /**
   * Reads a journal. Only newline-terminated lines are entries: a trailing fragment is a line
   * whose write was cut short (a writer killed mid-append) and is ignored. Ignoring it is the
   * conservative reading, because it can only turn an acknowledgement into "unknown"; reading
   * the fragment `ACK 1` of `ACK 12` would be a wrong acknowledgement.
   */
  def read(path: Path): Contents = {
    val text = new String(Files.readAllBytes(path), UTF_8)
    val lines = text.split("\n", -1).toVector.dropRight(1)
    lines.zipWithIndex.foldLeft(Contents(Vector.empty, None)) {
      case (contents @ Contents(_, Some(_)), (_, n)) =>
        throw malformed(path, n, s"entry after a FAIL entry ($contents)")
      case (contents, (line, n)) =>
        line.split(" ", 3).toList match {
          case Ack :: Version(k) :: Nil => contents.copy(acked = contents.acked :+ k)
          case Fail :: Version(k) :: error :: Nil =>
            contents.copy(failure = Some(Failure(k, error)))
          case _ => throw malformed(path, n, s"unrecognised entry '$line'")
        }
    }
  }

  private object Version {
    def unapply(s: String): Option[Long] = s.toLongOption.filter(_ > 0)
  }

  private def malformed(path: Path, lineIndex: Int, detail: String): IllegalStateException =
    new IllegalStateException(s"$path line ${lineIndex + 1}: $detail")

  /** One line, bounded: `<class>: <message>`. */
  private def describe(error: Throwable): String = {
    val message = Option(error.getMessage).getOrElse("").replaceAll("\\s+", " ").trim
    s"${error.getClass.getName}: ${message.take(MaxMessageLength)}"
  }
}
