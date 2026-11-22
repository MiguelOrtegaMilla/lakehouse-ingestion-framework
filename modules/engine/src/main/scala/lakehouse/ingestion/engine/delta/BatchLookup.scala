package lakehouse.ingestion.engine.delta

import java.io.IOException
import java.net.URI
import java.nio.file.{Files, Path, Paths}

import scala.util.Using
import scala.util.control.NonFatal

import lakehouse.ingestion.core.config.TableLocation
import lakehouse.ingestion.core.control.BatchId
import lakehouse.ingestion.logstore.{CoordinatedTables, TableIdentity}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}

/**
 * Whether a table has a batch, for a decision that must not be taken on a guess: giving the
 * batch up (ADR-0021 §2). A run that recovers a batch asks the table it writes, which it has just
 * opened (`Recovery`); an operator asks a table that the batch was planned for, which may be in
 * another lake, on a disk that is not mounted, or under a directory that the process may not
 * read.
 *
 * "Not there" is only said where it is known:
 *  - a table that the coordination store has not registered has no commit (`CoordinatedTables`),
 *    and its directory, there or not, has no batch;
 *  - a table that is registered is read: its directory must be there and readable, and so must
 *    its log, which Delta then reads. A registered table whose directory is not there may be on
 *    a disk that is not mounted, and is not known to be without the batch.
 *
 * Anything that cannot be read is said as such, with why, and not as an answer.
 */
final class BatchLookup(tables: DeltaTableTransactions, conf: () => Configuration) {
  import BatchLookup._

  def of(table: TableLocation, batch: BatchId): Found =
    try {
      val directory = Paths.get(URI.create(table.uri))
      val registered = CoordinatedTables.registered(conf(), new HadoopPath(table.uri))
      if (Files.notExists(directory)) {
        if (registered)
          Found.Unknown(
            s"the table at ${table.uri} is registered in the coordination store, and its " +
              "directory is not there: it may be on a disk that is not mounted, or have been " +
              "moved, and whether it has the batch is not known"
          )
        else Found.NotWritten
      } else if (!Files.exists(directory))
        Found.Unknown(s"whether ${table.uri} is there cannot be told: its path cannot be read")
      else {
        readable(directory)
        val log = directory.resolve(TableIdentity.LogDirectory)
        if (Files.notExists(log)) Found.NotWritten
        else {
          readable(log)
          if (tables.lastVersion(table, batch.app).exists(_ >= batch.seq.value)) Found.Written
          else Found.NotWritten
        }
      }
    } catch {
      case e: Exception if NonFatal(e) && unreadable(e) =>
        Found.Unknown(s"the table at ${table.uri} could not be read: ${said(e)}")
    }

  /** Lists a directory, to know that it can be read: a stat says less than that. */
  private def readable(directory: Path): Unit =
    Using.resource(Files.newDirectoryStream(directory))(_.iterator().hasNext): Unit
}

object BatchLookup {

  sealed trait Found

  object Found {

    /** The table has the batch: its data is written. */
    case object Written extends Found

    /** The table does not have the batch, and is known not to. */
    case object NotWritten extends Found

    /** Whether the table has the batch is not known, for the reason given. */
    final case class Unknown(why: String) extends Found
  }

  /** A failure to read, whatever Spark or Delta wrapped it in. */
  private def unreadable(e: Throwable): Boolean =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).exists(_.isInstanceOf[IOException])

  private def said(e: Throwable): String = {
    val root = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).toSeq.last
    s"${root.getClass.getSimpleName}: ${root.getMessage}"
  }
}
