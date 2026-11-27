package lakehouse.ingestion.engine.control

import java.sql.{Connection, SQLException}
import java.time.OffsetDateTime

import scala.util.Using

import lakehouse.ingestion.core.Text
import lakehouse.ingestion.core.config.{RelativePath, SourceId}
import lakehouse.ingestion.core.control.BatchId
import lakehouse.ingestion.core.files.{FileName, FilePlan, LandedFile}

/**
 * The files that batches of a file source have dealt with (discovery §13.2): what keeps a file
 * from being taken twice.
 *
 * A file is `TAKEN` by the commit of its batch, in the same transaction that moves the state of
 * its source (`LeasedRun.commit`). One that could not be read, under a source that skips such
 * files, is `FAILED`, with why, by the run that gives its batch up for it, in the transaction
 * that does (`LeasedRun.abandon`, ADR-0023). Nothing else records a file. Before that it is in
 * the plan of the batch, and nowhere here: a batch that is planned is replayed, and the files of
 * its plan are taken by that replay.
 *
 * A path is the directory of the source under the landing root, and the name of the file in it.
 * A source whose directory changes looks for its files under the new paths.
 *
 * Every method runs on the connection it is given.
 */
object ProcessedFiles {

  /**
   * The files of `directory` with these names that batches of `source` have taken, or found
   * that they could not read: of each name, every version, by its size and time. A plan leaves
   * out both. Names that are not there, or not names that a file may have, are simply not found.
   */
  @throws[SQLException]
  def among(
      connection: Connection,
      source: SourceId,
      directory: RelativePath,
      names: Seq[String]
  ): Set[LandedFile] =
    if (names.isEmpty) Set.empty
    else
      Using.resource(
        connection.prepareStatement(
          """SELECT path, size, modified_at FROM control.processed_file
            | WHERE source_id = ? AND path = ANY (?)""".stripMargin
        )
      ) { statement =>
        val paths = names.distinct.map(pathOf(directory, _))
        statement.setString(1, source.value)
        statement.setArray(2, connection.createArrayOf("text", paths.toArray[AnyRef]))
        Using.resource(statement.executeQuery()) { rows =>
          Iterator
            .continually(rows)
            .takeWhile(_.next())
            .map { row =>
              val path = row.getString(1)
              val name = FileName
                .parse(path.stripPrefix(s"${directory.value}/"))
                .fold(
                  reason =>
                    throw new IllegalStateException(
                      s"control.processed_file has a file of $source whose name $reason"
                    ),
                  identity
                )
              LandedFile(
                name,
                row.getLong(2),
                row.getObject(3, classOf[OffsetDateTime]).toInstant
              )
            }
            .toSet
        }
      }

  /**
   * Records the files of a plan as taken by its batch.
   *
   * @return false if a file of it is recorded already, and nothing was written: the state is not
   *   what the run that commits took it for
   */
  @throws[SQLException]
  def record(connection: Connection, source: SourceId, batch: BatchId, plan: FilePlan): Boolean =
    Using.resource(
      connection.prepareStatement(
        s"""INSERT INTO control.processed_file
           |  (source_id, path, size, modified_at, txn_app_id, batch_seq, status, error)
           |SELECT ?, file.path, file.size,
           |       timestamptz 'epoch' + file.modified_ms * interval '1 millisecond', ?, ?,
           |       '$Taken', NULL
           |  FROM unnest(?::text[], ?::bigint[], ?::bigint[]) AS file (path, size, modified_ms)
           |ON CONFLICT DO NOTHING""".stripMargin
      )
    ) { statement =>
      val files = plan.files
      statement.setString(1, source.value)
      statement.setString(2, batch.app.value)
      statement.setLong(3, batch.seq.value)
      setFiles(connection, statement, plan.directory, files, from = 4)
      // All of them or none: one that was there already says that this batch is not what the
      // run took it for, and the transaction of the commit ends with nothing done.
      statement.executeUpdate() == files.size
    }

  /**
   * A file of a plan that a batch could not read, with why (ADR-0023).
   *
   * @param error for a person: what Spark said of the file, by the condition of its error and
   *   the type of its cause, or what the landing zone found. Never the text of an exception
   */
  final case class FailedFile(file: LandedFile, error: String) {
    require(error.exists(!_.isWhitespace), "a file that failed says why")
  }

  /**
   * Records files of a plan as ones that its batch could not read, each with why: a plan made
   * after it leaves them out, as it does the files taken.
   *
   * @return false if a file of them is recorded already, and nothing was written
   */
  @throws[SQLException]
  def fail(
      connection: Connection,
      source: SourceId,
      batch: BatchId,
      directory: RelativePath,
      files: Seq[FailedFile]
  ): Boolean =
    Using.resource(
      connection.prepareStatement(
        s"""INSERT INTO control.processed_file
           |  (source_id, path, size, modified_at, txn_app_id, batch_seq, status, error)
           |SELECT ?, file.path, file.size,
           |       timestamptz 'epoch' + file.modified_ms * interval '1 millisecond', ?, ?,
           |       '$Failed', file.error
           |  FROM unnest(?::text[], ?::bigint[], ?::bigint[], ?::text[])
           |       AS file (path, size, modified_ms, error)
           |ON CONFLICT DO NOTHING""".stripMargin
      )
    ) { statement =>
      statement.setString(1, source.value)
      statement.setString(2, batch.app.value)
      statement.setLong(3, batch.seq.value)
      setFiles(connection, statement, directory, files.map(_.file).toVector, from = 4)
      statement.setArray(
        7,
        connection.createArrayOf(
          "text",
          files.map(failed => Text.upTo(failed.error.trim, Runs.MaxMessageLength)).toArray[AnyRef]
        )
      )
      statement.executeUpdate() == files.size
    }

  /** The status of a file that a committed batch took. */
  val Taken = "TAKEN"

  /** The status of a file that a batch could not read. */
  val Failed = "FAILED"

  /** The paths, the sizes and the times of files, as three arrays from the parameter `from`. */
  private def setFiles(
      connection: Connection,
      statement: java.sql.PreparedStatement,
      directory: RelativePath,
      files: Vector[LandedFile],
      from: Int
  ): Unit = {
    statement.setArray(
      from,
      connection.createArrayOf(
        "text",
        files.map(f => pathOf(directory, f.name.value)).toArray[AnyRef]
      )
    )
    statement.setArray(
      from + 1,
      connection.createArrayOf("bigint", files.map(f => Long.box(f.size)).toArray[AnyRef])
    )
    // In milliseconds since the epoch, and made a time by the database: exact, and in no time
    // zone of this JVM's, which is what the driver would put a timestamp in.
    statement.setArray(
      from + 2,
      connection.createArrayOf(
        "bigint",
        files.map(f => Long.box(f.modifiedAt.toEpochMilli)).toArray[AnyRef]
      )
    )
  }

  private def pathOf(directory: RelativePath, name: String): String = s"${directory.value}/$name"
}
