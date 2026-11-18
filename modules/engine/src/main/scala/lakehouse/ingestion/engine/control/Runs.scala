package lakehouse.ingestion.engine.control

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Types}
import java.time.OffsetDateTime
import java.util.UUID

import scala.util.Using

import lakehouse.ingestion.core.config.SourceId
import lakehouse.ingestion.core.control._

/**
 * The runs of the audit trail (discovery §13.2): one row for each attempt to ingest from a
 * source, written when it starts and once more when it ends.
 *
 * Every method runs on the connection it is given, so that the caller decides what is one
 * transaction: a run is opened with the lease it takes (ADR-0006), and ends well with the
 * commit of the state of its source (ADR-0005 §3, step 7).
 *
 * A run is changed only while it is `RUNNING`, and the methods that change one say whether
 * they did. A run that is told "no" was ended by another: the run that took its lease over
 * found it silent, and marked it abandoned. It is a zombie, and what it did is not committed.
 */
object Runs {

  /** A message is for a person to read, not a place to keep a stack trace. */
  val MaxMessageLength = 2000

  private val Columns =
    """run_id, source_id, layer, run_kind, txn_app_id, batch_seq, status, instance_id, app_version,
      |config_hash, lease_epoch, started_at, finished_at, rows_read, rows_written, rows_rejected,
      |bytes_read, retries, write_conflicts, commit_contention, error_class, error_message""".stripMargin

  private val Insert =
    """INSERT INTO control.ingestion_run
      |  (run_id, source_id, layer, run_kind, txn_app_id, status, instance_id, app_version,
      |   config_hash, lease_epoch, finished_at)
      |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CASE WHEN ? THEN now() END)""".stripMargin

  /**
   * Opens a run that took its lease: `RUNNING`, under the epoch it was given.
   *
   * Its configuration is stored first (`ConfigSnapshots.store`): a run points at what it was told.
   */
  @throws[SQLException]
  def open(connection: Connection, start: RunStart, epoch: Epoch): Unit =
    insert(connection, start, RunStatus.Running, Some(epoch))

  /**
   * Records a run that found the lease of its source taken: `SKIPPED_LOCKED`. It read nothing
   * and wrote nothing, and is in the audit trail all the same: that a trigger found the source
   * busy is something an operator asks about.
   */
  @throws[SQLException]
  def skip(connection: Connection, start: RunStart): Unit =
    insert(connection, start, RunStatus.SkippedLocked, None)

  private def insert(
      connection: Connection,
      start: RunStart,
      status: RunStatus,
      epoch: Option[Epoch]
  ): Unit =
    Using.resource(connection.prepareStatement(Insert)) { statement =>
      statement.setObject(1, start.id.uuid)
      statement.setString(2, start.source.value)
      statement.setString(3, start.layer.name)
      statement.setString(4, start.kind.name)
      statement.setString(5, start.txnAppId.value)
      statement.setString(6, status.name)
      statement.setString(7, start.instance.value)
      statement.setString(8, start.appVersion)
      statement.setString(9, start.configHash)
      setLong(statement, 10, epoch.map(_.value))
      statement.setBoolean(11, status.ended)
      statement.executeUpdate(): Unit
    }

  /**
   * Ends a run that is running, with what it counted: all in one statement, since what ended
   * does not change.
   *
   * @return false if the run was not `RUNNING`, and nothing was changed: another ended it
   */
  @throws[SQLException]
  def end(connection: Connection, run: RunId, end: RunEnd): Boolean =
    Using.resource(
      connection.prepareStatement(
        """UPDATE control.ingestion_run
          |   SET status = ?, finished_at = now(),
          |       rows_read = ?, rows_written = ?, rows_rejected = ?, bytes_read = ?,
          |       retries = ?, write_conflicts = ?, commit_contention = ?,
          |       error_class = ?, error_message = ?
          | WHERE run_id = ? AND status = 'RUNNING'""".stripMargin
      )
    ) { statement =>
      val failure = end match {
        case RunEnd.Failed(failure, message, _) => Some(RunFailure(failure, message))
        case _: RunEnd.Succeeded => None
      }
      statement.setString(1, end.status.name)
      setLong(statement, 2, end.counts.rowsRead)
      setLong(statement, 3, end.counts.rowsWritten)
      setLong(statement, 4, end.counts.rowsRejected)
      setLong(statement, 5, end.counts.bytesRead)
      setInt(statement, 6, end.counts.retries)
      setInt(statement, 7, end.counts.writeConflicts)
      setInt(statement, 8, end.counts.commitContention)
      statement.setString(9, failure.map(_.failure.name).orNull)
      statement.setString(10, failure.map(_.message.take(MaxMessageLength)).orNull)
      statement.setObject(11, run.uuid)
      statement.executeUpdate() == 1
    }

  /**
   * Records the batch that a running run has taken on: the one it planned, or the one it found
   * planned by a run before it. One batch may be the batch of several runs, of which one
   * commits it.
   *
   * @return false if the run was not `RUNNING`, and nothing was changed
   */
  @throws[SQLException]
  def took(connection: Connection, run: RunId, batch: BatchId): Boolean =
    Using.resource(
      connection.prepareStatement(
        "UPDATE control.ingestion_run SET batch_seq = ? " +
          "WHERE run_id = ? AND txn_app_id = ? AND status = 'RUNNING'"
      )
    ) { statement =>
      statement.setLong(1, batch.seq.value)
      statement.setObject(2, run.uuid)
      statement.setString(3, batch.app.value)
      statement.executeUpdate() == 1
    }

  /**
   * Marks a run that is running as `ABANDONED`: said by the run that takes over its lease, which
   * expired. The run that held it died, or stopped renewing, and ended halfway without a word.
   *
   * @return false if the run was not `RUNNING`: it had ended by itself, and only its lease was left
   */
  @throws[SQLException]
  def abandon(connection: Connection, run: RunId): Boolean =
    Using.resource(
      connection.prepareStatement(
        """UPDATE control.ingestion_run SET status = 'ABANDONED', finished_at = now()
          | WHERE run_id = ? AND status = 'RUNNING'""".stripMargin
      )
    ) { statement =>
      statement.setObject(1, run.uuid)
      statement.executeUpdate() == 1
    }

  @throws[SQLException]
  def find(connection: Connection, run: RunId): Option[RunRecord] =
    Using.resource(
      connection.prepareStatement(s"SELECT $Columns FROM control.ingestion_run WHERE run_id = ?")
    ) { statement =>
      statement.setObject(1, run.uuid)
      Using.resource(statement.executeQuery())(rows => Option.when(rows.next())(record(rows)))
    }

  /**
   * A row, as the model has it. What the database holds was written through the model and is
   * held to it by its constraints, so a value that the model does not know is a schema and an
   * application that are not each other's: it is said, and not read as something else.
   */
  private def record(row: ResultSet): RunRecord = {
    def known[A](column: String, parse: String => Either[String, A]): A = {
      val text = row.getString(column)
      parse(text).fold(
        reason => throw new IllegalStateException(s"control.ingestion_run.$column '$text' $reason"),
        identity
      )
    }
    def optional[A](column: String, parse: String => Either[String, A]): Option[A] =
      Option(row.getString(column)).map(_ => known(column, parse))
    def long(column: String): Option[Long] = {
      val value = row.getLong(column)
      Option.unless(row.wasNull())(value)
    }
    def instant(column: String) =
      Option(row.getObject(column, classOf[OffsetDateTime])).map(_.toInstant)

    RunRecord(
      start = RunStart(
        id = RunId
          .of(row.getObject("run_id", classOf[UUID]))
          .fold(
            reason => throw new IllegalStateException(s"control.ingestion_run.run_id $reason"),
            identity
          ),
        source = known("source_id", SourceId.parse),
        layer = known("layer", Layer.parse),
        kind = known("run_kind", RunKind.parse),
        instance = known("instance_id", InstanceId.parse),
        appVersion = row.getString("app_version"),
        configHash = row.getString("config_hash")
      ),
      txnAppId = known("txn_app_id", TxnAppId.parse),
      batch = long("batch_seq").map(seq => known("batch_seq", _ => BatchSeq.of(seq))),
      status = known("status", RunStatus.parse),
      epoch = long("lease_epoch").map(epoch => known("lease_epoch", _ => Epoch.of(epoch))),
      startedAt = instant("started_at").get,
      finishedAt = instant("finished_at"),
      counts = RunCounts(
        rowsRead = long("rows_read"),
        rowsWritten = long("rows_written"),
        rowsRejected = long("rows_rejected"),
        bytesRead = long("bytes_read"),
        retries = long("retries").map(_.toInt),
        writeConflicts = long("write_conflicts").map(_.toInt),
        commitContention = long("commit_contention").map(_.toInt)
      ),
      failure = optional("error_class", FailureClass.parse).map { failure =>
        RunFailure(failure, Option(row.getString("error_message")).getOrElse(""))
      }
    )
  }

  private def setLong(statement: PreparedStatement, index: Int, value: Option[Long]): Unit =
    value.fold(statement.setNull(index, Types.BIGINT))(statement.setLong(index, _))

  private def setInt(statement: PreparedStatement, index: Int, value: Option[Int]): Unit =
    value.fold(statement.setNull(index, Types.INTEGER))(statement.setInt(index, _))
}
