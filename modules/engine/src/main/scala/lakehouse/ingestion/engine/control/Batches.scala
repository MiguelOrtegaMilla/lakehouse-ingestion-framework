package lakehouse.ingestion.engine.control

import java.sql.{Connection, ResultSet, SQLException}
import java.time.OffsetDateTime
import java.util.UUID

import scala.util.Using

import io.circe.Json
import lakehouse.ingestion.core.config.{RelativePath, SourceId}
import lakehouse.ingestion.core.control._

/**
 * The batches of the write-ahead log (ADR-0005 §3): the plan of each batch, written down before
 * anything is read, and what became of it.
 *
 * A plan is what makes a run that died recoverable. Whatever a run was in the middle of, the
 * plan is there, and says exactly what the batch was to read: the next run reads the same, and
 * Delta, which knows the batch by its number, takes it once.
 *
 * Every method runs on the connection it is given. Those that write are to be called in a
 * transaction that has the lease of the source locked and checked (`Leases.holds`), which
 * `LeasedRun` sees to.
 */
object Batches {

  private val Columns =
    """txn_app_id, batch_seq, source_id, layer, run_kind, plan::text AS plan, status,
      |planned_by_run, planned_at, committed_by_run, committed_at, outputs::text AS outputs,
      |abandoned_by_run, abandoned_at, abandon_reason, abandon_detail::text AS abandon_detail""".stripMargin

  /**
   * The batch of a sequence that is planned and not committed. There is one at most: the next
   * batch is planned once this one is committed.
   */
  @throws[SQLException]
  def planned(connection: Connection, app: TxnAppId): Option[PlannedBatch] =
    Using.resource(
      connection.prepareStatement(
        s"SELECT $Columns FROM control.ingestion_batch WHERE txn_app_id = ? AND status = 'PLANNED'"
      )
    ) { statement =>
      statement.setString(1, app.value)
      Using.resource(statement.executeQuery()) { row =>
        Option.when(row.next())(record(row)).map { batch =>
          PlannedBatch(batch.id, batch.plan, batch.plannedBy, batch.plannedAt)
        }
      }
    }

  /**
   * Writes a plan down, as the next batch of the sequence of `start`: the number after the
   * last one the sequence has, whatever became of that one. A number belongs to one plan.
   *
   * @throws java.lang.IllegalArgumentException if the plan could not be read back (`StoredJson`):
   *   it is not written, since a plan that cannot be read is a source that cannot be recovered
   * @throws java.sql.SQLException with state 23505 if the sequence has a planned batch already
   */
  @throws[SQLException]
  def plan(connection: Connection, start: RunStart, plan: Json): PlannedBatch = {
    readable("plan", plan)
    Using.resource(
      connection.prepareStatement(
        """INSERT INTO control.ingestion_batch
          |  (txn_app_id, batch_seq, source_id, layer, run_kind, plan, planned_by_run)
          |SELECT ?, COALESCE(max(batch_seq), 0) + 1, ?, ?, ?, ?::jsonb, ?
          |  FROM control.ingestion_batch WHERE txn_app_id = ?
          |RETURNING batch_seq, planned_at""".stripMargin
      )
    ) { statement =>
      statement.setString(1, start.txnAppId.value)
      statement.setString(2, start.source.value)
      statement.setString(3, start.layer.name)
      statement.setString(4, start.kind.name)
      statement.setString(5, plan.noSpaces)
      statement.setObject(6, start.id.uuid)
      statement.setString(7, start.txnAppId.value)
      Using.resource(statement.executeQuery()) { row =>
        if (!row.next())
          throw new IllegalStateException(s"no batch was written for ${start.txnAppId}")
        PlannedBatch(
          BatchId(start.txnAppId, known("batch_seq", BatchSeq.of(row.getLong("batch_seq")))),
          plan,
          start.id,
          row.getObject("planned_at", classOf[OffsetDateTime]).toInstant
        )
      }
    }
  }

  /**
   * Marks a planned batch as committed by `run`, with what it wrote.
   *
   * @return false if the batch was not planned: it was committed already, or given up
   */
  @throws[SQLException]
  def commit(
      connection: Connection,
      batch: BatchId,
      run: RunId,
      outputs: Seq[BatchOutput]
  ): Boolean =
    Using.resource(
      connection.prepareStatement(
        """UPDATE control.ingestion_batch
          |   SET status = 'COMMITTED', committed_by_run = ?, committed_at = now(), outputs = ?::jsonb
          | WHERE txn_app_id = ? AND batch_seq = ? AND status = 'PLANNED'""".stripMargin
      )
    ) { statement =>
      statement.setObject(1, run.uuid)
      statement.setString(2, written(outputs).noSpaces)
      statement.setString(3, batch.app.value)
      statement.setLong(4, batch.seq.value)
      statement.executeUpdate() == 1
    }

  /**
   * Marks a planned batch as given up by `run`, with why (ADR-0018). Its plan stays as it was
   * written, and its number is not used again: the next batch of the sequence has the next.
   *
   * @param reason why, for a person: what is kept of it is its first `Runs.MaxMessageLength`
   *   characters
   * @param detail what of the plan could not be run, in a form of the caller's own
   * @return false if the batch was not planned: it was committed already, or given up
   * @throws java.lang.IllegalArgumentException if the reason says nothing, or the detail could
   *   not be read back (`StoredJson`): nothing is written
   */
  @throws[SQLException]
  def abandon(
      connection: Connection,
      batch: BatchId,
      run: RunId,
      reason: String,
      detail: Option[Json]
  ): Boolean = {
    abandonable(reason, detail)
    Using.resource(
      connection.prepareStatement(
        """UPDATE control.ingestion_batch
          |   SET status = 'ABANDONED', abandoned_by_run = ?, abandoned_at = now(),
          |       abandon_reason = ?, abandon_detail = ?::jsonb
          | WHERE txn_app_id = ? AND batch_seq = ? AND status = 'PLANNED'""".stripMargin
      )
    ) { statement =>
      statement.setObject(1, run.uuid)
      statement.setString(2, reason.trim.take(Runs.MaxMessageLength))
      statement.setString(3, detail.map(_.noSpaces).orNull)
      statement.setString(4, batch.app.value)
      statement.setLong(5, batch.seq.value)
      statement.executeUpdate() == 1
    }
  }

  /** Refuses what an abandoned batch could not say of itself, before anything is written. */
  private[control] def abandonable(reason: String, detail: Option[Json]): Unit = {
    require(reason.trim.nonEmpty, "a batch is not given up without a reason")
    detail.foreach(readable("detail of why a batch is given up", _))
  }

  @throws[SQLException]
  def find(connection: Connection, batch: BatchId): Option[BatchRecord] =
    Using.resource(
      connection.prepareStatement(
        s"SELECT $Columns FROM control.ingestion_batch WHERE txn_app_id = ? AND batch_seq = ?"
      )
    ) { statement =>
      statement.setString(1, batch.app.value)
      statement.setLong(2, batch.seq.value)
      Using.resource(statement.executeQuery())(row => Option.when(row.next())(record(row)))
    }

  /** Refuses a document that could not be read back, before it is written. */
  private[control] def readable(what: String, document: Json): Unit =
    StoredJson.readable(document).left.foreach { reason =>
      throw new IllegalArgumentException(
        s"the $what cannot be stored, since it could not be read back: $reason"
      )
    }

  // What a batch wrote, as a list of tables and not as a mapping by table: a path is longer than
  // a key that is read back may be, and a list can be asked which batch wrote a version.
  private def written(outputs: Seq[BatchOutput]): Json = Json.fromValues(outputs.map { output =>
    Json.obj(
      "table" -> Json.fromString(output.table.value),
      "version" -> Json.fromLong(output.version)
    )
  })

  private def outputsOf(batch: BatchId, text: String): Vector[BatchOutput] = {
    def wrong(why: String) = throw new IllegalStateException(
      s"the outputs of the batch $batch $why"
    )
    val entries = stored(batch, "outputs", text).asArray.getOrElse(wrong("are not a list"))
    entries.map { entry =>
      val fields = entry.hcursor
      val output = for {
        table <- fields.get[String]("table").left.map(_ => "have an entry with no table")
        path <- RelativePath.parse(table).left.map(reason => s"name a table that $reason")
        version <- fields.get[Long]("version").left.map(_ => "have an entry with no version")
      } yield BatchOutput(path, version)
      output.fold(wrong, identity)
    }
  }

  private def stored(batch: BatchId, what: String, text: String): Json =
    StoredJson
      .parse(text)
      .fold(
        reason =>
          throw new IllegalStateException(
            s"the $what of the batch $batch cannot be read back: $reason"
          ),
        identity
      )

  private def record(row: ResultSet): BatchRecord = {
    val id = BatchId(
      known("txn_app_id", TxnAppId.parse(row.getString("txn_app_id"))),
      known("batch_seq", BatchSeq.of(row.getLong("batch_seq")))
    )
    def run(column: String): Option[RunId] =
      Option(row.getObject(column, classOf[UUID])).map(uuid => known(column, RunId.of(uuid)))
    def instant(column: String) =
      Option(row.getObject(column, classOf[OffsetDateTime])).map(_.toInstant)

    BatchRecord(
      id = id,
      source = known("source_id", SourceId.parse(row.getString("source_id"))),
      layer = known("layer", Layer.parse(row.getString("layer"))),
      kind = known("run_kind", RunKind.parse(row.getString("run_kind"))),
      plan = stored(id, "plan", row.getString("plan")),
      status = known("status", BatchStatus.parse(row.getString("status"))),
      plannedBy = run("planned_by_run").get,
      plannedAt = instant("planned_at").get,
      committedBy = run("committed_by_run"),
      committedAt = instant("committed_at"),
      outputs = Option(row.getString("outputs")).map(outputsOf(id, _)),
      abandoned = instant("abandoned_at").map { at =>
        BatchAbandonment(
          by = run("abandoned_by_run"),
          at = at,
          reason = row.getString("abandon_reason"),
          detail = Option(row.getString("abandon_detail")).map(stored(id, "abandon detail", _))
        )
      }
    )
  }

  private def known[A](column: String, value: Either[String, A]): A =
    value.fold(
      reason => throw new IllegalStateException(s"control.ingestion_batch.$column $reason"),
      identity
    )
}
