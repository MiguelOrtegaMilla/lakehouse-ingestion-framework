package lakehouse.ingestion.it.control

import java.sql.{Connection, SQLException}
import java.util.UUID

import scala.util.Using

import lakehouse.ingestion.cli.Main
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.it.support.{BootstrappedPostgres, ControlPlaneSchema}
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import org.postgresql.util.PSQLException
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The schema of the control plane as its migration builds it (ADR-0003, ADR-0005, ADR-0006):
 * what the database guarantees whatever the code does, and what each role may do (ADR-0013 §3).
 *
 * The statements are run as the role the engine runs as, against the same image and bootstrap
 * as the local service, on a schema that the command `migrate` made. What the engine itself
 * writes, and when, is the repositories' (P4.4 to P4.6): here are the rows and the rules.
 */
final class ControlSchemaSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private lazy val pg = BootstrappedPostgres.start()

  // Start and migrate in beforeAll: if either fails, the suite aborts once, with that error.
  override def beforeAll(): Unit = {
    val ran = ControlPlaneSchema.migrate(pg)
    require(ran.exit == Main.Exit.Ok, ran.err)
  }

  override def afterAll(): Unit = pg.stop()

  private val UniqueViolation = "23505"
  private val ForeignKeyViolation = "23503"
  private val CheckViolation = "23514"
  private val NotNullViolation = "23502"
  private val InsufficientPrivilege = "42501"

  private val Hash = "0123456789abcdef" * 4

  private def as[T](role: String)(body: Connection => T): T =
    Using.resource(pg.connect(role, Database))(body)

  private def sqlState(action: => Any): String = intercept[SQLException](action).getSQLState

  /** The constraint that refused `action`, as the server names it. */
  private def violated(action: => Any): String =
    intercept[PSQLException](action).getServerErrorMessage.getConstraint

  private def text(value: Any): String = s"'$value'"

  /** A source of a test's own: no test finds the rows of another. */
  private def newSource(): String = s"test.s${UUID.randomUUID().toString.take(8)}"

  private def snapshot(c: Connection, hash: String = Hash): Int =
    update(
      c,
      "INSERT INTO control.config_snapshot (config_hash, source_id, content) " +
        "VALUES (?, 'test.source', '{}'::jsonb) ON CONFLICT DO NOTHING",
      hash
    )

  /**
   * Inserts a row of defaults, with the columns that a test says otherwise, each as SQL.
   *
   * @return the SQL that was run, for the clue of a failure
   */
  private def insert(c: Connection, table: String, defaults: Seq[(String, String)])(
      columns: (String, String)*
  ): String = {
    val row = columns.foldLeft(defaults) { case (all, (name, value)) =>
      if (all.exists(_._1 == name))
        all.map(column => if (column._1 == name) name -> value else column)
      else all :+ (name -> value)
    }
    val sql = s"INSERT INTO control.$table (${row.map(_._1).mkString(", ")}) " +
      s"VALUES (${row.map(_._2).mkString(", ")})"
    update(c, sql): Unit
    sql
  }

  /** A run that is running with epoch 1, unless `columns` say otherwise. */
  private def insertRun(c: Connection, source: String, run: RunId)(
      columns: (String, String)*
  ): String =
    insert(
      c,
      "ingestion_run",
      Seq(
        "run_id" -> text(run),
        "source_id" -> text(source),
        "layer" -> text(Layer.Bronze),
        "run_kind" -> text(RunKind.Regular),
        "txn_app_id" -> text(s"$source/bronze"),
        "status" -> text(RunStatus.Running),
        "instance_id" -> "'host:1'",
        "app_version" -> "'0.1.0'",
        "config_hash" -> text(Hash),
        "lease_epoch" -> "1"
      )
    )(columns: _*)

  /** A batch that is planned by `run`, unless `columns` say otherwise. */
  private def insertBatch(c: Connection, source: String, seq: Int, run: RunId)(
      columns: (String, String)*
  ): String =
    insert(
      c,
      "ingestion_batch",
      Seq(
        "txn_app_id" -> text(s"$source/bronze"),
        "batch_seq" -> seq.toString,
        "source_id" -> text(source),
        "layer" -> text(Layer.Bronze),
        "run_kind" -> text(RunKind.Regular),
        "plan" -> """'{"from": 0, "to": 10}'::jsonb""",
        "planned_by_run" -> text(run)
      )
    )(columns: _*)

  private val Ended = "status = 'SUCCEEDED', finished_at = now()"
  private val Committed =
    """status = 'COMMITTED', committed_at = now(), outputs = '{"bronze/t": 4}'::jsonb"""
  private val Abandoned =
    "status = 'ABANDONED', abandoned_at = now(), abandon_reason = 'a file of it was written again'"

  test("the names that the database takes are those of the model, no more and no fewer") {
    // A status that the model has and the database refuses fails a run where it ends; one that
    // the database takes and the model does not know cannot be read back.
    val checks = as(Engine) { c =>
      query(
        c,
        """SELECT conrelid::regclass::text || '.' || conname, pg_get_constraintdef(oid)
          |FROM pg_constraint
          |WHERE connamespace = 'control'::regnamespace AND contype = 'c'""".stripMargin
      ).map(row => row.head -> """'([^']+)'::text""".r.findAllMatchIn(row(1)).map(_.group(1)).toSet)
    }.toMap

    val layers = Layer.All.map(_.name).toSet
    val kinds = RunKind.All.map(_.name).toSet
    checks("control.ingestion_run.ingestion_run_status_check") shouldBe RunStatus.All
      .map(_.name)
      .toSet
    checks("control.ingestion_run.ingestion_run_error_class_check") shouldBe
      FailureClass.All.map(_.name).toSet
    checks("control.ingestion_batch.ingestion_batch_status_check") shouldBe
      BatchStatus.All.map(_.name).toSet
    Seq("ingestion_run", "ingestion_batch", "source_state", "state_change_log", "backfill")
      .foreach { table =>
        withClue(table)(checks(s"control.$table.${table}_layer_check") shouldBe layers)
      }
    checks("control.ingestion_run.ingestion_run_run_kind_check") shouldBe kinds
    // A batch is of a kind that plans one (ADR-0021 §4).
    checks("control.ingestion_batch.ingestion_batch_run_kind_check") shouldBe
      RunKind.OfBatches.map(_.name).toSet
    // A file is taken, or could not be read (ADR-0023 §6).
    checks("control.processed_file.processed_file_status_check") shouldBe Set(
      lakehouse.ingestion.engine.control.ProcessedFiles.Taken,
      lakehouse.ingestion.engine.control.ProcessedFiles.Failed
    )
  }

  test("the engine's role can write the whole life of a run, from its lease to its commit") {
    val source = newSource()
    val key = LeaseKey.parse(s"$source/bronze").toOption.get.value
    val run = RunId.generate()

    as(Engine) { c =>
      c.setAutoCommit(false)

      // The lease and the run that takes it, in one transaction: the lease first, for its epoch.
      snapshot(c)
      update(
        c,
        "INSERT INTO control.source_lease (lease_key) VALUES (?) ON CONFLICT DO NOTHING",
        key
      )
      val epoch = query(
        c,
        """UPDATE control.source_lease
          |   SET holder_run_id = ?, instance_id = 'host:1', epoch = epoch + 1,
          |       acquired_at = now(), expires_at = now() + interval '2 minutes'
          | WHERE lease_key = ? AND (holder_run_id IS NULL OR expires_at < now())
          |RETURNING epoch""".stripMargin,
        run.uuid,
        key
      )
      epoch shouldBe Seq(Seq("1"))
      insertRun(c, source, run)()
      c.commit()

      // The plan, before anything is read.
      insertBatch(c, source, 1, run)()
      update(
        c,
        "UPDATE control.ingestion_run SET batch_seq = 1 WHERE run_id = ?",
        run.uuid
      ) shouldBe 1
      c.commit()

      // The commit of the state: the watermark, the batch and the run, on condition of the epoch.
      update(
        c,
        """INSERT INTO control.source_state
          |  (source_id, layer, watermark, last_committed_batch_seq, updated_by_run)
          |SELECT ?, 'bronze', '{"updated_at": "2026-11-18T09:00:00Z"}'::jsonb, 1, ?
          | WHERE EXISTS (SELECT 1 FROM control.source_lease
          |                WHERE lease_key = ? AND holder_run_id = ? AND epoch = 1)""".stripMargin,
        source,
        run.uuid,
        key,
        run.uuid
      ) shouldBe 1
      update(
        c,
        s"UPDATE control.ingestion_batch SET $Committed, committed_by_run = ? " +
          "WHERE txn_app_id = ? AND batch_seq = 1 AND status = 'PLANNED'",
        run.uuid,
        key
      ) shouldBe 1
      update(
        c,
        s"UPDATE control.ingestion_run SET $Ended, rows_read = 4, rows_written = 4, rows_rejected = 0 " +
          "WHERE run_id = ? AND status = 'RUNNING'",
        run.uuid
      ) shouldBe 1
      update(
        c,
        """UPDATE control.source_lease
          |   SET holder_run_id = NULL, instance_id = NULL, acquired_at = NULL, expires_at = NULL
          | WHERE lease_key = ? AND holder_run_id = ? AND epoch = 1""".stripMargin,
        key,
        run.uuid
      ) shouldBe 1
      c.commit()

      query(
        c,
        "SELECT status, lease_epoch, batch_seq, rows_written, started_at <= finished_at " +
          "FROM control.ingestion_run WHERE run_id = ?",
        run.uuid
      ) shouldBe Seq(Seq("SUCCEEDED", "1", "1", "4", "t"))
      // The lease is free and keeps its epoch: the next run that takes it has the next.
      query(
        c,
        "SELECT epoch, holder_run_id FROM control.source_lease WHERE lease_key = ?",
        key
      ) shouldBe
        Seq(Seq("1", null))
    }
  }

  test("a run that was skipped is written whole, with no epoch, by the engine's role") {
    val source = newSource()
    as(Engine) { c =>
      snapshot(c)
      insertRun(c, source, RunId.generate())(
        "status" -> text(RunStatus.SkippedLocked),
        "lease_epoch" -> "NULL",
        "finished_at" -> "now()"
      )
      query(c, "SELECT status FROM control.ingestion_run WHERE source_id = ?", source) shouldBe
        Seq(Seq("SKIPPED_LOCKED"))
    }
  }

  test("a run is refused that says of itself what cannot be") {
    val source = newSource()
    def refused(columns: (String, String)*): String = as(Engine) { c =>
      snapshot(c)
      sqlState(insertRun(c, source, RunId.generate())(columns: _*))
    }

    val notRuns = Seq[(String, Seq[(String, String)])](
      "running, and finished" -> Seq("finished_at" -> "now()"),
      "ended, and not finished" -> Seq("status" -> "'SUCCEEDED'"),
      "running, with no epoch" -> Seq("lease_epoch" -> "NULL"),
      "skipped, with an epoch" -> Seq("status" -> "'SKIPPED_LOCKED'", "finished_at" -> "now()"),
      "failed, and no class of failure" -> Seq("status" -> "'FAILED'", "finished_at" -> "now()"),
      "succeeded, with a class of failure" ->
        Seq("status" -> "'SUCCEEDED'", "finished_at" -> "now()", "error_class" -> "'transient'"),
      "a status that is none" -> Seq("status" -> "'DONE'", "finished_at" -> "now()"),
      "a layer that is none" -> Seq("layer" -> "'gold'"),
      "a kind that is none" -> Seq("run_kind" -> "'replay'"),
      // ADR-0021 §1: who asked and why, for an operator's action alone, and both or neither.
      "a regular run that says why" ->
        Seq("requested_by" -> "'ops'", "reason" -> "'because'"),
      "an operator's action that does not say why" ->
        Seq("run_kind" -> text(RunKind.Reset)),
      "an operator's action that says why and not who" ->
        Seq("run_kind" -> text(RunKind.Reset), "reason" -> "'because'"),
      "an operator's action that says who and not why" ->
        Seq("run_kind" -> text(RunKind.Reset), "requested_by" -> "'ops'"),
      "a reason of white space" ->
        Seq("run_kind" -> text(RunKind.Reset), "requested_by" -> "'ops'", "reason" -> "E' \\t '"),
      "a reason that is too long" ->
        Seq(
          "run_kind" -> text(RunKind.Reset),
          "requested_by" -> "'ops'",
          "reason" -> "repeat('x', 1001)"
        ),
      "a user that is no name" ->
        Seq("run_kind" -> text(RunKind.Reset), "requested_by" -> "' '", "reason" -> "'because'"),
      "a class of failure that is none" ->
        Seq("status" -> "'FAILED'", "finished_at" -> "now()", "error_class" -> "'fatal'"),
      "an epoch before the first" -> Seq("lease_epoch" -> "0"),
      "rows that are fewer than none" -> Seq("rows_read" -> "-1")
    )
    notRuns.foreach { case (what, columns) =>
      withClue(s"$what: ")(refused(columns: _*) shouldBe CheckViolation)
    }

    withClue("a configuration that was not stored: ") {
      refused("config_hash" -> text("f" * 64)) shouldBe ForeignKeyViolation
    }
    withClue("a batch that was not planned: ") {
      refused("batch_seq" -> "7") shouldBe ForeignKeyViolation
    }
    as(Engine) { c =>
      withClue("a hash that is not one: ")(sqlState(snapshot(c, "ABC")) shouldBe CheckViolation)
    }
  }

  test("an operator's action is a run that says who asked for it and why, and keeps both") {
    val source = newSource()
    val run = RunId.generate()
    as(Engine) { c =>
      snapshot(c)
      insertRun(c, source, run)(
        "run_kind" -> text(RunKind.Abandon),
        "requested_by" -> "'ops.oncall'",
        "reason" -> "'the directory of the source was moved'"
      )
      query(
        c,
        "SELECT run_kind, requested_by, reason FROM control.ingestion_run WHERE run_id = ?",
        run.uuid
      ) shouldBe Seq(Seq("abandon", "ops.oncall", "the directory of the source was moved"))
      // Written with the run, and not changed afterwards: the engine has no UPDATE on them.
      Seq("requested_by = 'someone else'", "reason = 'another reason'").foreach { change =>
        withClue(s"$change: ") {
          sqlState(
            update(c, s"UPDATE control.ingestion_run SET $change WHERE run_id = ?", run.uuid)
          ) shouldBe InsufficientPrivilege
        }
      }
    }
  }

  test("a change of the state of a source is written once, by a run, and never changed") {
    val source = newSource()
    val run = RunId.generate()
    as(Engine) { c =>
      snapshot(c)
      insertRun(c, source, run)(
        "run_kind" -> text(RunKind.Reset),
        "requested_by" -> "'ops'",
        "reason" -> "'the table of the source was renamed'"
      )
      def change(columns: (String, String)*): String =
        insert(
          c,
          "state_change_log",
          Seq(
            "run_id" -> text(run),
            "source_id" -> text(source),
            "layer" -> text(Layer.Bronze),
            "before" -> """'{"watermark": null}'::jsonb""",
            "after" -> """'{"watermark": null}'::jsonb"""
          )
        )(columns: _*)
      change()
      query(
        c,
        "SELECT run_id::text, changed_at <= now() FROM control.state_change_log WHERE source_id = ?",
        source
      ) shouldBe Seq(Seq(run.toString, "t"))

      withClue("by a run that is not one: ")(
        sqlState(change("run_id" -> text(RunId.generate()))) shouldBe ForeignKeyViolation
      )
      withClue("of a layer that is none: ")(
        sqlState(change("layer" -> "'gold'")) shouldBe CheckViolation
      )
      withClue("with no state before: ")(sqlState(change("before" -> "NULL")) shouldBe "23502")
      withClue("with an id of its own: ")(sqlState(change("change_id" -> "7")) shouldBe "428C9")
      Seq(
        "UPDATE control.state_change_log SET source_id = 'x'",
        "UPDATE control.state_change_log SET after = '{}'::jsonb",
        "DELETE FROM control.state_change_log"
      ).foreach { statement =>
        withClue(s"$statement: ")(sqlState(update(c, statement)) shouldBe InsufficientPrivilege)
      }
    }
  }

  test("what ended stays as it ended, whoever asks: a run") {
    val source = newSource()
    val (succeeded, abandoned) = (RunId.generate(), RunId.generate())
    as(Engine) { c =>
      snapshot(c)
      Seq(succeeded, abandoned).foreach(run => insertRun(c, source, run)())
      update(
        c,
        s"UPDATE control.ingestion_run SET $Ended WHERE run_id = ?",
        succeeded.uuid
      ) shouldBe 1
      // The run that takes over an expired lease says this of the one that held it.
      update(
        c,
        "UPDATE control.ingestion_run SET status = 'ABANDONED', finished_at = now() WHERE run_id = ?",
        abandoned.uuid
      ) shouldBe 1

      val late = Seq(
        "error_message = 'said late'",
        "rows_written = 9",
        "status = 'FAILED', error_class = 'transient'",
        // The zombie that finishes its work after its lease was taken.
        Ended,
        "status = 'RUNNING', finished_at = NULL"
      )
      for (run <- Seq(succeeded, abandoned); change <- late)
        withClue(s"$change: ") {
          val refused = intercept[SQLException](
            update(c, s"UPDATE control.ingestion_run SET $change WHERE run_id = ?", run.uuid)
          )
          refused.getSQLState shouldBe CheckViolation
          refused.getMessage should include("and what ended does not change")
        }

      // The way the engine asks: it changes nothing, and is told so by the count.
      update(
        c,
        s"UPDATE control.ingestion_run SET $Ended WHERE run_id = ? AND status = 'RUNNING'",
        abandoned.uuid
      ) shouldBe 0
      query(c, "SELECT status FROM control.ingestion_run WHERE run_id = ?", abandoned.uuid) shouldBe
        Seq(Seq("ABANDONED"))
    }
    // Not the owner of the table either: the rule is the table's, not a privilege.
    as(Migrator) { c =>
      sqlState(update(c, "UPDATE control.ingestion_run SET error_message = 'rewritten'")) shouldBe
        CheckViolation
    }
  }

  test("what ended stays as it ended, whoever asks: a batch") {
    val source = newSource()
    val run = RunId.generate()
    as(Engine) { c =>
      snapshot(c)
      insertRun(c, source, run)()
      insertBatch(c, source, 1, run)()
      update(
        c,
        s"UPDATE control.ingestion_batch SET $Committed, committed_by_run = ? WHERE source_id = ?",
        run.uuid,
        source
      ) shouldBe 1

      Seq(
        """outputs = '{"bronze/t": 5}'::jsonb""",
        "status = 'ABANDONED', committed_by_run = NULL, committed_at = NULL, outputs = NULL",
        s"$Abandoned, committed_by_run = NULL, committed_at = NULL, outputs = NULL",
        "status = 'PLANNED', committed_by_run = NULL, committed_at = NULL, outputs = NULL"
      ).foreach { change =>
        withClue(s"$change: ") {
          sqlState(
            update(c, s"UPDATE control.ingestion_batch SET $change WHERE source_id = ?", source)
          ) shouldBe CheckViolation
        }
      }

      // And a batch that was given up: who gave it up and why are said once.
      insertBatch(c, source, 2, run)()
      update(
        c,
        s"UPDATE control.ingestion_batch SET $Abandoned, abandoned_by_run = ? " +
          "WHERE source_id = ? AND batch_seq = 2 AND status = 'PLANNED'",
        run.uuid,
        source
      ) shouldBe 1
      Seq(
        "abandon_reason = 'another reason'",
        """abandon_detail = '{"changed": []}'::jsonb""",
        "abandoned_by_run = NULL",
        "abandoned_at = now()",
        s"$Committed, committed_by_run = abandoned_by_run, abandoned_by_run = NULL, " +
          "abandoned_at = NULL, abandon_reason = NULL",
        "status = 'PLANNED', abandoned_by_run = NULL, abandoned_at = NULL, abandon_reason = NULL"
      ).foreach { change =>
        withClue(s"$change: ") {
          val refused = intercept[SQLException](
            update(
              c,
              s"UPDATE control.ingestion_batch SET $change WHERE source_id = ? AND batch_seq = 2",
              source
            )
          )
          refused.getSQLState shouldBe CheckViolation
          refused.getMessage should include("and what ended does not change")
        }
      }
    }
  }

  test("the engine's role gives a planned batch up, saying who, when and why, and plans the next") {
    val source = newSource()
    val run = RunId.generate()
    as(Engine) { c =>
      snapshot(c)
      insertRun(c, source, run)()
      insertBatch(c, source, 1, run)()

      update(
        c,
        s"""UPDATE control.ingestion_batch
           |   SET $Abandoned, abandoned_by_run = ?,
           |       abandon_detail = '{"changed": [{"item": "a.csv", "change": "is gone"}]}'::jsonb
           | WHERE txn_app_id = ? AND batch_seq = 1 AND status = 'PLANNED'""".stripMargin,
        run.uuid,
        s"$source/bronze"
      ) shouldBe 1

      query(
        c,
        """SELECT status, abandoned_by_run::text, abandoned_at <= now(), abandon_reason,
          |       abandon_detail #>> '{changed,0,item}', plan::text, committed_by_run::text, outputs::text
          |  FROM control.ingestion_batch WHERE source_id = ?""".stripMargin,
        source
      ) shouldBe Seq(
        Seq(
          "ABANDONED",
          run.toString,
          "t",
          "a file of it was written again",
          "a.csv",
          // The plan that was never run is still what the log says was to be read.
          """{"to": 10, "from": 0}""",
          null,
          null
        )
      )

      // Its number is not used again, and the sequence goes on from the next.
      withClue("the number of a batch that was given up: ")(
        sqlState(insertBatch(c, source, 1, run)()) shouldBe UniqueViolation
      )
      insertBatch(c, source, 2, run)()

      // One that no run gave up: an operator's, when there is a command for it (P7).
      val other = newSource()
      val ofOther = RunId.generate()
      insertRun(c, other, ofOther)()
      insertBatch(c, other, 1, ofOther)(
        "status" -> "'ABANDONED'",
        "abandoned_at" -> "now()",
        "abandon_reason" -> "'the source was reset'"
      )
    }
  }

  test("one batch of a sequence is planned at a time, and a number is used once") {
    val source = newSource()
    val run = RunId.generate()
    as(Engine) { c =>
      snapshot(c)
      insertRun(c, source, run)()
      insertBatch(c, source, 1, run)()

      // Two runs that both plan: a zombie, and the run that took its place.
      withClue("a second plan: ")(
        sqlState(insertBatch(c, source, 2, run)()) shouldBe UniqueViolation
      )
      withClue("the same number: ")(
        sqlState(insertBatch(c, source, 1, run)()) shouldBe UniqueViolation
      )

      update(
        c,
        s"UPDATE control.ingestion_batch SET $Committed, committed_by_run = ? WHERE source_id = ?",
        run.uuid,
        source
      ) shouldBe 1
      insertBatch(c, source, 2, run)()
      withClue("a number that was committed: ") {
        sqlState(insertBatch(c, source, 1, run)()) shouldBe UniqueViolation
      }
      // Another source has a sequence of its own, which a run of its own plans in.
      val other = newSource()
      val ofOther = RunId.generate()
      insertRun(c, other, ofOther)()
      insertBatch(c, other, 1, ofOther)()
    }
  }

  test("a batch is refused that says of itself what cannot be") {
    val source = newSource()
    val run = RunId.generate()
    as(Engine) { c =>
      snapshot(c)
      insertRun(c, source, run)()

      val notBatches = Seq[(String, Seq[(String, String)])](
        "committed, by no run" ->
          Seq("status" -> "'COMMITTED'", "committed_at" -> "now()", "outputs" -> "'{}'::jsonb"),
        "committed, at no time" ->
          Seq(
            "status" -> "'COMMITTED'",
            "committed_by_run" -> text(run),
            "outputs" -> "'{}'::jsonb"
          ),
        "planned, with what it wrote" -> Seq("outputs" -> "'{}'::jsonb"),
        "planned, and committed by a run" -> Seq("committed_by_run" -> text(run)),
        "abandoned, at no time" ->
          Seq("status" -> "'ABANDONED'", "abandon_reason" -> "'a file was written again'"),
        "abandoned, for no reason" -> Seq("status" -> "'ABANDONED'", "abandoned_at" -> "now()"),
        "abandoned, for a reason that says nothing" ->
          Seq("status" -> "'ABANDONED'", "abandoned_at" -> "now()", "abandon_reason" -> "' \n'"),
        "abandoned, with what it wrote" ->
          Seq(
            "status" -> "'ABANDONED'",
            "abandoned_at" -> "now()",
            "abandon_reason" -> "'a file was written again'",
            "outputs" -> "'{}'::jsonb"
          ),
        "planned, and given up by a run" -> Seq("abandoned_by_run" -> text(run)),
        "planned, and given up at a time" -> Seq("abandoned_at" -> "now()"),
        "planned, with a reason to give it up" -> Seq("abandon_reason" -> "'why'"),
        "planned, with the detail of why it was given up" ->
          Seq("abandon_detail" -> "'{}'::jsonb"),
        "committed, with a reason to give it up" ->
          Seq(
            "status" -> "'COMMITTED'",
            "committed_by_run" -> text(run),
            "committed_at" -> "now()",
            "abandon_reason" -> "'why'"
          ),
        "a status that is none" -> Seq("status" -> "'RUNNING'"),
        "a number before the first" -> Seq("batch_seq" -> "0"),
        "a reprocess of no batch" -> Seq("run_kind" -> text(RunKind.Reprocess)),
        "a regular batch that reprocesses one" -> Seq("reprocess_of" -> "1"),
        "a reprocess of itself" -> Seq("run_kind" -> text(RunKind.Reprocess), "reprocess_of" -> "1")
      )
      notBatches.foreach { case (what, columns) =>
        withClue(s"$what: ")(
          sqlState(insertBatch(c, source, 1, run)(columns: _*)) shouldBe CheckViolation
        )
      }
      withClue("planned by a run that is none: ") {
        sqlState(
          insertBatch(c, source, 1, run)("planned_by_run" -> text(RunId.generate()))
        ) shouldBe
          ForeignKeyViolation
      }
      withClue("given up by a run that is none: ") {
        sqlState(
          insertBatch(c, source, 1, run)(
            "status" -> "'ABANDONED'",
            "abandoned_at" -> "now()",
            "abandon_reason" -> "'why'",
            "abandoned_by_run" -> text(RunId.generate())
          )
        ) shouldBe ForeignKeyViolation
      }
    }
  }

  test("a reprocess names a batch of its sequence before it, which the engine's role writes") {
    val source = newSource()
    val (first, again) = (RunId.generate(), RunId.generate())
    as(Engine) { c =>
      snapshot(c)
      insertRun(c, source, first)()
      insertBatch(c, source, 1, first)()
      update(
        c,
        s"UPDATE control.ingestion_batch SET $Committed, committed_by_run = '$first' " +
          s"WHERE source_id = '$source'"
      ) shouldBe 1
      insertRun(c, source, again)(
        "run_kind" -> text(RunKind.Reprocess),
        "requested_by" -> "'ops'",
        "reason" -> "'the parser was fixed'"
      )
      withClue("of a batch that there is not: ") {
        sqlState(
          insertBatch(c, source, 3, again)(
            "run_kind" -> text(RunKind.Reprocess),
            "reprocess_of" -> "2"
          )
        ) shouldBe ForeignKeyViolation
      }
      withClue("of a batch of another sequence: ") {
        val other = newSource()
        sqlState(
          insertBatch(c, other, 2, again)(
            "run_kind" -> text(RunKind.Reprocess),
            "reprocess_of" -> "1"
          )
        ) shouldBe ForeignKeyViolation
      }
      insertBatch(c, source, 2, again)("run_kind" -> text(RunKind.Reprocess), "reprocess_of" -> "1")
      query(
        c,
        "SELECT run_kind, reprocess_of::text FROM control.ingestion_batch WHERE source_id = ? " +
          "AND batch_seq = 2",
        source
      ) shouldBe Seq(Seq("reprocess", "1"))
      // What it reprocesses is written with its plan, and never changed.
      sqlState(
        update(
          c,
          s"UPDATE control.ingestion_batch SET reprocess_of = 1 WHERE source_id = '$source'"
        )
      ) shouldBe InsufficientPrivilege
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Backfills (V7, ADR-0021 §5)
  // -----------------------------------------------------------------------------------------------

  private val Asked = Seq("requested_by" -> "'ops'", "reason" -> "'load the history'")

  /** A run of `kind` on `sequence`, which an operator asked for unless the kind is regular. */
  private def runOn(c: Connection, source: String, sequence: String, kind: RunKind)(
      columns: (String, String)*
  ): RunId = {
    val run = RunId.generate()
    val asked = if (kind.operators) Asked else Nil
    insertRun(c, source, run)(
      (Seq("run_kind" -> text(kind), "txn_app_id" -> text(sequence)) ++ asked ++ columns): _*
    )
    run
  }

  /** A backfill of `source` that `run` creates, unless `columns` say otherwise. */
  private def insertBackfill(c: Connection, source: String, run: RunId)(
      columns: (String, String)*
  ): String =
    insert(
      c,
      "backfill",
      Seq(
        "source_id" -> text(source),
        "layer" -> text(Layer.Bronze),
        "backfill_id" -> "'history-2024'",
        "range_from" -> "'2024-01-01T00:00:00Z'",
        "range_to" -> "'2024-06-01T00:00:00Z'",
        "chunk" -> "interval '7 days'",
        "created_by_run" -> text(run)
      )
    )(columns: _*)

  test("a run acts on a sequence of its own source and layer that its kind may act on") {
    val source = newSource()
    val regular = s"$source/bronze"
    val history = s"$source/bronze/backfill/history-2024"
    as(Engine) { c =>
      snapshot(c)
      // A backfill on its own sequence; an abandonment on either; every other kind on the
      // regular one, as the model has it (RunKind.ofRegular, ofBackfills).
      RunKind.All.foreach { kind =>
        if (kind.ofRegular) runOn(c, source, regular, kind)()
        if (kind.ofBackfills) runOn(c, source, history, kind)()
      }

      val notRuns = Seq[(String, String, RunKind)](
        ("a backfill on the regular sequence", regular, RunKind.Backfill),
        ("a regular run on a backfill's", history, RunKind.Regular),
        ("a reset on a backfill's", history, RunKind.Reset),
        ("a reprocess on a backfill's", history, RunKind.Reprocess),
        ("the sequence of another source", s"${newSource()}/bronze", RunKind.Regular),
        ("the sequence of another layer", s"$source/silver", RunKind.Regular),
        ("a backfill of another source", s"${newSource()}/bronze/backfill/x", RunKind.Backfill),
        ("a backfill of another layer", s"$source/silver/backfill/x", RunKind.Backfill),
        ("an id that is not one", s"$source/bronze/backfill/History", RunKind.Backfill),
        ("an id with a '/'", s"$source/bronze/backfill/a/b", RunKind.Backfill),
        ("no id", s"$source/bronze/backfill/", RunKind.Backfill),
        ("an id that is too long", s"$source/bronze/backfill/${"x" * 64}", RunKind.Backfill),
        ("another word than backfill", s"$source/bronze/refill/x", RunKind.Backfill),
        // ADR-0006 had the lease of a backfill under this; ADR-0021 has it under its sequence.
        ("the key of ADR-0006", "backfill/history-2024", RunKind.Backfill)
      )
      notRuns.foreach { case (what, sequence, kind) =>
        withClue(s"$what: ") {
          violated(runOn(c, source, sequence, kind)()) shouldBe "run_of_a_sequence_of_its_kind"
        }
      }
    }
  }

  test("a batch is of a sequence that its kind plans in: a chunk of a backfill on its own") {
    val source = newSource()
    val regular = s"$source/bronze"
    val history = s"$source/bronze/backfill/history-2024"
    as(Engine) { c =>
      snapshot(c)
      val run = runOn(c, source, history, RunKind.Backfill)()
      // Chunk 1 of the backfill, and batch 1 of the regular sequence: one number in two
      // sequences, which are two batches, each planned by a run of its own sequence.
      insertBatch(c, source, 1, run)(
        "txn_app_id" -> text(history),
        "run_kind" -> text(RunKind.Backfill)
      )
      val ofRegular = runOn(c, source, regular, RunKind.Regular)()
      insertBatch(c, source, 1, ofRegular)()

      // A run of one sequence plans, commits and gives up no batch of another.
      // Given up already, so that the one batch of a sequence that is planned is not in the way.
      withClue("a regular batch planned by a run of a backfill: ") {
        violated(
          insertBatch(c, source, 2, run)(
            "status" -> "'ABANDONED'",
            "abandoned_at" -> "now()",
            "abandon_reason" -> "'why'"
          )
        ) shouldBe "batch_planned_by_a_run_of_its_sequence"
      }
      withClue("a chunk planned by a run of the regular sequence: ") {
        violated(
          insertBatch(c, source, 2, ofRegular)(
            "txn_app_id" -> text(history),
            "run_kind" -> text(RunKind.Backfill),
            "status" -> "'ABANDONED'",
            "abandoned_at" -> "now()",
            "abandon_reason" -> "'why'"
          )
        ) shouldBe "batch_planned_by_a_run_of_its_sequence"
      }
      withClue("a chunk committed by a run of the regular sequence: ") {
        violated(
          update(
            c,
            s"UPDATE control.ingestion_batch SET $Committed, committed_by_run = ? " +
              "WHERE txn_app_id = ? AND batch_seq = 1",
            ofRegular.uuid,
            history
          )
        ) shouldBe "batch_committed_by_a_run_of_its_sequence"
      }
      withClue("a chunk given up by a run of the regular sequence: ") {
        violated(
          update(
            c,
            s"UPDATE control.ingestion_batch SET $Abandoned, abandoned_by_run = ? " +
              "WHERE txn_app_id = ? AND batch_seq = 1",
            ofRegular.uuid,
            history
          )
        ) shouldBe "batch_abandoned_by_a_run_of_its_sequence"
      }

      val notBatches = Seq[(String, String, RunKind)](
        ("a chunk on the regular sequence", regular, RunKind.Backfill),
        ("a regular batch on a backfill's", history, RunKind.Regular),
        ("a reprocess on a backfill's", history, RunKind.Reprocess),
        (
          "a chunk of another source's backfill",
          s"${newSource()}/bronze/backfill/x",
          RunKind.Backfill
        ),
        ("a chunk of an id that is not one", s"$source/bronze/backfill/X", RunKind.Backfill)
      )
      notBatches.foreach { case (what, sequence, kind) =>
        withClue(s"$what: ") {
          val reprocessOf = Option.when(kind == RunKind.Reprocess)("reprocess_of" -> "1").toSeq
          violated(
            insertBatch(c, source, 2, run)(
              (Seq("txn_app_id" -> text(sequence), "run_kind" -> text(kind)) ++ reprocessOf): _*
            )
          ) shouldBe "batch_of_a_sequence_of_its_kind"
        }
      }
    }
  }

  test("a backfill is created by a run of its sequence, and what it reads is written once") {
    val source = newSource()
    val history = s"$source/bronze/backfill/history-2024"
    as(Engine) { c =>
      snapshot(c)
      val run = runOn(c, source, history, RunKind.Backfill)()
      insertBackfill(c, source, run)()
      // The name of its sequence is made by the database, from its source, layer and id.
      query(
        c,
        "SELECT txn_app_id, range_from < range_to, chunk::text, progress, completed_at " +
          "FROM control.backfill WHERE source_id = ?",
        source
      ) shouldBe Seq(Seq(history, "t", "7 days", null, null))

      withClue("the same id again: ")(
        sqlState(insertBackfill(c, source, run)()) shouldBe UniqueViolation
      )
      val notBackfills = Seq[(String, Seq[(String, String)], String)](
        (
          "a range of no moment",
          Seq("range_to" -> "'2024-01-01T00:00:00Z'"),
          "range_holds_a_moment"
        ),
        ("a range backwards", Seq("range_to" -> "'2023-01-01T00:00:00Z'"), "range_holds_a_moment"),
        (
          "a range before 1900",
          Seq("range_from" -> "'1899-12-31T23:59:59.999999Z'"),
          "range_of_moments_a_plan_holds"
        ),
        (
          "a range after 9999",
          Seq("range_to" -> "'infinity'"),
          "range_of_moments_a_plan_holds"
        ),
        ("a chunk of nothing", Seq("chunk" -> "interval '0'"), "chunk_is_a_stretch_of_time"),
        ("a chunk backwards", Seq("chunk" -> "interval '-1 day'"), "chunk_is_a_stretch_of_time"),
        ("an id that is not one", Seq("backfill_id" -> "'History'"), "backfill_backfill_id_check"),
        ("an id with a '/'", Seq("backfill_id" -> "'a/b'"), "backfill_backfill_id_check"),
        ("a layer that is none", Seq("layer" -> "'gold'"), "backfill_layer_check"),
        (
          "progress with no chunk committed",
          Seq("backfill_id" -> "'other'", "progress" -> """'{"value": 1}'::jsonb"""),
          "moved_whole_or_not_at_all"
        ),
        (
          "complete with nothing read",
          Seq("backfill_id" -> "'other'", "completed_at" -> "now()"),
          "completed_by_a_chunk"
        )
      )
      notBackfills.foreach { case (what, columns, constraint) =>
        withClue(s"$what: ")(
          violated(insertBackfill(c, source, run)(columns: _*)) shouldBe constraint
        )
      }
      withClue("with a name of its own: ") {
        sqlState(insertBackfill(c, source, run)("txn_app_id" -> text(history))) shouldBe "428C9"
      }

      // Created by a run of its sequence, and by no other: not by a run of the regular sequence
      // of its source, nor by one of another backfill.
      val regular = runOn(c, source, s"$source/bronze", RunKind.Regular)()
      val another = runOn(c, source, s"$source/bronze/backfill/other", RunKind.Backfill)()
      Seq(regular, another, RunId.generate()).foreach { by =>
        withClue(s"created by $by: ") {
          violated(insertBackfill(c, source, by)("backfill_id" -> "'third'")) shouldBe
            "backfill_created_by_a_run_of_its_sequence"
        }
      }

      // What it reads, and who created it, are written once: the engine has no UPDATE on them.
      Seq(
        "range_from = '2023-01-01T00:00:00Z'",
        "range_to = '2024-02-01T00:00:00Z'",
        "chunk = interval '1 day'",
        s"created_by_run = '$regular'",
        "created_at = now()",
        "backfill_id = 'renamed'",
        "source_id = 'other'",
        "layer = 'silver'"
      ).foreach { change =>
        withClue(s"$change: ") {
          sqlState(
            update(c, s"UPDATE control.backfill SET $change WHERE source_id = ?", source)
          ) shouldBe InsufficientPrivilege
        }
      }
    }
  }

  test("a backfill moves whole, by a run of its sequence, and once complete does not change") {
    val source = newSource()
    val history = s"$source/bronze/backfill/history-2024"
    as(Engine) { c =>
      snapshot(c)
      val first = runOn(c, source, history, RunKind.Backfill)()
      insertBackfill(c, source, first)()
      def move(change: String): Int =
        update(c, s"UPDATE control.backfill SET $change WHERE source_id = ?", source)
      def moved(seq: Int, by: RunId) =
        s"""progress = '{"value": $seq}'::jsonb, last_committed_batch_seq = $seq, """ +
          s"updated_by_run = '$by', updated_at = now()"

      move(moved(1, first)) shouldBe 1
      // A later run of it moves it on.
      val second = runOn(c, source, history, RunKind.Backfill)()
      move(moved(2, second)) shouldBe 1

      withClue("in part: ")(
        violated(move("last_committed_batch_seq = NULL")) shouldBe "moved_whole_or_not_at_all"
      )
      withClue("a chunk before the first: ")(
        sqlState(move(moved(0, second))) shouldBe CheckViolation
      )
      withClue("moved by a run of the regular sequence: ") {
        val regular = runOn(c, source, s"$source/bronze", RunKind.Regular)()
        violated(move(moved(3, regular))) shouldBe "backfill_moved_by_a_run_of_its_sequence"
      }

      move(s"${moved(3, second)}, completed_at = now()") shouldBe 1
      // Whoever asks, and whatever it sets: what is complete stays as it is.
      Seq(moved(4, second), "completed_at = NULL", "progress = progress").foreach { change =>
        withClue(s"$change: ") {
          val refused = intercept[SQLException](move(change))
          refused.getSQLState shouldBe CheckViolation
          refused.getMessage should include(s"the backfill $history is complete")
        }
      }
      // Nor the superuser: it is a rule of the table, and not a privilege.
      Using.resource(pg.connectAsSuperuser(Database)) { superuser =>
        sqlState(
          update(
            superuser,
            "UPDATE control.backfill SET completed_at = NULL WHERE source_id = ?",
            source
          )
        ) shouldBe CheckViolation
      }
      query(
        c,
        "SELECT last_committed_batch_seq, completed_at IS NOT NULL FROM control.backfill " +
          "WHERE source_id = ?",
        source
      ) shouldBe Seq(Seq("3", "t"))
    }
  }

  test("a file is taken by a batch that exists, with a size and a time to the millisecond") {
    val source = newSource()
    val run = RunId.generate()
    as(Engine) { c =>
      snapshot(c)
      insertRun(c, source, run)()
      insertBatch(c, source, 1, run)()

      def file(columns: (String, String)*): String =
        insert(
          c,
          "processed_file",
          Seq(
            "source_id" -> text(source),
            "path" -> "'in/a.csv'",
            "size" -> "10",
            "modified_at" -> "'2026-11-18T10:00:00.123Z'",
            "txn_app_id" -> text(s"$source/bronze"),
            "batch_seq" -> "1",
            "status" -> "'TAKEN'"
          )
        )(columns: _*)

      file()
      withClue("the same file again: ")(sqlState(file()) shouldBe UniqueViolation)
      // Written again under its path, it is another file.
      file("size" -> "11")
      file("modified_at" -> "'2026-11-18T10:00:00.124Z'")

      withClue("finer than a millisecond: ")(
        sqlState(file("modified_at" -> "'2026-11-18T10:00:00.1231Z'")) shouldBe CheckViolation
      )
      withClue("a size that is none: ")(
        sqlState(file("path" -> "'in/b.csv'", "size" -> "-1")) shouldBe CheckViolation
      )
      withClue("taken by a batch that is none: ")(
        sqlState(file("path" -> "'in/c.csv'", "batch_seq" -> "2")) shouldBe ForeignKeyViolation
      )

      // A file that could not be read says why, and one that was taken says nothing of it
      // (V9, ADR-0023 §6).
      file("path" -> "'in/d.csv'", "status" -> "'FAILED'", "error" -> "'unreadable: from x'")
      withClue("failed, and not why: ")(
        sqlState(file("path" -> "'in/e.csv'", "status" -> "'FAILED'")) shouldBe CheckViolation
      )
      withClue("failed, for a reason that says nothing: ")(
        sqlState(
          file("path" -> "'in/e.csv'", "status" -> "'FAILED'", "error" -> "' \t'")
        ) shouldBe CheckViolation
      )
      withClue("taken, with an error: ")(
        sqlState(file("path" -> "'in/e.csv'", "error" -> "'unreadable'")) shouldBe CheckViolation
      )
      withClue("neither: ")(
        sqlState(file("path" -> "'in/e.csv'", "status" -> "'SKIPPED'")) shouldBe CheckViolation
      )
      // A row says which it is: an insert that does not is refused, not taken for a file taken.
      withClue("no status: ")(
        sqlState(
          update(
            c,
            "INSERT INTO control.processed_file " +
              "(source_id, path, size, modified_at, txn_app_id, batch_seq) " +
              "VALUES (?, 'in/f.csv', 1, '2026-11-18T10:00:00Z', ?, 1)",
            source,
            s"$source/bronze"
          )
        ) shouldBe NotNullViolation
      )
    }
  }

  test("a lease is held whole or not at all, by a run that exists when its transaction ends") {
    val key = s"${newSource()}/bronze"
    as(Engine) { c =>
      update(c, "INSERT INTO control.source_lease (lease_key) VALUES (?)", key) shouldBe 1
      query(
        c,
        "SELECT epoch, holder_run_id FROM control.source_lease WHERE lease_key = ?",
        key
      ) shouldBe
        Seq(Seq("0", null))

      val held =
        "instance_id = 'host:1', acquired_at = now(), expires_at = now() + interval '2 minutes'"
      withClue("a holder and nothing else: ") {
        sqlState(
          update(
            c,
            "UPDATE control.source_lease SET holder_run_id = ? WHERE lease_key = ?",
            UUID.randomUUID(),
            key
          )
        ) shouldBe CheckViolation
      }
      withClue("an expiry and no holder: ") {
        sqlState(
          update(c, s"UPDATE control.source_lease SET $held WHERE lease_key = ?", key)
        ) shouldBe CheckViolation
      }

      // The run is written after its lease, so the reference waits for the end of the
      // transaction. A lease that is held by no run does not get past it.
      c.setAutoCommit(false)
      update(
        c,
        s"UPDATE control.source_lease SET holder_run_id = ?, epoch = epoch + 1, $held WHERE lease_key = ?",
        UUID.randomUUID(),
        key
      ) shouldBe 1
      withClue("held by a run that is none: ")(sqlState(c.commit()) shouldBe ForeignKeyViolation)
      c.rollback()
      c.setAutoCommit(true)
      query(
        c,
        "SELECT epoch, holder_run_id FROM control.source_lease WHERE lease_key = ?",
        key
      ) shouldBe
        Seq(Seq("0", null))
    }
  }

  test(
    "the engine's role cannot delete, rewrite what identifies a run, define, or read elsewhere"
  ) {
    val denied = Seq(
      // No deletes: a defect in the engine cannot erase a run, a batch or what it was told.
      "DELETE FROM control.ingestion_run",
      "DELETE FROM control.ingestion_batch",
      "DELETE FROM control.config_snapshot",
      "DELETE FROM control.source_state",
      "DELETE FROM control.source_lease",
      "DELETE FROM control.processed_file",
      "DELETE FROM control.backfill",
      "TRUNCATE control.ingestion_run",
      // A run ends, and is counted. Who ran, what, with which configuration and under which
      // epoch never change.
      "UPDATE control.ingestion_run SET run_id = gen_random_uuid()",
      "UPDATE control.ingestion_run SET source_id = 'other'",
      "UPDATE control.ingestion_run SET layer = 'silver'",
      "UPDATE control.ingestion_run SET txn_app_id = 'other/bronze'",
      "UPDATE control.ingestion_run SET instance_id = 'other'",
      "UPDATE control.ingestion_run SET app_version = '9'",
      s"UPDATE control.ingestion_run SET config_hash = '$Hash'",
      "UPDATE control.ingestion_run SET lease_epoch = 9",
      "UPDATE control.ingestion_run SET started_at = now()",
      // A batch is committed or abandoned. Its plan is the one that was written down.
      "UPDATE control.ingestion_batch SET plan = '{}'::jsonb",
      "UPDATE control.ingestion_batch SET batch_seq = 9",
      "UPDATE control.ingestion_batch SET planned_by_run = gen_random_uuid()",
      "UPDATE control.ingestion_batch SET planned_at = now()",
      // Nor is where it was planned to write: it is the table that recovery asks (V4).
      "UPDATE control.ingestion_batch SET target_table = 'file:///lake/elsewhere'",
      // What a run was told is written once.
      "UPDATE control.config_snapshot SET content = '{}'::jsonb",
      // That a file was taken, and by which batch, is written once: taking it again is an
      // operator's decision (P7).
      "UPDATE control.processed_file SET batch_seq = 9",
      "UPDATE control.processed_file SET size = 0",
      // The schema version is the migrator's to set.
      "INSERT INTO control.schema_version (singleton, version) VALUES (false, 1)",
      "UPDATE control.schema_version SET version = 9",
      // No DDL from the engine, and no way round the rule of what ended.
      "CREATE TABLE control.scratch (id int)",
      "ALTER TABLE control.ingestion_run ADD COLUMN extra text",
      "ALTER TABLE control.ingestion_run DISABLE TRIGGER ingestion_run_ended_stays",
      "DROP TRIGGER ingestion_run_ended_stays ON control.ingestion_run",
      "DROP TABLE control.ingestion_run",
      "SELECT control.refuse_change_of_what_ended()",
      "ALTER TABLE control.backfill DISABLE TRIGGER backfill_complete_stays",
      "SELECT control.refuse_change_of_a_complete_backfill()",
      // Nothing outside its own tables: not the migration history, not the commit coordination.
      "SELECT * FROM control.flyway_schema_history",
      "SELECT * FROM delta_coordination.commit_entry",
      "SELECT * FROM delta_coordination.writer_epoch",
      "INSERT INTO delta_coordination.table_taint (table_uri, reason) VALUES ('t', 'r')"
    )

    as(Engine) { c =>
      denied.foreach { statement =>
        withClue(s"$statement: ") { sqlState(update(c, statement)) shouldBe InsufficientPrivilege }
      }
    }
  }

  test("no other role has anything in the control plane") {
    val denied = Seq(
      "SELECT * FROM control.ingestion_run",
      "SELECT * FROM control.source_state",
      "SELECT * FROM control.processed_file",
      "SELECT * FROM control.state_change_log",
      "SELECT * FROM control.backfill",
      "SELECT version FROM control.schema_version",
      "INSERT INTO control.source_lease (lease_key) VALUES ('test.source/bronze')",
      "UPDATE control.source_lease SET epoch = epoch + 1",
      "DELETE FROM control.ingestion_batch"
    )

    Seq("ingest_coordination", "ingest_maintenance").foreach { role =>
      as(role) { c =>
        denied.foreach { statement =>
          withClue(s"$role, $statement: ") {
            sqlState(update(c, statement)) shouldBe InsufficientPrivilege
          }
        }
      }
    }
  }
}
