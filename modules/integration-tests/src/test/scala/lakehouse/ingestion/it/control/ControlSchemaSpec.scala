package lakehouse.ingestion.it.control

import java.sql.{Connection, SQLException}
import java.util.UUID

import scala.util.Using

import lakehouse.ingestion.cli.Main
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.it.support.{BootstrappedPostgres, ControlPlaneSchema}
import lakehouse.ingestion.it.support.ControlPlaneSchema._
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
  private val InsufficientPrivilege = "42501"

  private val Hash = "0123456789abcdef" * 4

  private def as[T](role: String)(body: Connection => T): T =
    Using.resource(pg.connect(role, Database))(body)

  private def sqlState(action: => Any): String = intercept[SQLException](action).getSQLState

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
    Seq("ingestion_run", "ingestion_batch", "source_state").foreach { table =>
      withClue(table)(checks(s"control.$table.${table}_layer_check") shouldBe layers)
    }
    Seq("ingestion_run", "ingestion_batch").foreach { table =>
      withClue(table)(checks(s"control.$table.${table}_run_kind_check") shouldBe kinds)
    }
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
      "a kind that is none" -> Seq("run_kind" -> "'backfill'"),
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
        "status = 'PLANNED', committed_by_run = NULL, committed_at = NULL, outputs = NULL"
      ).foreach { change =>
        withClue(s"$change: ") {
          sqlState(
            update(c, s"UPDATE control.ingestion_batch SET $change WHERE source_id = ?", source)
          ) shouldBe CheckViolation
        }
      }
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
      // Another source has a sequence of its own.
      val other = newSource()
      insertRun(c, other, RunId.generate())()
      insertBatch(c, other, 1, run)()
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
        "a status that is none" -> Seq("status" -> "'RUNNING'"),
        "a number before the first" -> Seq("batch_seq" -> "0")
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
      // What a run was told is written once.
      "UPDATE control.config_snapshot SET content = '{}'::jsonb",
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
      // Nothing outside its own tables: not the migration history, not the commit coordination.
      "SELECT * FROM control.flyway_schema_history",
      "SELECT * FROM delta_coordination.commit_entry",
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
