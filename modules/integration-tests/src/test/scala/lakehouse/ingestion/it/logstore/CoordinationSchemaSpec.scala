package lakehouse.ingestion.it.logstore

import java.sql.{Connection, SQLException}

import scala.util.Using

import lakehouse.ingestion.it.support.{BootstrappedPostgres, CoordinationSchema}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The coordination schema as its migration builds it (ADR-0004 §3.1): what the constraints
 * guarantee whatever the code does, and what each role may do (ADR-0013 §3).
 *
 * The statements are the ones the commit path runs, as the role it runs as, against the same
 * image and bootstrap as the local service.
 */
final class CoordinationSchemaSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private lazy val pg = BootstrappedPostgres.start()

  // Start and migrate in beforeAll: if either fails, the suite aborts once, with that error.
  override def beforeAll(): Unit = CoordinationSchema.migrate(pg): Unit

  override def afterAll(): Unit = pg.stop()

  private val CommitPath = "ingest_coordination"
  private val Maintenance = "ingest_maintenance"

  private val UniqueViolation = "23505"
  private val ForeignKeyViolation = "23503"
  private val CheckViolation = "23514"
  private val InsufficientPrivilege = "42501"

  private val Register =
    "INSERT INTO delta_coordination.coordinated_table (table_uri, registered_version) VALUES (?, ?)"
  private val TakeVersion =
    """INSERT INTO delta_coordination.commit_entry (table_uri, version, temp_path, writer_instance)
      |VALUES (?, ?, ?, ?)""".stripMargin
  private val TakeVersionIfFree = s"$TakeVersion ON CONFLICT DO NOTHING"
  private val Complete =
    """UPDATE delta_coordination.commit_entry SET complete = true, completed_at = now()
      |WHERE table_uri = ? AND version = ? AND temp_path = ?""".stripMargin
  private val Taint =
    """INSERT INTO delta_coordination.table_taint (table_uri, reason) VALUES (?, ?)
      |ON CONFLICT DO NOTHING""".stripMargin

  test("the migrations build version 2 of the schema, and applying them again changes nothing") {
    CoordinationSchema.migrate(pg).migrationsExecuted shouldBe 0

    as(CommitPath) { c =>
      query(c, "SELECT version FROM delta_coordination.schema_version") shouldBe Seq(Seq("2"))
    }
    Using.resource(pg.connectAsSuperuser(CoordinationSchema.Database)) { c =>
      val owners = query(
        c,
        "SELECT tablename, tableowner FROM pg_tables WHERE schemaname = ? ORDER BY tablename",
        CoordinationSchema.Schema
      )
      owners shouldBe Seq(
        "commit_entry",
        "coordinated_table",
        "flyway_schema_history",
        "schema_version",
        "table_taint"
      ).map(Seq(_, CoordinationSchema.Migrator))
    }
  }

  test("the commit path registers a table, takes a version and completes it") {
    val table = "file:/lake/complete"
    as(CommitPath) { c =>
      update(c, Register, table, Long.box(-1)) shouldBe 1
      update(c, TakeVersionIfFree, table, Long.box(0), ".tmp/0.json.a", "host:1:app") shouldBe 1

      val state = "SELECT complete, completed_at IS NULL, created_at IS NULL " +
        "FROM delta_coordination.commit_entry WHERE table_uri = ? AND version = 0"
      // The entry's times come from the database's clock, not from the writer.
      query(c, state, table) shouldBe Seq(Seq("f", "t", "f"))

      update(c, Complete, table, Long.box(0), ".tmp/0.json.a") shouldBe 1
      query(c, state, table) shouldBe Seq(Seq("t", "f", "f"))
    }
  }

  test("a version belongs to the first writer that inserts it") {
    val table = "file:/lake/exclusive"
    as(CommitPath) { c =>
      update(c, Register, table, Long.box(-1))
      update(c, TakeVersionIfFree, table, Long.box(7), ".tmp/7.json.first", "host:1:app") shouldBe 1

      // The second writer is told so without an error, and nothing is overwritten.
      update(
        c,
        TakeVersionIfFree,
        table,
        Long.box(7),
        ".tmp/7.json.second",
        "host:2:app"
      ) shouldBe 0
      query(
        c,
        "SELECT temp_path FROM delta_coordination.commit_entry WHERE table_uri = ?",
        table
      ) shouldBe Seq(Seq(".tmp/7.json.first"))

      // The primary key enforces it, whatever statement is used.
      sqlState(
        update(c, TakeVersion, table, Long.box(7), ".tmp/7.json.third", "host:3:app")
      ) shouldBe UniqueViolation
    }
  }

  test("the database refuses an entry or a taint for a table that is not registered") {
    as(CommitPath) { c =>
      val unregistered = "file:/lake/alias-of-a-registered-table"
      sqlState(
        update(c, TakeVersionIfFree, unregistered, Long.box(0), ".tmp/0.json.a", "host:1:app")
      ) shouldBe ForeignKeyViolation
      sqlState(update(c, Taint, unregistered, "gap at version 3")) shouldBe ForeignKeyViolation
    }
  }

  test("an entry is complete exactly when it has a completion time; versions are not negative") {
    val table = "file:/lake/checks"
    as(CommitPath) { c =>
      update(c, Register, table, Long.box(-1))
      val completeWithoutTime =
        """INSERT INTO delta_coordination.commit_entry
          |  (table_uri, version, temp_path, writer_instance, complete)
          |VALUES (?, 0, '.tmp/0.json.a', 'host:1:app', true)""".stripMargin
      sqlState(update(c, completeWithoutTime, table)) shouldBe CheckViolation
      sqlState(
        update(c, TakeVersion, table, Long.box(-1), ".tmp/x", "host:1:app")
      ) shouldBe CheckViolation
      sqlState(update(c, Register, "file:/lake/checks-2", Long.box(-2))) shouldBe CheckViolation
    }
  }

  test("the commit path taints a table once, and the first reason stays") {
    val table = "file:/lake/tainted"
    as(CommitPath) { c =>
      update(c, Register, table, Long.box(-1))
      update(c, Taint, table, "gap at version 3") shouldBe 1
      update(c, Taint, table, "publish race at version 9") shouldBe 0
      query(
        c,
        "SELECT reason, tainted_at IS NULL FROM delta_coordination.table_taint WHERE table_uri = ?",
        table
      ) shouldBe Seq(Seq("gap at version 3", "f"))
    }
  }

  test("the commit path cannot delete, rewrite, define, or read outside its tables") {
    val denied = Seq(
      // No deletes: a defect on the commit path cannot erase a commit, a registration or a taint.
      "DELETE FROM delta_coordination.commit_entry",
      "DELETE FROM delta_coordination.coordinated_table",
      "DELETE FROM delta_coordination.table_taint",
      "TRUNCATE delta_coordination.commit_entry",
      // An entry can only be marked complete: who owns a version, and its bytes, never change.
      "UPDATE delta_coordination.commit_entry SET temp_path = '.tmp/other'",
      "UPDATE delta_coordination.commit_entry SET version = 9",
      "UPDATE delta_coordination.commit_entry SET writer_instance = 'other'",
      "UPDATE delta_coordination.coordinated_table SET registered_version = 9",
      "UPDATE delta_coordination.table_taint SET reason = 'nothing happened'",
      // The schema version is the migrator's to set.
      "INSERT INTO delta_coordination.schema_version (singleton, version) VALUES (false, 2)",
      "UPDATE delta_coordination.schema_version SET version = 2",
      // No DDL from the data path (ADR-0004, deviation 10).
      "CREATE TABLE delta_coordination.scratch (id int)",
      "ALTER TABLE delta_coordination.commit_entry ADD COLUMN extra text",
      "DROP TABLE delta_coordination.commit_entry",
      // Nothing outside its own tables: not the migration history, not the control plane.
      "SELECT * FROM delta_coordination.flyway_schema_history",
      "SELECT * FROM control.run",
      "CREATE TABLE control.scratch (id int)"
    )

    as(CommitPath) { c =>
      denied.foreach { statement =>
        withClue(s"$statement: ") { sqlState(update(c, statement)) shouldBe InsufficientPrivilege }
      }
    }
  }

  test("maintenance adopts a table, moves its version and deletes rows; nothing else") {
    val table = "file:/lake/reset"
    as(CommitPath) { c =>
      update(c, Register, table, Long.box(-1))
      update(c, TakeVersionIfFree, table, Long.box(0), ".tmp/0.json.a", "host:1:app")
      update(c, Taint, table, "gap at version 3")
    }

    as(Maintenance) { c =>
      val registration = "DELETE FROM delta_coordination.coordinated_table WHERE table_uri = ?"
      // A registration cannot go while entries or a taint still refer to it.
      sqlState(update(c, registration, table)) shouldBe ForeignKeyViolation
      update(c, "DELETE FROM delta_coordination.table_taint WHERE table_uri = ?", table) shouldBe 1
      sqlState(update(c, registration, table)) shouldBe ForeignKeyViolation
      update(c, "DELETE FROM delta_coordination.commit_entry WHERE table_uri = ?", table) shouldBe 1
      update(c, registration, table) shouldBe 1

      // Since V2: adopting a table registers it, and adopting it again moves its version.
      update(c, Register, table, Long.box(41)) shouldBe 1
      update(
        c,
        "UPDATE delta_coordination.coordinated_table " +
          "SET registered_version = 50, delta_table_id = 'id', registered_at = now() " +
          "WHERE table_uri = ?",
        table
      ) shouldBe 1

      Seq(
        // A table is never renamed in the registry.
        "UPDATE delta_coordination.coordinated_table SET table_uri = 'file:/lake/elsewhere'",
        // Entries and taints are the commit path's to make: maintenance only removes them.
        "INSERT INTO delta_coordination.commit_entry (table_uri, version, temp_path, writer_instance) " +
          "VALUES ('file:/lake/reset', 51, '.tmp/x', 'operator')",
        "INSERT INTO delta_coordination.table_taint (table_uri, reason) " +
          "VALUES ('file:/lake/reset', 'by hand')",
        "UPDATE delta_coordination.commit_entry SET complete = true, completed_at = now()",
        "UPDATE delta_coordination.schema_version SET version = 3",
        "TRUNCATE delta_coordination.commit_entry",
        "CREATE TABLE delta_coordination.scratch (id int)",
        "SELECT * FROM control.run"
      ).foreach { statement =>
        withClue(s"$statement: ") { sqlState(update(c, statement)) shouldBe InsufficientPrivilege }
      }
    }
  }

  private def as[T](role: String)(body: Connection => T): T =
    Using.resource(pg.connect(role, CoordinationSchema.Database))(body)

  private def sqlState(action: => Any): String = intercept[SQLException](action).getSQLState

  /** Runs one statement with bind parameters; returns the number of rows it changed. */
  private def update(c: Connection, sql: String, parameters: AnyRef*): Int =
    Using.resource(c.prepareStatement(sql)) { statement =>
      parameters.zipWithIndex.foreach { case (value, i) => statement.setObject(i + 1, value) }
      statement.executeUpdate()
    }

  /** The rows of a query, every column as text (booleans as PostgreSQL prints them: t, f). */
  private def query(c: Connection, sql: String, parameters: AnyRef*): Seq[Seq[String]] =
    Using.resource(c.prepareStatement(sql)) { statement =>
      parameters.zipWithIndex.foreach { case (value, i) => statement.setObject(i + 1, value) }
      Using.resource(statement.executeQuery()) { rows =>
        val columns = rows.getMetaData.getColumnCount
        Iterator
          .continually(rows)
          .takeWhile(_.next())
          .map(row => (1 to columns).map(row.getString))
          .toList
      }
    }
}
