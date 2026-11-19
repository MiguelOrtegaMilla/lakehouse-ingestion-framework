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
  private val GeneratedAlways = "428C9"

  private val Register =
    "INSERT INTO delta_coordination.coordinated_table (table_uri, registered_version) VALUES (?, ?)"
  private val TakeVersion =
    """INSERT INTO delta_coordination.commit_entry
      |  (table_uri, version, temp_path, writer_instance, generation)
      |VALUES (?, ?, ?, ?, ?)""".stripMargin
  private val NewGeneration =
    "UPDATE delta_coordination.coordinated_table SET generation = DEFAULT WHERE table_uri = ?"
  private val TakeVersionIfFree = s"$TakeVersion ON CONFLICT DO NOTHING"
  private val Complete =
    """UPDATE delta_coordination.commit_entry SET complete = true, completed_at = now()
      |WHERE table_uri = ? AND version = ? AND temp_path = ?""".stripMargin
  private val Taint =
    """INSERT INTO delta_coordination.table_taint (table_uri, reason) VALUES (?, ?)
      |ON CONFLICT DO NOTHING""".stripMargin

  test("the migrations build version 4 of the schema, and applying them again changes nothing") {
    CoordinationSchema.migrate(pg).migrationsExecuted shouldBe 0

    as(CommitPath) { c =>
      query(c, "SELECT version FROM delta_coordination.schema_version") shouldBe Seq(Seq("4"))
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
        "table_taint",
        "writer_epoch"
      ).map(Seq(_, CoordinationSchema.Migrator))
    }
  }

  test("the commit path registers a table, takes a version and completes it") {
    val table = "file:/lake/complete"
    as(CommitPath) { c =>
      update(c, Register, table, Long.box(-1)) shouldBe 1
      update(
        c,
        TakeVersionIfFree,
        table,
        Long.box(0),
        ".tmp/0.json.a",
        "host:1:app",
        generationOf(c, table)
      ) shouldBe 1

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
      val generation = generationOf(c, table)
      update(
        c,
        TakeVersionIfFree,
        table,
        Long.box(7),
        ".tmp/7.json.first",
        "host:1:app",
        generation
      ) shouldBe 1

      // The second writer is told so without an error, and nothing is overwritten.
      update(
        c,
        TakeVersionIfFree,
        table,
        Long.box(7),
        ".tmp/7.json.second",
        "host:2:app",
        generation
      ) shouldBe 0
      query(
        c,
        "SELECT temp_path FROM delta_coordination.commit_entry WHERE table_uri = ?",
        table
      ) shouldBe Seq(Seq(".tmp/7.json.first"))

      // The primary key enforces it, whatever statement is used.
      sqlState(
        update(c, TakeVersion, table, Long.box(7), ".tmp/7.json.third", "host:3:app", generation)
      ) shouldBe UniqueViolation
    }
  }

  test("the database refuses an entry or a taint for a table that is not registered") {
    as(CommitPath) { c =>
      val unregistered = "file:/lake/alias-of-a-registered-table"
      sqlState(
        update(
          c,
          TakeVersionIfFree,
          unregistered,
          Long.box(0),
          ".tmp/0.json.a",
          "host:1:app",
          Long.box(1)
        )
      ) shouldBe ForeignKeyViolation
      sqlState(update(c, Taint, unregistered, "gap at version 3")) shouldBe ForeignKeyViolation
    }
  }

  test("an entry names the generation of its table's registration, and is held to it") {
    val table = "file:/lake/generations"
    def take(c: Connection, version: Long, generation: Long): Int =
      update(
        c,
        TakeVersionIfFree,
        table,
        Long.box(version),
        s".tmp/$version.json.a",
        "host:1:app",
        Long.box(generation)
      )

    val first = as(CommitPath) { c =>
      update(c, Register, table, Long.box(-1))
      val generation = generationOf(c, table).longValue
      // Not a generation the registration has, whichever way it is wrong.
      Seq(generation - 1, generation + 1000).foreach { wrong =>
        sqlState(take(c, version = 0, wrong)) shouldBe ForeignKeyViolation
      }
      take(c, version = 0, generation) shouldBe 1
      // Nobody chooses a generation: the database gives the next one.
      sqlState(
        update(
          c,
          "INSERT INTO delta_coordination.coordinated_table " +
            "(table_uri, registered_version, generation) VALUES ('file:/lake/chosen', -1, 7)"
        )
      ) shouldBe GeneratedAlways
      generation
    }

    // An operator adopts the table again: a new generation, and the entry that stays follows.
    val second = as(Maintenance) { c =>
      update(c, NewGeneration, table) shouldBe 1
      val generation = generationOf(c, table).longValue
      generation should be > first
      query(
        c,
        "SELECT generation FROM delta_coordination.commit_entry WHERE table_uri = ?",
        table
      ) shouldBe Seq(Seq(generation.toString))
      sqlState(
        update(
          c,
          "UPDATE delta_coordination.coordinated_table SET generation = 7 WHERE table_uri = ?",
          table
        )
      ) shouldBe GeneratedAlways
      generation
    }

    // What a writer read before the operator's change no longer takes a version.
    as(CommitPath) { c =>
      sqlState(take(c, version = 1, first)) shouldBe ForeignKeyViolation
      take(c, version = 1, second) shouldBe 1
    }
  }

  test("an entry is complete exactly when it has a completion time; versions are not negative") {
    val table = "file:/lake/checks"
    as(CommitPath) { c =>
      update(c, Register, table, Long.box(-1))
      val generation = generationOf(c, table)
      val completeWithoutTime =
        """INSERT INTO delta_coordination.commit_entry
          |  (table_uri, version, temp_path, writer_instance, complete, generation)
          |VALUES (?, 0, '.tmp/0.json.a', 'host:1:app', true, ?)""".stripMargin
      sqlState(update(c, completeWithoutTime, table, generation)) shouldBe CheckViolation
      sqlState(
        update(c, TakeVersion, table, Long.box(-1), ".tmp/x", "host:1:app", generation)
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

  test("the commit path announces an epoch, which only grows, and locks it to take a version") {
    val key = "sales.epochs/bronze"
    val announce =
      """INSERT INTO delta_coordination.writer_epoch AS held (epoch_key, epoch) VALUES (?, ?)
        |ON CONFLICT (epoch_key) DO UPDATE SET epoch = EXCLUDED.epoch, announced_at = now()
        | WHERE held.epoch < EXCLUDED.epoch""".stripMargin
    val inForce =
      "SELECT epoch, announced_at IS NULL FROM delta_coordination.writer_epoch WHERE epoch_key = ?"
    as(CommitPath) { c =>
      update(c, announce, key, Long.box(2)) shouldBe 1
      // The time of an announcement is the database's.
      query(c, inForce, key) shouldBe Seq(Seq("2", "f"))
      update(c, announce, key, Long.box(5)) shouldBe 1
      update(c, announce, key, Long.box(3)) shouldBe 0
      query(c, inForce, key) shouldBe Seq(Seq("5", "f"))
      // The lock that the claim of a version takes on the epoch of its key.
      query(c, s"$inForce FOR SHARE", key) shouldBe Seq(Seq("5", "f"))

      // An earlier epoch is refused by the database, whatever statement asks for it.
      sqlState(
        update(c, "UPDATE delta_coordination.writer_epoch SET epoch = 4 WHERE epoch_key = ?", key)
      ) shouldBe CheckViolation
      query(c, inForce, key) shouldBe Seq(Seq("5", "f"))
      // An epoch is 1 or more, and is of a key that says something.
      sqlState(update(c, announce, "sales.none/bronze", Long.box(0))) shouldBe CheckViolation
      sqlState(update(c, announce, " ", Long.box(1))) shouldBe CheckViolation
    }
  }

  test("an entry says the epoch that it was taken under whole, or says none") {
    val table = "file:/lake/epochs"
    val withEpoch =
      """INSERT INTO delta_coordination.commit_entry
        |  (table_uri, version, temp_path, writer_instance, generation, epoch_key, epoch)
        |VALUES (?, ?, ?, 'host:1:app', ?, ?, ?)""".stripMargin
    as(CommitPath) { c =>
      update(c, Register, table, Long.box(-1))
      val generation = generationOf(c, table)
      update(
        c,
        withEpoch,
        table,
        Long.box(0),
        ".tmp/0",
        generation,
        "sales.x/bronze",
        Long.box(3)
      ) shouldBe 1
      update(c, withEpoch, table, Long.box(1), ".tmp/1", generation, null, null) shouldBe 1
      sqlState(
        update(c, withEpoch, table, Long.box(2), ".tmp/2", generation, "sales.x/bronze", null)
      ) shouldBe CheckViolation
      sqlState(
        update(c, withEpoch, table, Long.box(2), ".tmp/2", generation, null, Long.box(3))
      ) shouldBe CheckViolation
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
      // The epoch that a commit was taken under is part of its entry, and an epoch that is in
      // force is not forgotten, nor moved to another key.
      "UPDATE delta_coordination.commit_entry SET epoch = 9",
      "UPDATE delta_coordination.commit_entry SET epoch_key = 'other'",
      "DELETE FROM delta_coordination.writer_epoch",
      "TRUNCATE delta_coordination.writer_epoch",
      "UPDATE delta_coordination.writer_epoch SET epoch_key = 'other'",
      "SELECT delta_coordination.refuse_earlier_epoch()",
      // A registration's generation is an operator's to change, and its row an operator's to
      // hold: the commit path waits on it through the foreign key of its entries, and no more.
      "UPDATE delta_coordination.coordinated_table SET generation = DEFAULT",
      "SELECT 1 FROM delta_coordination.coordinated_table FOR SHARE",
      // The schema version is the migrator's to set.
      "INSERT INTO delta_coordination.schema_version (singleton, version) VALUES (false, 3)",
      "UPDATE delta_coordination.schema_version SET version = 3",
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
      update(
        c,
        TakeVersionIfFree,
        table,
        Long.box(0),
        ".tmp/0.json.a",
        "host:1:app",
        generationOf(c, table)
      )
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

      // Since V4: the epochs of a key are forgotten, when whoever numbers its writers starts
      // its numbers again.
      update(
        c,
        "DELETE FROM delta_coordination.writer_epoch WHERE epoch_key = ?",
        "sales.epochs/bronze"
      ) shouldBe 1

      // Since V2: adopting a table registers it, and adopting it again moves its version.
      // Since V3: and gives it a new generation.
      update(c, Register, table, Long.box(41)) shouldBe 1
      update(
        c,
        "UPDATE delta_coordination.coordinated_table " +
          "SET registered_version = 50, delta_table_id = 'id', registered_at = now(), " +
          "generation = DEFAULT WHERE table_uri = ?",
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
        "UPDATE delta_coordination.schema_version SET version = 4",
        // An epoch is announced by the writers that it numbers, and by nobody else.
        "INSERT INTO delta_coordination.writer_epoch (epoch_key, epoch) VALUES ('by.hand/bronze', 1)",
        "UPDATE delta_coordination.writer_epoch SET epoch = 99",
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

  /** The generation of the registration of `table`, as a writer reads it before a commit. */
  private def generationOf(c: Connection, table: String): java.lang.Long =
    query(
      c,
      "SELECT generation FROM delta_coordination.coordinated_table WHERE table_uri = ?",
      table
    ).head.head.toLong

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
