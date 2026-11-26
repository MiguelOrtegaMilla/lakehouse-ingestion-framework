package lakehouse.ingestion.it.logstore

import java.io.IOException

import scala.concurrent.duration._
import scala.util.Using

import io.circe.Json
import lakehouse.ingestion.engine.control.StoredJson
import lakehouse.ingestion.it.support.{BootstrappedPostgres, CoordinationSchema, CoordinationStores}
import lakehouse.ingestion.logstore.coordination.CoordinationStoreContract.newTable
import lakehouse.ingestion.logstore.coordination._
import org.scalatest.BeforeAndAfterAll

/**
 * `PostgresRegistryMaintenance` keeps the contract of every `RegistryMaintenance`, against the
 * migrated schema of a real PostgreSQL: the commit path connects as its own role, and the
 * operator's tools as `ingest_maintenance` (ADR-0013 §3). Then what the database itself adds:
 * the commit path's role cannot do an operator's work, a change and its row of the log are made
 * whole or not at all, and the row says what the change found, as the database read it.
 */
final class PostgresRegistryMaintenanceSpec
    extends RegistryMaintenanceContract
    with BeforeAndAfterAll {

  private lazy val pg = BootstrappedPostgres.start()
  private lazy val commitPath =
    CoordinationStores.settings(pg.jdbcUrl(CoordinationSchema.Database), pg)
  private lazy val operator = CoordinationStores.settings(
    pg.jdbcUrl(CoordinationSchema.Database),
    pg,
    role = CoordinationSchema.MaintenanceRole
  )

  private lazy val postgresStore = new PostgresCoordinationStore(commitPath)
  private lazy val postgresMaintenance = new PostgresRegistryMaintenance(operator)

  override protected def store: CoordinationStore = postgresStore
  override protected def maintenance: RegistryMaintenance = postgresMaintenance

  override protected def logged(table: TableUri): Seq[(String, MaintenanceRequest)] =
    rowsOf(table).map { case (operation, request, _) => (operation, request) }

  /** The rows of the log of `table`, oldest first, read as the owner of the schema. */
  private def rowsOf(table: TableUri): Seq[(String, MaintenanceRequest, Json)] =
    Using.resource(pg.connectAsSuperuser(CoordinationSchema.Database)) { c =>
      Using.resource(
        c.prepareStatement(
          "SELECT operation, requested_by, reason, detail::text " +
            "FROM delta_coordination.maintenance_log WHERE table_uri = ? ORDER BY entry_id"
        )
      ) { statement =>
        statement.setString(1, table.value)
        Using.resource(statement.executeQuery()) { rows =>
          Iterator
            .continually(rows)
            .takeWhile(_.next())
            .map { row =>
              val detail =
                StoredJson.parse(row.getString(4)).fold(problem => fail(problem), identity)
              (row.getString(1), MaintenanceRequest(row.getString(2), row.getString(3)), detail)
            }
            .toVector
        }
      }
    }

  private def detailOf(table: TableUri, operation: String): Json =
    rowsOf(table).collect { case (`operation`, _, detail) => detail } match {
      case Seq(detail) => detail
      case other => fail(s"$table has ${other.size} rows of $operation in the log")
    }

  override def beforeAll(): Unit = CoordinationSchema.migrate(pg): Unit

  override def afterAll(): Unit =
    try postgresMaintenance.close()
    finally
      try postgresStore.close()
      finally pg.stop()

  test("the commit path's role cannot adopt a table again, nor forget one") {
    val table = newTable()
    store.register(table, Registration.NotYetCreated, Some("its-id"))
    store.take(claim(table, 0L)) shouldBe TakeResult.Taken
    store.taint(table, "version 0 is not the commit that took it")
    val before = (store.registration(table), store.entry(table, 0L))

    // The same code, connected as the role of the commit path: the database refuses it.
    Using.resource(new PostgresRegistryMaintenance(commitPath)) { asCommitPath =>
      val examined = asCommitPath.unfinished(table)
      an[IOException] should be thrownBy asCommitPath.readopt(table, 0L, None, examined, asked)
      an[IOException] should be thrownBy asCommitPath.forget(table, examined, asked)
      an[IOException] should be thrownBy asCommitPath.collected(table, asked, Nil)
    }

    // And nothing of what it tried was done, not even in part.
    (store.registration(table), store.entry(table, 0L)) shouldBe before
    logged(table) shouldBe empty
  }

  test("an operator's change is whole: if a part of it fails, none of it was made") {
    val table = newTable()
    store.register(table, Registration.NotYetCreated, None)
    store.take(claim(table, 0L)) shouldBe TakeResult.Taken
    store.taint(table, "version 0 has no entry")

    // Forgetting takes the registration, then deletes the taint, the entries and the
    // registration. A session that holds the entry makes the entries' step wait until the
    // statement is cancelled.
    Using.resource(pg.connect(CoordinationSchema.CommitPathRole, CoordinationSchema.Database)) {
      holder =>
        holder.setAutoCommit(false)
        Using.resource(
          holder.prepareStatement(
            "SELECT 1 FROM delta_coordination.commit_entry WHERE table_uri = ? FOR SHARE"
          )
        ) { statement =>
          statement.setString(1, table.value)
          statement.executeQuery().close()
        }
        val impatient =
          operator.copy(statementTimeout = 1.second)
        Using.resource(new PostgresRegistryMaintenance(impatient)) { blocked =>
          an[IOException] should be thrownBy
            blocked.forget(table, blocked.unfinished(table), asked)
        }
        holder.rollback()
    }

    // The taint, deleted first, is back with everything else; and the row of the log, written
    // before anything was deleted, is not there.
    store.registration(table).flatMap(_.taintReason) shouldBe Some("version 0 has no entry")
    store.entry(table, 0L).isDefined shouldBe true
    logged(table) shouldBe empty
  }

  test("the log says what each change found, as the database read it from the registry") {
    val table = newTable()
    maintenance.adopt(table, 1L, Some("its-id"), asked) shouldBe true
    detailOf(table, "adopt") shouldBe
      Json.obj("version" -> Json.fromLong(1L), "delta_table_id" -> Json.fromString("its-id"))

    // A taint, a version above the log and one that is not complete.
    val above = claim(table, 2L)
    store.take(above) shouldBe TakeResult.Taken
    store.complete(table, 2L, above.tempPath) shouldBe true
    val unfinished = claim(table, 3L)
    store.take(unfinished) shouldBe TakeResult.Taken
    store.taint(table, "version 2 is not the commit that took it")
    def entry(version: Long, of: Claim, complete: Boolean): Json =
      Json.obj(
        "version" -> Json.fromLong(version),
        "temp_path" -> Json.fromString(of.tempPath),
        "complete" -> Json.fromBoolean(complete),
        "writer_instance" -> Json.fromString(of.writerInstance),
        "run_id" -> of.runId.fold(Json.Null)(id => Json.fromString(id.toString))
      )

    maintenance.readopt(table, 1L, None, maintenance.contradicted(table, 1L), asked) shouldBe a[
      RegistryChange.Applied[_]
    ]
    detailOf(table, "readopt") shouldBe Json.obj(
      "version_before" -> Json.fromLong(1L),
      "version" -> Json.fromLong(1L),
      "delta_table_id" -> Json.fromString("its-id"),
      "cleared_taint" -> Json.fromString("version 2 is not the commit that took it"),
      "removed_entries" -> Json.arr(entry(2L, above, complete = true), entry(3L, unfinished, false))
    )

    // Forgotten with a commit that is complete and one that is not.
    val complete = claim(table, 2L)
    store.take(complete) shouldBe TakeResult.Taken
    store.complete(table, 2L, complete.tempPath) shouldBe true
    val left = claim(table, 3L)
    store.take(left) shouldBe TakeResult.Taken
    maintenance.forget(table, maintenance.unfinished(table), asked) shouldBe RegistryChange
      .Applied(())
    detailOf(table, "reset") shouldBe Json.obj(
      "version" -> Json.fromLong(1L),
      "delta_table_id" -> Json.fromString("its-id"),
      "cleared_taint" -> Json.Null,
      "entries" -> Json.fromLong(2L),
      "unfinished_entries" -> Json.arr(entry(3L, left, complete = false))
    )

    maintenance.collected(table, asked, Seq("a" -> "removed_orphan", "b" -> "kept_recent"))
    detailOf(table, "collect_temps") shouldBe Json.obj(
      "temps" -> Json.obj(
        "a" -> Json.fromString("removed_orphan"),
        "b" -> Json.fromString("kept_recent")
      )
    )
  }
}
