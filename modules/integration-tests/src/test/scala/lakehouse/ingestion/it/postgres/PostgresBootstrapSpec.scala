package lakehouse.ingestion.it.postgres

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.sql.{Connection, SQLException}

import scala.util.Using

import lakehouse.ingestion.it.support.BootstrappedPostgres
import lakehouse.ingestion.it.support.BootstrappedPostgres.LoginRoles
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The cluster bootstrap in `docker/postgres/init` (ADR-0003, ADR-0013 §3): roles, databases,
 * schemas and database-level privileges. Grants on tables belong to the migrations and are
 * tested with them (P2a, P4).
 */
final class PostgresBootstrapSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private lazy val pg = BootstrappedPostgres.start()

  // Start in beforeAll: if the container cannot start, the suite aborts once, with that error.
  override def beforeAll(): Unit = { pg; () }

  override def afterAll(): Unit = pg.stop()

  private val InsufficientPrivilege = "42501"
  private val InvalidPassword = "28P01"

  /** Which database each login role may connect to. */
  private val grantedDatabase = Map(
    "ingest_migrator" -> "ingestion_control",
    "ingest_control" -> "ingestion_control",
    "ingest_coordination" -> "ingestion_control",
    "ingest_maintenance" -> "ingestion_control",
    "sales_owner" -> "sales",
    "sales_ro" -> "sales"
  )

  private def sqlState(action: => Any): String = intercept[SQLException](action).getSQLState

  private def update(c: Connection, sql: String): Unit =
    Using.resource(c.createStatement())(_.execute(sql): Unit)

  private def queryOne(c: Connection, sql: String): String =
    Using.resource(c.createStatement()) { st =>
      Using.resource(st.executeQuery(sql)) { rs =>
        rs.next() shouldBe true
        rs.getString(1)
      }
    }

  test("the compose service is pinned, non-root, hardened and bound to loopback") {
    val compose = new String(Files.readAllBytes(BootstrappedPostgres.composeFile), UTF_8)
    // Throws unless the image is pinned by tag and digest.
    BootstrappedPostgres.imageFromCompose(BootstrappedPostgres.composeFile)
    val required = Seq(
      "non-root user" -> """(?m)^\s+user: postgres$""",
      "no-new-privileges" -> """(?m)^\s+- no-new-privileges:true$""",
      "cap_drop ALL" -> """(?m)^\s+cap_drop:\s*\n\s+- ALL$""",
      "loopback-only port" -> """(?m)^\s+- "127\.0\.0\.1:[^"]*:5432"$"""
    )
    val missing = required.collect {
      case (control, pattern) if pattern.r.findFirstIn(compose).isEmpty => control
    }
    withClue(s"${BootstrappedPostgres.composeFile} lacks: ") { missing shouldBe empty }
  }

  test("the server runs as the non-root postgres user") {
    pg.exec("id", "-un") shouldBe "postgres"
    pg.exec("id", "-u") shouldBe "999"
  }

  test("every role of the bootstrap is a plain login role without elevated attributes") {
    grantedDatabase.keySet shouldBe LoginRoles.toSet
    Using.resource(pg.connectAsSuperuser("postgres")) { c =>
      LoginRoles.foreach { role =>
        val attributes = queryOne(
          c,
          s"""select concat_ws(',', rolcanlogin, rolsuper, rolcreatedb, rolcreaterole,
             |                      rolreplication, rolbypassrls)
             |from pg_roles where rolname = '$role'""".stripMargin
        )
        withClue(s"$role (login, super, createdb, createrole, replication, bypassrls): ") {
          attributes shouldBe "t,f,f,f,f,f"
        }
      }
    }
  }

  test("each role connects with its own secret to the database it is granted") {
    grantedDatabase.foreach { case (role, database) =>
      Using.resource(pg.connect(role, database)) { c =>
        queryOne(c, "select current_user || '@' || current_database()") shouldBe s"$role@$database"
      }
    }
  }

  test("a role cannot connect to a database it is not granted") {
    val allDatabases = Set("ingestion_control", "sales", "postgres")
    for {
      (role, granted) <- grantedDatabase
      database <- allDatabases - granted
    } withClue(s"$role -> $database: ") {
      sqlState(pg.connect(role, database).close()) shouldBe InsufficientPrivilege
    }
  }

  test("a wrong password is rejected, so TCP connections do not use trust authentication") {
    sqlState(
      pg.connect("ingest_control", "ingestion_control", password = "wrong").close()
    ) shouldBe InvalidPassword
  }

  test("schemas control and delta_coordination exist and belong to ingest_migrator") {
    Using.resource(pg.connect("ingest_migrator", "ingestion_control")) { c =>
      queryOne(
        c,
        """select string_agg(nspname || ':' || nspowner::regrole::text, ',' order by nspname)
          |from pg_namespace where nspname in ('control', 'delta_coordination')""".stripMargin
      ) shouldBe "control:ingest_migrator,delta_coordination:ingest_migrator"
    }
  }

  test("no runtime role can create a schema in ingestion_control, ingest_migrator included") {
    grantedDatabase.collect { case (role, "ingestion_control") => role }.foreach { role =>
      Using.resource(pg.connect(role, "ingestion_control")) { c =>
        withClue(s"$role: ") {
          sqlState(update(c, "create schema rogue")) shouldBe InsufficientPrivilege
        }
      }
    }
  }

  test("no runtime role can create objects in schema public of ingestion_control") {
    Seq("ingest_control", "ingest_coordination").foreach { role =>
      Using.resource(pg.connect(role, "ingestion_control")) { c =>
        withClue(s"$role: ") {
          sqlState(update(c, "create table public.rogue (i int)")) shouldBe InsufficientPrivilege
        }
      }
    }
  }

  test("sales_owner owns schema public of sales, and not the database") {
    Using.resource(pg.connect("sales_owner", "sales")) { c =>
      queryOne(
        c,
        "select nspowner::regrole::text from pg_namespace where nspname = 'public'"
      ) shouldBe "sales_owner"
      queryOne(
        c,
        "select datdba::regrole::text from pg_database where datname = current_database()"
      ) shouldBe BootstrappedPostgres.Superuser
    }
  }

  test("sales_owner creates, writes and drops the tables of the source without the superuser") {
    Using.resource(pg.connect("sales_owner", "sales")) { owner =>
      update(owner, "create table public.customers (id int primary key, name text)")
      update(owner, "create index customers_name on public.customers (name)")
      update(owner, "insert into public.customers values (1, 'a'), (2, 'b')")
      update(owner, "update public.customers set name = 'c' where id = 1")
      update(owner, "delete from public.customers where id = 2")
      queryOne(owner, "select string_agg(name, ',') from public.customers") shouldBe "c"
      update(owner, "truncate public.customers")
      update(owner, "drop table public.customers")
    }
  }

  test("sales_owner can create no schema in sales, and change no setting of the database") {
    Using.resource(pg.connect("sales_owner", "sales")) { c =>
      sqlState(update(c, "create schema rogue")) shouldBe InsufficientPrivilege
      sqlState(
        update(c, "alter database sales set statement_timeout = '1s'")
      ) shouldBe InsufficientPrivilege
      sqlState(update(c, "create role rogue")) shouldBe InsufficientPrivilege
    }
  }

  test("sales_ro reads tables the source owner creates later, and cannot change them") {
    Using.resource(pg.connect("sales_owner", "sales")) { owner =>
      update(owner, "create table public.orders (id int primary key, amount numeric)")
      update(owner, "insert into public.orders values (1, 9.99)")
    }
    Using.resource(pg.connect("sales_ro", "sales")) { c =>
      queryOne(c, "select count(*) from public.orders") shouldBe "1"
      Seq(
        "insert into public.orders values (2, 1.00)",
        "update public.orders set amount = 0",
        "delete from public.orders",
        "truncate public.orders",
        "drop table public.orders",
        "alter table public.orders add column rogue int",
        "create table public.rogue (i int)"
      ).foreach { statement =>
        withClue(s"$statement: ") {
          sqlState(update(c, statement)) shouldBe InsufficientPrivilege
        }
      }
    }
    Using.resource(pg.connect("sales_owner", "sales")) { owner =>
      queryOne(owner, "select id || ':' || amount from public.orders") shouldBe "1:9.99"
    }
  }

  test("sales_ro reads only what the source owner creates: not a table of another role's") {
    Using.resource(pg.connectAsSuperuser("sales")) { c =>
      update(c, "create table public.not_the_sources (i int)")
    }
    Using.resource(pg.connect("sales_ro", "sales")) { c =>
      sqlState(
        queryOne(c, "select count(*) from public.not_the_sources")
      ) shouldBe InsufficientPrivilege
    }
  }
}
