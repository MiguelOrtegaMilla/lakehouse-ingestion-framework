package lakehouse.ingestion.it.ingestion

import java.sql.Connection
import java.time.{Clock, Instant, ZoneOffset}

import scala.util.Using

import lakehouse.ingestion.core.config.{SecretResolver, Source}
import lakehouse.ingestion.core.state.StoredWatermark
import lakehouse.ingestion.engine.connector.ConnectorRegistry
import lakehouse.ingestion.engine.files.{FileSourceConnector, Landing}
import lakehouse.ingestion.engine.jdbc.JdbcSourceConnector
import lakehouse.ingestion.engine.{FaultHooks, IngestionService}
import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers

/**
 * What the suites that run a table of a database through `IngestionService` share, on top of
 * `IngestionSuite`: a source of a table of the demo database `sales`, written as a file writes
 * one; the service that reads it, with the JDBC connector alone; and the hands of the owner of
 * its tables, who makes and changes them as the applications of a source would.
 *
 * A source reads as `sales_ro`, the role of the engine, unless it says another: the suite of
 * what reaches a source as SQL reads as `sales_owner`, which could do the harm that it looks
 * for.
 */
trait JdbcIngestionSuite extends IngestionSuite {
  this: Suite with Matchers =>

  /** The section `load` of a source whose table is read whole. */
  protected val FullLoad = "load: { mode: full }"

  // ---------------------------------------------------------------------------------------------
  // The owner of the tables of the source
  // ---------------------------------------------------------------------------------------------

  /** A connection as the owner of the tables of `sales`, each time it is called. */
  protected def ownersConnection: () => Connection = () => pg.connect("sales_owner", "sales")

  /** What `body` does as the owner of the tables of `sales`, on a connection of its own. */
  protected def asOwner[T](body: Connection => T): T = Using.resource(ownersConnection())(body)

  protected def execute(c: Connection, sql: String): Unit =
    Using.resource(c.createStatement())(_.execute(sql): Unit)

  // ---------------------------------------------------------------------------------------------
  // A source of a table, and the service that reads it
  // ---------------------------------------------------------------------------------------------

  /**
   * The passwords of the roles of `sales`, each under the variable that a source of a suite
   * names: `SALES_RO_PASSWORD`, `SALES_OWNER_PASSWORD`. A suite with a server of its own adds
   * its own.
   */
  protected def passwords: Map[String, String] =
    Seq("sales_ro", "sales_owner").map(role => variableOf(role) -> pg.password(role)).toMap

  private def variableOf(role: String): String = s"${role.toUpperCase}_PASSWORD"

  /**
   * A source that reads a table of `sales`, under an id of its own unless it is given one.
   *
   * @param table as a file names it: `public.orders`
   * @param load the section `load` of the file, whole
   * @param role the role that it connects as
   * @param password where the password of the role is; the variable of the role, unless said
   * @param statement the longest that a statement of it may run; that of a source, unless said
   * @param textColumns the columns that it reads as text
   */
  protected def jdbcSource(
      table: String,
      load: String = FullLoad,
      id: String = newSourceId().value,
      role: String = "sales_ro",
      url: String = pg.jdbcUrl("sales"),
      password: Option[String] = None,
      statement: Option[String] = None,
      textColumns: Seq[String] = Nil
  ): Source =
    Source
      .load(
        s"$id.yaml",
        s"""id: $id
           |connector:
           |  type: jdbc
           |  connection:
           |    url: "$url"
           |    user: $role
           |    password: { secretRef: "${password.getOrElse(s"env:${variableOf(role)}")}" }
           |    tls: { mode: disable }
           |    ${statement.fold("")(longest => s"timeouts: { statement: $longest }")}
           |  table: $table
           |  text_columns: [${textColumns.mkString(", ")}]
           |$load
           |targets: { bronze: { table: bronze/${id.replace('.', '/')} } }
           |""".stripMargin
      )
      .value
      .value

  /** The ledger asks the plans of a table, and those of files of a suite that has both. */
  override protected def connectors: ConnectorRegistry =
    ConnectorRegistry(
      new FileSourceConnector(new Landing(landingRoot)),
      new JdbcSourceConnector(SecretResolver(passwords.get))
    )

  /** The service, as an application puts it together, with the JDBC connector alone. */
  protected def reading(hooks: FaultHooks = FaultHooks.Off): IngestionService =
    new IngestionService(
      database,
      ConnectorRegistry(new JdbcSourceConnector(SecretResolver(passwords.get))),
      spark,
      lakeRoot,
      timing,
      Clock.fixed(Now, ZoneOffset.UTC),
      hooks
    )

  // ---------------------------------------------------------------------------------------------
  // What a run left, and what the server was sent
  // ---------------------------------------------------------------------------------------------

  /** The watermark of a source, as the control plane has it. */
  protected def watermarkOf(of: Source): Option[Instant] =
    stateOf(of).flatMap(_.watermark).map(StoredWatermark.fromJson(_).value.value)

  /** Where the log of the server ends now: what it logs after is what a test caused. */
  protected def logMark(): Int = pg.serverLog().length
}
