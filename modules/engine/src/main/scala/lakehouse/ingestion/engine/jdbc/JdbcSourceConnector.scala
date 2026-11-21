package lakehouse.ingestion.engine.jdbc

import java.util.Locale

import scala.util.control.NonFatal

import io.circe.Json
import lakehouse.ingestion.core.config.{
  ConnectorKind,
  DatabaseConnection,
  JdbcConnector,
  JdbcConnectorConfig,
  Load,
  Secret,
  SecretResolver,
  Source,
  SqlIdentifier
}
import lakehouse.ingestion.core.jdbc.FullLoadPlan
import lakehouse.ingestion.engine.RunFailedException
import lakehouse.ingestion.engine.connector.{
  PlanContext,
  Progress,
  ReadContext,
  ReadResult,
  SourceConnector
}
import lakehouse.ingestion.engine.control.PostgresJdbc
import org.apache.spark.sql.functions.{array, col, element_at, lit, raise_error, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Reads a table of a relational database over JDBC: PostgreSQL (discovery §17, P6).
 *
 * '''A full load is one statement, and so one snapshot of its table.''' The plan of a batch is
 * the table (`FullLoadPlan`). The read is one statement on one partition, with a cursor: the
 * rows come as they are asked for, and are never held whole, in the driver or in Spark. They
 * are read from the source each time they are gone over, which the read says
 * (`ReadResult.OnePass`), and the engine goes over them once, as it writes them.
 *
 * '''What is asked of the source before a plan, and before a read''': the columns of the
 * table, and their types (`SourceDatabase.columns`). A table that is not there, or that the
 * role may not read, fails the run before a batch is planned. Two columns whose names differ
 * only in their case are refused, since a table of Delta cannot have both.
 *
 * '''The types of the source are kept, and no value is changed without a word'''
 * (`BronzeTypes`). A column of a type that Spark reads into one of its own that changes its
 * values is read as text, the value as the server writes it. A date or a timestamp that is
 * `infinity` cannot be told by its type, so the source says it of each row, in the statement
 * that gives the row (`JdbcSql.flagged`), and such a row fails its batch, with the column
 * named: Spark would write it as the last day of the year 9999, which a row may hold for
 * itself. A column that is known to hold it is named in the configuration
 * (`connector.text_columns`), and is read as text as well, whatever its type.
 *
 * '''Every connection to a source is opened the same way''' (`PostgresJdbc.ofASource`): TLS as
 * the configuration says, its timeouts, and the settings of a session that a read stands on,
 * whatever the source has them at. The password is resolved when a connection is opened, and
 * goes to the driver alone: it is in no plan of Spark's and in no message (pinned by
 * `SparkJdbcContractSpec`).
 *
 * '''The engine writes no statement from a value''' (ADR-0013 §1, `JdbcSql`).
 *
 * '''A failure says its class''' (`JdbcFailures`), wherever it comes up: when the source is
 * asked for its columns, when Spark asks for them again, and while the engine goes over the
 * rows (`failure`).
 *
 * @param secrets where the password that a source points at is read from
 */
final class JdbcSourceConnector(secrets: SecretResolver)
    extends SourceConnector[JdbcConnectorConfig] {
  import JdbcSourceConnector._

  override val kind: ConnectorKind = JdbcConnector

  override def plan(
      source: Source,
      config: JdbcConnectorConfig,
      context: PlanContext
  ): Option[Json] = {
    val columns = columnsOf(source, config)
    source.load match {
      case Load.Full => Some(FullLoadPlan(config.table).toJson)
      case incremental: Load.Incremental =>
        val column = incremental.watermark.column.value
        WatermarkColumn.check(columns, column) match {
          case Left(problem) =>
            throw RunFailedException.permanent(
              s"the table ${config.table} of ${where(source, config)} $problem. Nothing was " +
                "planned"
            )
          case Right(warning) =>
            warning.foreach(text => log.warn(s"${source.id}: in the table ${config.table}, $text"))
        }
        throw RunFailedException.permanent(
          s"${source.id} is loaded by a watermark, and this version of the connector reads " +
            "a table whole (load.mode: full). Nothing was planned"
        )
      case other =>
        throw RunFailedException.permanent(
          s"${source.id} has the load ${other.mode.name}, which is no load of a table"
        )
    }
  }

  override def read(
      source: Source,
      config: JdbcConnectorConfig,
      plan: Json,
      context: ReadContext
  ): ReadResult = {
    val planned = FullLoadPlan.fromJson(plan) match {
      case Right(full) => full
      case Left(problem) =>
        throw RunFailedException.permanent(
          s"the plan of the batch of ${source.id} is not one that the JDBC connector reads, " +
            s"and nothing was read: it $problem"
        )
    }
    if (planned.table != config.table)
      throw RunFailedException.permanent(
        s"the batch of ${source.id} was planned as a snapshot of the table ${planned.table}, " +
          s"and the configuration of the source reads ${config.table} now. A plan is read as " +
          "it was written, and nothing was read. Give the source the table that it had " +
          "(connector.table) and run it again"
      )

    val columns = columnsOf(source, config)
    // As text: what the configuration says, and what the type of a column says.
    val named = config.textColumns.map(_.value).toSet
    val text = columns.filter { column =>
      named(column.name) || BronzeTypes.of(column) != BronzeTypes.AsSparkReads
    }
    // A column that is read as text holds infinity as the source writes it.
    val finite = columns.filter(BronzeTypes.mayBeInfinite).filterNot(column => named(column.name))
    val flag = unusedName(columns, Flag)
    val password = passwordOf(source, config)
    val table =
      if (finite.isEmpty) JdbcSql.table(config.table)
      else JdbcSql.flagged(config.table, finite.map(_.name), flag)

    /** The rows, read `fetch` of them at a time. Nothing is read before they are gone over. */
    def rows(fetch: Int): DataFrame = {
      val read =
        try
          load(context.spark, config.connection, password, table, text.map(_.name), fetch)
        catch {
          // Spark asks for the columns when the read is made, on a connection of its own.
          case NonFatal(e) => throw failure(source, config, e).getOrElse(e)
        }
      if (finite.isEmpty) read
      else {
        // What a row of each column is refused with, by the place that the source gives.
        val refusals = array(finite.map { column =>
          lit(JdbcFailures.Raised + infinite(source, config, column))
        }: _*)
        read
          .filter(
            when(
              col(quotedForSpark(flag)).isNotNull,
              raise_error(element_at(refusals, col(quotedForSpark(flag))))
            )
              .otherwise(lit(true))
          )
          .drop(flag)
      }
    }

    ReadResult.OnePass(rows(FetchSize), probe = rows(fetch = 1))
  }

  /** A full load leaves nothing to start from: the next batch is the table again. */
  override def progress(source: Source, config: JdbcConnectorConfig, plan: Json): Progress =
    FullLoadPlan.fromJson(plan) match {
      case Right(_) => Progress(watermark = None, files = None)
      case Left(problem) =>
        throw RunFailedException.permanent(
          s"the plan of the batch of ${source.id} is not one that the JDBC connector reads: " +
            s"it $problem"
        )
    }

  override def failure(
      source: Source,
      config: JdbcConnectorConfig,
      error: Throwable
  ): Option[RunFailedException] =
    JdbcFailures.of(where(source, config), config.connection, error)

  /**
   * The columns of the table of a source, asked of the source.
   *
   * @throws RunFailedException with its class, if the source cannot be asked, has no such
   *   table, or has one that Bronze cannot keep
   */
  private def columnsOf(source: Source, config: JdbcConnectorConfig): Seq[SourceColumn] = {
    val password = passwordOf(source, config)
    val columns =
      try {
        val database = SourceDatabase.open(config.connection, password)
        try database.columns(config.table)
        finally database.close()
      } catch {
        case NonFatal(e) => throw failure(source, config, e).getOrElse(e)
      }

    def refused(problem: String): Nothing =
      throw RunFailedException.permanent(
        s"the table ${config.table} of ${where(source, config)} $problem. Nothing was read"
      )
    if (columns.isEmpty) refused("has no column")
    val alike = columns
      .groupBy(_.name.toLowerCase(Locale.ROOT))
      .values
      .filter(_.size > 1)
      .map(_.map(_.name).mkString(" and "))
      .toSeq
      .sorted
    if (alike.nonEmpty)
      refused(
        s"has columns whose names differ only in their case, ${alike.mkString("; ")}, and a " +
          "table of Delta cannot have both"
      )
    val unknown = config.textColumns.map(_.value).filterNot(columns.map(_.name).toSet)
    if (unknown.nonEmpty)
      refused(
        s"has no column ${unknown.mkString(", ")}, which connector.text_columns names: a " +
          "column is named as the source writes it, with its case. Its columns are " +
          columns.map(_.name).mkString(", ")
      )
    columns
  }

  private def passwordOf(source: Source, config: JdbcConnectorConfig): Secret =
    secrets.resolve(config.connection.password) match {
      case Right(password) => password
      case Left(problem) =>
        throw RunFailedException.permanent(
          s"the password of ${where(source, config)} could not be read, and the source was " +
            s"not asked anything: $problem"
        )
    }
}

object JdbcSourceConnector {

  private val log = LoggerFactory.getLogger(classOf[JdbcSourceConnector])

  /**
   * How many rows a fetch of the cursor of a read brings. Without one, the driver holds the
   * whole result of the statement (pinned by `SparkJdbcContractSpec`). It bounds what a read
   * holds at a time, by the width of a row; what suits a source best is measured, with the
   * rest of what a read costs (E3).
   */
  val FetchSize = 1000

  /** The column that the source is asked to add, for a row with a date that is not finite. */
  val Flag = "_lakehouse_infinite"

  /** The source, for the start of a message: what it is in the configuration, and where. */
  def where(source: Source, config: JdbcConnectorConfig): String =
    s"the source ${source.id} (${config.connection.url.value}, as ${config.connection.user})"

  /**
   * The options of a read of Spark's JDBC source: how the connection is opened, and how the
   * rows are fetched.
   *
   * @param table what is read, as it is to be written into the statement (`JdbcSql`)
   * @param fetch how many rows a fetch of the cursor brings
   */
  def options(
      connection: DatabaseConnection,
      password: Secret,
      table: String,
      fetch: Int
  ): Map[String, String] =
    PostgresJdbc.ofASource(connection) ++ Map(
      "url" -> connection.url.value,
      "user" -> connection.user.value,
      "password" -> password.reveal,
      "dbtable" -> table,
      // A cursor: without it the driver reads the whole result before it gives a row.
      "fetchsize" -> fetch.toString
    )

  private def load(
      spark: SparkSession,
      connection: DatabaseConnection,
      password: Secret,
      table: String,
      asText: Seq[String],
      fetch: Int
  ): DataFrame = {
    val reader = spark.read.format("jdbc").options(options(connection, password, table, fetch))
    // The columns that are read as text; every other, as Spark reads its type.
    val typed =
      if (asText.isEmpty) reader
      else
        reader.option(
          "customSchema",
          asText.map(name => s"${quotedForSpark(name)} STRING").mkString(", ")
        )
    typed.load()
  }

  /** A name as Spark takes one, whatever it holds. */
  private def quotedForSpark(name: String): String = "`" + name.replace("`", "``") + "`"

  /** `name`, or the first of it with underscores after it that no column has, case aside. */
  private def unusedName(columns: Seq[SourceColumn], name: String): String = {
    val taken = columns.map(_.name.toLowerCase(Locale.ROOT)).toSet
    Iterator.iterate(name)(_ + "_").find(candidate => !taken(candidate)).get
  }

  private def infinite(source: Source, config: JdbcConnectorConfig, column: SourceColumn): String =
    s"the column ${column.name} of the table ${config.table} of ${where(source, config)} has " +
      s"infinity or -infinity in a row of the batch, which PostgreSQL has for a " +
      s"${column.typeName} and Spark has not: it would be written to Bronze as another " +
      "date, the last day of the year 9999 for infinity. Nothing of the batch was written. " +
      (SqlIdentifier.parse(column.name) match {
        case Right(_) =>
          s"Add ${column.name} to connector.text_columns, and the column is read as text, " +
            "each value as the source writes it"
        case Left(_) =>
          "connector.text_columns would have the column read as text, each value as the " +
            "source writes it, and takes a name of letters, digits and underscores, which " +
            "this one is not: read the table through a view that gives the column such a name"
      })
}
