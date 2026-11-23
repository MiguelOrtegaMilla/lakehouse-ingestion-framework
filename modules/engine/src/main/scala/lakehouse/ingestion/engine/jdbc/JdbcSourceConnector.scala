package lakehouse.ingestion.engine.jdbc

import java.time.{Duration => Span, Instant, ZoneId, ZoneOffset}
import java.util.Locale

import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import scala.util.control.NonFatal

import io.circe.Json
import lakehouse.ingestion.core.config.{
  ConnectorKind,
  DatabaseConnection,
  Durations,
  EngineColumns,
  JdbcConnector,
  JdbcConnectorConfig,
  Load,
  Profile,
  Secret,
  SecretResolver,
  Source,
  SqlIdentifier,
  Timestamps
}
import lakehouse.ingestion.core.jdbc.FullLoadPlan
import lakehouse.ingestion.core.state.StoredWatermark
import lakehouse.ingestion.core.watermark.{
  BackfillRange,
  WatermarkHorizon,
  WatermarkPlan,
  WatermarkPlanner
}
import lakehouse.ingestion.engine.RunFailedException
import lakehouse.ingestion.engine.connector.{
  PlanContext,
  Progress,
  RangePlanner,
  ReadContext,
  ReadResult,
  SourceConnector
}
import lakehouse.ingestion.engine.control.PostgresJdbc
import lakehouse.ingestion.engine.delta.DeltaNames
import org.apache.spark.sql.functions.{array, col, element_at, lit, raise_error, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
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
 * '''A load by a watermark reads a window of the clock of its source''' (ADR-0020). The plan of
 * a batch is the window (`WatermarkPlan`): the rows whose watermark column is after its lower
 * bound and no later than its upper one. The connector asks the source what time it is, with a
 * constant statement, and the window is a function of that, of the watermark that the engine
 * gives it and of the settings of the load (`WatermarkPlanner`): the clock of the engine bounds
 * nothing. The bounds reach the source as predicates of the statement of the rows, which
 * Spark writes from a filter on the rows; they are exact only where the JVM and the session
 * are in UTC, so a window is not read anywhere else. What the commit of the batch records is
 * the upper bound of its plan, whatever the rows were, and though there were none. A window
 * that has no moment in it, since the source has been read up to where it may be, is no
 * batch: the run ends well, with nothing read.
 *
 * '''A backfill of such a source is read in windows as well''' (ADR-0021 §5, `ranges`): each
 * chunk is a window of the same column, planned by the same planner with the range of the
 * backfill as its bounds, and read and recorded as any window. A table loaded whole has no
 * range to backfill.
 *
 * '''A plan is read under the load that it was planned under.''' A window is not read as a
 * table, nor a table as a window, nor a window of one column under another: the source is
 * given back the load that it had, its batch is finished, and its load is changed then. Nor
 * is a window read from a table that no longer has its column, as a timestamp.
 *
 * '''What is asked of the source before a plan, and before a read''': the columns of the
 * table, and their types (`SourceDatabase.columns`). A table that is not there, or that the
 * role may not read, fails the run before a batch is planned. Two columns whose names differ
 * only in their case are refused, since a table of Delta cannot have both; and so is a column
 * whose name a table of Delta cannot have (`DeltaNames`), or that is under a name of the
 * columns that the engine adds to a row (`EngineColumns`). Any other name is taken, whatever
 * it holds: it is written quoted wherever the engine writes it (`JdbcSql`).
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
 * goes to the driver alone. Spark is given where it is, and not the password, which a provider
 * of the engine's reads as it connects (`SecretRefConnectionProvider`): Spark's own logs every
 * property of a connection at `DEBUG`.
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

  // What the connections that Spark opens read their passwords with (`SecretRefConnectionProvider`).
  private val resolver = SecretRefConnectionProvider.register(secrets)

  override val kind: ConnectorKind = JdbcConnector

  override def plan(
      source: Source,
      config: JdbcConnectorConfig,
      context: PlanContext
  ): Option[Json] =
    source.load match {
      case Load.Full =>
        columnsOf(source, config): Unit
        Some(FullLoadPlan(config.table).toJson)
      case incremental: Load.Incremental =>
        // The horizon of the chain that this run is in, after its first: the clock is not
        // asked again, so that the chain ends though the source goes on writing (ADR-0020, 9).
        val chain = context.horizon.map(horizonOf(source, _))
        // On one connection: what the table is, and what time it is where the table is.
        val (said, clock) = asked(source, config) { database =>
          (database.columns(config.table), if (chain.isEmpty) Some(database.clock()) else None)
        }
        windowed(source, config, incremental, kept(source, config, said))
        (chain, clock) match {
          case (Some(horizon), _) =>
            windowUpTo(source, incremental, context.watermark, horizon.value, clock = None)
          case (None, Some(said)) =>
            nextWindow(source, config, incremental, context.watermark, said)
          case (None, None) => throw new IllegalStateException("the clock was not asked")
        }
      case other =>
        throw RunFailedException.permanent(
          s"${source.id} has the load ${other.mode.name}, which is no load of a table"
        )
    }

  /**
   * A backfill reads a range of the watermark of a source loaded by one (ADR-0021 §5), in
   * chunks that are windows of its watermark column: a chunk is planned as a window is, read
   * as one, and recorded as one. A table loaded whole has no range to read.
   */
  override def ranges(source: Source, config: JdbcConnectorConfig): Option[RangePlanner] =
    source.load match {
      case incremental: Load.Incremental =>
        Some(new RangePlanner {
          override def horizon(): Instant =
            horizonBy(source, config, incremental, asked(source, config)(_.clock()))

          override def chunk(
              range: BackfillRange,
              chunk: FiniteDuration,
              progress: Option[Json]
          ): Option[Json] = {
            // The table as a window of it needs it, as for the plan of a batch.
            windowed(source, config, incremental, columnsOf(source, config))
            val reached = progress.map(storedWatermark(source, _))
            WatermarkPlanner.chunk(incremental.watermark.column, range, chunk, reached) match {
              case WatermarkPlanner.Planned(plan) => Some(plan.toJson)
              case _: WatermarkPlanner.NothingNew => None
            }
          }
        })
      case _ => None
    }

  override def read(
      source: Source,
      config: JdbcConnectorConfig,
      plan: Json,
      context: ReadContext
  ): ReadResult = {
    // The window of the plan: none, for a plan that is its table whole.
    val window = planOf(source, plan) match {
      case Whole(full) =>
        if (full.table != config.table)
          throw RunFailedException.permanent(
            s"the batch of ${source.id} was planned as a snapshot of the table ${full.table}, " +
              s"and the configuration of the source reads ${config.table} now. A plan is " +
              "read as it was written, and nothing was read. Give the source the table that " +
              "it had (connector.table) and run it again"
          )
        None
      case Window(bounds) =>
        inUtc(source, context.spark)
        Some(bounds)
    }

    val columns = columnsOf(source, config)
    // A window that a run left planned is read from a table that may have changed since: by
    // a column that it still has, of a timestamp. Without the column there is nothing to
    // bound the rows by; under another type Spark would compare the values itself, over the
    // whole table.
    window.foreach { bounds =>
      WatermarkColumn.check(columns, bounds.column.value).left.foreach { problem =>
        throw RunFailedException.permanent(
          s"the table ${config.table} of ${where(source, config)} $problem. The batch of " +
            s"${source.id} was planned as a window of that column, as the table had it then, " +
            "and nothing was read"
        )
      }
    }
    // As text: what the configuration says, and what the type of a column says.
    val named = config.textColumns.map(_.value).toSet
    val text = columns.filter { column =>
      named(column.name) || BronzeTypes.of(column) != BronzeTypes.AsSparkReads
    }
    // A column that is read as text holds infinity as the source writes it.
    val finite = columns.filter(BronzeTypes.mayBeInfinite).filterNot(column => named(column.name))
    val flag = unusedName(columns, Flag)
    val table =
      if (finite.isEmpty) JdbcSql.table(config.table)
      else JdbcSql.flagged(config.table, finite.map(_.name), flag)

    /** The rows, read `fetch` of them at a time. Nothing is read before they are gone over. */
    def rows(fetch: Int): DataFrame = {
      val read =
        try
          load(context.spark, config.connection, resolver, table, text.map(_.name), fetch)
        catch {
          // Spark asks for the columns when the read is made, on a connection of its own.
          case NonFatal(e) => throw failure(source, config, e).getOrElse(e)
        }
      // The bounds first, so that what is asked of each row after them is asked of the rows
      // of the window, wherever it is asked. Spark writes them into the statement of the
      // source, with the other filter behind them (pinned by `JdbcSourceConnectorSpec`).
      val inWindow = window.fold(read)(bounds => read.filter(within(bounds)))
      if (finite.isEmpty) inWindow
      else {
        // What a row of each column is refused with, by the place that the source gives.
        val refusals = array(finite.map { column =>
          val watermark = window.exists(_.column.value == column.name)
          lit(JdbcFailures.Raised + infinite(source, config, column, watermark))
        }: _*)
        inWindow
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

  /**
   * A full load leaves nothing to start from: the next batch is the table again. A window
   * leaves its upper bound, which the next one starts after (ADR-0020).
   */
  override def progress(source: Source, config: JdbcConnectorConfig, plan: Json): Progress =
    planOf(source, plan) match {
      case Whole(_) => Progress(watermark = None, files = None)
      case Window(bounds) => Progress(watermark = Some(bounds.watermark.toJson), files = None)
    }

  /** More while a window ends before its horizon; a snapshot of a table is all there is. */
  override def more(source: Source, config: JdbcConnectorConfig, plan: Json): Option[Json] =
    planOf(source, plan) match {
      case Whole(_) => None
      case Window(bounds) => bounds.next.map(_.toJson)
    }

  /**
   * A snapshot of a table is the table as it is when it is read: its plan names the table, and
   * a run that reads it again reads what a plan made anew would. A window is its bounds,
   * whenever it is read.
   */
  override def wholeWhenRead(source: Source, config: JdbcConnectorConfig, plan: Json): Boolean =
    planOf(source, plan) match {
      case Whole(_) => true
      case Window(_) => false
    }

  /** The horizon of a chain, as the plan before this one gave it. */
  private def horizonOf(source: Source, document: Json): WatermarkHorizon =
    WatermarkHorizon
      .fromJson(document)
      .fold(
        problem =>
          throw RunFailedException.permanent(
            s"the horizon that ${source.id} was given to plan against is not one that this " +
              s"application wrote, and nothing was planned: it $problem"
          ),
        identity
      )

  override def failure(
      source: Source,
      config: JdbcConnectorConfig,
      error: Throwable
  ): Option[RunFailedException] =
    JdbcFailures.of(where(source, config), config.connection, error)

  /**
   * Refuses a table that a window of its watermark column cannot be read from, before a window
   * is planned: one that has not the column, or has it of another type than a timestamp. What
   * is only to be warned of, is.
   *
   * @throws RunFailedException a permanent failure, with nothing planned
   */
  private def windowed(
      source: Source,
      config: JdbcConnectorConfig,
      load: Load.Incremental,
      columns: Seq[SourceColumn]
  ): Unit =
    WatermarkColumn.check(columns, load.watermark.column.value) match {
      case Left(problem) =>
        throw RunFailedException.permanent(
          s"the table ${config.table} of ${where(source, config)} $problem. Nothing was planned"
        )
      case Right(warnings) =>
        warnings.foreach(text => log.warn(s"${source.id}: in the table ${config.table}, $text"))
    }

  /**
   * The columns of the table of a source, asked of the source.
   *
   * @throws RunFailedException with its class, if the source cannot be asked, has no such
   *   table, or has one that Bronze cannot keep
   */
  private def columnsOf(source: Source, config: JdbcConnectorConfig): Seq[SourceColumn] =
    kept(source, config, asked(source, config)(_.columns(config.table)))

  /**
   * What the source answers, on one connection that is opened for the question and closed
   * after it.
   *
   * @throws RunFailedException with its class, if the source cannot be asked
   */
  private def asked[T](source: Source, config: JdbcConnectorConfig)(
      question: SourceDatabase => T
  ): T = {
    val password = passwordOf(source, config)
    try {
      val database = SourceDatabase.open(config.connection, password)
      try question(database)
      finally database.close()
    } catch {
      case NonFatal(e) => throw failure(source, config, e).getOrElse(e)
    }
  }

  /**
   * The columns of a table, as the source said them, if Bronze can keep them.
   *
   * @throws RunFailedException a permanent failure, for a table that Bronze cannot keep
   */
  private def kept(
      source: Source,
      config: JdbcConnectorConfig,
      columns: Seq[SourceColumn]
  ): Seq[SourceColumn] = {
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
    val unfit = columns.map(_.name).filterNot(DeltaNames.fits)
    if (unfit.nonEmpty)
      refused(
        s"has columns whose names a table of Delta cannot have, " +
          s"${unfit.map(JdbcSql.quoted).mkString(", ")}: a name may hold no space, tab or line " +
          "break, and none of ,;{}()=. A view of the table can give them other names"
      )
    // Before a batch is planned, and not when its rows are written: the writer of Bronze
    // refuses four of the names then, with the batch planned for every run to fail on, and
    // takes the two that a read of files gives a row as that read's own.
    val engines = columns.map(_.name).filter(EngineColumns.reserved(_).nonEmpty)
    if (engines.nonEmpty)
      refused(
        s"has columns under names of the columns that the engine adds to every row of " +
          s"Bronze, ${engines.map(JdbcSql.quoted).mkString(", ")}: " +
          s"${EngineColumns.All.mkString(", ")}, whatever their case. A column of the " +
          "source would be taken there for the engine's own. A view of the table can give " +
          "them other names"
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
   * rows are fetched. The password is not among them: where it is, and what reads it, are
   * (`SecretRefConnectionProvider`).
   *
   * @param resolver what reads the password, as `SecretRefConnectionProvider.register` named it
   * @param table what is read, as it is to be written into the statement (`JdbcSql`)
   * @param fetch how many rows a fetch of the cursor brings
   */
  def options(
      connection: DatabaseConnection,
      resolver: String,
      table: String,
      fetch: Int
  ): Map[String, String] =
    PostgresJdbc.ofASource(connection) ++
      SecretRefConnectionProvider.options(connection.password, resolver) ++ Map(
        "url" -> connection.url.value,
        "user" -> connection.user.value,
        "dbtable" -> table,
        // A cursor: without it the driver reads the whole result before it gives a row.
        "fetchsize" -> fetch.toString
      )

  private def load(
      spark: SparkSession,
      connection: DatabaseConnection,
      resolver: String,
      table: String,
      asText: Seq[String],
      fetch: Int
  ): DataFrame = {
    val reader = spark.read.format("jdbc").options(options(connection, resolver, table, fetch))
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

  // ---------------------------------------------------------------------------------------------
  // A plan, and the window of one
  // ---------------------------------------------------------------------------------------------

  /** What a plan of a source has it read. */
  sealed private trait Planned

  /** The table, whole: one snapshot of it. */
  final private case class Whole(plan: FullLoadPlan) extends Planned

  /** The rows of a window of the watermark of the source. */
  final private case class Window(plan: WatermarkPlan) extends Planned

  /**
   * The plan of a batch of a source, read in the form of the load that the source has: a plan
   * is read under the load that it was planned under, and under its watermark column.
   *
   * @throws RunFailedException a permanent failure, if the plan is of another load, of another
   *   watermark column, or of no form that this connector wrote
   */
  private def planOf(source: Source, plan: Json): Planned = {

    /** @param plannedAs what the plan is, if it is one of the other load of a table */
    def notRead(problem: String, plannedAs: Option[String]): Nothing =
      throw RunFailedException.permanent(
        plannedAs match {
          case Some(other) =>
            s"the batch of ${source.id} was planned as $other, and the source has load.mode: " +
              s"${source.load.mode.name} now. A plan is read under the load that it was " +
              "planned under, and nothing was read. Give the source the load that it had, " +
              "run it, and change its load once the batch is committed"
          case None =>
            s"the plan of the batch of ${source.id} is not one that the JDBC connector " +
              s"reads, and nothing was read: it $problem"
        }
      )

    source.load match {
      case Load.Full =>
        FullLoadPlan.fromJson(plan) match {
          case Right(full) => Whole(full)
          case Left(problem) =>
            notRead(
              problem,
              Option.when(WatermarkPlan.fromJson(plan).isRight)(
                "a window of a watermark (load.mode: incremental)"
              )
            )
        }
      case incremental: Load.Incremental =>
        WatermarkPlan.fromJson(plan) match {
          case Right(window) if window.column == incremental.watermark.column => Window(window)
          case Right(window) =>
            throw RunFailedException.permanent(
              s"the batch of ${source.id} was planned as a window of the column " +
                s"${window.column} (load.watermark.column), and the configuration of the " +
                s"source has ${incremental.watermark.column} now. A plan is read as it was " +
                "written, and nothing was read. Give the source the watermark column that " +
                "it had and run it again"
            )
          case Left(problem) =>
            notRead(
              problem,
              Option.when(FullLoadPlan.fromJson(plan).isRight)(
                "a snapshot of its table (load.mode: full)"
              )
            )
        }
      case other =>
        throw RunFailedException.permanent(
          s"${source.id} has the load ${other.mode.name}, which is no load of a table"
        )
    }
  }

  /**
   * The plan of the next window of a source, by what its clock says, or none if the source has
   * been read as far as it may be (ADR-0020). It asks nothing: the clock is given, and so is
   * the watermark, as the control plane has it.
   *
   * No window is planned while the horizon is not after where a batch counts from. That is
   * how a source that is up to date ends its run, and it is said so. It is also what a clock
   * that went back gives, and that is warned of, with both moments: what the source stamps
   * until its clock has passed the watermark again is at or before the watermark, and is in no
   * window to come.
   *
   * @param stored the watermark of the source, in the form that this connector wrote
   * @param clock what the source says the time is
   * @throws RunFailedException a permanent failure, if the watermark is not one that this
   *   application wrote, or the clock says a time that no plan holds
   */
  private[jdbc] def nextWindow(
      source: Source,
      config: JdbcConnectorConfig,
      load: Load.Incremental,
      stored: Option[Json],
      clock: Instant
  ): Option[Json] = {
    stored.foreach(storedWatermark(source, _))
    windowUpTo(source, load, stored, horizonBy(source, config, load, clock), Some(clock))
  }

  /**
   * How far a source may be read by what its clock says: the clock less `safety_margin`
   * (ADR-0020). It bounds the windows of an invocation, and the range of a backfill when the
   * backfill is created.
   *
   * @throws RunFailedException a permanent failure, if the clock says a time that no plan holds
   */
  private def horizonBy(
      source: Source,
      config: JdbcConnectorConfig,
      load: Load.Incremental,
      clock: Instant
  ): Instant =
    WatermarkPlanner.horizon(load.watermark, clock) match {
      case Right(moment) => moment
      case Left(problem) =>
        throw RunFailedException.permanent(
          s"no batch of ${where(source, config)} can be planned: $problem. A batch of a " +
            "watermark is bounded by the clock of its source, and a plan holds no time " +
            s"outside ${Timestamps.Min} to ${Timestamps.Max}"
        )
    }

  /**
   * The plan of the next window of a source up to a horizon, or none if the source has been
   * read up to it.
   *
   * @param clock what the clock of the source said, for the first plan of an invocation; none
   *   for a later one, which plans against the horizon of its chain and asks no clock
   */
  private def windowUpTo(
      source: Source,
      load: Load.Incremental,
      stored: Option[Json],
      horizon: Instant,
      clock: Option[Instant]
  ): Option[Json] = {
    val watermark = stored.map(storedWatermark(source, _))
    WatermarkPlanner.plan(load, watermark, horizon) match {
      case WatermarkPlanner.Planned(plan) => Some(plan.toJson)
      case nothing: WatermarkPlanner.NothingNew =>
        val margin = load.watermark.safetyMargin
        watermark match {
          // Further behind than the margin: it is the clock that is behind the watermark.
          case Some(reached)
              if clock.nonEmpty && nothing.behind.compareTo(Span.ofMillis(margin.toMillis)) > 0 =>
            log.warn(
              s"${source.id}: the clock of its source says ${clock.get}, which is before the " +
                s"watermark of the source, ${reached.value}. The clock went back, or it is " +
                "that of another server. No batch is planned until it has passed the " +
                s"watermark by safety_margin (${Durations.render(margin)}): what the source " +
                "stamps until then is at or before the watermark, and is never ingested " +
                "unless load.watermark.lookback reaches it (ADR-0020)"
            )
          case _ =>
            log.info(
              s"${source.id}: nothing new. A batch would be read up to ${nothing.horizon}, " +
                s"the clock of the source less safety_margin (${Durations.render(margin)}), " +
                s"which is not after ${nothing.from}, " +
                (if (watermark.nonEmpty) "the watermark of the source"
                 else "where the source starts (load.watermark.start)")
            )
        }
        None
    }
  }

  /** The watermark of a source, as this connector wrote it. */
  private def storedWatermark(source: Source, document: Json): StoredWatermark =
    StoredWatermark.fromJson(document) match {
      case Right(reached) => reached
      case Left(problem) =>
        throw RunFailedException.permanent(
          s"the watermark that the control plane has of ${source.id} is not one that this " +
            s"application wrote, and nothing was planned: it $problem. A batch is not " +
            "planned from a moment that was guessed"
        )
    }

  /** The rows of a window: after its lower bound, if it has one, and no later than its upper. */
  private def within(window: WatermarkPlan): Column = {
    val at = col(quotedForSpark(window.column.value))
    val upTo = at <= lit(window.upper)
    window.lower.fold(upTo)(lower => at > lit(lower) && upTo)
  }

  /**
   * Refuses to read a window anywhere but in UTC. Spark writes a bound into the statement of
   * the source as a time with no zone, in the zone of the JVM or in that of the session, and
   * the source reads it in the zone of its connection, which is the JVM's: in two zones a
   * bound is off by their difference, and in one whose clocks go back two bounds an hour
   * apart are one (pinned by `SparkJdbcContractSpec`). The command `run` sets both, and what
   * is checked here is whatever else runs the engine.
   *
   * It is the JVM of the driver that is looked at, where the tasks of a local session run
   * (discovery §20 has the JVM of an executor on another machine).
   */
  private def inUtc(source: Source, spark: SparkSession): Unit = {
    def utc(zone: ZoneId): Boolean = zone.getRules == ZoneOffset.UTC.getRules
    val jvm = ZoneId.systemDefault()
    val session = spark.conf.get(Profile.EngineSpark.SessionTimeZone)
    val elsewhere = Seq(
      Option.when(!utc(jvm))(s"the JVM is in ${jvm.getId}"),
      Option.when(!Try(ZoneId.of(session, ZoneId.SHORT_IDS)).toOption.exists(utc))(
        s"the session of Spark is in $session (${Profile.EngineSpark.SessionTimeZone})"
      )
    ).flatten
    if (elsewhere.nonEmpty)
      throw RunFailedException.permanent(
        s"${source.id} is loaded by a watermark, and ${elsewhere.mkString(" and ")}, which " +
          "is not UTC. The bounds of a batch are written into the statement of the source " +
          "as times with no zone, and are the times that were planned only where the JVM " +
          "and the session are both in UTC: anywhere else rows would be read twice, or not " +
          "at all. Nothing was read. Start the JVM with -Duser.timezone=UTC, and leave " +
          s"${Profile.EngineSpark.SessionTimeZone} to the engine, as the command run does"
      )
  }

  /** `name`, or the first of it with underscores after it that no column has, case aside. */
  private def unusedName(columns: Seq[SourceColumn], name: String): String = {
    val taken = columns.map(_.name.toLowerCase(Locale.ROOT)).toSet
    Iterator.iterate(name)(_ + "_").find(candidate => !taken(candidate)).get
  }

  /**
   * What a row with a date that is not finite fails its batch with.
   *
   * @param watermark whether the column is the watermark of a window. Of the two, a window
   *   holds `-infinity` alone, and only one that has no lower bound: `infinity` is after
   *   every upper bound, and is in no window, as a null is in none
   */
  private def infinite(
      source: Source,
      config: JdbcConnectorConfig,
      column: SourceColumn,
      watermark: Boolean
  ): String =
    if (watermark)
      s"the column ${column.name} of the table ${config.table} of ${where(source, config)} " +
        "has -infinity in a row of the batch, and it is the watermark of the source " +
        "(load.watermark.column): the row was changed at no time that a batch is bounded " +
        "by, and Spark would write it to Bronze as another date. Nothing of the batch was " +
        "written. A watermark is read as the timestamp that it is, and never as text: give " +
        "the row a time at its source, or have the source start after it " +
        "(load.watermark.start), since a window that has a lower bound holds no -infinity"
    else
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
