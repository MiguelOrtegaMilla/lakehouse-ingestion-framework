package lakehouse.ingestion.it.ingestion

import java.time.{Clock, ZoneOffset}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import cats.syntax.all._
import io.circe.Json
import lakehouse.ingestion.core.config.Read.{Obj, Result}
import lakehouse.ingestion.core.config._
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.bronze.{BronzeBatch, BronzeWriter}
import lakehouse.ingestion.engine.connector._
import lakehouse.ingestion.engine.delta.{BatchAppend, BatchCommit, Lake}
import lakehouse.ingestion.engine.files.{FileSourceConnector, Landing}
import lakehouse.ingestion.engine.{FaultHooks, FaultPoint, IngestionService, RunFailedException}
import lakehouse.ingestion.logstore.CommitContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.functions.{col, udf}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A batch whose rows are read from their source as they are written (P6.5): the run asks for a
 * first row, goes over the rows once, in the write, and says of them what the commit of their
 * table counted.
 *
 * The source is one of the test's own, as a table of a database is to the engine: each time
 * its rows are gone over it is asked again, and answers with what it has by then. It counts
 * every row that it gives, so that a test says how often the rows of a batch were gone over,
 * and it loses rows when a test has it so, at a fault point of the engine. What Spark's JDBC
 * source and Delta do with a real PostgreSQL is pinned apart (`SparkJdbcContractSpec`,
 * `DeltaAppendContractSpec`).
 */
final class OnePassIngestionSpec extends AnyFunSuite with Matchers with IngestionSuite {

  import OnePassIngestionSpec._

  private val PerBatch = 5

  /** A source of numbers, `PerBatch` in each batch, from where the last one left off. */
  private def numbersSource(): Source = {
    val id = newSourceId().value
    Source
      .load(
        s"$id.yaml",
        s"""id: $id
           |connector: { type: numbers, per_batch: $PerBatch }
           |load: { mode: full }
           |targets: { bronze: { table: bronze/${id.replace('.', '/')} } }
           |""".stripMargin,
        ConnectorRegistry(new NumbersConnector(held = false)).kinds
      )
      .value
      .value
  }

  /**
   * @param held whether the connector says that its rows are held apart from the source, which
   *   they are not
   * @param whole whether the connector says that a plan of it is its source whole, as it is
   *   when the plan is read
   */
  /** The ledger asks the plans of the numbers and of files. */
  override protected def connectors: ConnectorRegistry =
    ConnectorRegistry(
      new FileSourceConnector(new Landing(landingRoot)),
      new NumbersConnector(held = false)
    )

  private def numbers(
      hooks: FaultHooks = FaultHooks.Off,
      held: Boolean = false,
      whole: Boolean = false
  ) =
    new IngestionService(
      database,
      ConnectorRegistry(new NumbersConnector(held, whole)),
      spark,
      lakeRoot,
      timing,
      Clock.fixed(Now, ZoneOffset.UTC),
      hooks
    )

  /** The rows of the table of a source, each by its number and the batch that wrote it. */
  private def written(of: Source): Seq[(Long, Long)] =
    spark.read
      .format("delta")
      .load(bronze(of).uri)
      .select("id", "_batch_seq")
      .collect()
      .map(row => row.getLong(0) -> row.getLong(1))
      .sorted
      .toSeq

  /** How many rows the source gave to whatever went over the rows of its batches. */
  private def givenAsRows(of: Source): Long = Tally.of(rowsKey(of.id))

  /** How many rows the source gave to whatever asked it for a first row. */
  private def givenToProbes(of: Source): Long = Tally.of(probeKey(of.id))

  private def dies(of: Source, at: FaultPoint): Unit = {
    intercept[Died](launch(of, numbers(hookAt(at)(_ => throw new Died))))
    expire(of)
  }

  test("the rows of a batch are gone over once, and counted by the commit of their table") {
    val source = numbersSource()

    val done = ingested(launch(source, numbers()))

    done.batch shouldBe batchId(source, 1)
    done.outputs shouldBe Seq(BatchOutput(source.targets.bronze.table, 0))
    // What was read and what was written are one pass, and one count: the commit's.
    done.counts shouldBe RunCounts(
      rowsRead = Some(5),
      rowsWritten = Some(5),
      rowsRejected = Some(0),
      bytesRead = None,
      retries = None,
      writeConflicts = Some(0),
      commitContention = Some(0)
    )
    runOf(done.run).counts shouldBe done.counts
    delta.commit(bronze(source), done.batch).value.rows shouldBe Some(5)
    written(source) shouldBe (0L until 5L).map(_ -> 1L)
    stateOf(source).value.watermark shouldBe Some(Json.fromLong(5))

    // Each row was given once to the write, and to nothing else: no count went before it.
    givenAsRows(source) shouldBe 5L
    // And the first row was asked for, of what the connector gave to ask it of, and not of
    // the rows: one row is taken, and the batch is not gone over for it.
    givenToProbes(source) should (be >= 1L and be < 5L)

    // The next batch, the same way.
    val next = ingested(launch(source, numbers()))
    next.outputs shouldBe Seq(BatchOutput(source.targets.bronze.table, 1))
    next.counts.rowsWritten shouldBe Some(5)
    written(source) shouldBe (0L until 5L).map(_ -> 1L) ++ (5L until 10L).map(_ -> 2L)
    givenAsRows(source) shouldBe 10L
    assertLedger(source)
  }

  test("a batch with no first row writes nothing, and its rows are never gone over") {
    val source = numbersSource()
    (0L until 5L).foreach(Lost.lose(source.id, _))

    val done = ingested(launch(source, numbers()))

    done.outputs shouldBe empty
    done.counts shouldBe RunCounts(
      rowsRead = Some(0),
      rowsWritten = Some(0),
      rowsRejected = Some(0)
    )
    tableIsThere(source) shouldBe false
    givenAsRows(source) shouldBe 0L
    // The batch is committed all the same, and the source has moved.
    val batch = batchOf(batchId(source, 1))
    batch.status shouldBe BatchStatus.Committed
    batch.outputs shouldBe Some(Vector.empty)
    stateOf(source).value.watermark shouldBe Some(Json.fromLong(5))
    assertLedger(source)
  }

  test("a batch whose rows are gone by the time it is written is a commit of no rows") {
    val source = numbersSource()
    // The source has rows when it is asked for the first, and none when it is asked for all.
    val emptied = numbers(hookAt(FaultPoint.AfterCounts) { _ =>
      (0L until 5L).foreach(Lost.lose(source.id, _))
    })

    val done = ingested(launch(source, emptied))

    done.outputs shouldBe Seq(BatchOutput(source.targets.bronze.table, 0))
    done.counts shouldBe RunCounts(
      rowsRead = Some(0),
      rowsWritten = Some(0),
      rowsRejected = Some(0),
      writeConflicts = Some(0),
      commitContention = Some(0)
    )
    // The table is made, with the columns of the source, and has the batch: it is not written
    // again, and the next batch is appended to it.
    written(source) shouldBe empty
    delta.commit(bronze(source), done.batch) shouldBe Some(BatchCommit(0, Some(0)))
    ingested(launch(source, numbers())).outputs shouldBe
      Seq(BatchOutput(source.targets.bronze.table, 1))
    written(source) shouldBe (5L until 10L).map(_ -> 2L)
    assertLedger(source)
  }

  test("a run that dies before it writes leaves a plan, which the next run reads once") {
    val source = numbersSource()
    dies(source, FaultPoint.AfterCounts)
    tableIsThere(source) shouldBe false
    givenAsRows(source) shouldBe 0L

    val done = ingested(launch(source, numbers()))

    done.batch shouldBe batchId(source, 1)
    done.counts.rowsRead shouldBe Some(5)
    done.counts.rowsWritten shouldBe Some(5)
    written(source) shouldBe (0L until 5L).map(_ -> 1L)
    givenAsRows(source) shouldBe 5L
    assertLedger(source)
  }

  test("a batch that a dead run wrote is rolled forward, and its source is asked nothing") {
    val source = numbersSource()
    dies(source, FaultPoint.AfterBronzeCommit)
    val (rows, probes) = (givenAsRows(source), givenToProbes(source))
    rows shouldBe 5L

    val outcome = launch(source, numbers())

    outcome shouldBe Outcome.RolledForward(
      outcome.run,
      batchId(source, 1),
      Seq(BatchOutput(source.targets.bronze.table, 0)),
      RunCounts(rowsRead = Some(0), bytesRead = Some(0))
    )
    written(source) shouldBe (0L until 5L).map(_ -> 1L)
    stateOf(source).value.watermark shouldBe Some(Json.fromLong(5))
    // Neither for a first row, nor for the rows.
    givenAsRows(source) shouldBe rows
    givenToProbes(source) shouldBe probes
    assertLedger(source)
  }

  test("a batch that another writer of its sequence wrote is not read, nor counted") {
    val source = numbersSource()
    val other = RunId.generate()
    // A run that lost its lease and went on writes the batch, between the first row and the
    // rows of this one.
    val overtaken = numbers(hookAt(FaultPoint.AfterCounts) { batch =>
      new BronzeWriter(
        new Lake(lakeRoot, spark.sessionState.newHadoopConf()),
        delta,
        CommitContext.forRun(other.uuid)
      ).write(
        bronze(source),
        BronzeBatch(other, source.id, batch.id, Now),
        spark.range(0, 5).toDF("id")
      ) shouldBe BatchAppend.Appended(0, conflicts = 0, rows = Some(5))
      ()
    })

    val done = ingested(launch(source, overtaken))

    done.outputs shouldBe Seq(BatchOutput(source.targets.bronze.table, 0))
    // What the batch read and wrote was the other writer's to count. No row of such a source
    // is ever rejected, which this run knows.
    done.counts shouldBe RunCounts(
      rowsRead = None,
      rowsWritten = None,
      rowsRejected = Some(0),
      writeConflicts = Some(0),
      commitContention = Some(0)
    )
    written(source) shouldBe (0L until 5L).map(_ -> 1L)
    givenAsRows(source) shouldBe 0L
  }

  test("a batch of another invocation that this run read ends the chain, if its plan is whole") {
    def leftBy(dead: FaultPoint): Source = {
      val source = numbersSource()
      dies(source, dead)
      source
    }

    // A plan that is its source whole, read by this run: it is what a plan made anew would
    // read, and nothing comes after it.
    val whole = leftBy(FaultPoint.AfterCounts)
    val read = ingested(launch(whole, numbers(whole = true)))
    read.batch shouldBe batchId(whole, 1)
    read.next shouldBe Outcome.Next.Ends
    givenAsRows(whole) shouldBe 5L

    // A plan that is what it was planned as, however late it is read: a plan made anew.
    val planned = leftBy(FaultPoint.AfterCounts)
    ingested(launch(planned, numbers())).next shouldBe Outcome.Next.Afresh

    // A plan that is whole, of a batch that another writer of its sequence wrote under this
    // run: what the table has is not what this run read.
    val overtaken = leftBy(FaultPoint.AfterPlan)
    val other = RunId.generate()
    val under = numbers(
      hookAt(FaultPoint.AfterCounts) { batch =>
        new BronzeWriter(
          new Lake(lakeRoot, spark.sessionState.newHadoopConf()),
          delta,
          CommitContext.forRun(other.uuid)
        ).write(
          bronze(overtaken),
          BronzeBatch(other, overtaken.id, batch.id, Now),
          spark.range(0, 5).toDF("id")
        ): Unit
      },
      whole = true
    )
    ingested(launch(overtaken, under)).next shouldBe Outcome.Next.Afresh
    givenAsRows(overtaken) shouldBe 0L

    // A batch that a dead run wrote is rolled forward, and is as old as that run.
    val written = leftBy(FaultPoint.AfterBronzeCommit)
    launch(written, numbers(whole = true)) shouldBe a[Outcome.RolledForward]

    // And a plan of this run's own is followed as it says, whole or not.
    ingested(launch(numbersSource(), numbers(whole = true))).next shouldBe Outcome.Next.Ends
  }

  test("a commit that does not count its rows leaves the counts of its run not known") {
    val source = numbersSource()
    // The session of a run has Delta count them (`RunSession`); this one is made not to.
    spark.conf.set(MetricsEnabled, "false")
    val done =
      try ingested(launch(source, numbers()))
      finally spark.conf.unset(MetricsEnabled)

    // The batch is in its table, and is committed: a count is not what a batch stands on.
    done.outputs shouldBe Seq(BatchOutput(source.targets.bronze.table, 0))
    done.counts.rowsRead shouldBe None
    done.counts.rowsWritten shouldBe None
    done.counts.rowsRejected shouldBe Some(0)
    runOf(done.run).status shouldBe RunStatus.Succeeded
    written(source) shouldBe (0L until 5L).map(_ -> 1L)
    assertLedger(source)
  }

  test("what a run wrote is what the commit says, and not what the run counted before") {
    val source = numbersSource()
    // A connector that says that its rows are held apart, and whose source is asked again all
    // the same: a row is gone between the count and the write.
    val losing = numbers(
      hookAt(FaultPoint.AfterCounts)(_ => Lost.lose(source.id, 3L)),
      held = true
    )

    val done = ingested(launch(source, losing))

    // Two counts, made apart, that differ: each is recorded as it was made, and the batch
    // goes on. What is to come of such a batch is not decided here (discovery §20).
    done.counts.rowsRead shouldBe Some(5)
    done.counts.rowsWritten shouldBe Some(4)
    done.counts.rowsRejected shouldBe Some(0)
    runOf(done.run).counts shouldBe done.counts
    written(source) shouldBe Seq(0L, 1L, 2L, 4L).map(_ -> 1L)
    assertLedger(source)
  }

  test("the rows of a batch of files are counted by the commit as well, and the counts agree") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n2\n3\n")

    val done = ingested(launch(orders))

    done.counts.rowsRead shouldBe Some(3)
    done.counts.rowsWritten shouldBe delta.commit(bronze(orders), done.batch).value.rows
    done.counts.rowsWritten shouldBe Some(3)
    assertLedger(orders)
  }
}

object OnePassIngestionSpec {

  private val MetricsEnabled = DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key

  final class Died extends RuntimeException("the run died here")

  def rowsKey(source: SourceId): String = s"${source.value}/rows"
  def probeKey(source: SourceId): String = s"${source.value}/probe"

  /**
   * What the sources of this suite gave, row by row. It is counted in the tasks of Spark, which
   * are in this JVM (`local`), and so by an object and not by anything of a test.
   */
  object Tally {
    private val counts = new ConcurrentHashMap[String, AtomicLong]

    def of(key: String): Long = Option(counts.get(key)).fold(0L)(_.get)

    def count(key: String): Unit = {
      counts.computeIfAbsent(key, _ => new AtomicLong).incrementAndGet()
      ()
    }
  }

  /** The rows that a source no longer has. */
  object Lost {
    private val lost = ConcurrentHashMap.newKeySet[(String, Long)]()

    def lose(source: SourceId, id: Long): Unit = { lost.add(source.value -> id); () }
    def has(source: String, id: Long): Boolean = !lost.contains(source -> id)
  }

  final case class Numbers(perBatch: Int) extends ConnectorConfig {
    override def kind: String = NumbersKind.name
    override def canonical: Json = Json.obj("per_batch" -> Json.fromInt(perBatch))
    override def origin: Seq[String] = Nil
  }

  object NumbersKind extends ConnectorKind {
    override val name = "numbers"
    override val loads: Set[Load.Mode] = Set(Load.Mode.Full)
    override def read(section: Obj): Result[ConnectorConfig] =
      section.orElse("per_batch", 1)(_.integer(1, 1000)).map(Numbers(_))
  }

  /**
   * Reads the numbers of a plan from a source that is asked each time they are gone over, and
   * counts every row that the source gives: to what asks for a first row, and to what goes
   * over the rows.
   *
   * @param held whether it answers that its rows are held apart from the source
   *   (`ReadResult.Kept`). They are not: it is what a connector that breaks its word does
   * @param whole what it answers when it is asked whether a plan is its source whole, as it
   *   is when the plan is read (`wholeWhenRead`)
   */
  final class NumbersConnector(held: Boolean, whole: Boolean = false)
      extends SourceConnector[Numbers] {

    override val kind: ConnectorKind = NumbersKind

    override def plan(source: Source, config: Numbers, context: PlanContext): Option[Json] = {
      val from = context.watermark.flatMap(_.asNumber).flatMap(_.toLong).getOrElse(0L)
      Some(
        Json.obj(
          "version" -> Json.fromInt(1),
          "from" -> Json.fromLong(from),
          "to" -> Json.fromLong(from + config.perBatch)
        )
      )
    }

    override def read(
        source: Source,
        config: Numbers,
        plan: Json,
        context: ReadContext
    ): ReadResult = {
      val (from, to) = rangeOf(source, plan)
      val name = source.id.value

      /**
       * The rows that the source has, each counted under `key` as it is given. They are counted
       * where the source says whether it has a row, which every pass over the rows goes
       * through: a count of them as much as a write. Counted in a column, a pass that reads
       * no column would count nothing, since Spark computes no column that nothing reads.
       */
      def asked(key: String): DataFrame = {
        val given = udf { (id: Long) =>
          val has = Lost.has(name, id)
          if (has) Tally.count(key)
          has
        }.asNondeterministic()
        context.spark.range(from, to).toDF("id").filter(given(col("id")))
      }

      val rows = asked(rowsKey(source.id))
      if (held) ReadResult.Kept(rows, unparsed = None)
      else ReadResult.OnePass(rows, probe = asked(probeKey(source.id)))
    }

    override def progress(source: Source, config: Numbers, plan: Json): Progress =
      Progress(watermark = Some(Json.fromLong(rangeOf(source, plan)._2)), files = None)

    override def wholeWhenRead(source: Source, config: Numbers, plan: Json): Boolean = whole

    private def rangeOf(source: Source, plan: Json): (Long, Long) = {
      val at = plan.hcursor
      val range = (
        at.get[Int]("version").toOption.filter(_ == 1),
        at.get[Long]("from").toOption,
        at.get[Long]("to").toOption
      ).mapN((_, from, to) => (from, to))
      range.getOrElse(
        throw RunFailedException.permanent(s"the plan of ${source.id} is not of version 1")
      )
    }
  }
}
