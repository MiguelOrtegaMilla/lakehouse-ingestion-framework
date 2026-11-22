package lakehouse.ingestion.it.ingestion

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path, Paths}
import java.time.{Clock, Instant, ZoneOffset}

import scala.concurrent.duration._
import scala.util.Try

import io.circe.Json
import lakehouse.ingestion.core.config.{
  EngineColumns,
  PathTableLocator,
  RelativePath,
  RootUri,
  Source,
  TableLocation
}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.connector.{ConnectorRegistry, Progress}
import lakehouse.ingestion.engine.control._
import lakehouse.ingestion.engine.delta.DeltaTableTransactions
import lakehouse.ingestion.engine.files.{FileSourceConnector, Landing}
import lakehouse.ingestion.engine.{FaultHooks, FaultPoint, IngestionService}
import lakehouse.ingestion.it.control.ControlPlaneSuite
import lakehouse.ingestion.it.ingestion.IngestionLedger.{Facts, TakenFile, Violation}
import lakehouse.ingestion.it.stress.LogReader
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.it.support.{CoordinationSchema, CoordinationStores, LocalSparkSuite}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.col
import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers

/**
 * What the tests of a run of a source stand on: a lake and a landing zone of the suite's own,
 * a real Delta on the coordinated LogStore, and a real control plane, with one PostgreSQL for
 * both schemas, as a deployment has it. And what a test asks of them afterwards: what a run
 * left in the lake, and what the control plane says of it.
 *
 * The clock of a service is the test's own, and so is the time of each file: whether a file is
 * complete does not depend on how long a test takes.
 */
trait IngestionSuite extends ControlPlaneSuite with LocalSparkSuite {
  this: Suite with Matchers =>

  // The commits of every table go through the LogStore, on the database that `migrate` made.
  protected lazy val coordination =
    CoordinationStores.settings(pg.jdbcUrl(CoordinationSchema.Database), pg)

  override protected def sparkConf: Map[String, String] =
    CoordinationStores.sparkSettings(coordination)

  protected lazy val lakeRoot =
    RootUri.parse(Files.createDirectories(workDir.resolve("lake")).toUri.toString).value
  protected lazy val landingDirectory = Files.createDirectories(workDir.resolve("landing"))
  protected lazy val landingRoot = RootUri.parse(landingDirectory.toUri.toString).value
  protected lazy val locator = new PathTableLocator(lakeRoot)
  protected lazy val delta = new DeltaTableTransactions(spark)

  /** The leases of the runs of a suite: long enough that none is lost unless a test has it so. */
  protected def timing: LeaseTiming = LeaseTiming(2.minutes)
  protected val Now = Instant.parse("2026-11-19T12:00:00Z")
  protected val anHourAgo = Now.minusSeconds(3600)

  // ---------------------------------------------------------------------------------------------
  // A source, its files, and a run of it
  // ---------------------------------------------------------------------------------------------

  /**
   * A source of files of its own, with its directory made in the landing zone.
   *
   * @param rejected the share of the records of a batch that it lets be rejected; none, unless
   *   it says so
   */
  protected def fileSource(
      format: String = "csv",
      options: String = "",
      rejected: Option[String] = None,
      id: String = newSourceId().value,
      table: Option[String] = None
  ): Source = {
    val directory = id.replace('.', '_')
    Files.createDirectories(landingDirectory.resolve(directory))
    Source
      .load(
        s"$id.yaml",
        s"""id: $id
           |connector: { type: file, path: $directory, format: $format$options }
           |load: { mode: new_files }
           |targets: { bronze: { table: ${table.getOrElse(s"bronze/${id.replace('.', '/')}")} } }
           |${rejected.fold("")(share => s"quality: { max_rejected_ratio: $share }")}
           |""".stripMargin
      )
      .value
      .value
  }

  protected def directoryOf(of: Source): Path =
    landingDirectory.resolve(of.id.value.replace('.', '_'))

  /** A file of the source, last changed an hour ago unless said: complete, for a plan made now. */
  protected def drop(of: Source, name: String, text: String, at: Instant = anHourAgo): Path = {
    val path = directoryOf(of).resolve(name)
    Files.write(path, text.getBytes(UTF_8))
    Files.setLastModifiedTime(path, FileTime.from(at))
    path
  }

  /**
   * The service, as an application puts it together.
   *
   * @param at what "now" is for it
   * @param afterRead what happens once the connector has read the files, before it looks at
   *   them again
   * @param on how it reaches the control plane: as the suite does, unless a test has a way of
   *   its own, to break
   */
  protected def service(
      hooks: FaultHooks = FaultHooks.Off,
      at: Instant = Now,
      afterRead: () => Unit = () => (),
      on: ControlDatabase = database
  ): IngestionService =
    new IngestionService(
      on,
      ConnectorRegistry(new FileSourceConnector(new Landing(landingRoot), afterRead)),
      spark,
      lakeRoot,
      timing,
      Clock.fixed(at, ZoneOffset.UTC),
      hooks
    )

  /**
   * One run of a source, by a service that has nothing done to it unless one is given.
   *
   * @param horizon that of the chain of runs that it is in, after its first
   */
  protected def launch(
      of: Source,
      by: IngestionService = service(),
      horizon: Option[Json] = None
  ): Outcome =
    by.run(of, versionOf(of), horizon)

  protected def ingested(outcome: Outcome): Outcome.Ingested = outcome match {
    case done: Outcome.Ingested => done
    case other => fail(s"no batch was committed: $other")
  }

  /** Hooks that do one thing, at one point of a run. */
  protected def hookAt(at: FaultPoint)(action: PlannedBatch => Unit): FaultHooks =
    (point, batch) => if (point == at) action(batch)

  // ---------------------------------------------------------------------------------------------
  // What a run left: in the lake, and in the control plane
  // ---------------------------------------------------------------------------------------------

  protected def bronze(of: Source): TableLocation = locator.locate(of.targets.bronze.table)

  protected def quarantineTable(of: Source): RelativePath = Quarantine.table(of.id, Layer.Bronze)
  protected def quarantine(of: Source): TableLocation = locator.locate(quarantineTable(of))

  protected def isThere(table: TableLocation): Boolean =
    Files.exists(Paths.get(URI.create(table.uri)))
  protected def tableIsThere(of: Source): Boolean = isThere(bronze(of))

  /** The records in the quarantine of a source: the batch, the file and the text of each. */
  protected def quarantinedOf(of: Source): Seq[(Long, String, String)] =
    spark.read
      .format("delta")
      .load(quarantine(of).uri)
      .select("batch_seq", "source_file", "raw_record")
      .collect()
      .map(row => (row.getLong(0), row.getString(1).split('/').last, row.getString(2)))
      .sorted
      .toSeq

  protected def tableVersion(of: Source): Long =
    DeltaLog.forTable(spark, bronze(of).uri).update().version

  /** The rows of Bronze, each by its id, the batch that wrote it and the file it came from. */
  protected def rowsOf(of: Source): Seq[(String, Long, String)] =
    spark.read
      .format("delta")
      .load(bronze(of).uri)
      .select("id", "_batch_seq", "_source_file")
      .collect()
      .map(row => (row.getString(0), row.getLong(1), row.getString(2).split('/').last))
      .sorted
      .toSeq

  protected def runOf(id: RunId): RunRecord = database.run(Runs.find(_, id)).value
  protected def batchOf(id: BatchId): BatchRecord = database.run(Batches.find(_, id)).value
  protected def stateOf(of: Source): Option[SourceState] =
    database.run(SourceStates.find(_, of.id, Layer.Bronze))
  protected def holderOf(of: Source): Option[RunId] =
    database.run(Leases.find(_, LeaseKey.of(of.id, Layer.Bronze))).flatMap(_.holder).map(_.run)

  protected def batchId(of: Source, seq: Long): BatchId =
    BatchId(TxnAppId.of(of.id, Layer.Bronze), BatchSeq.of(seq).value)

  /** The batches of a source, each with how it stands. */
  protected def batchesOf(of: Source): Seq[(Long, String)] = asSuperuser { c =>
    query(
      c,
      "SELECT batch_seq::text, status FROM control.ingestion_batch WHERE source_id = ? ORDER BY 1",
      of.id.value
    ).map(row => (row(0).toLong, row(1)))
  }

  /** The files that batches of a source took: the name, the size, and the batch. */
  protected def takenOf(of: Source): Seq[(String, Long, Long)] = asSuperuser { c =>
    query(
      c,
      "SELECT path, size::text, batch_seq::text FROM control.processed_file " +
        "WHERE source_id = ? ORDER BY path, batch_seq",
      of.id.value
    ).map(row => (row(0).split('/').last, row(1).toLong, row(2).toLong))
  }

  /** The lease of a source runs out in the database, whatever its holder believes of it. */
  protected def expire(of: Source): Unit = asSuperuser { c =>
    update(
      c,
      "UPDATE control.source_lease SET expires_at = now() - interval '1 second' WHERE lease_key = ?",
      LeaseKey.of(of.id, Layer.Bronze).value
    ) shouldBe 1: Unit
  }

  // ---------------------------------------------------------------------------------------------
  // The ledger of a source (P7.2)
  // ---------------------------------------------------------------------------------------------

  /**
   * The connectors that the ledger asks what the plan of a batch commits: the watermark and the
   * files. A suite whose sources are of another kind gives its own.
   */
  protected def connectors: ConnectorRegistry =
    ConnectorRegistry(new FileSourceConnector(new Landing(landingRoot)))

  /** Every way in which the control plane, the tables and the files taken of a source disagree. */
  protected def ledgerViolations(of: Source, earlier: Source*): Seq[Violation] =
    IngestionLedger.check(ledgerOf(of, earlier: _*))

  /**
   * Asks the ledger of a source, once no run of it is in flight.
   *
   * @param earlier the configurations that the source had before, that a batch may have been
   *   planned under: a plan is read by its connector as it was written, and a connector refuses
   *   one of another configuration (a window of another watermark column)
   */
  protected def assertLedger(of: Source, earlier: Source*): Unit = {
    val violations = ledgerViolations(of, earlier: _*)
    withClue(violations.mkString(s"the ledger of ${of.id}:\n  ", "\n  ", "\n")) {
      violations shouldBe empty
    }: Unit
  }

  /**
   * What the control plane, the tables and `processed_file` say of the regular sequence of a
   * source.
   *
   * Delta is asked before the log is read as files: a read publishes a commit that a writer took
   * and did not publish (ADR-0004 §3.3), and what is looked at is the table that the next reader
   * gets.
   */
  protected def ledgerOf(of: Source, earlier: Source*): Facts = {
    val app = TxnAppId.of(of.id, Layer.Bronze)
    val bound = (of +: earlier).map(connectors.bind)
    val configured = bronze(of)

    /** What the commit of a plan records, by the first configuration that reads it. */
    def progressOf(plan: Json): Progress =
      bound.iterator
        .map(source => Try(source.progress(plan)))
        .find(_.isSuccess)
        .getOrElse(Try(bound.head.progress(plan)))
        .get

    val records = asSuperuser { c =>
      query(
        c,
        "SELECT batch_seq::text FROM control.ingestion_batch WHERE txn_app_id = ? ORDER BY 1",
        app.value
      )
    }.map(row =>
      database.run(Batches.find(_, BatchId(app, BatchSeq.of(row.head.toLong).value))).value
    )

    val batches = records.map { record =>
      val progress = progressOf(record.plan)
      IngestionLedger.Batch(
        seq = record.id.seq.value,
        status = record.status,
        target = record.target.getOrElse(configured.uri),
        committedBy = record.committedBy,
        outputs = record.outputs.toSeq.flatten.map { output =>
          IngestionLedger.Output(locator.locate(output.table).uri, output.version)
        },
        watermark = progress.watermark,
        files = progress.files.toSeq.flatMap { plan =>
          plan.files.map { file =>
            TakenFile(
              s"${plan.directory.value}/${file.name.value}",
              file.size,
              file.modifiedAt.toEpochMilli
            )
          }
        }.toSet,
        reprocessOf = record.reprocessOf.map(_.value)
      )
    }

    val runs = asSuperuser { c =>
      query(
        c,
        "SELECT run_id::text FROM control.ingestion_run WHERE source_id = ? AND layer = ?",
        of.id.value,
        Layer.Bronze.name
      )
    }.map { row =>
      val run = runOf(RunId.parse(row.head).value)
      IngestionLedger.Run(
        run.start.id,
        run.batch.map(_.value),
        run.status,
        run.counts.rowsWritten,
        run.start.kind
      )
    }

    val resets = asSuperuser { c =>
      query(
        c,
        "SELECT run_id::text, before::text, after::text FROM control.state_change_log " +
          "WHERE source_id = ? AND layer = ? ORDER BY change_id",
        of.id.value,
        Layer.Bronze.name
      )
    }.map { row =>
      def state(text: String): IngestionLedger.State = {
        val document = StoredJson.parse(text).value.hcursor
        IngestionLedger.State(
          document.downField("watermark").focus.filterNot(_.isNull),
          document.get[Long]("last_committed_batch_seq").value,
          RunId.parse(document.get[String]("updated_by_run").value).value
        )
      }
      IngestionLedger.Reset(RunId.parse(row(0)).value, state(row(1)), state(row(2)))
    }

    val taken = asSuperuser { c =>
      query(
        c,
        "SELECT path, size::text, (extract(epoch FROM modified_at) * 1000)::bigint::text, " +
          "batch_seq::text FROM control.processed_file WHERE source_id = ? AND txn_app_id = ?",
        of.id.value,
        app.value
      )
    }.map(row => TakenFile(row(0), row(1).toLong, row(2).toLong) -> row(3).toLong).toSet

    val data = (batches.map(_.target) :+ configured.uri).distinct.map { uri =>
      ledgerTable(
        uri,
        app,
        EngineColumns.RunId,
        EngineColumns.BatchSeq,
        // A table of several sources has the rows of each: these are the source's.
        Some(col(EngineColumns.SourceId) === of.id.value)
      )
    }
    val apart = ledgerTable(quarantine(of).uri, app, Quarantine.RunId, Quarantine.BatchSeq, None)

    Facts(
      batches,
      runs,
      stateOf(of).map(state =>
        IngestionLedger.State(state.watermark, state.lastCommitted.value, state.updatedBy)
      ),
      taken,
      data,
      apart,
      resets
    )
  }

  private def ledgerTable(
      uri: String,
      app: TxnAppId,
      run: String,
      batch: String,
      ofTheSource: Option[org.apache.spark.sql.Column]
  ): IngestionLedger.Table = {
    val directory = Paths.get(URI.create(uri))
    if (!Files.isDirectory(directory.resolve("_delta_log"))) IngestionLedger.Table(uri)
    else {
      val location = locator.locate(
        RelativePath.stored(uri.stripPrefix(s"${lakeRoot.value}/")).fold(e => fail(e), identity)
      )
      val lastTxn = delta.lastVersion(location, app)
      val log = LogReader.logView(spark, directory)
      withClue(s"the log of $uri: ")(log.unreadable shouldBe empty)
      val all = spark.read.format("delta").load(uri)
      val rows = ofTheSource
        .fold(all)(all.filter)
        .groupBy(col(run), col(batch))
        .count()
        .collect()
        .map(row => IngestionLedger.Rows(row.getString(0), row.getLong(1), row.getLong(2)))
        .toSeq
      IngestionLedger.Table(
        uri,
        log.commits.map(commit =>
          IngestionLedger.Commit(
            commit.version,
            commit.txns.filter(_.appId == app.value).map(_.version)
          )
        ),
        log.versions.minOption,
        lastTxn,
        rows
      )
    }
  }

  protected val NothingCounted = RunCounts(
    rowsRead = Some(0),
    rowsWritten = Some(0),
    rowsRejected = Some(0),
    bytesRead = Some(0)
  )
}
