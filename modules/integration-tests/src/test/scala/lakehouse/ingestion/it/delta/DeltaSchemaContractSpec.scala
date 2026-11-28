package lakehouse.ingestion.it.delta

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Try

import io.delta.exceptions.{MetadataChangedException, ProtocolChangedException}
import lakehouse.ingestion.it.logstore.RecordingLogStore
import lakehouse.ingestion.it.support.LocalSparkSuite
import org.apache.spark.SparkThrowable
import org.apache.spark.sql.delta.actions.{Action, AddFile, Metadata, Protocol, RemoveFile}
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{StreamingQueryException, Trigger}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{AnalysisException, DataFrame, DataFrameReader}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What Delta 4.4.1 does with a change of the schema of a table: which changes of a type it takes,
 * what the commit of rows that change a table holds, how type widening is turned on for a table
 * and taken off it, what a write meets when another changes the table under it, and what reads a
 * table by its versions through a widening. The facts that the policy of the schema of a source
 * is built on (discovery §10, §17, P9; §20).
 *
 * These are characterisation tests, as those of `DeltaAppendContractSpec` are: they test Delta,
 * and no code of the engine is in them. Each table is made as the engine makes one of Bronze,
 * by an append with no property of Delta, unless a test says otherwise; and rows are appended
 * as `BatchAppend` appends them, by a write of a `DataFrame`, with `mergeSchema` given. An
 * upgrade that changes one of these facts fails here, with the name of the assumption that it
 * breaks (ADR-0014, upgrade procedure).
 *
 * A write that another writer changes the table under is held at the commit of its file, on
 * the thread that makes it, by `RecordingLogStore`, while the other commits: the conflict that it
 * then meets is Delta's own, as a run meets it. Delta takes a lock of its own around a commit in
 * one JVM where a store says that a partial write may be seen, and the other writer would wait
 * for the held one: the store says otherwise while a write is held.
 */
final class DeltaSchemaContractSpec
    extends AnyFunSuite
    with Matchers
    with LocalSparkSuite
    with BeforeAndAfterEach {

  import DeltaSchemaContractSpec._

  override protected def sparkConf: Map[String, String] = Map(
    "spark.delta.logStore.file.impl" -> classOf[RecordingLogStore].getName
  )

  override def beforeEach(): Unit = {
    super.beforeEach()
    RecordingLogStore.reset()
  }

  override def afterEach(): Unit =
    try RecordingLogStore.reset()
    finally super.afterEach()

  // ---------------------------------------------------------------------------------------------
  // Which changes of a type a table takes
  // ---------------------------------------------------------------------------------------------

  test("a table with type widening takes each wider type by mergeSchema, and every value it had") {
    Widened.foreach { case (from, to) =>
      withClue(s"$from to $to: ") {
        val path = made("widened", from, TypeWidening -> "true")
        append(path, rows(to, id = 2), mergeSchema = true)

        typeOf(path) shouldBe to
        // The row of the old type, read as the new one, is what Spark makes of it by a cast,
        // and the new row has a value that the old type does not hold.
        valueOf(path, id = 1) shouldBe cast(from, to)
        valueOf(path, id = 2) shouldBe cast(to, to)
      }
    }
  }

  test("and refuses every other change of a type, as an analysis that writes nothing") {
    Refused.foreach { case (from, to, cause) =>
      withClue(s"$from to $to: ") {
        val path = made("refused", from, TypeWidening -> "true")
        val refusal = refusedBy(path)(append(path, rows(to, id = 2), mergeSchema = true))

        refusal shouldBe an[AnalysisException]
        conditionOf(refusal) shouldBe Some("DELTA_FAILED_TO_MERGE_FIELDS")
        conditionOf(refusal.getCause) shouldBe Some(cause)
        typeOf(path) shouldBe from
      }
    }
  }

  test("a date is widened to a timestamp without a zone only where the table has that type too") {
    val without = made("date", "DATE", TypeWidening -> "true")
    val refusal =
      refusedBy(without)(append(without, rows(TimestampNtz, id = 2), mergeSchema = true))
    // Not an analysis: what a write that the engine does not classify ends in.
    refusal.getClass.getName shouldBe "org.apache.spark.sql.delta.DeltaTableFeatureException"
    refusal should not be an[AnalysisException]
    conditionOf(refusal) shouldBe Some("DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT")

    val within =
      made("date-ntz", "DATE", TypeWidening -> "true", TimestampNtzFeature -> "supported")
    append(within, rows(TimestampNtz, id = 2), mergeSchema = true)
    typeOf(within) shouldBe TimestampNtz
    valueOf(within, id = 1) shouldBe cast("DATE", TimestampNtz)
  }

  test(
    "without type widening, mergeSchema widens byte and short to int all the same, and no more"
  ) {
    Seq("TINYINT" -> "SMALLINT", "TINYINT" -> "INT", "SMALLINT" -> "INT").foreach {
      case (from, to) =>
        withClue(s"$from to $to: ") {
          val path = made("legacy", from)
          append(path, rows(to, id = 2), mergeSchema = true)

          typeOf(path) shouldBe to
          valueOf(path, id = 1) shouldBe cast(from, to)
          // With no feature in the protocol, and no record of the change on the column.
          protocolOf(path) shouldBe Legacy
          typeChangesOf(path) shouldBe empty
        }
    }
    Seq("INT" -> "BIGINT", "TINYINT" -> "BIGINT", "FLOAT" -> "DOUBLE", "INT" -> "DOUBLE").foreach {
      case (from, to) =>
        withClue(s"$from to $to: ") {
          val path = made("legacy-refused", from)
          val refusal = refusedBy(path)(append(path, rows(to, id = 2), mergeSchema = true))
          conditionOf(refusal.getCause) shouldBe Some("DELTA_MERGE_INCOMPATIBLE_DATATYPE")
        }
    }
  }

  test("rows of a narrower type than their column are refused, but for byte and short to int") {
    Seq(true, false).foreach { widening =>
      Seq(true, false).foreach { merge =>
        withClue(s"type widening $widening, mergeSchema $merge: ") {
          val properties = if (widening) Seq(TypeWidening -> "true") else Nil
          val path = made("narrower", "BIGINT", properties: _*)
          val refusal = refusedBy(path)(append(path, rows("INT", id = 2), mergeSchema = merge))
          conditionOf(refusal.getCause) shouldBe Some("DELTA_MERGE_INCOMPATIBLE_DATATYPE")
        }
      }
    }
    Seq(
      "BIGINT" -> "TINYINT",
      "DOUBLE" -> "FLOAT",
      "DECIMAL(12,2)" -> "DECIMAL(10,2)",
      TimestampNtz -> "DATE"
    ).foreach { case (column, row) =>
      withClue(s"$row to $column: ") {
        val path = made("narrower", column, TypeWidening -> "true")
        refusedBy(path)(append(path, rows(row, id = 2), mergeSchema = true)) shouldBe
          an[AnalysisException]
      }
    }
    Seq("INT" -> "TINYINT", "INT" -> "SMALLINT", "SMALLINT" -> "TINYINT").foreach {
      case (column, row) =>
        withClue(s"$row to $column: ") {
          val path = made("narrower-taken", column)
          append(path, rows(row, id = 2), mergeSchema = false)
          typeOf(path) shouldBe column
          valueOf(path, id = 2) shouldBe cast(row, column)
          lastCommit(path).metadata shouldBe empty
        }
    }
  }

  test("rows cast to the types of the table are taken, and change nothing of it") {
    val path = made("cast", "BIGINT", TypeWidening -> "true")
    val cast = rows("INT", id = 2).select(col("id"), col("v").cast("BIGINT").as("v"))
    append(path, cast, mergeSchema = true)

    typeOf(path) shouldBe "BIGINT"
    lastCommit(path).metadata shouldBe empty
    lastCommit(path).files shouldBe 1
  }

  test("without mergeSchema, a wider type is refused before a row is written") {
    val path = made("unmerged", "INT", TypeWidening -> "true")
    val refusal = refusedBy(path)(append(path, rows("BIGINT", id = 2), mergeSchema = false))

    refusal shouldBe an[AnalysisException]
    conditionOf(refusal) shouldBe Some("DELTA_METADATA_MISMATCH")
  }

  test("the log records each widening of a column on its field, and a read does not show it") {
    val path = made("recorded", "TINYINT", TypeWidening -> "true")
    append(path, rows("INT", id = 2), mergeSchema = true)
    append(path, rows("BIGINT", id = 3), mergeSchema = true)

    typeChangesOf(path) shouldBe Seq("byte" -> "integer", "integer" -> "long")
    spark.read.format("delta").load(path).schema("v").metadata.json shouldBe "{}"
  }

  // ---------------------------------------------------------------------------------------------
  // Type widening turned on for a table that is there
  // ---------------------------------------------------------------------------------------------

  test("the option of a write does not turn type widening on for a table that is there") {
    val path = made("option", "INT")
    val refusal = refusedBy(path) {
      rows("BIGINT", id = 2).write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .option(TypeWidening, "true")
        .save(path)
    }

    conditionOf(refusal.getCause) shouldBe Some("DELTA_MERGE_INCOMPATIBLE_DATATYPE")
    protocolOf(path) shouldBe Legacy
    snapshot(path).metadata.configuration shouldBe empty
    // Nor does the session: a table is made with a property by default only where it says so.
    spark.conf.getOption("spark.databricks.delta.properties.defaults.enableTypeWidening") shouldBe
      None
  }

  test("ALTER TABLE turns type widening on in one commit, of the protocol and of the metadata") {
    val path = made("turned-on", "INT")
    turnOn(path)

    val commit = lastCommit(path)
    commit.version shouldBe 1L
    commit.files shouldBe 0
    commit.metadata.map(_.configuration) shouldBe Seq(Map(TypeWidening -> "true"))
    commit.protocols.map(describe) shouldBe Seq(Widening)
    // Every reader and every writer of the table is to know the feature from now on.
    protocolOf(path) shouldBe Widening

    append(path, rows("BIGINT", id = 2), mergeSchema = true)
    typeOf(path) shouldBe "BIGINT"
    protocolOf(path) shouldBe Widening
  }

  test("ALTER TABLE that sets type widening where it is set commits the metadata again") {
    val path = made("set-twice", "INT")
    turnOn(path)
    turnOn(path)

    val commit = lastCommit(path)
    commit.version shouldBe 2L
    commit.metadata.map(_.configuration) shouldBe Seq(Map(TypeWidening -> "true"))
    commit.protocols shouldBe empty
  }

  test("the feature in the protocol of a table, without its property, widens nothing") {
    val path = made("supported", "INT")
    support(path, "typeWidening")
    protocolOf(path) shouldBe Widening

    val refusal = refusedBy(path)(append(path, rows("BIGINT", id = 2), mergeSchema = true))
    conditionOf(refusal.getCause) shouldBe Some("DELTA_MERGE_INCOMPATIBLE_DATATYPE")
  }

  // ---------------------------------------------------------------------------------------------
  // The commit of rows that change a table
  // ---------------------------------------------------------------------------------------------

  test("mergeSchema on rows that the table takes as they are commits no metadata") {
    val path = made("same", "INT", TypeWidening -> "true")
    append(path, rows("INT", id = 2), mergeSchema = true)

    lastCommit(path).metadata shouldBe empty
    lastCommit(path).files shouldBe 1
  }

  test("columns added by rows go at the end, in the order of the rows, in the commit of the rows") {
    val path = made("added", "INT")
    append(
      path,
      spark.sql("SELECT 'b' AS b, 2 AS id, 'c' AS c, CAST(1 AS INT) AS v"),
      mergeSchema = true
    )

    columnsOf(path) shouldBe Seq("id", "v", "b", "c")
    lastCommit(path).metadata.map(m => m.schema.fieldNames.toSeq) shouldBe
      Seq(Seq("id", "v", "b", "c"))
    lastCommit(path).files shouldBe 1
    spark.read
      .format("delta")
      .load(path)
      .where(col("id") === 1)
      .select("b")
      .head()
      .isNullAt(0) shouldBe
      true
  }

  test("a column of the rows is the column of the table of its name whatever its case") {
    val path = made("case", "INT")
    append(path, spark.sql("SELECT 2 AS ID, CAST(7 AS INT) AS V"), mergeSchema = true)

    columnsOf(path) shouldBe Seq("id", "v")
    lastCommit(path).metadata shouldBe empty
    valueOf(path, id = 2) shouldBe 7
  }

  test("rows with two columns whose names differ only in their case are refused") {
    val path = made("case-twice", "INT")
    val refusal = refusedBy(path) {
      append(
        path,
        spark.sql("SELECT 2 AS id, 1 AS v, 'x' AS Extra, 'y' AS EXTRA"),
        mergeSchema = true
      )
    }

    refusal shouldBe an[AnalysisException]
    conditionOf(refusal) shouldBe Some("DELTA_DUPLICATE_COLUMNS_FOUND.DATA")
  }

  test("an append of no rows changes the schema all the same: a column, or a type") {
    val path = made("empty", "INT", TypeWidening -> "true")
    append(
      path,
      spark.sql("SELECT 2 AS id, CAST(1 AS INT) AS v, 'x' AS extra").limit(0),
      mergeSchema = true
    )

    lastCommit(path).files shouldBe 0
    lastCommit(path).metadata should have size 1
    columnsOf(path) shouldBe Seq("id", "v", "extra")

    append(path, spark.sql("SELECT 3 AS id, CAST(1 AS BIGINT) AS v").limit(0), mergeSchema = true)
    lastCommit(path).files shouldBe 0
    typeOf(path) shouldBe "BIGINT"
  }

  test("a column added and a type widened by the same rows are one change of the metadata") {
    val path = made("both", "INT", TypeWidening -> "true")
    append(
      path,
      spark.sql("SELECT 2 AS id, CAST(5 AS BIGINT) AS v, 'n' AS extra"),
      mergeSchema = true
    )

    val commit = lastCommit(path)
    commit.metadata.map(_.schema.simpleString) shouldBe Seq("struct<id:int,v:bigint,extra:string>")
    commit.files shouldBe 1
  }

  // ---------------------------------------------------------------------------------------------
  // A change of the table under a write
  // ---------------------------------------------------------------------------------------------

  test("a change of the metadata by another fails an append with MetadataChangedException") {
    val changes = Seq[(String, String => Unit)](
      "a column added by ALTER TABLE" -> { p =>
        spark.sql(s"ALTER TABLE delta.`$p` ADD COLUMNS (other STRING)").collect(): Unit
      },
      "a column added by rows" -> { p =>
        append(
          p,
          spark.sql("SELECT 3 AS id, CAST(1 AS INT) AS v, 'x' AS other"),
          mergeSchema = true
        )
      },
      "a column widened by rows" -> (p => append(p, rows("BIGINT", id = 3), mergeSchema = true)),
      "a property of the table" -> { p =>
        spark.sql(s"ALTER TABLE delta.`$p` SET TBLPROPERTIES ('lakehouse.x' = 'y')").collect(): Unit
      },
      "type widening taken off" -> { p =>
        spark.sql(s"ALTER TABLE delta.`$p` DROP FEATURE typeWidening").collect(): Unit
      }
    )
    val appends = Seq[(String, DataFrame)](
      "rows that the table takes" -> rows("INT", id = 2),
      "rows that add a column" -> spark.sql("SELECT 2 AS id, CAST(1 AS INT) AS v, 'x' AS mine"),
      "rows that widen a column" -> rows("BIGINT", id = 2)
    )
    for ((change, theirs) <- changes; (kind, ours) <- appends)
      withClue(s"$kind, under $change: ") {
        val path = made("metadata", "INT", TypeWidening -> "true")
        val outcome = underChange(path)(append(path, ours, mergeSchema = true))(theirs(path))
        failureOf(outcome) shouldBe a[MetadataChangedException]
      }

    // And so does turning type widening on.
    val path = made("turned-on-under", "INT")
    val outcome = underChange(path)(append(path, rows("INT", id = 2), mergeSchema = true)) {
      turnOn(path)
    }
    failureOf(outcome) shouldBe a[MetadataChangedException]
  }

  test("an append by another, or a change of the protocol alone, fails no append") {
    val appends = Seq[(String, DataFrame)](
      "rows that the table takes" -> rows("INT", id = 2),
      "rows that add a column" -> spark.sql("SELECT 2 AS id, CAST(1 AS INT) AS v, 'x' AS mine"),
      "rows that widen a column" -> rows("BIGINT", id = 2)
    )
    appends.foreach { case (kind, ours) =>
      withClue(s"$kind, beside an append: ") {
        val path = made("beside", "INT", TypeWidening -> "true")
        val outcome = underChange(path)(append(path, ours, mergeSchema = true)) {
          append(path, rows("INT", id = 3), mergeSchema = false)
        }
        outcome.isSuccess shouldBe true
        snapshot(path).version shouldBe 2L
      }
    }

    val path = made("protocol-alone", "INT")
    val outcome = underChange(path)(append(path, rows("INT", id = 2), mergeSchema = true)) {
      support(path, "typeWidening")
    }
    outcome.isSuccess shouldBe true
    protocolOf(path) shouldBe Widening
  }

  test("written again, rows of a column that another added are taken, of one that it widened not") {
    val added = made("again-added", "INT", TypeWidening -> "true")
    failureOf(underChange(added)(append(added, rows("INT", id = 2), mergeSchema = true)) {
      append(
        added,
        spark.sql("SELECT 3 AS id, CAST(1 AS INT) AS v, 'x' AS other"),
        mergeSchema = true
      )
    }) shouldBe a[MetadataChangedException]
    append(added, rows("INT", id = 2), mergeSchema = true)
    columnsOf(added) shouldBe Seq("id", "v", "other")

    val widened = made("again-widened", "INT", TypeWidening -> "true")
    failureOf(underChange(widened)(append(widened, rows("INT", id = 2), mergeSchema = true)) {
      append(widened, rows("BIGINT", id = 3), mergeSchema = true)
    }) shouldBe a[MetadataChangedException]
    val refusal = refusedBy(widened)(append(widened, rows("INT", id = 2), mergeSchema = true))
    conditionOf(refusal.getCause) shouldBe Some("DELTA_MERGE_INCOMPATIBLE_DATATYPE")
    // Cast to the type that the column has now, they are.
    append(
      widened,
      rows("INT", id = 2).withColumn("v", col("v").cast("BIGINT")),
      mergeSchema = true
    )
    valueOf(widened, id = 2) shouldBe cast("INT", "BIGINT")
  }

  test("turning type widening on fails where another changed the protocol, or the metadata") {
    val changes = Seq[(String, String => Unit, Class[_])](
      ("type widening turned on", p => turnOn(p), classOf[ProtocolChangedException]),
      ("a feature supported", p => support(p, "timestampNtz"), classOf[ProtocolChangedException]),
      (
        "a column added by rows",
        p => append(p, spark.sql("SELECT 3 AS id, CAST(1 AS INT) AS v, 'x' AS other"), true),
        classOf[MetadataChangedException]
      )
    )
    changes.foreach { case (change, theirs, expected) =>
      withClue(s"under $change: ") {
        val path = made("turn-on-under", "INT")
        val outcome = underChange(path)(turnOn(path))(theirs(path))
        val failure = failureOf(outcome)
        withClue(s"${failure.getClass.getName}: ")(expected.isInstance(failure) shouldBe true)
      }
    }

    val path = made("turn-on-beside", "INT")
    underChange(path)(turnOn(path))(
      append(path, rows("INT", id = 3), mergeSchema = false)
    ).isSuccess shouldBe
      true
    protocolOf(path) shouldBe Widening
  }

  // ---------------------------------------------------------------------------------------------
  // Type widening taken off
  // ---------------------------------------------------------------------------------------------

  test("DROP FEATURE takes type widening off a table, and keeps its types, values and history") {
    val path = made("dropped", "INT", TypeWidening -> "true")
    val before = DeltaLog.forTable(spark, path).update().allFiles.collect().map(_.path).toSet
    append(path, rows("BIGINT", id = 2), mergeSchema = true)
    val widenedAt = snapshot(path).version

    spark.sql(s"ALTER TABLE delta.`$path` DROP FEATURE typeWidening").collect(): Unit

    val after = snapshot(path)
    typeOf(path) shouldBe "BIGINT"
    valueOf(path, id = 1) shouldBe cast("INT", "BIGINT")
    typeChangesOf(path) shouldBe empty
    after.metadata.configuration shouldBe
      Map("delta.requireCheckpointProtectionBeforeVersion" -> after.version.toString)
    // Any reader reads the table again; a writer is to know the protection of its checkpoints.
    protocolOf(path) shouldBe
      ProtocolOf(1, 7, None, Some(Set("appendOnly", "invariants", "checkpointProtection")))
    // The files of the old type were written again with the new one.
    val removed = commits(path).filter(_.version > widenedAt).flatMap(_.removed).toSet
    removed shouldBe before
    // The versions before are read still, each with its own type.
    spark.read
      .format("delta")
      .option("versionAsOf", 0L)
      .load(path)
      .schema("v")
      .dataType
      .sql shouldBe
      "INT"
    spark.read.format("delta").option("versionAsOf", widenedAt).load(path).count() shouldBe 2L

    val again = intercept[Exception] {
      spark.sql(s"ALTER TABLE delta.`$path` DROP FEATURE typeWidening TRUNCATE HISTORY").collect()
    }
    conditionOf(again) shouldBe Some("DELTA_FEATURE_DROP_FEATURE_NOT_PRESENT")
  }

  // ---------------------------------------------------------------------------------------------
  // A table read by its versions through a widening
  // ---------------------------------------------------------------------------------------------

  /**
   * A table of change data with a widening at version 2: two rows of `INT` before it, at
   * versions 0 and 1, and two of `BIGINT` from it on, at versions 2 and 3.
   */
  private def widenedAtTwo(name: String): String = {
    val path = made(name, "INT", TypeWidening -> "true", ChangeDataFeed -> "true")
    append(path, rows("INT", id = 2), mergeSchema = false)
    append(path, rows("BIGINT", id = 3), mergeSchema = true)
    append(path, rows("BIGINT", id = 4), mergeSchema = false)
    snapshot(path).version shouldBe 3L
    path
  }

  test("a read of a version of a table has the types that the version had") {
    val path = widenedAtTwo("as-of")

    Seq(0L -> "INT", 1L -> "INT", 2L -> "BIGINT", 3L -> "BIGINT").foreach { case (v, sql) =>
      spark.read
        .format("delta")
        .option("versionAsOf", v)
        .load(path)
        .schema("v")
        .dataType
        .sql shouldBe
        sql
    }
    pairsOf(spark.read.format("delta").load(path)) shouldBe AllFour
  }

  test("a read of the change data from before a widening fails, even of versions all before it") {
    val path = widenedAtTwo("changes")

    Seq(0L -> None, 0L -> Some(1L), 1L -> Some(2L)).foreach { case (from, to) =>
      withClue(s"from $from to $to: ") {
        val refusal = intercept[Exception](changes(path, from, to).collect())
        refusal shouldBe an[UnsupportedOperationException]
        conditionOf(refusal) shouldBe Some("DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA")
      }
    }
    // From the widening on, it reads.
    pairsOf(changes(path, 2L, None)) shouldBe AllFour.drop(2)

    // A column added is no such change.
    val added = made("changes-added", "INT", ChangeDataFeed -> "true")
    append(
      added,
      spark.sql("SELECT 2 AS id, CAST(1 AS INT) AS v, 'x' AS extra"),
      mergeSchema = true
    )
    changes(added, 0L, None).count() shouldBe 2L
  }

  test(
    "the change data from before a widening is read with the schema of a version, or unchecked"
  ) {
    val path = widenedAtTwo("changes-read")

    // With the schema of the last version before the widening, by a setting internal to Delta.
    val before = withConf(TimeTravelForSchema -> "true") {
      pairsOf(changesFrom(0L, Some(1L)).option("versionAsOf", 1L).load(path))
    }
    before shouldBe AllFour.take(2)
    // Without the setting, a version is not to be named.
    conditionOf(intercept[Exception] {
      changesFrom(0L, Some(1L)).option("versionAsOf", 1L).load(path).collect()
    }) shouldBe Some("DELTA_UNSUPPORTED_TIME_TRAVEL_VIEWS")

    // All of it with the schema of the table, by a setting internal to Delta that checks nothing.
    withConf(UnsafeChanges -> "true")(pairsOf(changes(path, 0L, None))) shouldBe AllFour
  }

  test("a stream of a table, or of its change data, reads through a widening from before it") {
    Seq(false, true).foreach { ofChanges =>
      withClue(s"of the change data $ofChanges: ") {
        val path =
          made(s"stream-$ofChanges", "INT", TypeWidening -> "true", ChangeDataFeed -> "true")
        val stream = new Stream(path, Map("readChangeFeed" -> ofChanges.toString))

        stream.run() shouldBe Seq(1 -> MaxInt)
        append(path, rows("INT", id = 2), mergeSchema = false)
        append(path, rows("BIGINT", id = 3), mergeSchema = true)
        append(path, rows("BIGINT", id = 4), mergeSchema = false)
        // Resumed from its checkpoint, with no tracking of its schema.
        stream.run() shouldBe AllFour
      }
    }
  }

  test("a stream that tracks its schema stops at a widening, and again, until it is let through") {
    val path = made("tracked", "INT", TypeWidening -> "true")
    val stream = new Stream(path, Map.empty, tracked = true)
    stream.run() shouldBe Seq(1 -> MaxInt)
    append(path, rows("BIGINT", id = 2), mergeSchema = true)

    conditionOf(stream.failure()) shouldBe Some("DELTA_STREAMING_METADATA_EVOLUTION")
    conditionOf(stream.failure()) shouldBe
      Some("DELTA_STREAMING_CANNOT_CONTINUE_PROCESSING_POST_SCHEMA_EVOLUTION")
    conditionOf(stream.failure()) shouldBe
      Some("DELTA_STREAMING_CANNOT_CONTINUE_PROCESSING_POST_SCHEMA_EVOLUTION")

    new Stream(path, Map("allowSourceColumnTypeChange" -> "always"), tracked = true, Some(stream))
      .run() shouldBe Seq(1 -> MaxInt, 2 -> MaxLong)
  }

  // ---------------------------------------------------------------------------------------------

  private var count = 0

  /** A table of its own, of one row: `id` 1, and `v` of the type given, at its largest. */
  private def made(name: String, of: String, properties: (String, String)*): String = {
    count += 1
    val path = tablePath(s"$name-$count")
    properties
      .foldLeft(rows(of, id = 1).write.format("delta")) { case (w, (k, v)) => w.option(k, v) }
      .save(path)
    path
  }

  /** A row of `id` and of `v`, of the type given, at its largest. */
  private def rows(of: String, id: Int): DataFrame =
    spark.sql(s"SELECT $id AS id, CAST(${largest(of)} AS $of) AS v")

  private def append(path: String, rows: DataFrame, mergeSchema: Boolean): Unit =
    rows.write.format("delta").mode("append").option("mergeSchema", mergeSchema.toString).save(path)

  private def turnOn(path: String): Unit =
    spark
      .sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('$TypeWidening' = 'true')")
      .collect(): Unit

  private def support(path: String, feature: String): Unit =
    spark
      .sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.feature.$feature' = 'supported')")
      .collect(): Unit

  /** What a write that the table refuses raised, the table untouched by it. */
  private def refusedBy(path: String)(write: => Unit): Throwable = {
    val before = snapshot(path).version
    val refusal = intercept[Exception](write)
    snapshot(path).version shouldBe before
    refusal
  }

  /**
   * Runs `ours` on a thread of its own, holds its first commit at the write of its file while
   * `theirs` runs on this one, and then lets it go on.
   *
   * @return what came of `ours`
   */
  private def underChange(path: String)(ours: => Unit)(theirs: => Unit): Try[Unit] = {
    val first = snapshot(path).version + 1
    val writer = new AtomicReference[Thread]
    val reached = new CountDownLatch(1)
    val released = new CountDownLatch(1)
    RecordingLogStore.answerPartialWriteVisible(false)
    RecordingLogStore.onCommitWrite { version =>
      if ((Thread.currentThread() eq writer.get) && version == first) {
        reached.countDown()
        if (!released.await(2, TimeUnit.MINUTES))
          throw new IllegalStateException("the held commit was never let go")
      }
      RecordingLogStore.Delegate
    }
    try {
      val outcome = Future {
        writer.set(Thread.currentThread())
        ours
      }(ExecutionContext.global)
      if (!reached.await(2, TimeUnit.MINUTES))
        throw new IllegalStateException(Try(Await.result(outcome, 1.second)).toString)
      try theirs
      finally released.countDown()
      Try(Await.result(outcome, 2.minutes))
    } finally RecordingLogStore.reset()
  }

  private def snapshot(path: String): Snapshot = DeltaLog.forTable(spark, path).update()

  private def typeOf(path: String): String =
    spark.read.format("delta").load(path).schema("v").dataType.sql

  private def columnsOf(path: String): Seq[String] =
    spark.read.format("delta").load(path).columns.toSeq

  private def valueOf(path: String, id: Int): Any =
    spark.read.format("delta").load(path).where(col("id") === id).select("v").head().get(0)

  /** The largest value of a type, cast by Spark to another. */
  private def cast(of: String, to: String): Any =
    spark.sql(s"SELECT CAST(CAST(${largest(of)} AS $of) AS $to)").head().get(0)

  private def protocolOf(path: String): ProtocolOf = describe(snapshot(path).protocol)

  /** The changes of type that the log records on `v`, from each type to the next. */
  private def typeChangesOf(path: String): Seq[(String, String)] = {
    val schema = DataType.fromJson(snapshot(path).metadata.schemaString).asInstanceOf[StructType]
    val field = schema("v").metadata
    if (!field.contains(TypeChanges)) Nil
    else
      field
        .getMetadataArray(TypeChanges)
        .toSeq
        .map(c => c.getString("fromType") -> c.getString("toType"))
  }

  private def commits(path: String): Seq[Commit] =
    DeltaLog.forTable(spark, path).getChanges(0L).toSeq.map { case (v, actions) =>
      Commit(v, actions)
    }

  private def lastCommit(path: String): Commit = commits(path).last

  /** A batch read of the change data of a table, from a version, to one if it names one. */
  private def changes(path: String, from: Long, to: Option[Long]): DataFrame =
    changesFrom(from, to).load(path)

  private def changesFrom(from: Long, to: Option[Long]): DataFrameReader = {
    val reader =
      spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", from)
    to.fold(reader)(reader.option("endingVersion", _))
  }

  /** What a write that was to fail raised. */
  private def failureOf(outcome: Try[Unit]): Throwable =
    outcome.fold(identity, _ => fail("the write was taken"))

  private def withConf[A](setting: (String, String))(body: => A): A = {
    spark.conf.set(setting._1, setting._2)
    try body
    finally spark.conf.unset(setting._1)
  }

  /**
   * A stream of the rows of a table, run to what the table has now (`AvailableNow`) each time
   * that it is run, from its checkpoint. Each batch is kept, as `id` and `v`.
   *
   * @param tracked whether it tracks the schema of the table, in its checkpoint
   * @param resuming another, whose checkpoint and whose rows it takes over
   */
  final private class Stream(
      path: String,
      options: Map[String, String],
      tracked: Boolean = false,
      resuming: Option[Stream] = None
  ) {
    val checkpoint: String =
      resuming.map(_.checkpoint).getOrElse {
        count += 1
        workDir.resolve(s"checkpoint-$count").toString
      }
    private val kept: ConcurrentLinkedQueue[(Int, Long)] =
      resuming.map(_.kept).getOrElse(new ConcurrentLinkedQueue[(Int, Long)])

    /** Runs it, and gives every row that it has kept, by `id`. */
    def run(): Seq[(Int, Long)] = {
      start().awaitTermination()
      kept.asScala.toSeq.sortBy(_._1)
    }

    /** Runs it, and gives what stopped it. */
    def failure(): Throwable =
      intercept[StreamingQueryException](start().awaitTermination()).getCause

    private def start() = {
      val all =
        if (tracked) options + ("schemaTrackingLocation" -> s"$checkpoint/_schema") else options
      spark.readStream
        .format("delta")
        .options(all)
        .load(path)
        .writeStream
        .option("checkpointLocation", checkpoint)
        .trigger(Trigger.AvailableNow())
        .foreachBatch { (batch: DataFrame, _: Long) =>
          pairsOf(batch).foreach(kept.add)
        }
        .start()
    }
  }
}

object DeltaSchemaContractSpec {

  private val TypeWidening = "delta.enableTypeWidening"
  private val ChangeDataFeed = "delta.enableChangeDataFeed"
  private val TimestampNtzFeature = "delta.feature.timestampNtz"
  private val TimestampNtz = "TIMESTAMP_NTZ"
  private val TypeChanges = "delta.typeChanges"

  private val TimeTravelForSchema =
    "spark.databricks.delta.changeDataFeed.allowTimeTravelOptionsForSchema"
  private val UnsafeChanges =
    "spark.databricks.delta.changeDataFeed.unsafeBatchReadOnIncompatibleSchemaChanges.enabled"

  private val MaxInt = Int.MaxValue.toLong
  private val MaxLong = Long.MaxValue

  /** The rows of a table widened at its version 2, as `pairsOf` gives them. */
  private val AllFour = Seq(1 -> MaxInt, 2 -> MaxInt, 3 -> MaxLong, 4 -> MaxLong)

  /** The changes of a type that a table with type widening takes, from one to the other. */
  private val Widened = Seq(
    "TINYINT" -> "SMALLINT",
    "TINYINT" -> "INT",
    "TINYINT" -> "BIGINT",
    "SMALLINT" -> "INT",
    "SMALLINT" -> "BIGINT",
    "INT" -> "BIGINT",
    "FLOAT" -> "DOUBLE",
    "TINYINT" -> "DOUBLE",
    "SMALLINT" -> "DOUBLE",
    "INT" -> "DOUBLE",
    // An integer to a decimal with room for every value of it: ten digits before the point for
    // the integers of 32 bits and fewer, twenty for those of 64.
    "TINYINT" -> "DECIMAL(10,0)",
    "SMALLINT" -> "DECIMAL(10,0)",
    "INT" -> "DECIMAL(10,0)",
    "INT" -> "DECIMAL(12,2)",
    "BIGINT" -> "DECIMAL(20,0)",
    "BIGINT" -> "DECIMAL(38,18)",
    // A decimal to one with no fewer digits before the point, nor after it.
    "DECIMAL(10,2)" -> "DECIMAL(12,2)",
    "DECIMAL(10,2)" -> "DECIMAL(12,4)",
    "DECIMAL(10,2)" -> "DECIMAL(11,3)",
    "DECIMAL(10,2)" -> "DECIMAL(38,2)",
    // Inside a struct, an array and a map, as at the top.
    "STRUCT<a: INT>" -> "STRUCT<a: BIGINT>",
    "ARRAY<INT>" -> "ARRAY<BIGINT>",
    "MAP<STRING, INT>" -> "MAP<STRING, BIGINT>"
  )

  private val Datatype = "DELTA_MERGE_INCOMPATIBLE_DATATYPE"
  private val Decimal = "DELTA_MERGE_INCOMPATIBLE_DECIMAL_TYPE"

  /** Changes of a type that it refuses, with the condition of the cause of the refusal. */
  private val Refused = Seq(
    // Wider, and with values that the new type does not hold exactly.
    ("BIGINT", "DOUBLE", Datatype),
    ("INT", "FLOAT", Datatype),
    ("BIGINT", "FLOAT", Datatype),
    ("TINYINT", "FLOAT", Datatype),
    // To a decimal with too few digits before the point: those of the type, not of a value.
    ("TINYINT", "DECIMAL(5,0)", Datatype),
    ("SMALLINT", "DECIMAL(5,0)", Datatype),
    ("INT", "DECIMAL(9,0)", Datatype),
    ("BIGINT", "DECIMAL(19,0)", Datatype),
    ("DECIMAL(10,2)", "DECIMAL(10,4)", Decimal),
    ("DECIMAL(10,2)", "DECIMAL(9,1)", Decimal),
    ("DECIMAL(10,2)", "DOUBLE", Datatype),
    ("DOUBLE", "DECIMAL(38,10)", Datatype),
    // Between a date and a timestamp with a zone, and between the two timestamps.
    ("DATE", "TIMESTAMP", Datatype),
    ("TIMESTAMP", TimestampNtz, Datatype),
    (TimestampNtz, "TIMESTAMP", Datatype),
    // Narrower.
    ("BIGINT", "INT", Datatype),
    ("DOUBLE", "FLOAT", Datatype),
    ("DECIMAL(12,2)", "DECIMAL(10,2)", Decimal),
    (TimestampNtz, "DATE", Datatype),
    // Of another kind.
    ("INT", "STRING", Datatype),
    ("STRING", "INT", Datatype),
    ("BOOLEAN", "INT", Datatype),
    ("INT", "BOOLEAN", Datatype),
    ("DATE", "STRING", Datatype),
    ("STRING", "BINARY", Datatype)
  )

  private val DecimalOf = """DECIMAL\((\d+),(\d+)\)""".r
  private val StructOf = """STRUCT<a: (.+)>""".r
  private val ArrayOf = """ARRAY<(.+)>""".r
  private val MapOf = """MAP<STRING, (.+)>""".r

  /** The largest value of a type, as Spark SQL writes it; a value of its own for the others. */
  private def largest(of: String): String =
    of match {
      case "TINYINT" => Byte.MaxValue.toString
      case "SMALLINT" => Short.MaxValue.toString
      case "INT" => Int.MaxValue.toString
      case "BIGINT" => Long.MaxValue.toString
      case "FLOAT" => "1.5"
      case "DOUBLE" => "1.0E300"
      case DecimalOf(precision, scale) =>
        val (p, s) = (precision.toInt, scale.toInt)
        ("9" * (p - s)) + (if (s > 0) "." + ("9" * s) else "")
      case "DATE" => "DATE'2026-11-28'"
      case "TIMESTAMP" => "TIMESTAMP'2026-11-28 10:30:00'"
      case TimestampNtz => "TIMESTAMP_NTZ'2026-11-28 10:30:00'"
      case "STRING" => "'text'"
      case "BOOLEAN" => "true"
      case "BINARY" => "X'01'"
      case StructOf(inner) => s"named_struct('a', CAST(${largest(inner)} AS $inner))"
      case ArrayOf(inner) => s"array(CAST(${largest(inner)} AS $inner))"
      case MapOf(inner) => s"map('k', CAST(${largest(inner)} AS $inner))"
      case other => throw new IllegalArgumentException(s"no value of $other")
    }

  private def conditionOf(error: Throwable): Option[String] = error match {
    case spark: SparkThrowable => Option(spark.getCondition)
    case _ => None
  }

  /** The rows of a read of `id` and `v`, by `id`, with `v` as a number of 64 bits. */
  private def pairsOf(rows: DataFrame): Seq[(Int, Long)] =
    rows
      .select("id", "v")
      .collect()
      .toSeq
      .map(row => row.getInt(0) -> row.getAs[Number](1).longValue)
      .sortBy(_._1)

  /** A protocol as the tests compare it. */
  final private case class ProtocolOf(
      reader: Int,
      writer: Int,
      readerFeatures: Option[Set[String]],
      writerFeatures: Option[Set[String]]
  )

  private def describe(protocol: Protocol): ProtocolOf =
    ProtocolOf(
      protocol.minReaderVersion,
      protocol.minWriterVersion,
      protocol.readerFeatures,
      protocol.writerFeatures
    )

  /** The protocol of a table that the engine makes: the versions of the first writers of Delta. */
  private val Legacy = ProtocolOf(1, 2, None, None)

  /** The protocol of such a table with type widening turned on. */
  private val Widening = ProtocolOf(
    3,
    7,
    Some(Set("typeWidening")),
    Some(Set("appendOnly", "invariants", "typeWidening"))
  )

  final private case class Commit(version: Long, actions: Seq[Action]) {
    def metadata: Seq[Metadata] = actions.collect { case m: Metadata => m }
    def protocols: Seq[Protocol] = actions.collect { case p: Protocol => p }
    def files: Int = actions.count(_.isInstanceOf[AddFile])
    def removed: Seq[String] = actions.collect { case r: RemoveFile => r.path }
  }
}
