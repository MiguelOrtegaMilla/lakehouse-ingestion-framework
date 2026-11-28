package lakehouse.ingestion.engine.bronze

import scala.collection.immutable.SortedMap

import lakehouse.ingestion.core.config.SchemaPolicy.{OnMissingColumn, OnNewColumn, OnTypeChange}
import lakehouse.ingestion.core.config.{EngineColumns, SchemaPolicy, SqlIdentifier}
import lakehouse.ingestion.engine.bronze.SchemaDecision.Reason._
import lakehouse.ingestion.engine.bronze.SchemaDecision._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The schema policy of a source, decided with no I/O (ADR-0025), by a table of cases: the parts
 * of a read, the table, the policy, and the columns of the rows or the reasons of the refusal.
 */
final class SchemaDecisionSpec extends AnyFunSuite with Matchers {

  import SchemaDecisionSpec._

  Cases.foreach { c =>
    test(c.name) {
      decide(c.parts, c.table, c.policy) shouldBe c.expected.left.map(Refusal(_))
    }
  }

  test("the widenings are those that Delta 4.4.1 makes, and no other (DeltaSchemaContractSpec)") {
    Taken.foreach { case (from, to) =>
      withClue(s"$from to $to: ")(widens(ddl(from), ddl(to)) shouldBe true)
    }
    NotTaken.foreach { case (from, to) =>
      withClue(s"$from to $to: ")(widens(ddl(from), ddl(to)) shouldBe false)
    }
  }

  test("a type is no widening of itself, whatever it says of nulls") {
    Seq("INT", "DECIMAL(10,2)", "STRUCT<a: INT>", "ARRAY<INT>", "MAP<STRING, INT>").foreach { t =>
      withClue(t) {
        widens(ddl(t), ddl(t)) shouldBe false
        sameType(ddl(t), ddl(t)) shouldBe true
      }
    }
    sameType(ArrayType(IntegerType, containsNull = false), ArrayType(IntegerType)) shouldBe true
    sameType(
      StructType(Seq(StructField("a", IntegerType, nullable = false))),
      StructType(Seq(StructField("a", IntegerType)))
    ) shouldBe true
    sameType(ddl("STRUCT<a: INT>"), ddl("STRUCT<b: INT>")) shouldBe false
  }

  test("a refusal says each reason, with the column, where it is and the setting that refuses it") {
    val refusal = decide(
      Seq(Part.Whole("the file landing/b.parquet", schema("id" -> IntegerType))),
      Some(table("id" -> ShortType)),
      SchemaPolicy.Default
    ).left.toOption.get

    refusal.message shouldBe
      "the column id is SMALLINT in the table and INT in the file landing/b.parquet, a widening " +
      "that the policy of the source does not make (schema.on_type_change: fail)"
  }
}

object SchemaDecisionSpec {

  final private case class Case(
      name: String,
      parts: Seq[Part],
      table: Option[StructType],
      policy: SchemaPolicy,
      expected: Either[Vector[Reason], Adjustment]
  )

  private def schema(fields: (String, DataType)*): StructType =
    StructType(fields.map { case (name, t) => StructField(name, t) })

  private def whole(label: String, fields: (String, DataType)*): Part =
    Part.Whole(label, schema(fields: _*))

  private def sparse(label: String, fields: (String, DataType)*): Part =
    Part.Sparse(label, schema(fields: _*))

  /** A table of Bronze: its columns, and those of the engine, which nothing compares. */
  private def table(fields: (String, DataType)*): StructType =
    schema(
      fields ++ Seq(
        EngineColumns.RunId -> StringType,
        EngineColumns.BatchSeq -> LongType,
        EngineColumns.IngestedAt -> TimestampType,
        EngineColumns.SourceId -> StringType
      ): _*
    )

  private def policy(
      onNewColumn: OnNewColumn = OnNewColumn.Add,
      onMissingColumn: OnMissingColumn = OnMissingColumn.Null,
      onTypeChange: OnTypeChange = OnTypeChange.Fail,
      renames: Seq[(String, String)] = Nil
  ): SchemaPolicy =
    SchemaPolicy(
      onNewColumn,
      onMissingColumn,
      onTypeChange,
      SortedMap.from(renames.map { case (from, to) =>
        from -> SqlIdentifier.parse(to).fold(e => throw new IllegalArgumentException(e), identity)
      })
    )

  private val Widen = policy(onTypeChange = OnTypeChange.Widen)

  private def column(name: String, t: DataType, read: String*): Column =
    Column(name, t, read.toVector)

  private def unchanged(columns: Column*): Either[Vector[Reason], Adjustment] =
    Right(Adjustment(columns.toVector, Vector.empty, Change.Unchanged))

  private def ddl(text: String): DataType = DataType.fromDDL(text)

  private val A = "the file landing/a.parquet"
  private val B = "the file landing/b.parquet"
  private val Orders = "the table public.orders"

  private val Cases = Seq(
    // --- A batch that makes its table -----------------------------------------------------------
    Case(
      "a batch with no table makes it as it is, whatever the policy says",
      Seq(whole(A, "id" -> StringType, "amount" -> StringType)),
      None,
      policy(OnNewColumn.Fail, OnMissingColumn.Fail, OnTypeChange.Fail),
      unchanged(column("id", StringType, "id"), column("amount", StringType, "amount"))
    ),
    Case(
      "files of Parquet of int and of bigint in one column make their table with bigint",
      Seq(whole(A, "v" -> IntegerType), whole(B, "v" -> LongType)),
      None,
      SchemaPolicy.Default,
      unchanged(column("v", LongType, "v"))
    ),
    Case(
      "a column of two types that neither widens to the other refuses its batch",
      Seq(whole(A, "v" -> IntegerType), whole(B, "v" -> StringType)),
      None,
      Widen,
      Left(Vector(TwoTypes("v", IntegerType, A, StringType, B)))
    ),
    Case(
      "with no table, a column has the first spelling met, in the order of the plan",
      Seq(whole(A, "Amount" -> StringType), whole(B, "amount" -> StringType)),
      None,
      SchemaPolicy.Default,
      unchanged(column("Amount", StringType, "Amount", "amount"))
    ),
    // --- Columns that the table has ---------------------------------------------------------------
    Case(
      "a read as its table is changes nothing",
      Seq(whole(Orders, "id" -> IntegerType, "total" -> DecimalType(10, 2))),
      Some(table("id" -> IntegerType, "total" -> DecimalType(10, 2))),
      policy(OnNewColumn.Fail, OnMissingColumn.Fail, OnTypeChange.Fail),
      unchanged(column("id", IntegerType, "id"), column("total", DecimalType(10, 2), "total"))
    ),
    Case(
      "the columns of a read are those of the table of their names, in its order and its spelling",
      Seq(whole(A, "AMOUNT" -> StringType, "ID" -> StringType)),
      Some(table("id" -> StringType, "Amount" -> StringType)),
      SchemaPolicy.Default,
      unchanged(column("id", StringType, "ID"), column("Amount", StringType, "AMOUNT"))
    ),
    Case(
      "a type narrower than its column is cast to it, and is no change, under fail",
      Seq(whole(Orders, "v" -> IntegerType, "total" -> DecimalType(10, 2), "ratio" -> FloatType)),
      Some(table("v" -> LongType, "total" -> DecimalType(12, 2), "ratio" -> DoubleType)),
      SchemaPolicy.Default,
      unchanged(
        column("v", LongType, "v"),
        column("total", DecimalType(12, 2), "total"),
        column("ratio", DoubleType, "ratio")
      )
    ),
    Case(
      "under fail, files of int and of bigint refuse a table of int",
      Seq(whole(A, "v" -> IntegerType), whole(B, "v" -> LongType)),
      Some(table("v" -> IntegerType)),
      SchemaPolicy.Default,
      Left(Vector(TypeChanged("v", IntegerType, LongType, B)))
    ),
    Case(
      "under fail, files of int and of bigint are taken by a table of bigint, the int cast",
      Seq(whole(A, "v" -> IntegerType), whole(B, "v" -> LongType)),
      Some(table("v" -> LongType)),
      SchemaPolicy.Default,
      unchanged(column("v", LongType, "v"))
    ),
    Case(
      "under widen, files of int and of bigint widen a table of int",
      Seq(whole(A, "v" -> IntegerType), whole(B, "v" -> LongType)),
      Some(table("v" -> IntegerType)),
      Widen,
      Right(
        Adjustment(
          Vector(column("v", LongType, "v")),
          Vector.empty,
          Change.Unchanged.copy(widened = Vector(Widening("v", IntegerType, LongType)))
        )
      )
    ),
    Case(
      "under widen, a wider type of a table of a database widens its column",
      Seq(whole(Orders, "id" -> IntegerType, "total" -> DecimalType(14, 4))),
      Some(table("id" -> IntegerType, "total" -> DecimalType(10, 2))),
      Widen,
      Right(
        Adjustment(
          Vector(column("id", IntegerType, "id"), column("total", DecimalType(14, 4), "total")),
          Vector.empty,
          Change.Unchanged.copy(
            widened = Vector(Widening("total", DecimalType(10, 2), DecimalType(14, 4)))
          )
        )
      )
    ),
    Case(
      "under fail, a wider type is refused",
      Seq(whole(Orders, "total" -> DecimalType(14, 4))),
      Some(table("total" -> DecimalType(10, 2))),
      SchemaPolicy.Default,
      Left(Vector(TypeChanged("total", DecimalType(10, 2), DecimalType(14, 4), Orders)))
    ),
    Case(
      "a short to an int is a widening as any other, which Delta would make by itself",
      Seq(whole(Orders, "qty" -> IntegerType)),
      Some(table("qty" -> ShortType)),
      SchemaPolicy.Default,
      Left(Vector(TypeChanged("qty", ShortType, IntegerType, Orders)))
    ),
    Case(
      "a change that is no widening is refused under widen: a long to a double",
      Seq(whole(Orders, "v" -> DoubleType)),
      Some(table("v" -> LongType)),
      Widen,
      Left(Vector(Incompatible("v", LongType, DoubleType, Orders)))
    ),
    Case(
      "a date to a timestamp without a zone is no widening of the engine's",
      Seq(whole(A, "day" -> TimestampNTZType)),
      Some(table("day" -> DateType)),
      Widen,
      Left(Vector(Incompatible("day", DateType, TimestampNTZType, A)))
    ),
    Case(
      "a change of kind is refused under widen",
      Seq(whole(A, "v" -> StringType)),
      Some(table("v" -> IntegerType)),
      Widen,
      Left(Vector(Incompatible("v", IntegerType, StringType, A)))
    ),
    Case(
      "under widen, a field of a struct widened widens its column",
      Seq(whole(A, "s" -> ddl("STRUCT<a: BIGINT, b: STRING>"))),
      Some(table("s" -> ddl("STRUCT<a: INT, b: STRING>"))),
      Widen,
      Right(
        Adjustment(
          Vector(column("s", ddl("STRUCT<a: BIGINT, b: STRING>"), "s")),
          Vector.empty,
          Change.Unchanged.copy(
            widened = Vector(
              Widening("s", ddl("STRUCT<a: INT, b: STRING>"), ddl("STRUCT<a: BIGINT, b: STRING>"))
            )
          )
        )
      )
    ),
    Case(
      "a field added inside a struct is a change of its type that is no widening",
      Seq(whole(A, "s" -> ddl("STRUCT<a: INT, b: STRING>"))),
      Some(table("s" -> ddl("STRUCT<a: INT>"))),
      Widen,
      Left(Vector(Incompatible("s", ddl("STRUCT<a: INT>"), ddl("STRUCT<a: INT, b: STRING>"), A)))
    ),
    // --- Columns that the table does not have ----------------------------------------------------
    Case(
      "under add, new columns go at the end of the table, in the order of the read",
      Seq(whole(A, "b" -> StringType, "id" -> StringType, "c" -> StringType)),
      Some(table("id" -> StringType)),
      SchemaPolicy.Default,
      Right(
        Adjustment(
          Vector(
            column("id", StringType, "id"),
            column("b", StringType, "b"),
            column("c", StringType, "c")
          ),
          Vector.empty,
          Change.Unchanged.copy(added =
            Vector(StructField("b", StringType), StructField("c", StringType))
          )
        )
      )
    ),
    Case(
      "under rescue, a new column is rescued, and a table of types is given _rescued_data",
      Seq(whole(Orders, "id" -> IntegerType, "note" -> StringType)),
      Some(table("id" -> IntegerType)),
      policy(onNewColumn = OnNewColumn.Rescue),
      Right(
        Adjustment(
          Vector(column("id", IntegerType, "id")),
          Vector(column("note", StringType, "note")),
          Change.Unchanged.copy(rescued = Vector("note"), rescuedData = true)
        )
      )
    ),
    Case(
      "under rescue, a table that has _rescued_data already is given nothing",
      Seq(whole(A, "id" -> StringType, "note" -> StringType)),
      Some(
        table(
          "id" -> StringType,
          EngineColumns.SourceFile -> StringType,
          EngineColumns.RescuedData -> StringType
        )
      ),
      policy(onNewColumn = OnNewColumn.Rescue),
      Right(
        Adjustment(
          Vector(column("id", StringType, "id")),
          Vector(column("note", StringType, "note")),
          Change.Unchanged.copy(rescued = Vector("note"))
        )
      )
    ),
    Case(
      "under fail, a new column is refused, with the part that has it first",
      Seq(whole(A, "id" -> StringType), whole(B, "id" -> StringType, "note" -> StringType)),
      Some(table("id" -> StringType)),
      policy(onNewColumn = OnNewColumn.Fail),
      Left(Vector(NewColumn("note", StringType, B)))
    ),
    // --- Columns that the read does not have ------------------------------------------------------
    Case(
      "under null, a column that the read lacks is written with nothing in it",
      Seq(whole(Orders, "id" -> IntegerType)),
      Some(table("id" -> IntegerType, "total" -> DecimalType(10, 2))),
      SchemaPolicy.Default,
      Right(
        Adjustment(
          Vector(column("id", IntegerType, "id"), column("total", DecimalType(10, 2))),
          Vector.empty,
          Change.Unchanged.copy(missing = Vector("total"))
        )
      )
    ),
    Case(
      "under fail, a column that the read lacks is refused",
      Seq(whole(Orders, "id" -> IntegerType)),
      Some(table("id" -> IntegerType, "total" -> DecimalType(10, 2))),
      policy(onMissingColumn = OnMissingColumn.Fail),
      Left(Vector(MissingColumn("total")))
    ),
    // --- Names ------------------------------------------------------------------------------------
    Case(
      "a rename gives a column of the source its name in Bronze, matched exactly",
      Seq(whole(Orders, "cust_name" -> StringType, "CUST_NAME" -> StringType)),
      Some(table("customer" -> StringType)),
      policy(renames = Seq("cust_name" -> "customer")),
      Right(
        Adjustment(
          Vector(
            column("customer", StringType, "cust_name"),
            column("CUST_NAME", StringType, "CUST_NAME")
          ),
          Vector.empty,
          Change.Unchanged.copy(added = Vector(StructField("CUST_NAME", StringType)))
        )
      )
    ),
    Case(
      "two names of one rename in two files are one column",
      Seq(whole(A, "old" -> StringType), whole(B, "new" -> StringType)),
      Some(table("new" -> StringType)),
      policy(renames = Seq("old" -> "new")),
      unchanged(column("new", StringType, "old", "new"))
    ),
    Case(
      "two names of one rename in one row refuse the batch",
      Seq(whole(A, "old" -> StringType, "new" -> StringType)),
      Some(table("new" -> StringType)),
      policy(renames = Seq("old" -> "new")),
      Left(Vector(RenamedTogether(Vector("old", "new"), "new", A)))
    ),
    Case(
      "two names of one rename in records of JSON are left to the read, which knows each record",
      Seq(sparse(A, "old" -> StringType, "new" -> StringType)),
      Some(table("new" -> StringType)),
      policy(renames = Seq("old" -> "new")),
      unchanged(column("new", StringType, "old", "new"))
    ),
    Case(
      "two columns of one row that differ only in their case refuse the batch",
      Seq(whole(A, "id" -> StringType, "ID" -> StringType)),
      None,
      SchemaPolicy.Default,
      Left(Vector(OneNameTwice(Vector("id", "ID"), A)))
    ),
    Case(
      "fields of JSON that differ only in their case are one column, of the spelling of the table",
      Seq(sparse(A, "id" -> StringType, "ID" -> StringType)),
      Some(table("Id" -> StringType)),
      SchemaPolicy.Default,
      unchanged(column("Id", StringType, "id", "ID"))
    ),
    Case(
      "a column under a name of the engine's is refused, case aside",
      Seq(whole(A, "_run_id" -> StringType, "_SOURCE_FILE" -> StringType, "id" -> StringType)),
      Some(table("id" -> StringType)),
      SchemaPolicy.Default,
      Left(Vector(EngineName("_run_id", "_run_id"), EngineName("_SOURCE_FILE", "_source_file")))
    ),
    Case(
      "a column under a name of the engine's is taken under another, by a rename",
      Seq(whole(A, "_run_id" -> StringType)),
      Some(table("source_run" -> StringType)),
      policy(renames = Seq("_run_id" -> "source_run")),
      unchanged(column("source_run", StringType, "_run_id"))
    ),
    Case(
      "a name that a table of Delta cannot have is refused, and taken under another by a rename",
      Seq(whole(A, "first name" -> StringType, "last;name" -> StringType)),
      None,
      policy(renames = Seq("first name" -> "first_name")),
      Left(Vector(NameRefused("last;name")))
    ),
    Case(
      "names that a table of Delta takes are taken as they are",
      Seq(
        whole(
          A,
          "a\"b" -> StringType,
          "it's" -> StringType,
          "t`ick" -> StringType,
          "a.b" -> StringType,
          "ñandú" -> StringType
        )
      ),
      None,
      SchemaPolicy.Default,
      unchanged(
        column("a\"b", StringType, "a\"b"),
        column("it's", StringType, "it's"),
        column("t`ick", StringType, "t`ick"),
        column("a.b", StringType, "a.b"),
        column("ñandú", StringType, "ñandú")
      )
    ),
    // --- Every reason at once ---------------------------------------------------------------------
    Case(
      "every reason is given at once, in the order in which it is met",
      Seq(
        whole(A, "_batch_seq" -> LongType, "v" -> LongType, "note" -> StringType),
        whole(B, "v" -> StringType)
      ),
      Some(table("v" -> IntegerType, "total" -> DecimalType(10, 2))),
      policy(OnNewColumn.Fail, OnMissingColumn.Fail, OnTypeChange.Fail),
      Left(
        Vector(
          EngineName("_batch_seq", "_batch_seq"),
          TwoTypes("v", LongType, A, StringType, B),
          TypeChanged("v", IntegerType, LongType, A),
          MissingColumn("total"),
          NewColumn("note", StringType, A)
        )
      )
    )
  )

  /** The widenings that `DeltaSchemaContractSpec` has Delta 4.4.1 make. */
  private val Taken = Seq(
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
    "TINYINT" -> "DECIMAL(10,0)",
    "SMALLINT" -> "DECIMAL(10,0)",
    "INT" -> "DECIMAL(10,0)",
    "INT" -> "DECIMAL(12,2)",
    "BIGINT" -> "DECIMAL(20,0)",
    "BIGINT" -> "DECIMAL(38,18)",
    "DECIMAL(10,2)" -> "DECIMAL(12,2)",
    "DECIMAL(10,2)" -> "DECIMAL(12,4)",
    "DECIMAL(10,2)" -> "DECIMAL(11,3)",
    "DECIMAL(10,2)" -> "DECIMAL(38,2)",
    "STRUCT<a: INT>" -> "STRUCT<a: BIGINT>",
    "ARRAY<INT>" -> "ARRAY<BIGINT>",
    "MAP<STRING, INT>" -> "MAP<STRING, BIGINT>"
  )

  /**
   * What it refuses, and a date to a timestamp without a zone, which it makes only where the
   * table has the feature of that type, and which the engine does not make (ADR-0025 §3).
   */
  private val NotTaken = Seq(
    "BIGINT" -> "DOUBLE",
    "INT" -> "FLOAT",
    "BIGINT" -> "FLOAT",
    "TINYINT" -> "FLOAT",
    "TINYINT" -> "DECIMAL(5,0)",
    "SMALLINT" -> "DECIMAL(5,0)",
    "INT" -> "DECIMAL(9,0)",
    "BIGINT" -> "DECIMAL(19,0)",
    "DECIMAL(10,2)" -> "DECIMAL(10,4)",
    "DECIMAL(10,2)" -> "DECIMAL(9,1)",
    "DECIMAL(10,2)" -> "DOUBLE",
    "DOUBLE" -> "DECIMAL(38,10)",
    "DATE" -> "TIMESTAMP",
    "DATE" -> "TIMESTAMP_NTZ",
    "TIMESTAMP" -> "TIMESTAMP_NTZ",
    "TIMESTAMP_NTZ" -> "TIMESTAMP",
    "BIGINT" -> "INT",
    "DOUBLE" -> "FLOAT",
    "DECIMAL(12,2)" -> "DECIMAL(10,2)",
    "TIMESTAMP_NTZ" -> "DATE",
    "INT" -> "STRING",
    "STRING" -> "INT",
    "BOOLEAN" -> "INT",
    "INT" -> "BOOLEAN",
    "DATE" -> "STRING",
    "STRING" -> "BINARY",
    "STRUCT<a: INT>" -> "STRUCT<a: INT, b: STRING>",
    "STRUCT<a: INT>" -> "STRUCT<b: BIGINT>",
    "MAP<INT, STRING>" -> "MAP<BIGINT, STRING>"
  )
}
