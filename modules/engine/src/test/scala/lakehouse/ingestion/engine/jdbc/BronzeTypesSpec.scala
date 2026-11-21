package lakehouse.ingestion.engine.jdbc

import lakehouse.ingestion.engine.jdbc.BronzeTypes.{AsSparkReads, AsText}
import org.scalatest.{EitherValues, OptionValues}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * How Bronze keeps a column, by what the source says of its type. What the driver says of each
 * type of PostgreSQL, and what Spark does with it, is held to a real server apart
 * (`JdbcSourceConnectorSpec`, `SparkJdbcContractSpec`).
 */
final class BronzeTypesSpec extends AnyFunSuite with Matchers with EitherValues with OptionValues {

  private def column(
      typeName: String,
      precision: Int = 0,
      scale: Int = 0,
      nullable: Boolean = true
  ) =
    SourceColumn("c", typeName, precision, scale, nullable)

  private def isText(kept: BronzeTypes.Kept): Boolean = kept.isInstanceOf[AsText]

  test("a type that Spark holds every value of is kept as Spark reads it") {
    Seq(
      column("int2", 5),
      column("int4", 10),
      column("int8", 19),
      column("numeric", 10, 2),
      column("numeric", 38, 10),
      column("numeric", 38, 38),
      column("float4", 8, 8),
      column("float8", 17, 17),
      column("bool", 1),
      column("text", Int.MaxValue),
      column("varchar", 20),
      column("bpchar", 5),
      column("date", 13),
      column("timestamp", 29, 6),
      column("timestamptz", 35, 6),
      column("bytea", Int.MaxValue),
      column("uuid", 36),
      column("jsonb", Int.MaxValue),
      column("interval", 49, 6),
      column("money", Int.MaxValue),
      column("oid", 10),
      column("varbit", 5),
      column("bit", 1),
      column("mood", Int.MaxValue),
      column("_int4", 10),
      column("_text", Int.MaxValue),
      column("_numeric", 10, 2)
    ).foreach(c => withClue(c)(BronzeTypes.of(c) shouldBe AsSparkReads))
  }

  test("a numeric that a decimal of Spark does not hold is kept as text") {
    // No bound, as the driver says it; more digits than 38; and a scale below zero, which
    // the driver gives as one beyond the digits.
    BronzeTypes.of(column("numeric")).asInstanceOf[AsText].why should include("rounds it")
    BronzeTypes.of(column("numeric", 39, 2)).asInstanceOf[AsText].why should
      include("39 digits")
    BronzeTypes.of(column("numeric", 50, 2)) shouldBe an[AsText]
    BronzeTypes.of(column("numeric", 5, 2046)).asInstanceOf[AsText].why should
      include("scale below zero")
    BronzeTypes.of(column("numeric", 5, -2)) shouldBe an[AsText]
  }

  test("a time of day and a string of bits are kept as text") {
    isText(BronzeTypes.of(column("time", 15, 6))) shouldBe true
    isText(BronzeTypes.of(column("timetz", 21, 6))) shouldBe true
    isText(BronzeTypes.of(column("bit", 3))) shouldBe true
  }

  test("an array is kept as text if its values would be, or may be infinite") {
    Seq("_numeric" -> 0, "_time" -> 15, "_timetz" -> 21, "_bit" -> 3).foreach {
      case (typeName, precision) =>
        withClue(typeName) {
          BronzeTypes.of(column(typeName, precision)).asInstanceOf[AsText].why should
            startWith("it is an array, and ")
        }
    }
    Seq("_date", "_timestamp", "_timestamptz").foreach { typeName =>
      withClue(typeName) {
        BronzeTypes.of(column(typeName, 13)).asInstanceOf[AsText].why should
          include("may hold infinity")
      }
    }
  }

  test("a date and a timestamp may be infinite, and nothing else is asked of the source") {
    Seq("date", "timestamp", "timestamptz").foreach { typeName =>
      withClue(typeName)(BronzeTypes.mayBeInfinite(column(typeName)) shouldBe true)
    }
    // An array of them is text already, and a number has an infinity that Spark holds.
    Seq("_date", "_timestamptz", "float8", "numeric", "time", "text", "interval").foreach {
      typeName => withClue(typeName)(BronzeTypes.mayBeInfinite(column(typeName)) shouldBe false)
    }
  }

  private val orders = Seq(
    SourceColumn("id", "int4", 10, 0, nullable = false),
    SourceColumn("updated_at", "timestamptz", 35, 6, nullable = false),
    SourceColumn("created", "timestamp", 29, 6, nullable = false),
    SourceColumn("touched_at", "timestamptz", 35, 6, nullable = true),
    SourceColumn("day", "date", 13, 0, nullable = false),
    SourceColumn("Updated_At", "text", Int.MaxValue, 0, nullable = false)
  )

  test("a watermark is a timestamp that the table has, under the name that it has it") {
    WatermarkColumn.check(orders, "updated_at").value shouldBe None
    WatermarkColumn.check(orders, "created").value shouldBe None
    WatermarkColumn.check(orders, "modified_at").left.value shouldBe
      "has no column modified_at (load.watermark.column). Its columns are id, updated_at, " +
      "created, touched_at, day, Updated_At"
    // A name is the source's, with its case.
    WatermarkColumn.check(orders, "UPDATED_AT").left.value should startWith("has no column")
    WatermarkColumn.check(orders, "day").left.value should startWith(
      "has its column day (load.watermark.column) of type date, and a watermark is a " +
        "timestamptz, or a timestamp without a zone"
    )
    WatermarkColumn.check(orders, "id").left.value should include("of type int4")
    WatermarkColumn.check(orders, "Updated_At").left.value should include("of type text")
  }

  test("a watermark that may be null is warned of, and is not refused") {
    WatermarkColumn.check(orders, "touched_at").value.value should startWith(
      "the column touched_at (load.watermark.column) may be null, and a row with no value " +
        "in it is in no batch"
    )
  }
}
