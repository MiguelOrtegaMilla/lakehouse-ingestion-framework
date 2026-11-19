package lakehouse.ingestion.engine

import io.circe.Json
import lakehouse.ingestion.engine.connector.SourceChangedException
import lakehouse.ingestion.engine.connector.SourceChangedException.Changed
import lakehouse.ingestion.engine.control.StoredJson
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What a batch that is given up keeps of what changed under its plan (ADR-0018): it is written
 * in the transaction that gives the batch up, so it must always be something that can be
 * stored. A detail that could not be would leave the batch planned, and its source stopped,
 * which is what giving a batch up is there to prevent.
 */
final class ChangedDetailSpec extends AnyFunSuite with Matchers with EitherValues {

  private def changed(count: Int, name: Int => String): SourceChangedException =
    new SourceChangedException(
      s"$count files of the batch changed since it was planned",
      (1 to count).map(i => Changed(name(i), s"is of ${i + 10} bytes, and was of 10"))
    )

  test("each thing that changed is kept by its name, with how, and how many there were") {
    val detail = IngestionService.changedDetail(changed(2, i => s"orders-$i.csv"))

    detail shouldBe Json.obj(
      "changed" -> Json.arr(
        Json.obj(
          "item" -> Json.fromString("orders-1.csv"),
          "change" -> Json.fromString("is of 11 bytes, and was of 10")
        ),
        Json.obj(
          "item" -> Json.fromString("orders-2.csv"),
          "change" -> Json.fromString("is of 12 bytes, and was of 10")
        )
      ),
      "total" -> Json.fromInt(2)
    )
    StoredJson.readable(detail) shouldBe Right(())
  }

  test("of the files of the largest batch, with the longest names, what is kept can be stored") {
    // A batch has ten thousand files at most, and a name 255 bytes: here in two-byte letters,
    // and with what JSON writes as an escape.
    val longest = (i: Int) => f"$i%05d-\"\\" + "ñ" * 124
    val detail = IngestionService.changedDetail(changed(10000, longest))

    val kept = detail.hcursor.downField("changed").values.toList.flatten
    kept should have size IngestionService.ChangedKept.toLong
    kept.head.hcursor.get[String]("item").value shouldBe longest(1)
    kept.last.hcursor.get[String]("item").value shouldBe longest(IngestionService.ChangedKept)
    // How many there were is said whole: the plan of the batch names the rest.
    detail.hcursor.get[Int]("total").value shouldBe 10000
    StoredJson.readable(detail) shouldBe Right(())
  }

  test("a source that changed says what changed") {
    an[IllegalArgumentException] should be thrownBy
      new SourceChangedException("nothing changed", Nil)
  }
}
