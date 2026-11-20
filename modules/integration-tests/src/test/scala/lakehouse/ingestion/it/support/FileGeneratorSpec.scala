package lakehouse.ingestion.it.support

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode}
import lakehouse.ingestion.it.support.FileGenerator._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The generator of files, by itself: that a seed means one delivery, that a delivery has what
 * its mix says, and that what it says is in a file is what the file holds.
 */
final class FileGeneratorSpec extends AnyFunSuite with Matchers {

  private val Everything =
    Mix(files = 4, added = 2, missing = 2, corrupt = 6, empty = 3, repeated = 2)

  private val formats = Seq(Format.Csv, Format.Json)

  private def digest(delivery: Delivery): String = {
    val sha = MessageDigest.getInstance("SHA-256")
    delivery.files.foreach { file =>
      sha.update(file.name.getBytes(UTF_8))
      sha.update(0.toByte)
      sha.update(file.text.getBytes(UTF_8))
      sha.update(0.toByte)
    }
    sha.digest().map(b => f"$b%02x").mkString
  }

  test("a seed gives the same delivery every time, and another seed gives another") {
    formats.foreach { format =>
      deliver(7, format, Everything) shouldBe deliver(7, format, Everything)
      deliver(7, format, Everything).files.map(_.text) should not be
        deliver(8, format, Everything).files.map(_.text)
    }
  }

  test("a seed means the same bytes on every machine, and for good") {
    // Whoever changes what the generator writes changes these on purpose: the data of every
    // test that names a seed changes with them.
    Map(
      "csv" -> digest(deliver(1, Format.Csv, Everything)),
      "json" -> digest(deliver(1, Format.Json, Everything))
    ) shouldBe Map(
      "csv" -> "0518aca6d3fb0dd7402446abdc221e8ed0b6e1b1ded9d6d1bac4d910d75a5c4c",
      "json" -> "7a3f031e260efbfe5ed77c4c489792c4f26e2501c809a0c3593a480d416660ae"
    )
  }

  test("a delivery has what its mix says, in files and in records") {
    for (format <- formats; seed <- 1L to 20L) {
      val delivery = deliver(seed, format, Everything)
      withClue(s"$format, seed $seed: ") {
        delivery.files should have size 13
        delivery.of(_ == Kind.Plain) should have size 4
        delivery.of(_ == Kind.WithAdded) should have size 2
        delivery.of(_.isInstanceOf[Kind.Without]) should have size 2
        delivery.of(_ == Kind.Empty) should have size 3
        delivery.of(_.isInstanceOf[Kind.Again]) should have size 2

        // The records that cannot be parsed, each once, not counting the files that are again.
        delivery.of(!_.isInstanceOf[Kind.Again]).map(_.corrupt.size).sum shouldBe 6
        // Of CSV, both kinds of empty file: of no bytes, and of a header alone.
        delivery.of(_ == Kind.Empty).map(_.text.isEmpty).distinct should have size
          (if (format == Format.Csv) 2L else 1L)
        delivery.of(_ == Kind.Empty).foreach { file =>
          file.records shouldBe empty
          file.corrupt shouldBe empty
        }
        delivery.files.filter(_.kind != Kind.Empty).foreach { file =>
          file.records.size should (be >= 1 and be <= Everything.records)
          file.records.foreach(_.keySet shouldBe file.columns.toSet)
        }
      }
    }
  }

  test("a column more is one that no other file has, and a column less is the same in every file") {
    for (format <- formats; seed <- 1L to 20L) {
      val delivery = deliver(seed, format, Everything)
      delivery.of(_ == Kind.Plain).foreach(_.columns shouldBe Columns)
      delivery.of(_ == Kind.WithAdded).foreach(_.columns shouldBe Columns :+ Added)

      val without = delivery.files.map(_.kind).collect { case Kind.Without(column) => column }
      without.distinct should have size 1
      // Never the id, which says which record a row is.
      Columns.tail should contain(without.head)
      delivery.of(_.isInstanceOf[Kind.Without]).foreach {
        _.columns shouldBe Columns.filterNot(_ == without.head)
      }
      delivery.columns.toSet shouldBe (Columns :+ Added).toSet
    }
  }

  test("a repeated file is another file of the delivery, byte for byte, under a name of its own") {
    for (format <- formats; seed <- 1L to 20L) {
      val delivery = deliver(seed, format, Everything)
      delivery.files.map(_.name).distinct should have size delivery.files.size.toLong
      delivery.files.collect { case again @ Generated(_, _, _, _, _, Kind.Again(of)) =>
        val original = delivery.files.find(_.name == of).get
        original.kind should (not be Kind.Empty and not be a[Kind.Again])
        again.text shouldBe original.text
        again.records shouldBe original.records
        again.corrupt shouldBe original.corrupt
      } should have size 2
    }
  }

  test("no id is given twice, in one delivery or in two of one seed") {
    val first = deliver(3, Format.Json, Everything, number = 1)
    val second = deliver(3, Format.Json, Everything, number = 2)
    val ids = (first.files ++ second.files)
      .filterNot(_.kind.isInstanceOf[Kind.Again])
      .flatMap(_.records.map(_("id")))

    ids.distinct should have size ids.size.toLong
    (first.files.map(_.name) ++ second.files.map(_.name)).distinct should have size 26
    first.files.map(_.text) should not be second.files.map(_.text)
  }

  test("a file of JSON holds its records, a line each, and its lines that are not JSON") {
    for (seed <- 1L to 20L; file <- deliver(seed, Format.Json, Everything).files) {
      val lines = file.text.linesIterator.toVector.map(line => line -> json(line))

      lines.collect { case (line, None) => line } shouldBe file.corrupt
      lines.collect { case (_, Some(node)) =>
        // The amount is a number in the file, with the digits that the record says.
        node.get("amount").isNumber shouldBe true
        node.properties().asScala.map(field => field.getKey -> field.getValue.asText()).toMap
      } shouldBe file.records
    }
  }

  test("a file of CSV has its header, and a record or a broken record for every other line") {
    for (seed <- 1L to 20L; file <- deliver(seed, Format.Csv, Everything).files) {
      if (file.columns.isEmpty) file.text shouldBe ""
      else {
        val records = csv(file.text)
        records.head shouldBe file.columns
        val (whole, broken) = records.tail.partition(_.size == file.columns.size)
        whole.map(values => file.columns.zip(values).toMap) shouldBe file.records
        broken.map(_.mkString(",")) shouldBe file.corrupt
        // A broken record has no quote: a parser would follow one to the end of the file.
        file.corrupt.foreach(_ should not include "\"")
      }
    }
  }

  test("a mix with nothing to repeat, or to put a broken record in, is refused") {
    an[IllegalArgumentException] should be thrownBy Mix(files = 0, repeated = 1)
    an[IllegalArgumentException] should be thrownBy Mix(files = 0, empty = 2, corrupt = 1)
    deliver(1, Format.Csv, Mix(files = 0, empty = 2)).files should have size 2
  }

  private val mapper = JsonMapper
    .builder()
    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
    // A number as it was written, with the zeros at its end.
    .disable(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES)
    .build()

  /** The object of a line of JSON, read by a parser that is not the engine's; none, if broken. */
  private def json(line: String): Option[JsonNode] =
    try Option(mapper.readTree(line)).filter(_.isObject)
    catch { case _: JsonProcessingException => None }

  /** The records of a text of CSV, by RFC 4180 and nothing else: the test's own reader. */
  private def csv(text: String): Vector[Vector[String]] = {
    val records = Vector.newBuilder[Vector[String]]
    var record = Vector.empty[String]
    val value = new StringBuilder
    var quoted = false
    var i = 0
    while (i < text.length) {
      val c = text.charAt(i)
      if (quoted) {
        if (c == '"' && i + 1 < text.length && text.charAt(i + 1) == '"') { value += '"'; i += 1 }
        else if (c == '"') quoted = false
        else value += c
      } else
        c match {
          case '"' => quoted = true
          case ',' => record :+= value.result(); value.clear()
          case '\n' =>
            records += (record :+ value.result())
            record = Vector.empty
            value.clear()
          case other => value += other
        }
      i += 1
    }
    quoted shouldBe false
    records.result()
  }
}
