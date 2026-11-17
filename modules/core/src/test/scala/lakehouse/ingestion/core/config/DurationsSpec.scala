package lakehouse.ingestion.core.config

import scala.concurrent.duration._

import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class DurationsSpec extends AnyFunSuite with Matchers with EitherValues {

  test("a whole number and a unit is a duration") {
    Durations.parse("500ms").value shouldBe 500.millis
    Durations.parse("30s").value shouldBe 30.seconds
    Durations.parse("10m").value shouldBe 10.minutes
    Durations.parse("2h").value shouldBe 2.hours
    Durations.parse("7d").value shouldBe 7.days
    Durations.parse("0s").value shouldBe Duration.Zero
    Durations.parse("90s").value shouldBe 90.seconds
    Durations.parse("3650d").value shouldBe Durations.Max
  }

  test("a number without a unit is refused: nobody knows what it was meant in") {
    Seq("30", "0", "5000").foreach { text =>
      withClue(text) {
        Durations.parse(text).left.value shouldBe
          "has no unit: write it as 500ms, 30s, 10m, 2h or 7d"
      }
    }
  }

  test("what is not written the one way is refused") {
    val notOne = Seq(
      "",
      " ",
      "10 m",
      " 10m",
      "10m ",
      "-5s",
      "+5s",
      "1.5h",
      "1,5h",
      "010m",
      "00s",
      "10M",
      "10S",
      "10min",
      "10sec",
      "1w",
      "1y",
      "PT10M",
      "10m30s",
      "ten minutes",
      "m",
      "s10",
      "1e3s",
      "0x10s",
      "\u0661\u0660m",
      "9999999999s"
    )

    notOne.foreach { text =>
      withClue(s"[$text]") {
        Durations.parse(text).left.value shouldBe
          "is not a duration: a whole number and a unit, as in 500ms, 30s, 10m, 2h or 7d"
      }
    }
  }

  test("a duration longer than ten years is refused, whatever its unit") {
    Seq("3651d", "999999999d", "999999999h", "87601h").foreach { text =>
      withClue(text) {
        Durations.parse(text).left.value shouldBe
          "is longer than 3650d, the longest a duration may be"
      }
    }
    Durations.parse("87600h").value shouldBe Durations.Max
  }

  test("a duration is rendered one way, and read back as itself") {
    Durations.render(Duration.Zero) shouldBe "0s"
    Durations.render(90.seconds) shouldBe "90s"
    Durations.render(120.seconds) shouldBe "2m"
    Durations.render(60.minutes) shouldBe "1h"
    Durations.render(48.hours) shouldBe "2d"
    Durations.render(1500.millis) shouldBe "1500ms"
    Durations.render(86400000.millis) shouldBe "1d"

    Seq("500ms", "30s", "10m", "2h", "7d", "0s", "90s", "36h").foreach { text =>
      withClue(text)(Durations.parse(text).map(Durations.render).value shouldBe text)
    }
    // Two ways to write one duration have one rendering: it is the one that is hashed.
    Durations.parse("120s").map(Durations.render) shouldBe Durations
      .parse("2m")
      .map(Durations.render)
  }
}
