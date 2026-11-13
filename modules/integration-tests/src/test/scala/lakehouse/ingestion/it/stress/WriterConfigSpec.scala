package lakehouse.ingestion.it.stress

import java.nio.file.Paths

import scala.concurrent.duration._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class WriterConfigSpec extends AnyFunSuite with Matchers {

  private val config = WriterConfig(Paths.get("/tmp/run"), 2, 200, 10, 150.seconds)

  test("survives the round trip through the command line") {
    WriterConfig.fromArgs(config.toArgs) shouldBe Right(config)
  }

  test("rejects malformed, unknown, missing and out-of-range arguments") {
    def without(key: String) = config.toArgs.filterNot(_.startsWith(s"$key="))
    def replaced(key: String, value: String) = without(key) :+ s"$key=$value"

    val cases = Seq(
      (config.toArgs :+ "verbose") -> "expected key=value",
      (config.toArgs :+ "colour=red") -> "unknown arguments: colour",
      without("commits") -> "missing arguments: commits",
      replaced("commits", "0") -> "commits must be a positive integer",
      replaced("writer", "-1") -> "writer must be a non-negative integer",
      replaced("rows-per-commit", "ten") -> "rows-per-commit must be a positive integer"
    )
    cases.foreach { case (args, expected) =>
      WriterConfig.fromArgs(args).left.map(_.startsWith(expected)) shouldBe Left(true)
    }
  }
}
