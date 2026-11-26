package lakehouse.ingestion.logstore.coordination

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** What the maintenance log refuses of who asked and why is refused before it is sent. */
final class MaintenanceRequestSpec extends AnyFunSuite with Matchers {

  test("a request names who asked, and says why, within what the log holds") {
    MaintenanceRequest("ops.oncall", "the taint was a false alarm").requestedBy shouldBe
      "ops.oncall"
    // In characters, as the log counts them: a character outside the BMP is one.
    MaintenanceRequest("o" * 200, "😀" * 1000).reason should have length 2000L
  }

  test("a request that says nothing, or too much, is a mistake of its caller") {
    Seq(
      () => MaintenanceRequest("", "why"),
      () => MaintenanceRequest(" \t", "why"),
      () => MaintenanceRequest("o" * 201, "why"),
      () => MaintenanceRequest("ops", ""),
      () => MaintenanceRequest("ops", "   "),
      () => MaintenanceRequest("ops", "x" * 1001)
    ).foreach(made => an[IllegalArgumentException] should be thrownBy made())
  }
}
