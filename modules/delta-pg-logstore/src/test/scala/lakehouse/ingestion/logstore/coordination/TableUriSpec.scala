package lakehouse.ingestion.logstore.coordination

import scala.reflect.runtime.universe.{typeOf, NoType}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class TableUriSpec extends AnyFunSuite with Matchers {

  test("two keys are the same key when their text is the same") {
    val key = new TableUri("file:/lake/orders")

    key shouldBe new TableUri("file:/lake/orders")
    key.hashCode shouldBe new TableUri("file:/lake/orders").hashCode
    key should not be new TableUri("file:/lake/orders/")
    key.toString shouldBe "file:/lake/orders"
  }

  test("a key is never empty") {
    an[IllegalArgumentException] should be thrownBy new TableUri("")
  }

  /**
   * Asked of the compiled class, when the test runs. A snippet that must not compile
   * (`assertTypeError`) is decided when the suite is compiled, and an incremental build does not
   * decide it again after `TableUri` changes.
   */
  test("only the LogStore module can build a key") {
    val key = typeOf[TableUri]

    val constructors = key.decls.filter(_.isConstructor).toSeq
    constructors.map(_.privateWithin.fullName) shouldBe Seq("lakehouse.ingestion.logstore")
    // A case class, or a companion with a factory, would be a second way in, and a public one.
    key.typeSymbol.asClass.isCaseClass shouldBe false
    key.companion shouldBe NoType
  }
}
