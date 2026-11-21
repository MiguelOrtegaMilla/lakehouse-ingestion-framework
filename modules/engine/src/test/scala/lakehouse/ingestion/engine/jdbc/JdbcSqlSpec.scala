package lakehouse.ingestion.engine.jdbc

import lakehouse.ingestion.core.config.QualifiedName
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The text of a statement, pinned: it is made of names, each of them one identifier. */
final class JdbcSqlSpec extends AnyFunSuite with Matchers with EitherValues {

  private def table(text: String): QualifiedName = QualifiedName.parse(text).value

  test("a name is one identifier, whatever it holds") {
    JdbcSql.quoted("orders") shouldBe "\"orders\""
    JdbcSql.quoted("Order Lines") shouldBe "\"Order Lines\""
    JdbcSql.quoted("odd\"name") shouldBe "\"odd\"\"name\""
    JdbcSql.quoted("x\"; DROP TABLE canary; --") shouldBe "\"x\"\"; DROP TABLE canary; --\""
    JdbcSql.quoted("a'b\\c") shouldBe "\"a'b\\c\""
  }

  test("a table is its name quoted, with its schema if it has one") {
    JdbcSql.table(table("orders")) shouldBe "\"orders\""
    JdbcSql.table(table("public.Orders")) shouldBe "\"public\".\"Orders\""
    JdbcSql.noRows(table("public.orders")) shouldBe
      "SELECT * FROM \"public\".\"orders\" WHERE 1=0"
  }

  test("the rows of a table with the place of the first column that is not finite") {
    JdbcSql.flagged(
      table("public.orders"),
      Seq("valid_to", "Odd\"Name"),
      "_lakehouse_infinite"
    ) shouldBe
      "(SELECT \"_lakehouse_source\".*, " +
      "CASE WHEN NOT isfinite(\"_lakehouse_source\".\"valid_to\") THEN 1 " +
      "WHEN NOT isfinite(\"_lakehouse_source\".\"Odd\"\"Name\") THEN 2 END " +
      "AS \"_lakehouse_infinite\" " +
      "FROM \"public\".\"orders\" AS \"_lakehouse_source\") AS \"_lakehouse_read\""
    an[IllegalArgumentException] should be thrownBy
      JdbcSql.flagged(table("orders"), Nil, "_lakehouse_infinite")
  }
}
