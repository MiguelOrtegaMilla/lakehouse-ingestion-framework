package lakehouse.ingestion.engine.bronze

import lakehouse.ingestion.core.config.EngineColumns
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Which columns of the rows of a source may not be written to Bronze (discovery §17, P5). */
final class BronzeColumnsSpec extends AnyFunSuite with Matchers {

  test("the columns of a source are written, and those that a read gives under their names") {
    BronzeWriter.refused(Seq("id", "amount", "_id", "run_id", "_source", "_rescued")) shouldBe empty
    BronzeWriter.refused(Seq("id", "_source_file", "_rescued_data")) shouldBe empty
  }

  test("a column under a name that the writer adds is refused, however it is cased") {
    val added =
      Seq(
        EngineColumns.RunId,
        EngineColumns.BatchSeq,
        EngineColumns.IngestedAt,
        EngineColumns.SourceId
      )
    added.foreach { name =>
      withClue(name) {
        BronzeWriter.refused(Seq("id", name)) shouldBe Seq(name -> name)
        BronzeWriter.refused(Seq(name.toUpperCase, "id")) shouldBe Seq(name.toUpperCase -> name)
      }
    }
    BronzeWriter.refused(Seq("_Run_Id", "id", "_BATCH_seq")) shouldBe
      Seq("_Run_Id" -> "_run_id", "_BATCH_seq" -> "_batch_seq")
  }

  test(
    "a column that is one of a read's by another casing is a column of the source, and refused"
  ) {
    // Spark would take it for the read's own, and the table would have one of the two.
    BronzeWriter.refused(Seq("_SOURCE_FILE", "_Rescued_Data")) shouldBe
      Seq("_SOURCE_FILE" -> "_source_file", "_Rescued_Data" -> "_rescued_data")
    BronzeWriter.refused(Seq("_source_file", "_Source_File")) shouldBe
      Seq("_Source_File" -> "_source_file")
  }

  test("every name of the engine is either added by the writer or given by a read") {
    // A column that is added to the list of the engine's is refused until the writer is told
    // what to do with it.
    EngineColumns.All.filter(name => BronzeWriter.refused(Seq(name)).isEmpty) shouldBe
      Seq(EngineColumns.SourceFile, EngineColumns.RescuedData)
  }
}
