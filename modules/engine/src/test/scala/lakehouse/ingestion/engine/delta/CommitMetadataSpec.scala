package lakehouse.ingestion.engine.delta

import io.circe.Json
import lakehouse.ingestion.core.config.SourceId
import lakehouse.ingestion.core.control.{BatchId, BatchSeq, Layer, RunId, TxnAppId}
import lakehouse.ingestion.engine.control.StoredJson
import org.scalatest.{EitherValues, OptionValues}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** What a commit of the engine says of itself (ADR-0005 §3, step 5). */
final class CommitMetadataSpec
    extends AnyFunSuite
    with Matchers
    with EitherValues
    with OptionValues {

  private val run = RunId.parse("019a9a2c-7f3e-7c4d-8a1b-2c3d4e5f6a7b").value
  private val source = SourceId.parse("sales.returns").value
  private val batch = BatchId(TxnAppId.of(source, Layer.Bronze), BatchSeq.of(42).value)

  test("a commit says its run and its batch in one form, which its readers depend on") {
    // What DESCRIBE HISTORY shows next to a version: a name or an order that changes here
    // changes for whoever reads it there.
    CommitMetadata(run, source, batch).text shouldBe
      """{"run_id":"019a9a2c-7f3e-7c4d-8a1b-2c3d4e5f6a7b","source_id":"sales.returns",""" +
      """"txn_app_id":"sales.returns/bronze","batch_seq":42}"""
    CommitMetadata.WriteOption shouldBe "userMetadata"
  }

  test("the largest number of a batch and the longest id of a source are said whole") {
    val longest = SourceId.parse("s" * SourceId.MaxBytes).value
    val app = TxnAppId.of(longest, Layer.Silver)
    val last = CommitMetadata(run, longest, BatchId(app, BatchSeq.of(Long.MaxValue).value))

    last.text should not include "\n"
    StoredJson.parse(last.text).value shouldBe Json.obj(
      "run_id" -> Json.fromString(run.toString),
      "source_id" -> Json.fromString(longest.value),
      "txn_app_id" -> Json.fromString(app.value),
      "batch_seq" -> Json.fromLong(Long.MaxValue)
    )
  }
}
