package lakehouse.ingestion.engine.delta

import java.util.UUID

import lakehouse.ingestion.core.config.SourceId
import lakehouse.ingestion.core.control.{BatchId, BatchSeq, Layer, RunId, TxnAppId}
import org.scalatest.{EitherValues, OptionValues}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What a commit of the engine says of itself, and that the `userMetadata` of any other commit
 * is not taken for it (ADR-0005 §3, step 5).
 */
final class CommitMetadataSpec
    extends AnyFunSuite
    with Matchers
    with EitherValues
    with OptionValues {

  private val run = RunId.parse("019a9a2c-7f3e-7c4d-8a1b-2c3d4e5f6a7b").value
  private val source = SourceId.parse("sales.returns").value
  private val batch = BatchId(TxnAppId.of(source, Layer.Bronze), BatchSeq.of(42).value)
  private val metadata = CommitMetadata(run, source, batch)

  test("a commit says its run and its batch in one form, which other readers depend on") {
    // What DESCRIBE HISTORY shows, and what the views of the lineage read (P11): a name or an
    // order that changes here changes for them.
    metadata.text shouldBe
      """{"run_id":"019a9a2c-7f3e-7c4d-8a1b-2c3d4e5f6a7b","source_id":"sales.returns",""" +
      """"txn_app_id":"sales.returns/bronze","batch_seq":42}"""
    CommitMetadata.WriteOption shouldBe "userMetadata"
  }

  test("what a commit says is read back as it was written, however it is spaced or ordered") {
    CommitMetadata.parse(metadata.text).value shouldBe metadata
    CommitMetadata
      .parse(
        """{ "batch_seq": 42, "txn_app_id": "sales.returns/bronze",
          |  "source_id": "sales.returns", "run_id": "019a9a2c-7f3e-7c4d-8a1b-2c3d4e5f6a7b" }""".stripMargin
      )
      .value shouldBe metadata
    // The largest number of a batch, and the longest id of a source.
    val longest = SourceId.parse("s" * SourceId.MaxBytes).value
    val last = CommitMetadata(
      run,
      longest,
      BatchId(TxnAppId.of(longest, Layer.Silver), BatchSeq.of(Long.MaxValue).value)
    )
    CommitMetadata.parse(last.text).value shouldBe last
  }

  test("a later version may say more, and what it adds is let be") {
    val more = metadata.text.dropRight(1) + ""","config_hash":"0123abcd","attempt":2}"""
    CommitMetadata.parse(more).value shouldBe metadata
  }

  test("the userMetadata of a commit that is not the engine's is not read as one") {
    val notOurs = Seq(
      "",
      "nightly compaction",
      "42",
      "null",
      "[1, 2, 3]",
      "{}",
      """{"job":"optimize","started_by":"operator"}""",
      // Not whole.
      metadata.text.replace(""""batch_seq":42""", """"batch":42"""),
      metadata.text.replace(""","batch_seq":42""", ""),
      metadata.text.replace(""""run_id":"019a9a2c-7f3e-7c4d-8a1b-2c3d4e5f6a7b",""", ""),
      // Not what the engine writes: a number as text, a batch before the first, a run id
      // that is no UUID of version 7, a sequence that is none.
      metadata.text.replace(""""batch_seq":42""", """"batch_seq":"42""""),
      metadata.text.replace(""""batch_seq":42""", """"batch_seq":0"""),
      metadata.text.replace(""""batch_seq":42""", """"batch_seq":4.2"""),
      metadata.text.replace(run.toString, UUID.randomUUID().toString),
      metadata.text.replace(run.toString, "not-a-run"),
      metadata.text.replace("sales.returns/bronze", "sales.returns/gold"),
      metadata.text.replace("sales.returns/bronze", "../bronze"),
      // Cut short, or not JSON at all.
      metadata.text.dropRight(1),
      metadata.text + metadata.text,
      "{\"run_id\": !!binary x}"
    )
    notOurs.foreach(text => withClue(text)(CommitMetadata.parse(text) shouldBe None))
  }

  test("text that is larger or deeper than a commit of the engine's is not read at all") {
    val padded = metadata.text.dropRight(1) + s""","note":"${"x" * 4096}"}"""
    CommitMetadata.parse(padded) shouldBe None
    val nested = metadata.text.dropRight(1) + ""","more":{"deep":{"deeper":1}}}"""
    CommitMetadata.parse(nested) shouldBe None
  }
}
