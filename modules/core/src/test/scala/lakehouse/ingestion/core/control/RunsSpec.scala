package lakehouse.ingestion.core.control

import java.time.Instant

import lakehouse.ingestion.core.config.SourceId
import org.scalatest.{EitherValues, OptionValues}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** What a run is known by when it starts, and how it says it ended. */
final class RunsSpec extends AnyFunSuite with Matchers with EitherValues with OptionValues {

  private val started = Instant.parse("2026-11-18T09:10:25.123Z")

  test("an instance is a host, a process and when the process started") {
    InstanceId.of("ingest-7f9c", 4242, started).value shouldBe "ingest-7f9c:4242:1794993025123"
    // In a container every process is number 1: the start tells two of them apart.
    InstanceId.of("worker", 1, started) should not be InstanceId.of(
      "worker",
      1,
      started.plusMillis(1)
    )
  }

  test("a host name that is not one does not get into the audit trail as it came") {
    InstanceId.of("", 7, started).value shouldBe "unknown-host:7:1794993025123"
    InstanceId.of("host:with\ncolons and spaces\u001b[2J", 7, started).value shouldBe
      "hostwithcolonsandspaces2J:7:1794993025123"
    InstanceId.of("h" * 500, 7, started).value.length should be <= InstanceId.MaxLength
  }

  test("an instance is read back as it was written, and what is not one is refused") {
    val id = InstanceId.of("db-host.internal", 99, started)
    InstanceId.parse(id.value).value shouldBe id
    // What the LogStore writes for its own commits is one as well.
    InstanceId.parse("host:1:app-20261118").isRight shouldBe true

    InstanceId.parse("").left.value shouldBe "is empty"
    InstanceId.parse("a b").left.value shouldBe "has a space or a control character in it"
    InstanceId.parse("x" * 201).left.value shouldBe "is longer than 200 characters"
  }

  test("a regular run takes the lease and the sequence of its source and its layer") {
    val start = RunStart(
      RunId.generate(),
      SourceId.parse("sales.orders").value,
      Layer.Silver,
      RunKind.Regular,
      InstanceId.of("host", 1, started),
      "0.1.0+3f2a9c1d7e4b",
      "0" * 64
    )

    start.leaseKey.value shouldBe "sales.orders/silver"
    start.txnAppId.value shouldBe "sales.orders/silver"
  }

  private def starting(
      kind: RunKind,
      request: Option[OperatorRequest],
      backfill: Option[BackfillId] = None
  ) =
    RunStart(
      RunId.generate(),
      SourceId.parse("sales.orders").value,
      Layer.Bronze,
      kind,
      InstanceId.of("host", 1, started),
      "0.1.0",
      "0" * 64,
      request,
      backfill
    )

  private val history = BackfillId.parse("history-2024").value
  private val asked = OperatorRequest.of("ops.oncall", "load the history before start").value

  test("an operator's action says who asked for it and why, and no other run does") {
    val moved = OperatorRequest.of("ops.oncall", "the directory of the source was moved").value
    val abandon = starting(RunKind.Abandon, Some(moved))
    abandon.request.value.requestedBy shouldBe "ops.oncall"
    abandon.request.value.reason shouldBe "the directory of the source was moved"
    // An action of an operator is on the sequence of the source, under its lease.
    abandon.leaseKey.value shouldBe "sales.orders/bronze"
    abandon.txnAppId.value shouldBe "sales.orders/bronze"

    an[IllegalArgumentException] should be thrownBy starting(RunKind.Reset, None)
    an[IllegalArgumentException] should be thrownBy starting(RunKind.Regular, Some(moved))
  }

  test("a run of a backfill takes the lease and the sequence of its backfill") {
    val chunk = starting(RunKind.Backfill, Some(asked), Some(history))
    chunk.backfill shouldBe Some(history)
    chunk.txnAppId shouldBe TxnAppId.backfill(chunk.source, Layer.Bronze, history)
    chunk.leaseKey shouldBe LeaseKey.backfill(chunk.source, Layer.Bronze, history)
    chunk.leaseKey.value shouldBe "sales.orders/bronze/backfill/history-2024"
    chunk.txnAppId.value shouldBe chunk.leaseKey.value
    // An operator's run that gives up a chunk of it acts on the same sequence.
    starting(RunKind.Abandon, Some(asked), Some(history)).leaseKey shouldBe chunk.leaseKey
  }

  test("a run acts on a sequence that its kind may act on, and on no other") {
    // A backfill on its own sequence, and nowhere else.
    val regular = the[IllegalArgumentException] thrownBy starting(RunKind.Backfill, Some(asked))
    regular.getMessage shouldBe "requirement failed: a run of the kind backfill does not act " +
      "on the regular sequence of its source"
    // The regular sequence alone for a run, a reset and a reprocess: the state of a source and
    // its files are its regular sequence's.
    Seq(RunKind.Regular -> None, RunKind.Reset -> Some(asked), RunKind.Reprocess -> Some(asked))
      .foreach { case (kind, request) =>
        withClue(s"$kind: ") {
          val refused =
            the[IllegalArgumentException] thrownBy starting(kind, request, Some(history))
          refused.getMessage shouldBe s"requirement failed: a run of the kind $kind does not " +
            "act on the sequence of the backfill history-2024"
        }
      }
    // A backfill is asked for by an operator, as every action of one.
    an[IllegalArgumentException] should be thrownBy starting(RunKind.Backfill, None, Some(history))
  }

  test("who asked is kept as a name, whatever the process says it is") {
    // A user of a domain, and one with characters that a terminal would obey.
    OperatorRequest.of("CORP\\ana.ortiz", "r").value.requestedBy shouldBe "CORP\\ana.ortiz"
    OperatorRequest.of("ops\u001b[2J user", "r").value.requestedBy shouldBe "ops2Juser"
    OperatorRequest.of("", "r").value.requestedBy shouldBe "unknown"
    OperatorRequest.of("\n\t", "r").value.requestedBy shouldBe "unknown"
    OperatorRequest
      .of("u" * 500, "r")
      .value
      .requestedBy
      .length shouldBe OperatorRequest.MaxRequestedBy
  }

  test("why is one line that a person wrote, kept as written, or refused") {
    OperatorRequest.of("u", "  the plan names a directory that is gone  ").value.reason shouldBe
      "the plan names a directory that is gone"
    OperatorRequest.of("u", "año, ¿por qué? ✓").value.reason shouldBe "año, ¿por qué? ✓"
    OperatorRequest.of("u", "x" * 1000).value.reason should have length 1000

    OperatorRequest.of("u", "   ").left.value should include("says nothing")
    OperatorRequest.of("u", "x" * 1001).left.value shouldBe
      "--reason is longer than 1000 characters"
    OperatorRequest.of("u", "two\nlines").left.value should startWith(
      "--reason has U+000A LINE FEED (LF) at position 4"
    )
    OperatorRequest.of("u", "clear\u001b[2J").left.value should include("U+001B")
    // A character that hides what is written: a mark that reverses the text after it.
    OperatorRequest.of("u", "abc\u202Edef").left.value should include("U+202E")
  }

  test("a run that was running says that it succeeded or failed, and nothing else") {
    RunEnd.Succeeded().status shouldBe RunStatus.Succeeded
    RunEnd
      .Failed(FailureClass.Transient, "the database did not answer")
      .status shouldBe RunStatus.Failed
    // Each is a status that a running run may come to, and neither is one it is born with.
    Seq(RunEnd.Succeeded().status, RunEnd.Failed(FailureClass.Data, "").status).foreach { status =>
      RunStatus.mayBecome(RunStatus.Running, status) shouldBe true
    }
  }

  test("a count that nothing counted is missing, which is not zero") {
    RunCounts.Nothing shouldBe RunCounts(None, None, None, None, None, None, None)
    RunCounts(rowsRead = Some(0)) should not be RunCounts.Nothing
    RunEnd.Succeeded().counts shouldBe RunCounts.Nothing
  }
}
