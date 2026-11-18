package lakehouse.ingestion.core.control

import java.time.Instant

import lakehouse.ingestion.core.config.SourceId
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** What a run is known by when it starts, and how it says it ended. */
final class RunsSpec extends AnyFunSuite with Matchers with EitherValues {

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
