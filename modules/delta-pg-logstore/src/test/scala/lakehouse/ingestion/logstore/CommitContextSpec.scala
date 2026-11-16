package lakehouse.ingestion.logstore

import java.io.IOException
import java.nio.file.FileAlreadyExistsException
import java.util.UUID

import lakehouse.ingestion.logstore.coordination.FaultyCoordinationStore._
import lakehouse.ingestion.logstore.coordination._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Attribution (ADR-0004 §3.7): which run a commit is for, and how many versions a run lost to
 * other writers. The commits go through the method Delta calls, on `LakeFixture`'s table.
 *
 * That Delta calls that method on the thread of the operation is `DeltaLogStoreContractSpec`'s
 * subject, and Delta inside a context is in `PostgresLogStoreDeltaSpec`.
 */
final class CommitContextSpec extends AnyFunSuite with Matchers with LakeFixture {

  import LakeFixture._

  private def newRun(): CommitContext = CommitContext.forRun(UUID.randomUUID())

  private def runsOf(lake: Lake, versions: Seq[Long]): Seq[Option[UUID]] =
    versions.map(lake.entry(_).value.runId)

  // ---------------------------------------------------------------------------------------------
  // The run of a commit
  // ---------------------------------------------------------------------------------------------

  test("a commit written inside a run's context has the run in its entry; one outside has none") {
    val lake = new Lake
    val run = newRun()

    run.attribute(lake.commit(0)(Creation))
    lake.commit(1)(append(1))
    run.attribute(lake.commit(2)(append(2)))

    runsOf(lake, 0L to 2L) shouldBe Seq(Some(run.runId), None, Some(run.runId))
    run.contention shouldBe 0L
  }

  test("a context ends with its block, also when the block fails") {
    val lake = new Lake().withCommits(last = 0)
    val run = newRun()

    run.attribute("the block's result") shouldBe "the block's result"
    an[IllegalStateException] should be thrownBy
      run.attribute(throw new IllegalStateException("the write failed"))

    CommitContext.current shouldBe None
    lake.commit(1)(append(1))
    lake.entry(1).value.runId shouldBe None
  }

  test("inside another context the inner run applies, and then the outer one again") {
    val lake = new Lake().withCommits(last = 0)
    val outer = newRun()
    val inner = newRun()

    outer.attribute {
      lake.commit(1)(append(1))
      inner.attribute(lake.commit(2)(append(2)))
      // Also when the inner block fails.
      an[IOException] should be thrownBy inner.attribute(throw new IOException("failed"))
      lake.commit(3)(append(3))
    }

    runsOf(lake, 1L to 3L) shouldBe Seq(Some(outer.runId), Some(inner.runId), Some(outer.runId))
    CommitContext.current shouldBe None
  }

  test("a context is its thread's: a thread started inside the block commits for no run") {
    val lake = new Lake().withCommits(last = 0)
    val run = newRun()

    run.attribute {
      // A pool created here outlives nothing in this test; one in an application would.
      Concurrently.atOnce(Seq(1L))(version => lake.commit(version)(append(version))): Unit
      lake.commit(2)(append(2))
    }

    runsOf(lake, 1L to 2L) shouldBe Seq(None, Some(run.runId))
  }

  test("runs that commit at the same moment, each on its thread, get their own entries") {
    val runs = Seq.fill(8)(newRun() -> new Lake().withCommits(last = 0))

    Concurrently.atOnce(runs) { case (run, lake) =>
      run.attribute((1L to 5L).foreach(version => lake.commit(version)(append(version))))
    }: Unit

    runs.foreach { case (run, lake) =>
      runsOf(lake, 1L to 5L).distinct shouldBe Seq(Some(run.runId))
      run.contention shouldBe 0L
    }
  }

  test("a commit that its writer took and another process finished keeps the writer's run") {
    val lake = new Lake().withCommits(last = 0)
    val dead = newRun()
    val next = newRun()

    val taken = dead.attribute(lake.crashedAt(FaultPoint.AfterInsert, version = 1))
    taken.runId shouldBe Some(dead.runId)
    taken.complete shouldBe false

    // The next writer publishes version 1 before it writes version 2.
    next.attribute(lake.commit(2)(append(2)))

    lake.entry(1).value shouldBe taken.copy(complete = true)
    lake.entry(2).value.runId shouldBe Some(next.runId)
    dead.contention shouldBe 0L
    next.contention shouldBe 0L
  }

  // ---------------------------------------------------------------------------------------------
  // The versions a run loses
  // ---------------------------------------------------------------------------------------------

  test("a version whose file is in the log is counted for the run that wanted it") {
    val lake = new Lake().withCommits(last = 1)
    val run = newRun()
    val bystander = newRun()

    a[FileAlreadyExistsException] should be thrownBy run.attribute(lake.commit(1)(append(99)))
    run.contention shouldBe 1L

    // What Delta does next: the same commit, at the next version.
    run.attribute(lake.commit(2)(append(99)))

    run.contention shouldBe 1L
    lake.entry(2).value.runId shouldBe Some(run.runId)
    // The winner's entry is the winner's.
    lake.entry(1).value.runId shouldBe None
    bystander.contention shouldBe 0L
  }

  test("a version lost at the INSERT, before its file appears, is counted the same") {
    val lake = new Lake().withCommits(last = 0)
    val winner = UUID.randomUUID()
    val another = Claim(lake.table, 1L, ".tmp/another-writer", "host-b:2:app", Some(winner))
    lake.memory.take(another) shouldBe TakeResult.Taken
    val run = newRun()

    a[FileAlreadyExistsException] should be thrownBy run.attribute(lake.commit(1)(append(1)))

    run.contention shouldBe 1L
    lake.entry(1).value.runId shouldBe Some(winner)
  }

  test("a creation lost to a writer that finished it in the meantime is counted the same") {
    val lake = new Lake
    lake.store.afterNext(ReadRegistration)(lake.commit(0)(Creation))
    val run = newRun()

    a[FileAlreadyExistsException] should be thrownBy run.attribute(lake.commit(0)(Creation))

    run.contention shouldBe 1L
  }

  test("F15: a claim that another writer's overtook while the reply was lost is counted once") {
    val lake = new Lake().withCommits(last = 0)
    val another = Claim(lake.table, 1L, ".tmp/another-writer", "host-b:2:app", runId = None)
    lake.store.failNext(Take, Preempted(by = another))
    val run = newRun()

    a[FileAlreadyExistsException] should be thrownBy run.attribute(lake.commit(1)(append(1)))

    run.contention shouldBe 1L
  }

  test("F15: a version taken while the reply was lost is the run's own, and no contention") {
    val lake = new Lake().withCommits(last = 0)
    lake.store.failNext(Take, ReplyLost)
    val run = newRun()

    run.attribute(lake.commit(1)(append(1)))

    run.contention shouldBe 0L
    lake.entry(1).value.runId shouldBe Some(run.runId)
  }

  test("a commit that fails for another reason is not contention") {
    val lake = new Lake().withCommits(last = 0)
    val run = newRun()

    lake.store.down()
    an[IOException] should be thrownBy run.attribute(lake.commit(1)(append(1)))
    lake.store.up()
    lake.memory.taint(lake.table, "tainted by the test")
    a[TableTaintedException] should be thrownBy run.attribute(lake.commit(1)(append(1)))

    run.contention shouldBe 0L
  }

  test("a version lost outside any context is counted for nobody") {
    val lake = new Lake().withCommits(last = 1)
    val run = newRun()

    a[FileAlreadyExistsException] should be thrownBy lake.commit(1)(append(99))

    run.contention shouldBe 0L
  }

  test("a run that writes from several threads counts the versions that all of them lose") {
    val lake = new Lake().withCommits(last = 1)
    val run = newRun()
    val writers = 16

    Concurrently.atOnce(1 to writers) { _ =>
      a[FileAlreadyExistsException] should be thrownBy run.attribute(lake.commit(1)(append(99)))
    }: Unit

    run.contention shouldBe writers.toLong
  }

  test("a context needs its run") {
    a[NullPointerException] should be thrownBy CommitContext.forRun(null)
  }
}
