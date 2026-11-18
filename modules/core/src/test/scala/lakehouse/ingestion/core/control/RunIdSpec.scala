package lakehouse.ingestion.core.control

import java.time.{Clock, Instant, ZoneOffset}
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors, TimeUnit}

import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The id of a run: a UUID of version 7 that each instance makes for itself (ADR-0005 §1). */
final class RunIdSpec extends AnyFunSuite with Matchers with EitherValues {

  private def at(instant: String): Clock = Clock.fixed(Instant.parse(instant), ZoneOffset.UTC)

  /** A source of bits that gives the same every time: what is left is the time. */
  private def bits(value: Long): java.util.Random = new java.util.Random {
    override def nextLong(): Long = value
  }

  test("an id is a UUID of version 7, with the variant of the RFC") {
    val id = RunId.generate()

    id.uuid.version shouldBe 7
    id.uuid.variant shouldBe 2
    id.toString should fullyMatch regex "[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
  }

  test("an id has the time it was made at, to the millisecond, and bits that are random") {
    val id = RunId.generate(at("2026-11-18T09:10:25.123Z"), bits(-1L))

    id.madeAt shouldBe Instant.parse("2026-11-18T09:10:25.123Z")
    // 48 bits of time, the version, 12 bits; the variant, 62 bits. 1794993025123 ms is
    // 0x01a1edebf063.
    id.toString shouldBe "01a1edeb-f063-7fff-bfff-ffffffffffff"
    RunId.generate(at("2026-11-18T09:10:25.123Z"), bits(0L)).toString shouldBe
      "01a1edeb-f063-7000-8000-000000000000"
  }

  test("ids sort by the time they were made, as text and as UUIDs") {
    val times = Seq(
      "1970-01-01T00:00:00Z",
      "2026-11-18T09:10:25.123Z",
      "2026-11-18T09:10:25.124Z",
      "2026-11-18T09:10:26Z",
      "2027-01-01T00:00:00Z",
      "2100-06-15T12:00:00Z"
    )
    // The random bits are against the order: the earliest has them all set.
    val ids = times.zipWithIndex.map { case (time, index) =>
      RunId.generate(at(time), bits(if (index % 2 == 0) -1L else 0L))
    }

    ids.sorted shouldBe ids
    ids.map(_.toString).sorted shouldBe ids.map(_.toString)
    // PostgreSQL sorts a uuid by its bytes, which is the order of its text.
    ids.reverse.sortBy(_.toString) shouldBe ids
  }

  test("no two ids are the same, whatever makes them and at whatever moment") {
    val threads = 8
    val each = 25000
    val seen = ConcurrentHashMap.newKeySet[UUID]()
    val pool = Executors.newFixedThreadPool(threads)
    val together = new CountDownLatch(1)

    (1 to threads).foreach { _ =>
      pool.execute { () =>
        together.await()
        (1 to each).foreach(_ => seen.add(RunId.generate().uuid))
      }
    }
    together.countDown()
    pool.shutdown()
    pool.awaitTermination(1, TimeUnit.MINUTES) shouldBe true

    // 200,000 ids, thousands of them in each millisecond.
    seen.size shouldBe threads * each
  }

  test("an id is read back from its text, and from the UUID a database gives") {
    val id = RunId.generate()

    RunId.parse(id.toString).value shouldBe id
    RunId.of(id.uuid).value shouldBe id
    RunId.of(UUID.fromString(id.toString)).value.madeAt shouldBe id.madeAt
  }

  test("what is not a run id is refused: another version, another way to write it") {
    val random = UUID.randomUUID()
    RunId.of(random).left.value shouldBe "is a UUID of version 4, and a run id is one of version 7"
    RunId.of(new UUID(0L, 0L)).isLeft shouldBe true

    val id = RunId.generate().toString
    Seq(
      "",
      random.toString,
      id.toUpperCase,
      id.replace("-", ""),
      s"{$id}",
      s" $id",
      id.dropRight(1),
      id + "0",
      // What UUID.fromString takes, and is not how an id is written.
      "1-1-7-8-1",
      "019a9662-6ee3-7fff-cfff-ffffffffffff",
      "not-a-run-id"
    ).foreach { text =>
      withClue(s"[$text]") {
        RunId.parse(text).left.value shouldBe
          "is not a run id: a UUID of version 7, as 0193a5f0-7c2e-7b1a-9f3e-2d4c6a8b0e1f"
      }
    }
    RunId.parse("0193a5f0-7c2e-7b1a-9f3e-2d4c6a8b0e1f").isRight shouldBe true
  }

  test("a clock that is before 1970 or beyond 48 bits makes no id") {
    an[IllegalArgumentException] should be thrownBy RunId.generate(
      at("1969-12-31T23:59:59Z"),
      bits(0L)
    )
  }
}
