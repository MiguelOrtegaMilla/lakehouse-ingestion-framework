package lakehouse.ingestion.it.ingestion

import java.nio.charset.StandardCharsets.UTF_8

import lakehouse.ingestion.core.config.Source
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.IngestionService.Outcome
import lakehouse.ingestion.engine.quarantine.QuarantineReader
import lakehouse.ingestion.engine.quarantine.QuarantineReader._
import org.apache.spark.sql.execution.{SortExec, TakeOrderedAndProjectExec}
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * What a quarantine is read as, for whoever asks what a source had set apart (P8.10a): the
 * records counted by their run, their file and why, and some of them as they were received. On
 * the quarantines that real runs write, on real Delta and a real control plane.
 *
 * That the records of a batch that was given up are left out, in every sequence of a source, is
 * held by `IngestionServiceSpec` and `BackfillSpec` as well, which have the fixtures of it.
 */
final class QuarantineReaderSpec
    extends AnyFunSuite
    with Matchers
    with OptionValues
    with IngestionSuite {

  private lazy val reader = new QuarantineReader(database, spark, lakeRoot)

  /** `source_file` of a file of the source, as its records have it. */
  private def fileOf(of: Source, name: String): String =
    s"${directoryOf(of).getFileName}/$name"

  private def committed(batch: BatchId): Option[Taken] =
    Some(Taken(batch, BatchStatus.Committed))

  /** A group, without when its latest record was set apart. */
  private def shape(group: Group): (RunId, Long, Option[Taken], Option[String], String, Long) =
    (group.run, group.batchSeq, group.taken, group.file, group.errorType, group.records)

  private val Nothing = LeftOut(0L, 0L)

  // ---------------------------------------------------------------------------------------------
  // The records, counted
  // ---------------------------------------------------------------------------------------------

  test("a source that never had a record set apart has no quarantine to count, nor to show") {
    val orders = fileSource()
    drop(orders, "a.csv", "id\n1\n")
    ingested(launch(orders))

    reader.counts(orders.id) shouldBe None
    reader.sample(orders.id, Asked.Everything, 10) shouldBe None
  }

  test("records are counted by their run, their file and why, the latest first, and by type") {
    val orders = fileSource("json", rejected = Some("0.9"))
    drop(
      orders,
      "a.json",
      Seq("""{"id":1}""", """{"id":2} x""", "not json", "nor this").mkString("\n")
    )
    drop(orders, "b.json", """{"id":3,"id":4}""" + "\n")
    val first = ingested(launch(orders))
    drop(orders, "c.json", "{\"id\":5}\nbroken\n")
    val second = ingested(launch(orders, service(at = Now.plusSeconds(60))))

    val counts = reader.counts(orders.id).value
    // The group of the latest records first; those of one moment, by their run, their file
    // and their type.
    counts.groups.map(shape) shouldBe Vector(
      (second.run, 2L, committed(second.batch), Some(fileOf(orders, "c.json")), "unparsed", 1L),
      (first.run, 1L, committed(first.batch), Some(fileOf(orders, "a.json")), "trailing_text", 1L),
      (first.run, 1L, committed(first.batch), Some(fileOf(orders, "a.json")), "unparsed", 2L),
      (first.run, 1L, committed(first.batch), Some(fileOf(orders, "b.json")), "duplicate_field", 1L)
    )
    counts.groups.map(_.last) shouldBe Vector(Now.plusSeconds(60), Now, Now, Now)
    counts.byType shouldBe Vector(
      OfType("duplicate_field", 1L, 1L),
      OfType("trailing_text", 1L, 1L),
      OfType("unparsed", 3L, 2L)
    )
    counts.records shouldBe 5L
    counts.groupsInAll shouldBe 4L
    counts.leftOut shouldBe Nothing

    // No more groups than are asked for, and how many there are is said still.
    val two = reader.counts(orders.id, groups = 2).value
    two.groups shouldBe counts.groups.take(2)
    two.groupsInAll shouldBe 4L
    two.records shouldBe 5L
    an[IllegalArgumentException] should be thrownBy reader.counts(orders.id, groups = 0)
    an[IllegalArgumentException] should be thrownBy
      reader.counts(orders.id, groups = MaxGroups + 1)
  }

  test("groups of one moment are shown by their run, the first that started first") {
    val orders = fileSource(rejected = Some("0.9"))
    drop(orders, "b.csv", "id\n1\n2,x\n")
    val first = ingested(launch(orders))
    drop(orders, "a.csv", "id\n3\n4,y\n")
    // By the same clock: the two batches set their records apart at one moment.
    val second = ingested(launch(orders))

    reader.counts(orders.id).value.groups.map(group => group.run -> group.file) shouldBe Vector(
      first.run -> Some(fileOf(orders, "b.csv")),
      second.run -> Some(fileOf(orders, "a.csv"))
    )
  }

  test("what is asked for is of a run, of a batch, of a file, of a type, or of several of them") {
    val orders = fileSource("json", rejected = Some("0.9"))
    drop(orders, "a.json", Seq("""{"id":1}""", """{"id":2} x""", "not json").mkString("\n"))
    drop(orders, "b.json", "{\"id\":3}\nbroken\n")
    val first = ingested(launch(orders))
    drop(orders, "c.json", "{\"id\":5}\nalso broken\n")
    val second = ingested(launch(orders, service(at = Now.plusSeconds(60))))

    def asked(what: Asked): Seq[(Long, String, String, Long)] = {
      val counts = reader.counts(orders.id, what).value
      counts.leftOut shouldBe Nothing
      // What is shown of the records is what is counted of them.
      val shown = reader.sample(orders.id, what, MaxRecords).value
      shown.size.toLong shouldBe counts.records
      shown.groupBy(r => (r.run, r.file, r.errorType)).view.mapValues(_.size.toLong).toMap shouldBe
        counts.groups.map(g => (g.run, g.file, g.errorType) -> g.records).toMap
      counts.groups.map(g => (g.batchSeq, g.file.value.split('/').last, g.errorType, g.records))
    }

    asked(Asked(run = Some(second.run))) shouldBe Seq((2L, "c.json", "unparsed", 1L))
    asked(Asked(batch = Some(first.batch))) shouldBe Seq(
      (1L, "a.json", "trailing_text", 1L),
      (1L, "a.json", "unparsed", 1L),
      (1L, "b.json", "unparsed", 1L)
    )
    asked(Asked(file = Some(fileOf(orders, "b.json")))) shouldBe Seq((1L, "b.json", "unparsed", 1L))
    asked(Asked(errorType = Some("unparsed"))) shouldBe Seq(
      (2L, "c.json", "unparsed", 1L),
      (1L, "a.json", "unparsed", 1L),
      (1L, "b.json", "unparsed", 1L)
    )
    asked(
      Asked(
        batch = Some(first.batch),
        file = Some(fileOf(orders, "a.json")),
        errorType = Some("unparsed")
      )
    ) shouldBe Seq((1L, "a.json", "unparsed", 1L))
    // A run and a batch that the run did not take, a batch that there is not, a file by its
    // name alone, and a type that no record has: nothing.
    asked(Asked(run = Some(second.run), batch = Some(first.batch))) shouldBe empty
    asked(Asked(run = Some(first.run), batch = Some(first.batch))) should have size 3
    asked(Asked(batch = Some(batchId(orders, 9)))) shouldBe empty
    asked(Asked(run = Some(RunId.generate()))) shouldBe empty
    asked(Asked(file = Some("b.json"))) shouldBe empty
    asked(Asked(errorType = Some("duplicate_field"))) shouldBe empty
  }

  test("a batch stopped by its threshold is shown as planned, and one given up is left out") {
    // The source lets no record be rejected.
    val orders = fileSource()
    def stopped(): RunId = launch(orders) match {
      case Outcome.Failed(run, FailureClass.Data, _) => run
      case other => fail(s"the batch was not stopped by its threshold: $other")
    }
    drop(orders, "a.csv", "id,amount\n1,2\n3\n")
    drop(orders, "b.csv", "id,amount\n4,5\n")
    val first = stopped()

    // Its records are what there is to look at.
    val planned = reader.counts(orders.id).value
    planned.groups.map(shape) shouldBe Vector(
      (
        first,
        1L,
        Some(Taken(batchId(orders, 1), BatchStatus.Planned)),
        Some(fileOf(orders, "a.csv")),
        "unparsed",
        1L
      )
    )
    planned.leftOut shouldBe Nothing

    // A file of it is written again: the run after gives the batch up, and its record counts
    // for nothing. It is said how much was left out, and of how many runs that set it apart.
    drop(orders, "b.csv", "id,amount\n6,7\n8,9\n", at = anHourAgo.plusSeconds(60))
    launch(orders) shouldBe a[Outcome.GivenUp]
    val givenUp = reader.counts(orders.id).value
    givenUp.groups shouldBe empty
    givenUp.records shouldBe 0L
    givenUp.byType shouldBe empty
    givenUp.leftOut shouldBe LeftOut(records = 1L, runs = 1L)
    reader.sample(orders.id, Asked.Everything, 10).value shouldBe empty

    // The batch after sets the same record apart, under its own number.
    val second = stopped()
    val again = reader.counts(orders.id).value
    again.groups.map(group => (group.run, group.batchSeq, group.taken.map(_.status))) shouldBe
      Vector((second, 2L, Some(BatchStatus.Planned)))
    again.leftOut shouldBe LeftOut(1L, 1L)
    reader.sample(orders.id, Asked.Everything, 10).value.map(r => r.run -> r.raw) shouldBe
      Vector(second -> "3")

    // Asked for by itself, what was given up is left out still; and what is left out is of
    // what is asked.
    val ofFirst = reader.counts(orders.id, Asked(batch = Some(batchId(orders, 1)))).value
    ofFirst.groups shouldBe empty
    ofFirst.leftOut shouldBe LeftOut(1L, 1L)
    reader.counts(orders.id, Asked(run = Some(first))).value.leftOut shouldBe LeftOut(1L, 1L)
    reader.sample(orders.id, Asked(run = Some(first)), 10).value shouldBe empty
    reader.counts(orders.id, Asked(batch = Some(batchId(orders, 2)))).value.leftOut shouldBe Nothing
    reader
      .counts(orders.id, Asked(errorType = Some("trailing_text")))
      .value
      .leftOut shouldBe Nothing
  }

  // ---------------------------------------------------------------------------------------------
  // Some records, as they were received
  // ---------------------------------------------------------------------------------------------

  test("the records shown are the latest, in an order that is whole, taken as the first of it") {
    val orders = fileSource(rejected = Some("0.9"))
    drop(orders, "a.csv", "id,name\n0,ok\n9,z,extra\n1,b,extra\n")
    drop(orders, "b.csv", "id,name\n5,m,extra\n")
    val first = ingested(launch(orders))
    drop(orders, "c.csv", "id,name\n7,k,extra\n8,ok\n")
    val second = ingested(launch(orders, service(at = Now.plusSeconds(60))))

    // The latest first; those of one batch by their file, and those of one file by their text.
    val all = reader.sample(orders.id, Asked.Everything, 10).value
    all.map(record => (record.batchSeq, record.raw)) shouldBe Vector(
      (2L, "7,k,extra"),
      (1L, "1,b,extra"),
      (1L, "9,z,extra"),
      (1L, "5,m,extra")
    )
    all.head shouldBe Record(
      second.run,
      2L,
      committed(second.batch),
      Some(fileOf(orders, "c.csv")),
      "unparsed",
      None,
      Now.plusSeconds(60),
      "7,k,extra",
      9L
    )
    all.head.truncated shouldBe false
    all.tail.map(_.run).distinct shouldBe Vector(first.run)

    // No more than are asked for, and those are the first of the same order.
    (1 to 4).foreach { n =>
      reader.sample(orders.id, Asked.Everything, n).value shouldBe all.take(n)
    }
    an[IllegalArgumentException] should be thrownBy reader.sample(orders.id, Asked.Everything, 0)
    an[IllegalArgumentException] should be thrownBy
      reader.sample(orders.id, Asked.Everything, MaxRecords + 1)

    // Spark takes the first of the order, each task keeping no more than are asked for, and
    // does not sort the quarantine (`TakeOrderedAndProject`).
    val plan = reader.sampleOf(orders.id, Asked.Everything, 2).value.queryExecution.sparkPlan
    plan.collect { case top: TakeOrderedAndProjectExec => top.limit } shouldBe Seq(2)
    plan.collect { case sort: SortExec => sort } shouldBe empty
  }

  test("the text of a record is cut at its characters, and never inside one") {
    val orders = fileSource(rejected = Some("0.9"))
    val emoji = "😀"
    // Records with a value too many, of which the text is the line. The emoji is the last
    // character that is kept of the first, and the first that is not of the second.
    val kept = "a" * (MaxChars - 1) + emoji + "zz,1,extra"
    val left = "b" * MaxChars + emoji + ",1,extra"
    val short = s"c$emoji,1,extra"
    drop(orders, "a.csv", s"id,name\n0,ok\n$kept\n$left\n$short\n")
    ingested(launch(orders))

    val shown = reader.sample(orders.id, Asked.Everything, 10).value
    shown should have size 3
    val (first, second, third) = (shown(0), shown(1), shown(2))
    // A character outside the basic plane is one character, and two units of a string.
    first.raw shouldBe "a" * (MaxChars - 1) + emoji
    first.raw.codePointCount(0, first.raw.length) shouldBe MaxChars
    first.raw.length shouldBe MaxChars + 1
    first.chars shouldBe (MaxChars + 10).toLong
    first.truncated shouldBe true

    second.raw shouldBe "b" * MaxChars
    second.chars shouldBe (MaxChars + 9).toLong
    second.truncated shouldBe true

    third.raw shouldBe short
    third.chars shouldBe 10L
    third.truncated shouldBe false
    // No half of a character is shown: half of one is not written as UTF-8, nor read back.
    Seq(first, second, third).foreach { record =>
      new String(record.raw.getBytes(UTF_8), UTF_8) shouldBe record.raw
      record.raw.exists(_.isSurrogate) shouldBe record.raw.contains(emoji)
    }
    // Counted, they are three records, whatever their size.
    reader.counts(orders.id).value.records shouldBe 3L
  }
}
