package lakehouse.ingestion.core.files

import java.time.Instant

import scala.concurrent.duration._
import scala.util.Random

import io.circe.Json
import lakehouse.ingestion.core.config.{FileConnectorConfig, RelativePath, SafeYaml}
import lakehouse.ingestion.core.files.FilePlanner.{NothingNew, Planned}
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The plan of a batch of files: which files a run takes, from what a directory lists, what was
 * processed and the time, and the plan as it is written down and read back (ADR-0005 §3).
 */
final class FilePlanSpec extends AnyFunSuite with Matchers with EitherValues {

  private val Now = Instant.parse("2026-11-18T12:00:00Z")
  private val Directory = RelativePath.parse("sales/returns").value

  private def config(
      pattern: Option[String] = None,
      minFileAge: FiniteDuration = 1.minute,
      maxFiles: Int = 1000
  ): FileConnectorConfig =
    FileConnectorConfig(
      Directory,
      FileConnectorConfig.Format.Csv,
      pattern.map(FileConnectorConfig.Pattern.parse(_).value),
      minFileAge,
      maxFiles
    )

  /** A file changed `age` before `Now`. */
  private def listed(name: String, size: Long = 100, age: FiniteDuration = 1.hour): Listed =
    Listed(name, size, Now.minusMillis(age.toMillis))

  private def landed(file: Listed): LandedFile =
    LandedFile(FileName.parse(file.name).value, file.size, file.modifiedAt)

  private def plan(
      listing: Seq[Listed],
      processed: Set[LandedFile] = Set.empty,
      under: FileConnectorConfig = config()
  ): FilePlanner.Planning = FilePlanner.plan(under, listing, processed, Now).value

  private def planned(planning: FilePlanner.Planning): Planned = planning match {
    case done: Planned => done
    case other => fail(s"no batch was planned: $other")
  }

  private def taken(planning: FilePlanner.Planning): Vector[String] = planning match {
    case Planned(plan, _, _) => plan.names.map(_.value)
    case NothingNew(_) => Vector.empty
  }

  // ---------------------------------------------------------------------------------------------
  // The pattern of a source
  // ---------------------------------------------------------------------------------------------

  private def matches(pattern: String, name: String): Boolean =
    FileConnectorConfig.Pattern.parse(pattern).value.matches(name)

  test("a pattern matches a whole name, with * for any run of characters and ? for one") {
    val cases = Seq(
      ("*.csv", "returns.csv", true),
      ("*.csv", "returns.csv.tmp", false),
      ("*.csv", "returns.CSV", false),
      ("returns-*.csv", "returns-.csv", true),
      ("returns-*.csv", "returns-2026-11-18.csv", true),
      ("returns-*.csv", "xreturns-1.csv", false),
      ("part-????.json", "part-0001.json", true),
      ("part-????.json", "part-001.json", false),
      ("part-????.json", "part-00001.json", false),
      ("*", "anything at all", true),
      ("a*b*c", "aXXbYYc", true),
      ("a*b*c", "abcb", false),
      ("a*b", "abab", true),
      ("*a*", "banana", true),
      ("data.parquet", "data.parquet", true),
      ("data.parquet", "data.parque", false),
      // A character is a code point: one ? for a letter of two bytes, or of two UTF-16 units.
      ("?.csv", "ñ.csv", true),
      ("?.csv", "📦.csv", true),
      ("??.csv", "📦.csv", false)
    )
    cases.foreach { case (pattern, name, expected) =>
      withClue(s"$pattern ~ $name")(matches(pattern, name) shouldBe expected)
    }
  }

  test("a pattern takes time of the name times the pattern, whatever the name is") {
    val started = System.nanoTime()
    matches("*a*a*a*a*a*a*a*a*b", "a" * 255) shouldBe false
    (System.nanoTime() - started).nanos should be < 1.second
  }

  // ---------------------------------------------------------------------------------------------
  // The name of a file
  // ---------------------------------------------------------------------------------------------

  test("a name is the producer's, unusual or not") {
    Seq(
      "returns.csv",
      "returns 2026-11-18.csv",
      "ventas_año.csv",
      "数据.json",
      "a%20b.csv",
      "a,b;c.csv",
      "a#1 (copy).csv",
      "x" * 255,
      "ñ" * 127 // 254 bytes
    ).foreach(name => withClue(name)(FileName.parse(name).value.value shouldBe name))
  }

  test("a name that would be read as something else, or that hides what it says, is refused") {
    val refused = Seq(
      "" -> "is empty",
      "." -> "is a directory, its own or the one above it",
      ".." -> "is a directory, its own or the one above it",
      ".returns.csv.part" -> "starts with '.': such names are hidden to Spark and Delta",
      "_SUCCESS" -> "starts with '_': such names are hidden to Spark and Delta",
      "a/b.csv" -> "has '/' (U+002F) at position 2: a name is in its directory, and a source lists one directory",
      "c:b.csv" -> "has ':' (U+003A) at position 2: Hadoop reads a name with it as a URI with a scheme",
      "a*.csv" -> "has '*' (U+002A) at position 2: Spark reads a path with it as a pattern, and would read other files",
      "a?.csv" -> "has '?' (U+003F) at position 2: Spark reads a path with it as a pattern, and would read other files",
      "a[1].csv" -> "has '[' (U+005B) at position 2: Spark reads a path with it as a pattern, and would read other files",
      "a]1.csv" -> "has ']' (U+005D) at position 2: Spark reads a path with it as a pattern, and would read other files",
      "a{b}.csv" -> "has '{' (U+007B) at position 2: Spark reads a path with it as a pattern, and would read other files",
      "a}.csv" -> "has '}' (U+007D) at position 2: Spark reads a path with it as a pattern, and would read other files",
      "a\\b.csv" -> "has '\\' (U+005C) at position 2: Spark reads a path with it as a pattern, and would read other files",
      "a\u0000.csv" -> "has U+0000 NULL at position 2: it is a character that a terminal obeys, or that hides what is written",
      "a\n.csv" -> "has U+000A LINE FEED (LF) at position 2: it is a character that a terminal obeys, or that hides what is written",
      "a\u001b[31m.csv" -> "has U+001B ESCAPE at position 2: it is a character that a terminal obeys, or that hides what is written",
      "a\u0085.csv" -> "has U+0085 NEXT LINE (NEL) at position 2: it is a character that a terminal obeys, or that hides what is written",
      "vsc.\u202Eexe" -> "has U+202E RIGHT-TO-LEFT OVERRIDE at position 5: it is a character that a terminal obeys, or that hides what is written",
      "a\u200Bb.csv" -> "has U+200B ZERO WIDTH SPACE at position 2: it is a character that a terminal obeys, or that hides what is written",
      "a\u2028.csv" -> "has U+2028 LINE SEPARATOR at position 2: it is a character that a terminal obeys, or that hides what is written",
      "a\uD800.csv" -> "has U+D800 HIGH SURROGATES D800 at position 2: it is a character that a terminal obeys, or that hides what is written",
      "x" * 256 -> "is 256 bytes long, and a name is 255 at most",
      "ñ" * 128 -> "is 256 bytes long, and a name is 255 at most"
    )
    refused.foreach { case (name, problem) =>
      withClue(name.codePoints.toArray.take(8).map(c => f"U+$c%04X").mkString(" "))(
        FileName.parse(name).left.value shouldBe problem
      )
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Which files a batch takes
  // ---------------------------------------------------------------------------------------------

  test("a batch takes the files that are new and complete, oldest first") {
    val listing = Seq(
      listed("b.csv", age = 2.hours),
      listed("c.csv", age = 3.hours),
      listed("a.csv", age = 1.hour),
      listed("young.csv", age = 10.seconds)
    )

    val Planned(filePlan, waiting, leftOver) = planned(plan(listing))
    filePlan.directory shouldBe Directory
    filePlan.names.map(_.value) shouldBe Vector("c.csv", "b.csv", "a.csv")
    filePlan.files shouldBe Vector(listing(1), listing(0), listing(2)).map(landed)
    filePlan.bytes shouldBe 300
    waiting shouldBe 1
    leftOver shouldBe 0
  }

  test("files of one moment are taken by their names, and no listing changes the plan") {
    val sameTime = (1 to 30).map(i => listed(f"f$i%02d.csv", age = 1.hour))
    val older = (1 to 30).map(i => listed(f"o$i%02d.csv", age = 2.hours + i.seconds))
    val listing = sameTime ++ older
    val expected = older.reverse.map(_.name) ++ sameTime.map(_.name)

    taken(plan(listing)) shouldBe expected
    (1 to 20).foreach { seed =>
      taken(plan(new Random(seed).shuffle(listing))) shouldBe expected
    }
  }

  test("a file is complete once it is min_file_age old, to the millisecond") {
    val atTheAge = Listed("ready.csv", 1, Now.minusMillis(60000))
    val justYounger = Listed("almost.csv", 1, Now.minusMillis(59999))
    // A clock that is behind the file system's makes a file from the future: it waits.
    val fromTheFuture = Listed("ahead.csv", 1, Now.plusMillis(5000))

    val Planned(filePlan, waiting, _) = planned(plan(Seq(atTheAge, justYounger, fromTheFuture)))
    filePlan.names.map(_.value) shouldBe Vector("ready.csv")
    waiting shouldBe 2

    // With 0s, every file is complete the moment it is listed.
    taken(plan(Seq(justYounger, atTheAge), under = config(minFileAge = Duration.Zero))) shouldBe
      Vector("ready.csv", "almost.csv")
  }

  test("a file that a batch took is not taken again, and one written again under its name is") {
    val first = Seq(listed("a.csv", age = 3.hours), listed("b.csv", age = 2.hours))
    val processed = first.map(landed).toSet

    plan(first, processed) shouldBe NothingNew(waiting = 0)

    val rewritten = Seq(listed("a.csv", size = 101, age = 30.minutes), first(1))
    taken(plan(rewritten, processed)) shouldBe Vector("a.csv")
    val touched = Seq(listed("a.csv", age = 30.minutes), first(1))
    taken(plan(touched, processed)) shouldBe Vector("a.csv")
  }

  test("a batch takes max_files_per_batch at most, and the next takes the rest") {
    val listing = (1 to 7).map(i => listed(s"f$i.csv", age = (10 - i).minutes))
    val withThree = config(maxFiles = 3)

    val first = planned(plan(listing, under = withThree))
    first.plan.names.map(_.value) shouldBe Vector("f1.csv", "f2.csv", "f3.csv")
    (first.waiting, first.leftOver) shouldBe ((0, 4))

    val second = planned(plan(listing, first.plan.files.toSet, under = withThree))
    (second.waiting, second.leftOver) shouldBe ((0, 1))
    val secondPlan = second.plan
    secondPlan.names.map(_.value) shouldBe Vector("f4.csv", "f5.csv", "f6.csv")

    val third = plan(listing, (first.plan.files ++ secondPlan.files).toSet, under = withThree)
    taken(third) shouldBe Vector("f7.csv")
  }

  test("hidden files and files that the pattern does not match are never the source's") {
    val listing = Seq(
      listed("returns-1.csv"),
      listed("returns-2.csv.tmp"),
      listed("orders-1.csv"),
      listed(".returns-3.csv"),
      listed("_SUCCESS"),
      listed("_returns-4.csv")
    )

    taken(plan(listing, under = config(pattern = Some("returns-*.csv")))) shouldBe
      Vector("returns-1.csv")
    taken(plan(listing)) shouldBe Vector("orders-1.csv", "returns-1.csv", "returns-2.csv.tmp")
  }

  test("nothing new is no batch, and says how many files are still being written") {
    plan(Nil) shouldBe NothingNew(waiting = 0)
    plan(Seq(listed("a.csv", age = 5.seconds), listed("b.csv", age = 1.second))) shouldBe
      NothingNew(waiting = 2)
  }

  test("a file of the source that no run can read is said, every one, and never passed over") {
    val listing = Seq(
      listed("good.csv", age = 3.hours),
      listed("bad[1].csv", size = 7, age = 2.hours),
      listed("worse\u001b.csv", size = 9, age = 1.hour),
      // Not the source's: its name is not looked at.
      listed("other[1].txt")
    )

    FilePlanner
      .plan(config(pattern = Some("*.csv")), listing, Set.empty, Now)
      .left
      .value
      .toChain
      .toVector shouldBe Vector(
      "the file of sales/returns that is number 2 of those it would take, oldest first (of 7 " +
        "bytes, changed at 2026-11-18T10:00:00Z), has a name that has '[' (U+005B) at " +
        "position 4: Spark reads a path with it as a pattern, and would read other files",
      "the file of sales/returns that is number 3 of those it would take, oldest first (of 9 " +
        "bytes, changed at 2026-11-18T11:00:00Z), has a name that has U+001B ESCAPE at " +
        "position 6: it is a character that a terminal obeys, or that hides what is written"
    )
  }

  test("a listing has each name once, and a time to the millisecond") {
    an[IllegalArgumentException] should be thrownBy
      FilePlanner.plan(config(), Seq(listed("a.csv"), listed("a.csv")), Set.empty, Now)
    an[IllegalArgumentException] should be thrownBy Listed("a.csv", 1, Now.plusNanos(1))
    an[IllegalArgumentException] should be thrownBy Listed("a.csv", -1, Now)
  }

  // ---------------------------------------------------------------------------------------------
  // The plan as it is written down
  // ---------------------------------------------------------------------------------------------

  private val Written = FilePlan(
    Directory,
    Vector(
      LandedFile(FileName.parse("returns 1.csv").value, 0, Instant.parse("2026-11-18T10:00:00Z")),
      LandedFile(
        FileName.parse("devoluciones-año.csv").value,
        123456789012L,
        Instant.parse("2026-11-18T10:00:00.123Z")
      )
    )
  )

  test("a plan is written in one form, which changes only with its version") {
    Written.toJson.noSpaces shouldBe
      """{"version":1,"directory":"sales/returns","files":[""" +
      """{"name":"returns 1.csv","size":0,"modified_at":"2026-11-18T10:00:00Z"},""" +
      """{"name":"devoluciones-año.csv","size":123456789012,"modified_at":"2026-11-18T10:00:00.123Z"}]}"""
  }

  test("a plan is read back as it was written, from its text as well") {
    FilePlan.fromJson(Written.toJson).value shouldBe Written
    val text = Written.toJson.noSpaces
    FilePlan.fromJson(SafeYaml.parse(text).value).value shouldBe Written
  }

  test("a plan of the largest batch, with the longest names, is read back") {
    val files = (1 to FileConnectorConfig.MaxFilesPerBatch).toVector.map { i =>
      LandedFile(
        FileName.parse(f"$i%05d" + "ñ" * 125).value, // 255 bytes
        Long.MaxValue,
        Instant.parse("2026-11-18T10:00:00.123Z")
      )
    }
    val largest = FilePlan(Directory, files)
    val text = largest.toJson.noSpaces
    // What the control plane stores of a plan (StoredJson.Limits, in `engine`).
    val stored = SafeYaml.Limits(maxCodePoints = 8 * 1024 * 1024, maxDepth = 32, maxAliases = 0)
    text.codePointCount(0, text.length) should be < stored.maxCodePoints / 2
    FilePlan.fromJson(SafeYaml.parse(text, stored).value).value shouldBe largest
  }

  test("a plan that is not one this application wrote is refused, and says why") {
    def without(field: String): Json = Written.toJson.mapObject(_.remove(field))
    val firstFile = Written.toJson.hcursor.downField("files").downArray.focus.get
    def withFile(change: Json => Json): Json =
      Written.toJson.mapObject(_.add("files", Json.arr(change(firstFile))))
    def refusal(json: Json): String = FilePlan.fromJson(json).left.value

    refusal(Json.fromString("a plan")) shouldBe "is not an object"
    refusal(Written.toJson.mapObject(_.add("version", Json.fromInt(2)))) shouldBe
      "is of version 2, and this application reads version 1"
    refusal(without("directory")) shouldBe "has no directory"
    refusal(Written.toJson.mapObject(_.add("extra", Json.True))) shouldBe "has a field extra"
    refusal(Written.toJson.mapObject(_.add("directory", Json.fromString("../x")))) shouldBe
      "directory has the segment '..': a path does not climb out of its root"
    refusal(Written.toJson.mapObject(_.add("files", Json.arr()))) shouldBe
      "a plan has a file at least"
    refusal(
      Written.toJson.mapObject(_.add("files", Json.arr(firstFile, firstFile)))
    ) shouldBe "a plan has each name once"

    refusal(withFile(_.mapObject(_.add("name", Json.fromString("a/b"))))) shouldBe
      "files[0].name has '/' (U+002F) at position 2: a name is in its directory, and a source lists one directory"
    refusal(withFile(_.mapObject(_.add("size", Json.fromInt(-1))))) shouldBe
      "files[0].size is not a size"
    refusal(withFile(_.mapObject(_.add("size", Json.fromString("1"))))) shouldBe
      "files[0].size is not a size"
    refusal(withFile(_.mapObject(_.add("modified_at", Json.fromString("yesterday"))))) shouldBe
      "files[0].modified_at is not a time"
    refusal(
      withFile(_.mapObject(_.add("modified_at", Json.fromString("2026-11-18T10:00:00.000001Z"))))
    ) shouldBe "files[0].modified_at is not to the millisecond"
    refusal(withFile(_.mapObject(_.remove("size")))) shouldBe "files[0] has no size"
    // A number written as text is not one, though circe's decoders would take it.
    refusal(Written.toJson.mapObject(_.add("version", Json.fromString("1")))) shouldBe
      "version is not a whole number"
  }
}
