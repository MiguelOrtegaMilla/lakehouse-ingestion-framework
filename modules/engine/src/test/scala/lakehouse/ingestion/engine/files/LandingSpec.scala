package lakehouse.ingestion.engine.files

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.Comparator

import scala.util.Using

import lakehouse.ingestion.core.config.{FileConnectorConfig, RelativePath, RootUri}
import lakehouse.ingestion.core.control.FailureClass
import lakehouse.ingestion.core.files.{FilePlanner, Listed}
import lakehouse.ingestion.engine.RunFailedException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterEach, EitherValues}

/**
 * The listing of the directory of a file source, confined to the landing zone (ADR-0013 §2), on
 * a real file system with real symbolic links.
 */
final class LandingSpec
    extends AnyFunSuite
    with Matchers
    with EitherValues
    with BeforeAndAfterEach {

  private var scratch: Path = _

  /** The landing root, `<scratch>/landing`, next to `<scratch>/outside`. */
  private def landing: Path = scratch.resolve("landing")
  private def outside: Path = scratch.resolve("outside")

  override def beforeEach(): Unit = {
    // The real path: on some systems the temporary directory is itself under a link.
    scratch = Files.createTempDirectory("landing-spec-").toRealPath()
    Files.createDirectories(landing.resolve("shop/orders"))
    Files.createDirectories(outside)
    ()
  }

  override def afterEach(): Unit =
    Using.resource(Files.walk(scratch)) { paths =>
      paths.sorted(Comparator.reverseOrder[Path]()).forEach(path => Files.delete(path))
    }

  private def rootAt(path: Path): RootUri = RootUri.parse(path.toUri.toString).value

  private def config(
      path: String = "shop/orders",
      pattern: Option[String] = None
  ): FileConnectorConfig =
    FileConnectorConfig(
      RelativePath.parse(path).value,
      FileConnectorConfig.Format.Csv,
      pattern.map(FileConnectorConfig.Pattern.parse(_).value)
    )

  private def list(cfg: FileConnectorConfig = config(), root: Path = landing): Vector[Listed] =
    new Landing(rootAt(root)).list(cfg)

  private val changed = Instant.parse("2026-11-19T08:30:15.123Z")

  /** A file of `bytes` bytes, last changed at `at`. */
  private def write(path: Path, bytes: Int = 3, at: Instant = changed): Path = {
    Files.createDirectories(path.getParent)
    Files.write(path, ("x" * bytes).getBytes(UTF_8))
    Files.setLastModifiedTime(path, FileTime.from(at))
    path
  }

  private def orders: Path = landing.resolve("shop/orders")

  private def refused(body: => Any): RunFailedException = {
    val failure = the[RunFailedException] thrownBy body
    failure.failure shouldBe FailureClass.Permanent
    failure
  }

  // ---------------------------------------------------------------------------------------------
  // What is listed
  // ---------------------------------------------------------------------------------------------

  test("the files of the directory, with their size and their time to the millisecond") {
    write(orders.resolve("a.csv"), bytes = 5)
    // A file system keeps the time to the nanosecond, and a listing gives it to the millisecond,
    // as Hadoop does and as the control plane records it.
    write(orders.resolve("b.csv"), bytes = 0, at = Instant.parse("2026-11-19T08:30:15.123456789Z"))

    list().sortBy(_.name) shouldBe Vector(
      Listed("a.csv", 5, changed),
      Listed("b.csv", 0, changed)
    )
  }

  test("not the hidden files, nor those of another pattern, nor what is under a directory") {
    write(orders.resolve("a.csv"))
    write(orders.resolve("b.json"))
    write(orders.resolve(".a.csv.tmp"))
    write(orders.resolve("_SUCCESS"))
    write(orders.resolve("old/c.csv"))
    // A directory of the pattern's name, as Spark writes one, is not a file.
    write(orders.resolve("d.csv/part-0.csv"))

    list(config(pattern = Some("*.csv"))).map(_.name) shouldBe Vector("a.csv")
  }

  test("an empty directory lists nothing") {
    list() shouldBe empty
  }

  test("a name that no run can read is listed as it is, for the planner to refuse") {
    write(orders.resolve("a[1].csv"))

    val listed = list()
    listed.map(_.name) shouldBe Vector("a[1].csv")
    FilePlanner
      .plan(config(), listed, Set.empty, changed.plusSeconds(3600))
      .left
      .value
      .head should include("has '[' (U+005B)")
  }

  // ---------------------------------------------------------------------------------------------
  // Symbolic links inside the landing zone
  // ---------------------------------------------------------------------------------------------

  test("a link to a file inside the landing zone is listed, with what the read will read") {
    val elsewhere = write(landing.resolve("shared/2026-11-19.csv"), bytes = 7)
    Files.createSymbolicLink(orders.resolve("today.csv"), elsewhere)

    list() shouldBe Vector(Listed("today.csv", 7, changed))
  }

  test("a relative link is resolved from where it is") {
    write(landing.resolve("shared/a.csv"), bytes = 2)
    Files.createSymbolicLink(
      orders.resolve("a.csv"),
      orders.relativize(landing.resolve("shared/a.csv"))
    )

    list() shouldBe Vector(Listed("a.csv", 2, changed))
  }

  test("a source directory that is a link to another directory of the landing zone") {
    write(landing.resolve("drop/a.csv"))
    Files.delete(orders)
    Files.createSymbolicLink(orders, landing.resolve("drop"))

    list().map(_.name) shouldBe Vector("a.csv")
  }

  test("a landing root that is a link: files are under where it leads") {
    write(orders.resolve("a.csv"))
    val mounted = Files.createSymbolicLink(scratch.resolve("mounted"), landing)

    list(root = mounted).map(_.name) shouldBe Vector("a.csv")
  }

  // ---------------------------------------------------------------------------------------------
  // What leads out (ADR-0013 §2)
  // ---------------------------------------------------------------------------------------------

  test("a link to a file outside the landing zone fails the run, and names the file") {
    write(orders.resolve("a.csv"))
    Files.createSymbolicLink(orders.resolve("passwd.csv"), write(outside.resolve("passwd")))

    val failure = refused(list())
    failure.getMessage should include("shop/orders")
    failure.getMessage should include("'passwd.csv' leads out of the landing zone")
    // Where it leads is not said: it is somewhere the source has no business with.
    failure.getMessage should not include outside.toString
  }

  test("a link to a system file outside the landing zone") {
    Files.createSymbolicLink(orders.resolve("passwd.csv"), Path.of("/etc/passwd"))

    refused(list()).getMessage should include("'passwd.csv' leads out of the landing zone")
  }

  test("a link out through a chain of links that each look inside") {
    Files.createSymbolicLink(landing.resolve("hop"), write(outside.resolve("x.csv")))
    Files.createSymbolicLink(orders.resolve("x.csv"), landing.resolve("hop"))

    refused(list()).getMessage should include("'x.csv' leads out of the landing zone")
  }

  test("a link to a place that starts with the name of the root is not under it") {
    // `landing2` starts with the text `landing`, and is not in it: paths are compared by their
    // segments.
    Files.createSymbolicLink(orders.resolve("a.csv"), write(scratch.resolve("landing2/a.csv")))

    refused(list()).getMessage should include("'a.csv' leads out of the landing zone")
  }

  test("a link that goes up with .. and out") {
    write(outside.resolve("a.csv"))
    Files.createSymbolicLink(orders.resolve("a.csv"), Path.of("../../../outside/a.csv"))

    refused(list()).getMessage should include("'a.csv' leads out of the landing zone")
  }

  test("a source directory that is a link out of the landing zone reads nothing") {
    write(outside.resolve("a.csv"))
    Files.delete(orders)
    Files.createSymbolicLink(orders, outside)

    refused(list()).getMessage should include(
      "the directory shop/orders of the landing zone leads out of it"
    )
  }

  test("a directory above the source's that is a link out of the landing zone") {
    write(outside.resolve("orders/a.csv"))
    Files.delete(orders)
    Files.delete(landing.resolve("shop"))
    Files.createSymbolicLink(landing.resolve("shop"), outside)

    refused(list()).getMessage should include("leads out of it")
  }

  test("a link to a directory outside the landing zone is not listed, as no directory is") {
    write(outside.resolve("a.csv"))
    Files.createSymbolicLink(orders.resolve("more.csv"), outside)
    write(orders.resolve("b.csv"))

    list().map(_.name) shouldBe Vector("b.csv")
  }

  test("a link out that is hidden, or of another pattern, is not the source's") {
    write(orders.resolve("a.csv"))
    Files.createSymbolicLink(orders.resolve(".passwd"), Path.of("/etc/passwd"))
    Files.createSymbolicLink(orders.resolve("passwd"), Path.of("/etc/passwd"))

    list(config(pattern = Some("*.csv"))).map(_.name) shouldBe Vector("a.csv")
  }

  test("every file that may not be read is named, and the first few of many") {
    Files.createSymbolicLink(orders.resolve("b.csv"), write(outside.resolve("b")))
    Files.createSymbolicLink(orders.resolve("a.csv"), write(outside.resolve("a")))

    val message = refused(list()).getMessage
    message should include("has 2 files that no run may read")
    message should include(
      "'a.csv' leads out of the landing zone, through a symbolic link; " +
        "'b.csv' leads out"
    )

    (1 to Landing.Named + 3).foreach { i =>
      Files.createSymbolicLink(orders.resolve(f"c$i%02d.csv"), outside.resolve("a"))
    }
    val many = refused(list()).getMessage
    many should include(s"has ${Landing.Named + 5} files")
    many should endWith(s"and 5 more")
  }

  // ---------------------------------------------------------------------------------------------
  // What is not a file
  // ---------------------------------------------------------------------------------------------

  test("a link to nothing fails the run: its file is gone, or never was") {
    Files.createSymbolicLink(orders.resolve("a.csv"), landing.resolve("gone.csv"))

    refused(list()).getMessage should include("'a.csv' is a symbolic link to nothing")
  }

  test("a loop of links fails the run, by its name, and not as a failure of the file system") {
    Files.createSymbolicLink(orders.resolve("a.csv"), orders.resolve("b.csv"))
    Files.createSymbolicLink(orders.resolve("b.csv"), orders.resolve("a.csv"))

    val message = refused(list()).getMessage
    message should include("'a.csv' is a symbolic link that cannot be followed: Too many levels")
    message should include("'b.csv' is a symbolic link that cannot be followed")
  }

  test("a source directory that is a loop of links") {
    Files.delete(orders)
    Files.createSymbolicLink(orders, orders)

    refused(list()).getMessage should include(
      "the directory shop/orders of the landing zone is a symbolic link that cannot be followed"
    )
  }

  test("a pipe fails the run: a read of it would wait for its writer") {
    val pipe = orders.resolve("a.csv")
    new ProcessBuilder("mkfifo", pipe.toString).inheritIO().start().waitFor() shouldBe 0

    refused(list()).getMessage should include("'a.csv' is not a file but a pipe")
  }

  test("a link inside the landing zone to a pipe") {
    val pipe = landing.resolve("pipe")
    new ProcessBuilder("mkfifo", pipe.toString).inheritIO().start().waitFor() shouldBe 0
    Files.createSymbolicLink(orders.resolve("a.csv"), pipe)

    refused(list()).getMessage should include("'a.csv' is not a file but a pipe")
  }

  // ---------------------------------------------------------------------------------------------
  // The directory and the root
  // ---------------------------------------------------------------------------------------------

  test("a directory that is not there") {
    refused(list(config(path = "shop/returns"))).getMessage should include(
      "the directory shop/returns of the landing zone is not there"
    )
  }

  test("a directory that is a file") {
    write(landing.resolve("shop/returns"))

    refused(list(config(path = "shop/returns"))).getMessage should include("is not a directory")
  }

  test("a root that is not there") {
    refused(list(root = scratch.resolve("unmounted"))).getMessage should include(
      "the root of the landing zone"
    )
  }

  test("a root on object storage is not read by this deployment") {
    val s3 = RootUri.parse("s3a://landing-bucket/zone").value

    refused(new Landing(s3).list(config())).getMessage should include("on file:// alone")
  }
}
