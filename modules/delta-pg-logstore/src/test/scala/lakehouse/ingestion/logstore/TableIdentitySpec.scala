package lakehouse.ingestion.logstore

import java.io.IOException
import java.net.URI
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files, Path => LocalPath, Paths}
import java.util.Comparator

import scala.util.Using

import lakehouse.ingestion.logstore.coordination.TableUri
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.http.HttpFileSystem
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * One table, one key (ADR-0004 §3.6): every way of naming a table's directory on `file:` gives
 * the key of its real path, and a location that has no single key is refused.
 *
 * The directories and the symbolic links are real ones, in a temporary directory per test.
 */
final class TableIdentitySpec extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  private val fs = FileSystem.getLocal(new Configuration())
  private var roots = List.empty[LocalPath]

  override def afterEach(): Unit =
    try roots.foreach(deleteTree)
    finally {
      roots = Nil
      super.afterEach()
    }

  /** A directory of this test's own, by its real path: the temporary directory may be a link. */
  private def newRoot(): LocalPath = {
    val root = Files.createTempDirectory("table-identity-").toRealPath()
    roots ::= root
    root
  }

  /** Deletes links as links: the walk does not follow them. */
  private def deleteTree(root: LocalPath): Unit =
    Using.resource(Files.walk(root)) {
      _.sorted(Comparator.reverseOrder[LocalPath]()).forEach(path => Files.delete(path))
    }

  private def keyOf(spelling: String): TableUri = TableIdentity.of(new Path(spelling), fs)
  private def keyOf(path: LocalPath): TableUri = keyOf(path.toString)

  test("every spelling of a table's path gives one key: the qualified form of its directory") {
    val root = newRoot()
    val table = Files.createDirectories(root.resolve("lake/orders"))

    val spellings = Seq(
      s"$table",
      s"file:$table",
      s"file://$table",
      s"$table/",
      s"file://$table/",
      s"$root//lake///orders",
      s"$root/lake/./orders/.",
      s"$root/lake/../lake/orders",
      // Hadoop removes `..` from the text of a path: `absent` is never looked up.
      s"file:$root/absent/../lake/orders"
    )

    spellings.foreach(spelling =>
      withClue(s"'$spelling': ")(keyOf(spelling).value shouldBe s"file:$table")
    )
    spellings.map(keyOf).toSet should have size 1
    // What Hadoop, and therefore Delta, calls the directory.
    keyOf(table).value shouldBe fs.makeQualified(new Path(table.toString)).toString
  }

  test("a relative path is read from the working directory of its file system") {
    val root = newRoot()
    val table = Files.createDirectories(root.resolve("lake/orders"))
    // An instance of its own: the working directory of the shared one is left alone.
    val inLake = FileSystem.newInstanceLocal(new Configuration())
    try {
      inLake.setWorkingDirectory(new Path(root.resolve("lake").toString))

      TableIdentity.of(new Path("orders"), inLake) shouldBe keyOf(table)
      TableIdentity.of(new Path("../lake/orders"), inLake) shouldBe keyOf(table)
    } finally inLake.close()
  }

  test("a symbolic link anywhere in the path gives the key of the real directory") {
    val root = newRoot()
    val table = Files.createDirectories(root.resolve("lake/orders"))
    val toTable = Files.createSymbolicLink(root.resolve("orders-link"), table)
    val toParent = Files.createSymbolicLink(root.resolve("lake-link"), root.resolve("lake"))
    val toLink = Files.createSymbolicLink(root.resolve("link-to-link"), toTable)
    val relative = Files.createSymbolicLink(root.resolve("lake/relative-link"), Paths.get("orders"))

    Seq(toTable, toParent.resolve("orders"), toLink, relative).foreach { path =>
      withClue(s"$path: ")(keyOf(path).value shouldBe s"file:$table")
    }
  }

  test("a table that does not exist yet has its key already, and keeps it once created") {
    val root = newRoot()
    val lake = Files.createDirectory(root.resolve("lake"))
    val link = Files.createSymbolicLink(root.resolve("lake-link"), lake)
    // Neither `bronze` nor `orders` exists: the link above them is still resolved.
    val planned = link.resolve("bronze/orders")

    val before = keyOf(planned)
    before.value shouldBe s"file:$lake/bronze/orders"

    Files.createDirectories(lake.resolve("bronze/orders/_delta_log"))
    keyOf(planned) shouldBe before
    keyOf(lake.resolve("bronze/orders")) shouldBe before
  }

  test("the key names the directory exactly, whatever characters its name has") {
    val root = newRoot()
    val names =
      Seq("with space", "with%20space", "100%", "hash#tag", "what?", "a:b", "ñandú", "plus+sign")

    val keys = names.map { name =>
      val table = Files.createDirectory(root.resolve(name))
      val key = keyOf(table)
      withClue(s"'$name': ") {
        key.value shouldBe s"file:$table"
        // Read back as a Hadoop path, the key is the directory again.
        new Path(key.value).toUri.getPath shouldBe table.toString
      }
      key
    }

    keys.distinct should have size names.size.toLong
  }

  test("a table whose _delta_log is a symbolic link is refused: two tables would share a log") {
    val root = newRoot()
    val orders = Files.createDirectories(root.resolve("orders/_delta_log")).getParent
    val shadow = Files.createDirectory(root.resolve("shadow"))
    Files.createSymbolicLink(shadow.resolve("_delta_log"), orders.resolve("_delta_log"))
    val toShadow = Files.createSymbolicLink(root.resolve("shadow-link"), shadow)

    Seq(shadow, toShadow).foreach { path =>
      val refused = the[UnsupportedTableLocationException] thrownBy keyOf(path)
      // The location as the caller named it.
      refused.getMessage should include(s"file:$path")
      refused.getMessage should include("_delta_log is a symbolic link")
    }
    keyOf(orders).value shouldBe s"file:$orders"
  }

  test("a symbolic link to a path that does not exist is refused, not kept as a name") {
    val root = newRoot()
    val target = root.resolve("mounted-later")
    val dangling = Files.createSymbolicLink(root.resolve("lake"), target)

    Seq(dangling, dangling.resolve("bronze/orders")).foreach { path =>
      val refused = the[UnsupportedTableLocationException] thrownBy keyOf(path)
      refused.getMessage should include(s"$dangling is a symbolic link")
    }

    // With the name kept, the key would have changed here.
    Files.createDirectory(target)
    keyOf(dangling.resolve("bronze/orders")).value shouldBe s"file:$target/bronze/orders"
  }

  test("a path that cannot be resolved is an error, never a guess") {
    val root = newRoot()

    val one = root.resolve("one")
    val other = Files.createSymbolicLink(root.resolve("other"), one)
    Files.createSymbolicLink(one, other)
    withClue("a loop of links: ")(an[IOException] should be thrownBy keyOf(one.resolve("orders")))

    val file = Files.createFile(root.resolve("a-file"))
    withClue("a file in the way: ")(
      an[IOException] should be thrownBy keyOf(file.resolve("orders"))
    )

    // A directory that may not be searched hides whether `lake` is a link.
    val locked = Files.createDirectories(root.resolve("locked/lake")).getParent
    Files.setPosixFilePermissions(locked, PosixFilePermissions.fromString("rw-------"))
    try {
      assume(!Files.isExecutable(locked), "this user may search any directory")
      withClue("no permission: ") {
        an[IOException] should be thrownBy keyOf(locked.resolve("lake/orders"))
      }
    } finally
      Files.setPosixFilePermissions(locked, PosixFilePermissions.fromString("rwx------")): Unit
  }

  test("a location on another scheme, or on a named host, is refused") {
    // A path of another file system than the one it is given with.
    Seq("s3a://lake/bronze/orders", "file://localhost/lake/bronze/orders").foreach { spelling =>
      val refused = the[UnsupportedTableLocationException] thrownBy keyOf(spelling)
      refused.getMessage should include(spelling)
    }

    // A plain path, on a file system that is not the local disk.
    val remote = new HttpFileSystem
    remote.initialize(URI.create("http://lake.invalid/"), new Configuration())
    val refused = the[UnsupportedTableLocationException] thrownBy
      TableIdentity.of(new Path("/bronze/orders"), remote)
    refused.getMessage should include("http://lake.invalid/bronze/orders")
  }
}
