package lakehouse.ingestion.it.config

import java.nio.file.{Files, Path => LocalPath}

import lakehouse.ingestion.core.config.{PathTableLocator, RelativePath, RootUri}
import lakehouse.ingestion.logstore.TableIdentity
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, EitherValues}

/**
 * `core` says where a table is, from the text of the configuration alone. The LogStore says
 * under which key the commits to it are coordinated, and for that it asks the file system
 * (`TableIdentity`, ADR-0004 §3.6). Neither module may depend on the other (ADR-0012), so
 * nothing but a test holds them to each other (ADR-0011, note of 2026-11-17).
 *
 * What must hold, for a lake without symbolic links: the URI that `core` gives a table is the
 * directory that the LogStore keys it by, one key for one table and two for two.
 *
 * And what `core` cannot know, and does not claim: with a symbolic link in the lake's root, the
 * key is that of the real directory. Two profiles that reach one lake by two names give their
 * tables two URIs and one key, and it is the key that keeps their commits apart.
 */
final class TableLocationAgreementSpec
    extends AnyFunSuite
    with Matchers
    with EitherValues
    with BeforeAndAfterAll {

  private val fs: FileSystem = FileSystem.getLocal(new Configuration())

  // The real path: on some systems the directory for temporary files is itself a link.
  private val work: LocalPath = Files.createTempDirectory("table-location-agreement").toRealPath()
  private val lake: LocalPath = Files.createDirectory(work.resolve("lake"))

  override def afterAll(): Unit = {
    def delete(path: LocalPath): Unit = {
      if (Files.isDirectory(path) && !Files.isSymbolicLink(path))
        Option(path.toFile.listFiles()).toSeq.flatten.foreach(child => delete(child.toPath))
      Files.deleteIfExists(path): Unit
    }
    delete(work)
  }

  private def locator(root: LocalPath): PathTableLocator =
    new PathTableLocator(RootUri.parse(s"file://$root").value)

  private def keyOf(uri: String): String = TableIdentity.of(new Path(uri), fs).value

  private val tables =
    Seq("bronze/sales/orders", "silver/sales/orders", "bronze/sales/orders2", "orders")
      .map(text => RelativePath.parse(text).value)

  test("the URI that core gives a table is the directory the LogStore keys it by") {
    tables.foreach { table =>
      withClue(table) {
        val location = locator(lake).locate(table)

        keyOf(location.uri) shouldBe s"file:$lake/$table"
        // The key is Hadoop's own spelling of the URI that core wrote, and of nothing else.
        keyOf(location.uri) shouldBe new Path(location.uri).toString
      }
    }
  }

  test("the key of a table is the same before the table exists and after") {
    val table = tables.head
    val location = locator(lake).locate(table)
    val before = keyOf(location.uri)

    Files.createDirectories(lake.resolve(table.value).resolve("_delta_log"))

    keyOf(location.uri) shouldBe before
  }

  test("one table has one key, and two tables have two") {
    val keys = tables.map(table => keyOf(locator(lake).locate(table).uri))
    keys.distinct should have size tables.size.toLong

    // However the lake was written in a profile.
    val slashed = new PathTableLocator(RootUri.parse(s"file://$lake/").value)
    tables.map(table => keyOf(slashed.locate(table).uri)) shouldBe keys
  }

  test("a link in the root of the lake is the LogStore's to see: two URIs, and one key") {
    val link = Files.createSymbolicLink(work.resolve("lake-by-another-name"), lake)
    val table = tables.head
    val direct = locator(lake).locate(table)
    val linked = locator(link).locate(table)

    // core reads text, and the two profiles wrote two texts.
    linked.uri should not be direct.uri
    // The LogStore asks the file system, and there is one directory.
    keyOf(linked.uri) shouldBe keyOf(direct.uri)
    keyOf(linked.uri) shouldBe s"file:$lake/$table"
  }
}
