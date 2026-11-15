package lakehouse.ingestion.logstore.storage

import java.net.URI

import lakehouse.ingestion.logstore.UnsupportedTableLocationException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.http.HttpFileSystem
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, RawLocalFileSystem}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Which file systems the LogStore has a storage for: fail closed on every other. */
final class LogStorageSpec extends AnyFunSuite with Matchers {

  import LogStorageSpec._

  private val conf = new Configuration()

  test("Hadoop's local file system, checksummed or raw, gets the local storage") {
    val checksummed = FileSystem.getLocal(conf)
    checksummed.getClass shouldBe classOf[LocalFileSystem]

    LogStorage.of(checksummed) shouldBe LocalLogStorage.shared
    // What `fs.file.impl` set to RawLocalFileSystem gives.
    LogStorage.of(checksummed.getRawFileSystem) shouldBe LocalLogStorage.shared
  }

  test("file: served by a replacement of Hadoop's local file system is refused") {
    val replaced = conf(classOf[RenamingElsewhere])
    val asConfigured = FileSystem.newInstance(URI.create("file:///"), replaced)
    val overAnotherDisk = new LocalFileSystem(new AnotherDisk)
    overAnotherDisk.initialize(URI.create("file:///"), conf)

    try
      Seq(asConfigured -> classOf[RenamingElsewhere], overAnotherDisk -> classOf[LocalFileSystem])
        .foreach { case (fs, served) =>
          fs.getUri.getScheme shouldBe "file"
          val refused = the[UnsupportedTableLocationException] thrownBy LogStorage.of(fs)
          refused.getMessage should include(served.getName)
        }
    finally {
      asConfigured.close()
      overAnotherDisk.close()
    }
  }

  test("another scheme has no storage") {
    val remote = new HttpFileSystem
    remote.initialize(URI.create("http://lake.invalid/"), conf)

    val refused = the[UnsupportedTableLocationException] thrownBy LogStorage.of(remote)

    refused.getMessage should include("http")
  }

  private def conf(fileSystem: Class[_ <: FileSystem]): Configuration = {
    val replaced = new Configuration(conf)
    replaced.set("fs.file.impl", fileSystem.getName)
    replaced
  }
}

object LogStorageSpec {

  /** What `fs.file.impl` can put in the place of Hadoop's file system: here, a subclass of it. */
  final class RenamingElsewhere extends LocalFileSystem

  /** Hadoop's checksummed file system, over a disk that is not Hadoop's. */
  final class AnotherDisk extends RawLocalFileSystem
}
