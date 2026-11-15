package lakehouse.ingestion.logstore

import java.nio.file.{Files, Path => LocalPath}
import java.util.Comparator

import scala.util.Using

import org.scalatest.{BeforeAndAfterEach, Suite}

/**
 * Real directories for a suite's tests, under the JVM's temporary directory, deleted after each
 * test.
 */
trait ScratchDirectories extends BeforeAndAfterEach { this: Suite =>

  private var roots = List.empty[LocalPath]

  /** A directory of the running test's own, by its real path: the one above may be a link. */
  protected def newDirectory(): LocalPath = {
    val root = Files.createTempDirectory(s"$suiteName-").toRealPath()
    roots ::= root
    root
  }

  override def afterEach(): Unit =
    try roots.foreach(deleteTree)
    finally {
      roots = Nil
      super.afterEach()
    }

  /** Deletes links as links: the walk does not follow them. */
  private def deleteTree(root: LocalPath): Unit =
    Using.resource(Files.walk(root)) {
      _.sorted(Comparator.reverseOrder[LocalPath]()).forEach(path => Files.delete(path))
    }
}
