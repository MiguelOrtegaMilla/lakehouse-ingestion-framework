package lakehouse.ingestion.it.stress

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import scala.util.Using

import lakehouse.ingestion.it.stress.LogModel._
import lakehouse.ingestion.it.support.LocalSparkSuite
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * `LogReader` on a hand-written `_delta_log`: the log view must come from the commit files alone,
 * whatever Delta, or Hadoop's checksum layer, would make of them.
 */
final class LogReaderSpec extends AnyFunSuite with Matchers with LocalSparkSuite {

  private def write(dir: Path, name: String, content: String): Unit = {
    Files.createDirectories(dir)
    Files.write(dir.resolve(name), content.getBytes(UTF_8)): Unit
  }

  private def commitFile(v: Long) = f"$v%020d.json"

  test("reads txns, adds, removes and the operation from commit files only") {
    val table = workDir.resolve("handwritten")
    val log = table.resolve("_delta_log")
    write(
      log,
      commitFile(0),
      """{"commitInfo":{"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists"}}}
        |{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
        |{"metaData":{"id":"t","format":{"provider":"parquet","options":{}},"schemaString":"{}","partitionColumns":[],"configuration":{}}}
        |""".stripMargin
    )
    write(
      log,
      commitFile(1),
      """{"commitInfo":{"operation":"WRITE"}}
        |{"txn":{"appId":"writer-0","version":1,"lastUpdated":1}}
        |{"add":{"path":"part%20one.parquet","size":1,"modificationTime":1,"dataChange":true}}
        |""".stripMargin
    )
    write(
      log,
      commitFile(2),
      """{"commitInfo":{"operation":"OPTIMIZE"}}
        |{"remove":{"path":"part%20one.parquet","dataChange":false}}
        |{"add":{"path":"part-two.parquet","size":1,"modificationTime":1,"dataChange":false}}
        |""".stripMargin
    )
    write(log, commitFile(3), """{"commitInfo":{"operation":"WRITE"}}""" + "\n{\"txn\": {\n")
    write(log, commitFile(4), "")
    // No version 5. Neither of these is a commit file, and both must be ignored.
    write(log, commitFile(6).replace(".json", ".00000000000000000007.compacted.json"), "garbage")
    write(log, "00000000000000000006.checkpoint.parquet", "garbage")
    write(log, commitFile(6), """{"commitInfo":{"operation":"WRITE"}}""" + "\n")

    val view = LogReader.logView(spark, table)

    view.versions shouldBe Seq(0L, 1L, 2L, 3L, 4L, 6L)
    view.commits shouldBe Seq(
      Commit(0, Nil, Nil, Nil, Some("WRITE")),
      Commit(1, Seq(Txn("writer-0", 1)), Seq("part one.parquet"), Nil, Some("WRITE")),
      Commit(2, Nil, Seq("part-two.parquet"), Seq("part one.parquet"), Some("OPTIMIZE")),
      Commit(6, Nil, Nil, Nil, Some("WRITE"))
    )
    view.unreadable.map(_.version) shouldBe Seq(3L, 4L)
    view.unreadable.map(_.detail).last shouldBe "no actions"
    view.activeFiles shouldBe Set("part-two.parquet")
  }

  test("a directory without commit files gives an empty view") {
    LogReader.logView(spark, workDir.resolve("nothing-here")) shouldBe LogView(Nil, Nil, Nil)
  }

  test("data files on disk exclude the log and hidden files, at any depth") {
    val table = workDir.resolve("disk")
    write(table, "part-a.parquet", "x")
    write(table, ".part-a.parquet.crc", "x")
    write(table.resolve("p=1"), "part-b.parquet", "x")
    write(table.resolve("_delta_log"), commitFile(0), "{}")

    LogReader.diskView(table) shouldBe DiskView(Set("part-a.parquet", "p=1/part-b.parquet"))
  }

  test("a path that is not a Delta table gives the reader's error, not an exception") {
    val uri = workDir.resolve("not-delta").toUri.toString
    Reader.all.foreach(r => LogReader.tableView(spark, uri, r).isLeft shouldBe true)
  }

  test("a commit file with a stale .crc sidecar is still read, and reported as a mismatch") {
    val table = workDir.resolve("stale-sidecar")
    val log = table.resolve("_delta_log")
    val original = """{"commitInfo":{"operation":"WRITE"}}""" + "\n"
    val overwrite = """{"commitInfo":{"operation":"MERGE"}}""" + "\n"
    // Hadoop's LocalFileSystem writes the sidecar `.<name>.crc` next to the file.
    Seq(0L, 1L).foreach { v =>
      val fs = FileSystem.getLocal(spark.sparkContext.hadoopConfiguration)
      Using.resource(fs.create(new HadoopPath(log.resolve(commitFile(v)).toUri), true)) {
        _.write(original.getBytes(UTF_8))
      }
    }
    // Version 1 is overwritten behind the sidecar's back, as a racing rename does.
    write(log, commitFile(1), overwrite)
    Files.exists(log.resolve(s".${commitFile(1)}.crc")) shouldBe true

    val view = LogReader.logView(spark, table)

    view.checksumMismatches shouldBe Seq(1L)
    view.commits.map(c => c.version -> c.operation) shouldBe Seq(
      0L -> Some("WRITE"),
      1L -> Some("MERGE")
    )
    view.unreadable shouldBe empty
  }

  test("a sidecar on a commit file is always listed; on another file of the log, if it is stale") {
    val table = workDir.resolve("sidecars")
    val log = table.resolve("_delta_log")
    val commit = """{"commitInfo":{"operation":"WRITE"}}""" + "\n"
    val fs = FileSystem.getLocal(spark.sparkContext.hadoopConfiguration)
    // Through Hadoop's checksummed file system, which leaves a sidecar that matches.
    def withSidecar(name: String, content: String): Unit =
      Using.resource(fs.create(new HadoopPath(log.resolve(name).toUri), true)) {
        _.write(content.getBytes(UTF_8))
      }

    // A commit as PostgresLogStore publishes it, and one as Delta's default LogStore does.
    write(log, commitFile(0), commit)
    withSidecar(commitFile(1), commit)
    // The other files of a log: with a sidecar, without one, and below the log directory.
    withSidecar("00000000000000000001.crc", """{"tableSizeBytes":1}""")
    withSidecar("_last_checkpoint", """{"version":1,"size":2}""")
    withSidecar("_sidecars/part-0.parquet", "PAR1 one")
    write(log, "00000000000000000000.crc", """{"tableSizeBytes":0}""")
    write(log.resolve(".tmp"), s"${commitFile(0)}.3f2a", commit)

    val intact = LogReader.logView(spark, table)
    intact.commitSidecars shouldBe Seq(1L)
    intact.staleSidecars shouldBe empty
    intact.checksumMismatches shouldBe empty

    // Two of them are rewritten behind their sidecars' backs.
    write(log, "_last_checkpoint", """{"version":7,"size":2}""")
    write(log.resolve("_sidecars"), "part-0.parquet", "PAR1 two")

    val stale = LogReader.logView(spark, table)
    stale.staleSidecars shouldBe Seq("_last_checkpoint", "_sidecars/part-0.parquet")
    // The commit's own sidecar still matches: it is listed for being there, and is not stale.
    stale.commitSidecars shouldBe Seq(1L)
    stale.checksumMismatches shouldBe empty
  }
}
