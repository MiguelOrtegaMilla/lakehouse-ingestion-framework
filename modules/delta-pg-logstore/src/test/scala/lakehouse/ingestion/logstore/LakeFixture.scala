package lakehouse.ingestion.logstore

import java.nio.file.{Files, Path => LocalPath}

import scala.jdk.CollectionConverters._
import scala.util.Using

import lakehouse.ingestion.logstore.coordination._
import lakehouse.ingestion.logstore.storage.{LocalLogStorage, LogStorage}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A table on a real directory, and the coordination store of the processes that read and write
 * it: what the suites that drive `PostgresLogStore` through the methods Delta calls work on.
 *
 * The store is the in-memory one, behind `FaultyCoordinationStore` so that a test can choose
 * which call fails and how. A process is a LogStore of its own. One that dies is a fault hook
 * that throws `SimulatedCrash`, and whoever comes after it is a new LogStore.
 */
trait LakeFixture extends ScratchDirectories { this: AnyFunSuite with Matchers =>

  import LakeFixture._

  protected val conf = new Configuration()
  protected val fs: FileSystem = FileSystem.getLocal(conf)

  /** A table directory with an empty `_delta_log`, and the store its processes coordinate in. */
  final protected class Lake {
    val memory = new InMemoryCoordinationStore
    val store = new FaultyCoordinationStore(memory)
    val directory: LocalPath = Files.createDirectories(newDirectory().resolve("lake/orders"))
    val log: LocalPath = Files.createDirectory(directory.resolve("_delta_log"))
    val table: TableUri = TableIdentity.of(hadoop(directory), fs)

    /** A new process, reader or writer: it has a LogStore of its own. */
    def logStore(
        hooks: Option[FaultHooks] = None,
        storageOf: FileSystem => LogStorage = LogStorage.of
    ): PostgresLogStore = LogStores.over(store, hooks, storageOf = storageOf)

    def commitFile(version: Long): LocalPath = log.resolve(f"$version%020d.json")

    def commit(version: Long, by: PostgresLogStore = logStore())(actions: Seq[String]): Unit =
      by.write(hadoop(commitFile(version)), actions.iterator.asJava, false, conf)

    /** The names a reader is given when it lists the log from version `from` on. */
    def list(from: Long = 0, by: PostgresLogStore = logStore()): Seq[String] =
      by.listFrom(hadoop(commitFile(from)), conf).asScala.map(_.getPath.getName).toSeq

    def committed(version: Long): Seq[String] =
      Files.readAllLines(commitFile(version)).asScala.toSeq

    def entry(version: Long): Option[CommitEntry] = memory.entry(table, version)

    /** The generation of the table's registration, as a writer reads it before it commits. */
    def generation: Long =
      memory.registration(table).getOrElse(fail("the table is not registered")).generation

    /** The claim of another process on `version`, which read the registration just now. */
    def claimBy(another: String, version: Long): Claim =
      Claim(table, version, s".tmp/$another", "host-b:2:app", runId = None, generation)

    def tempOf(entry: CommitEntry): LocalPath = log.resolve(entry.tempPath)

    /** The temporary files in the log, whether an entry points at them or not. */
    def temps: Seq[LocalPath] = {
      val tempDirectory = log.resolve(".tmp")
      if (!Files.isDirectory(tempDirectory)) Nil
      else Using.resource(Files.list(tempDirectory))(_.iterator.asScala.toList)
    }

    /** Versions 0 to `last`, committed without incident. */
    def withCommits(last: Long): Lake = {
      commit(0)(Creation)
      (1L to last).foreach(version => commit(version)(append(version)))
      this
    }

    /** A writer that reaches `point` while committing `version`, and dies there. */
    def crashAt(point: FaultPoint, version: Long): Unit =
      a[SimulatedCrash] should be thrownBy
        commit(version, logStore(LogStores.crashAt(point)))(append(version)): Unit

    /** The entry a writer leaves when it takes `version` and dies at `point`. */
    def crashedAt(point: FaultPoint, version: Long): CommitEntry = {
      crashAt(point, version)
      entry(version).getOrElse(fail(s"the writer died at $point without an entry"))
    }
  }

  implicit protected class Defined[A](option: Option[A]) {

    /** The value, or a failed test. */
    def value: A = option.getOrElse(fail("expected a value, found None"))
  }
}

object LakeFixture {

  val TableId = "5a0d1a66-0bb5-4d1c-8a8b-6a1f6f5a3c11"

  /** The actions of a table's first commit, as Delta writes them. */
  val Creation: Seq[String] = Seq(
    """{"commitInfo":{"timestamp":1763200000000,"operation":"CREATE TABLE","engineInfo":"Apache-Spark/4.1.4 Delta-Lake/4.4.1"}}""",
    """{"metaData":{"id":"""" + TableId + """","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1763200000000}}""",
    """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"""
  )

  def append(version: Long): Seq[String] =
    Seq(s"""{"add":{"path":"part-$version.parquet","dataChange":true}}""")

  def hadoop(path: LocalPath): Path = new Path(LocalLogStorage.Scheme, null, path.toString)

  /** What a registration says, without the generation the registry gave it. */
  def saidBy(registration: Option[Registration]): Option[(Long, Option[String], Option[String])] =
    registration.map(r => (r.registeredVersion, r.deltaTableId, r.taintReason))
}
