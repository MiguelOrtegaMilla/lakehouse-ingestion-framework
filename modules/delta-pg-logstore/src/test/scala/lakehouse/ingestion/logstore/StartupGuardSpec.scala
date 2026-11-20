package lakehouse.ingestion.logstore

import java.io.IOException
import java.nio.file.{Files, Path => LocalPath}

import scala.jdk.CollectionConverters._
import scala.util.Using

import io.delta.storage.HDFSLogStore
import lakehouse.ingestion.logstore.coordination.FaultyCoordinationStore.ReadRegistration
import lakehouse.ingestion.logstore.coordination.{
  CoordinationSettings,
  FaultyCoordinationStore,
  InMemoryCoordinationStore
}
import lakehouse.ingestion.logstore.storage.{
  LocalLogStorage,
  LogStorage,
  LogStorageSpec,
  PublishResult
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The startup guard (ADR-0004 §3.5, layer 1): a process that is not set up to coordinate its
 * commits is told everything that is wrong, before it touches a table.
 *
 * The lake is a real directory. The coordination database is the in-memory one here; the
 * integration tests ask a real PostgreSQL, with and without its schema.
 */
final class StartupGuardSpec
    extends AnyFunSuite
    with Matchers
    with OptionValues
    with ScratchDirectories {

  import StartupGuardSpec._

  private val store = new FaultyCoordinationStore(new InMemoryCoordinationStore)

  /** A configuration with which a process may start. */
  private def goodConf(): Configuration = {
    val conf = new Configuration()
    conf.set(StartupGuard.logStoreKey("file"), classOf[PostgresLogStore].getName)
    conf.set(CoordinationSettings.JdbcUrl, "jdbc:postgresql://db.invalid/ingestion_control")
    conf.set(CoordinationSettings.User, "ingest_coordination")
    conf.set(CoordinationSettings.PasswordRef, "env:COORDINATION_PASSWORD")
    conf
  }

  private def problems(
      conf: Configuration,
      lakeRoot: LocalPath,
      storageOf: FileSystem => LogStorage = LogStorage.of
  ): Seq[String] = StartupGuard.problems(conf, hadoop(lakeRoot), _ => store, storageOf)

  private def filesIn(directory: LocalPath): Seq[String] =
    Using.resource(Files.list(directory))(_.iterator.asScala.map(_.getFileName.toString).toList)

  test("a process that is set up to coordinate its commits may start, and leaves no trace") {
    val lake = newDirectory()

    problems(goodConf(), lake) shouldBe empty

    // The probe wrote a file in the lake root and published it; both names are gone.
    filesIn(lake) shouldBe empty
    // One question to the database, which opens the connection and checks the schema.
    store.calls shouldBe Seq(ReadRegistration)
  }

  test("Delta must be told to use this LogStore for the lake's scheme") {
    val lake = newDirectory()
    val ours = classOf[PostgresLogStore].getName

    val unset = goodConf()
    unset.unset(StartupGuard.logStoreKey("file"))
    val missing = problems(unset, lake)
    missing should have size 1
    missing.head should include("spark.delta.logStore.file.impl is not set")
    missing.head should include("default LogStore")

    val another = goodConf()
    another.set(StartupGuard.logStoreKey("file"), classOf[HDFSLogStore].getName)
    problems(another, lake).head should include(classOf[HDFSLogStore].getName)

    // The setting for every scheme wins over the one for a scheme, in Delta and so here.
    val forEveryScheme = goodConf()
    forEveryScheme.unset(StartupGuard.logStoreKey("file"))
    forEveryScheme.set(StartupGuard.LogStoreClassKey, ours)
    problems(forEveryScheme, lake) shouldBe empty

    val overridden = goodConf()
    overridden.set(StartupGuard.LogStoreClassKey, classOf[HDFSLogStore].getName)
    problems(overridden, lake).head should include("spark.delta.logStore.class is")
  }

  test("a lake root that is not there, or is not a directory, is a problem") {
    val lake = newDirectory()
    val file = Files.createFile(lake.resolve("a-file"))

    Seq(lake.resolve("absent"), file).foreach { root =>
      val found = problems(goodConf(), root)
      found should have size 1
      found.head should include("does not exist, or is not a directory")
    }
    // Nothing was created to find out.
    filesIn(lake) shouldBe Seq("a-file")
  }

  test("a lake the LogStore has no storage for is a problem, and nothing is tried on it") {
    val lake = newDirectory()
    val replaced = goodConf()
    replaced.set("fs.file.impl", classOf[LogStorageSpec.RenamingElsewhere].getName)
    replaced.setBoolean("fs.file.impl.disable.cache", true)
    val callsBefore = store.calls.size

    val found = problems(replaced, lake)

    found should have size 1
    found.head should include(classOf[LogStorageSpec.RenamingElsewhere].getName)
    filesIn(lake) shouldBe empty
    store.calls.size shouldBe callsBefore
  }

  test("a lake root where a file cannot be published under a second name is a problem") {
    val lake = newDirectory()

    val found = problems(goodConf(), lake, _ => NoHardLinks)

    found should have size 1
    found.head should include("cannot hold a coordinated log")
    found.head should include("hard links are not supported here")
    filesIn(lake) shouldBe empty
  }

  test("coordination settings that are missing are each reported, and the database is not asked") {
    val lake = newDirectory()
    val incomplete = goodConf()
    incomplete.unset(CoordinationSettings.JdbcUrl)
    incomplete.unset(CoordinationSettings.PasswordRef)
    val callsBefore = store.calls.size

    val found = problems(incomplete, lake)

    found should have size 2
    found.exists(_.contains(CoordinationSettings.JdbcUrl)) shouldBe true
    found.exists(_.contains(CoordinationSettings.PasswordRef)) shouldBe true
    store.calls.size shouldBe callsBefore
  }

  test("a coordination database that does not answer is a problem") {
    val lake = newDirectory()

    store.down()
    val found =
      try problems(goodConf(), lake)
      finally store.up()

    found should have size 1
    found.head should include("the coordination database cannot be used")
  }

  test("a process that may not start is told every problem, and what the database failed with") {
    val lake = newDirectory()
    val unset = goodConf()
    unset.unset(StartupGuard.logStoreKey("file"))
    def refusal(conf: Configuration): StartupRefusedException =
      intercept[StartupRefusedException](
        StartupGuard.require(conf, hadoop(lake), _ => store, LogStorage.of)
      )

    // A configuration that is wrong: nothing that the database said.
    val misconfigured = refusal(unset)
    misconfigured.problems should have size 1
    misconfigured.coordinationFailure shouldBe empty
    misconfigured.getCause shouldBe null

    // The database that does not answer, as well: what it failed with is kept, for the caller
    // to tell whether it may pass, and it is the cause.
    store.down()
    val both =
      try refusal(unset)
      finally store.up()
    both.problems should have size 2
    both.problems.last should include("the coordination database cannot be used")
    both.coordinationFailure.value shouldBe a[IOException]
    both.getCause shouldBe both.coordinationFailure.value

    noException should be thrownBy StartupGuard.require(
      goodConf(),
      hadoop(lake),
      _ => store,
      LogStorage.of
    )
  }

  test(
    "a fault hook setting that cannot be read is a problem; one that can is the LogStore's to announce"
  ) {
    val lake = newDirectory()

    val unreadable = goodConf()
    unreadable.set(FaultHooks.delayKey(FaultPoint.AfterInsert), "soon")
    val found = problems(unreadable, lake)
    found should have size 1
    found.head should include("lakehouse.logstore.fault.afterInsert.delayMs")

    val delayed = goodConf()
    delayed.setLong(FaultHooks.delayKey(FaultPoint.AfterInsert), 50L)
    problems(delayed, lake) shouldBe empty
  }

  test("everything that is wrong is reported at once, and refuses the start") {
    val lake = newDirectory()
    val wrong = new Configuration()
    wrong.set(FaultHooks.delayKey(FaultPoint.BeforeInsert), "-1")

    // Through the function a process calls, with nothing injected.
    val found = StartupGuard.problems(wrong, hadoop(lake))
    val refused = the[StartupRefusedException] thrownBy StartupGuard.require(wrong, hadoop(lake))

    // The LogStore is not named, three settings are missing, and a fault setting is unreadable.
    found should have size 5
    refused.problems shouldBe found
    found.foreach(problem => refused.getMessage should include(problem))
    refused shouldBe an[IOException]
  }
}

object StartupGuardSpec {

  private def hadoop(path: LocalPath): Path = new Path(LocalLogStorage.Scheme, null, path.toString)

  /** A storage on a file system without hard links: a temporary file is written, never published. */
  private object NoHardLinks extends LogStorage {
    private val disk = LocalLogStorage.shared

    override def partialWritesVisible: Boolean = disk.partialWritesVisible
    override def writeTemp(temp: Path, lines: Iterator[String]): Unit = disk.writeTemp(temp, lines)
    override def replace(temp: Path, target: Path): Unit = disk.replace(temp, target)
    override def discard(temp: Path): Unit = disk.discard(temp)

    override def publish(temp: Path, target: Path): PublishResult =
      throw new IOException("hard links are not supported here")
  }
}
