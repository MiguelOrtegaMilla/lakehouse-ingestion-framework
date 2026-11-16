package lakehouse.ingestion.logstore

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path => LocalPath}
import java.time.Instant
import java.time.temporal.ChronoUnit

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import io.delta.storage.HDFSLogStore
import lakehouse.ingestion.logstore.coordination.FaultyCoordinationStore.{
  ReadEntry,
  ReadLatestEntry
}
import lakehouse.ingestion.logstore.coordination.{
  CommitEntry,
  Readoption,
  Registration,
  RegistryChange,
  RegistryMaintenance,
  TableUri,
  TakeResult
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * An operator's work on a coordinated table (ADR-0004 §3.5, §3.8): what gets a table out of
 * each state the fault scenarios leave it in, and what is refused because the table's log does
 * not bear it out.
 *
 * The table, its log and its registry are `LakeFixture`'s; the registry is reached here the way
 * an operator's tools reach it. The writer without the LogStore is Delta's default one.
 */
final class TableMaintenanceSpec extends AnyFunSuite with Matchers with LakeFixture {

  import LakeFixture._
  import TableMaintenanceSpec._

  private def maintenanceOf(lake: Lake) = new TableMaintenance(lake.memory)

  /** Commits `version` as a process without this LogStore does. */
  private def foreignCommit(lake: Lake, version: Long): Unit =
    new HDFSLogStore(conf)
      .write(hadoop(lake.commitFile(version)), ForeignActions.iterator.asJava, false, conf)

  private def registrationOf(lake: Lake): Option[Registration] =
    lake.memory.registration(lake.table)

  /**
   * The registry of `lake`, where other processes act at the two moments an operator's
   * operation leaves them: just before it examines the entries, and just before it changes the
   * registry on what it examined.
   */
  private def registryWhere(
      lake: Lake
  )(beforeExamining: () => Unit = () => (), beforeChanging: () => Unit = () => ()) =
    new RegistryMaintenance {
      private val registry = lake.memory

      override def registration(table: TableUri): Option[Registration] =
        registry.registration(table)

      override def entry(table: TableUri, version: Long): Option[CommitEntry] =
        registry.entry(table, version)

      override def unfinished(table: TableUri): Seq[CommitEntry] = {
        beforeExamining()
        registry.unfinished(table)
      }

      override def contradicted(table: TableUri, version: Long): Seq[CommitEntry] = {
        beforeExamining()
        registry.contradicted(table, version)
      }

      override def adopt(table: TableUri, version: Long, id: Option[String]): Boolean =
        registry.adopt(table, version, id)

      override def readopt(
          table: TableUri,
          version: Long,
          id: Option[String],
          examined: Seq[CommitEntry]
      ): RegistryChange[Readoption] = {
        beforeChanging()
        registry.readopt(table, version, id, examined)
      }

      override def forget(table: TableUri, examined: Seq[CommitEntry]): RegistryChange[Unit] = {
        beforeChanging()
        registry.forget(table, examined)
      }
    }

  /** `effect`, the first time it is asked for and never again. */
  private def once(effect: => Unit): () => Unit = {
    var done = false
    () =>
      if (!done) {
        done = true
        effect
      }
  }

  private def age(file: LocalPath, by: FiniteDuration): Unit =
    Files.setLastModifiedTime(
      file,
      FileTime.from(Instant.now().minus(by.toMillis, ChronoUnit.MILLIS))
    ): Unit

  // ---------------------------------------------------------------------------------------------
  // Adopting a table
  // ---------------------------------------------------------------------------------------------

  test("a table made without the LogStore is adopted at the version its log is at") {
    val lake = new Lake
    (0L to 2L).foreach(foreignCommit(lake, _))
    // Not coordinated: a write through the LogStore is refused.
    a[TableNotRegisteredException] should be thrownBy lake.commit(3)(append(3))

    val adopted = maintenanceOf(lake).adopt(hadoop(lake.directory), fs, Some(TableId))

    adopted shouldBe Adoption(lake.table, version = 2L)
    saidBy(registrationOf(lake)) shouldBe Some((2L, Some(TableId), None))
    // From the next version on it is written through the LogStore, and read without alarm.
    lake.commit(3)(append(3))
    lake.list() shouldBe (0L to 3L).map(commit)
    registrationOf(lake).value.isTainted shouldBe false
  }

  test("a table is not adopted if it has no commit, or if it is registered already") {
    val empty = new Lake
    val nothing = the[MaintenanceRefusedException] thrownBy
      maintenanceOf(empty).adopt(hadoop(empty.directory), fs, None)
    nothing.getMessage should include("no commit")
    registrationOf(empty) shouldBe None

    val coordinated = new Lake().withCommits(last = 1)
    val before = registrationOf(coordinated)
    val already = the[MaintenanceRefusedException] thrownBy
      maintenanceOf(coordinated).adopt(hadoop(coordinated.directory), fs, Some("another-id"))
    already.getMessage should include("registered already")
    registrationOf(coordinated) shouldBe before
  }

  // ---------------------------------------------------------------------------------------------
  // Adopting a table again: out of every state that stops its writers or its readers
  // ---------------------------------------------------------------------------------------------

  test("after F10, adopting the table again accepts the foreign commit and clears the taint") {
    val lake = new Lake().withCommits(last = 1)
    foreignCommit(lake, 2)
    lake.list() shouldBe (0L to 2L).map(commit)
    val reason = registrationOf(lake).value.taintReason.value

    val readopted = maintenanceOf(lake).readopt(hadoop(lake.directory), fs, None)

    readopted shouldBe Readopted(lake.table, 2L, Readoption(Some(reason), Nil))
    saidBy(registrationOf(lake)) shouldBe Some((2L, Some(TableId), None))
    // The commits made before keep their entries; writers and readers go on.
    (0L to 1L).map(lake.entry(_).value.complete) shouldBe Seq(true, true)
    lake.commit(3)(append(3))
    lake.list() shouldBe (0L to 3L).map(commit)
    registrationOf(lake).value.isTainted shouldBe false
  }

  test("after F11, adopting the table again gives up our commit and hands back its entry") {
    val lake = new Lake().withCommits(last = 0)
    val foreignWriterWins = LogStores.at(FaultPoint.AfterInsert)(foreignCommit(lake, 1))
    an[UncoordinatedWriteException] should be thrownBy
      lake.commit(1, lake.logStore(foreignWriterWins))(append(1))
    val ours = lake.entry(1).value
    an[UncoordinatedWriteException] should be thrownBy lake.list()

    val readopted = maintenanceOf(lake).readopt(hadoop(lake.directory), fs, None)

    // The entry of the commit that lost its name is returned: its bytes are still in its file.
    readopted.removed.removedEntries shouldBe Seq(ours)
    readopted.removed.clearedTaint.value should include("version 1")
    Files.readAllLines(lake.tempOf(ours)).asScala shouldBe append(1)
    // The table is the log as it stands: version 1 is the foreign commit.
    lake.list() shouldBe Seq(commit(0), commit(1))
    lake.committed(1) shouldBe ForeignActions
    lake.commit(2)(append(2))
    lake.entry(2).value.complete shouldBe true
  }

  test("after F17, adopting the table again frees the version of the commit that was lost") {
    val lake = new Lake().withCommits(last = 0)
    val unfinished = lake.crashedAt(FaultPoint.AfterInsert, version = 1)
    Files.delete(lake.tempOf(unfinished))
    an[UnpublishableCommitException] should be thrownBy lake.list()

    val readopted = maintenanceOf(lake).readopt(hadoop(lake.directory), fs, None)

    readopted shouldBe Readopted(lake.table, 0L, Readoption(None, Seq(unfinished)))
    lake.list() shouldBe Seq(commit(0))
    lake.commit(1)(Seq("the commit that takes its place"))
    lake.committed(1) shouldBe Seq("the commit that takes its place")
  }

  test("after F14, adopting the table again takes the log without the commit that is gone") {
    val lake = new Lake().withCommits(last = 2)
    val gone = lake.entry(2).value
    Files.delete(lake.commitFile(2))
    a[CommitMissingException] should be thrownBy lake.list()

    val readopted = maintenanceOf(lake).readopt(hadoop(lake.directory), fs, None)

    readopted shouldBe Readopted(lake.table, 1L, Readoption(None, Seq(gone)))
    lake.list() shouldBe Seq(commit(0), commit(1))
    lake.commit(2)(append(2))
    lake.entry(2).value.complete shouldBe true
  }

  test("a commit that can still be published is not an operator's to discard") {
    val lake = new Lake().withCommits(last = 0)
    val unfinished = lake.crashedAt(FaultPoint.AfterInsert, version = 1)

    val refused = the[MaintenanceRefusedException] thrownBy
      maintenanceOf(lake).readopt(hadoop(lake.directory), fs, None)

    refused.getMessage should include("version 1")
    refused.getMessage should include(unfinished.tempPath)
    lake.entry(1) shouldBe Some(unfinished)

    // A read publishes it, as the refusal says; then there is nothing left to discard.
    lake.list() shouldBe Seq(commit(0), commit(1))
    maintenanceOf(lake).readopt(hadoop(lake.directory), fs, None) shouldBe
      Readopted(lake.table, 1L, Readoption(None, Nil))
    lake.entry(1).value.complete shouldBe true
  }

  test("a commit that a reader publishes while the table is adopted again keeps its entry") {
    val lake = new Lake().withCommits(last = 1)
    lake.crashAt(FaultPoint.AfterInsert, version = 2)
    // The operation has seen a log that ends at version 1. Before it asks the registry for
    // the entries that log contradicts, a reader publishes version 2 and acknowledges it.
    val registry = registryWhere(lake)(beforeExamining = once(lake.list(): Unit))

    val readopted = new TableMaintenance(registry).readopt(hadoop(lake.directory), fs, None)

    // The table is adopted as its log is now, and version 2 is still a commit of ours.
    readopted shouldBe Readopted(lake.table, 2L, Readoption(None, Nil))
    lake.entry(2).value.complete shouldBe true
    lake.list() shouldBe (0L to 2L).map(commit)
    registrationOf(lake).value.isTainted shouldBe false
    lake.commit(3)(append(3))
  }

  test("a table is not adopted again if it is not registered, or if its log is gone") {
    val foreign = new Lake
    foreignCommit(foreign, 0)
    val unknown = the[MaintenanceRefusedException] thrownBy
      maintenanceOf(foreign).readopt(hadoop(foreign.directory), fs, None)
    unknown.getMessage should include("not registered")

    val dropped = new Lake().withCommits(last = 1)
    val before = registrationOf(dropped)
    (0L to 1L).foreach(version => Files.delete(dropped.commitFile(version)))
    val noLog = the[MaintenanceRefusedException] thrownBy
      maintenanceOf(dropped).readopt(hadoop(dropped.directory), fs, None)
    noLog.getMessage should include("reset its registry")
    registrationOf(dropped) shouldBe before
  }

  // ---------------------------------------------------------------------------------------------
  // Resetting the registry of a table that was dropped
  // ---------------------------------------------------------------------------------------------

  test(
    "F9: once its registry is reset, a table can be created again at the path of a dropped one"
  ) {
    val lake = new Lake().withCommits(last = 2)
    (0L to 2L).foreach(version => Files.delete(lake.commitFile(version)))
    lake.temps.foreach(Files.delete)
    a[StaleRegistrationException] should be thrownBy lake.commit(0)(Creation)

    maintenanceOf(lake).reset(hadoop(lake.directory), fs)

    registrationOf(lake) shouldBe None
    (0L to 2L).foreach(lake.entry(_) shouldBe None)
    // The new table registers itself with its first commit, like any other.
    lake.commit(0)(Creation)
    saidBy(registrationOf(lake)) shouldBe Some((Registration.NotYetCreated, Some(TableId), None))
    lake.list() shouldBe Seq(commit(0))
  }

  test("the registry of a table that still has a log is not reset, nor one that has no registry") {
    val live = new Lake().withCommits(last = 1)
    val before = (registrationOf(live), lake0(live))
    val exists = the[MaintenanceRefusedException] thrownBy
      maintenanceOf(live).reset(hadoop(live.directory), fs)
    exists.getMessage should include("up to version 1")
    (registrationOf(live), lake0(live)) shouldBe before

    val unknown = new Lake
    val nothing = the[MaintenanceRefusedException] thrownBy
      maintenanceOf(unknown).reset(hadoop(unknown.directory), fs)
    nothing.getMessage should include("not registered")
  }

  // ---------------------------------------------------------------------------------------------
  // A writer that was not stopped (ADR-0004 §3.5): the operator's change and the writer's claim
  // are each made on what was examined, or not at all
  // ---------------------------------------------------------------------------------------------

  /** The table is dropped, its registry reset, and a table created again at its path. */
  private def dropAndCreateAgain(lake: Lake, upTo: Long): Unit = {
    val latest = lake.memory.latestEntry(lake.table).value.version
    (0L to latest).foreach(version => Files.deleteIfExists(lake.commitFile(version)))
    lake.temps.foreach(Files.delete)
    maintenanceOf(lake).reset(hadoop(lake.directory), fs)
    lake.commit(0)(Creation)
    (1L to upTo).foreach(version => lake.commit(version)(append(version)))
  }

  test("a writer stalled in its checks while its table is dropped and created again is refused") {
    val lake = new Lake().withCommits(last = 5)
    // The writer of version 6 has asked for the entry of version 5, the last of its questions
    // to the registry, when it stalls. It goes on in another table.
    lake.store.afterNext(ReadEntry)(dropAndCreateAgain(lake, upTo = 2))

    val missing = the[CommitMissingException] thrownBy lake.commit(6)(append(6))

    // The commit before its own is not in the log of the new table: nothing is written.
    missing.version shouldBe 5L
    lake.entry(6) shouldBe None
    lake.list() shouldBe (0L to 2L).map(commit)
  }

  test("a writer stalled before its INSERT gets no version of the table created in its place") {
    val lake = new Lake().withCommits(last = 5)
    val stalled = LogStores.at(FaultPoint.BeforeInsert)(dropAndCreateAgain(lake, upTo = 2))

    // The registration it read is gone, and the one in its place has another generation.
    a[RegistrationChangedException] should be thrownBy
      lake.commit(6, lake.logStore(stalled))(append(6))

    // The new table is as its own writers left it: read, and written on.
    lake.entry(6) shouldBe None
    Files.exists(lake.commitFile(6)) shouldBe false
    lake.list() shouldBe (0L to 2L).map(commit)
    lake.commit(3)(append(3))
  }

  test("a writer of a dropped table gets no version of a new one that reached its own") {
    val lake = new Lake().withCommits(last = 5)
    // The one case nothing else stops. The writer of version 6 stalls in its checks; the table
    // created in its place gets as far as version 5, so the commit before the writer's is in
    // the log, its temporary file is written into the new log, and version 6 is free.
    lake.store.afterNext(ReadEntry)(dropAndCreateAgain(lake, upTo = 5))

    a[RegistrationChangedException] should be thrownBy lake.commit(6)(append(6))

    // A commit computed on the old table is not in the new one.
    lake.entry(6) shouldBe None
    Files.exists(lake.commitFile(6)) shouldBe false
    lake.temps.map(_.getFileName.toString).toSet shouldBe
      (0L to 5L).map(lake.entry(_).value.tempPath.stripPrefix(".tmp/")).toSet
    lake.list() shouldBe (0L to 5L).map(commit)
  }

  test("a writer stalled before its INSERT while its table is adopted again gets no version") {
    val lake = new Lake().withCommits(last = 1)
    val whileAdopted = LogStores.at(FaultPoint.BeforeInsert) {
      maintenanceOf(lake).readopt(hadoop(lake.directory), fs, None): Unit
    }
    val tempsBefore = lake.temps

    a[RegistrationChangedException] should be thrownBy
      lake.commit(2, lake.logStore(whileAdopted))(append(2))

    lake.entry(2) shouldBe None
    lake.temps shouldBe tempsBefore
    // It is not a refusal of the table: the transaction, started again, commits.
    lake.commit(2)(append(2))
    lake.list() shouldBe (0L to 2L).map(commit)
  }

  test("a commit made while its table is adopted again keeps its entry") {
    val lake = new Lake().withCommits(last = 1)
    // The operator has examined a table that ends at version 1. Before the registry is
    // changed, a writer that was not stopped commits version 2.
    val registry = registryWhere(lake)(beforeChanging = once(lake.commit(2)(append(2))))

    val readopted = new TableMaintenance(registry).readopt(hadoop(lake.directory), fs, None)

    readopted shouldBe Readopted(lake.table, 2L, Readoption(None, Nil))
    lake.entry(2).value.complete shouldBe true
    lake.list() shouldBe (0L to 2L).map(commit)
    registrationOf(lake).value.isTainted shouldBe false
  }

  test("a version taken while a table is adopted again is not the operator's to discard") {
    val lake = new Lake().withCommits(last = 1)
    // As above, and the writer has only taken its version when the registry is changed.
    val registry =
      registryWhere(lake)(beforeChanging = once(lake.crashAt(FaultPoint.AfterInsert, 2)))
    val before = registrationOf(lake)

    val refused = the[MaintenanceRefusedException] thrownBy
      new TableMaintenance(registry).readopt(hadoop(lake.directory), fs, None)

    refused.getMessage should include("version 2")
    refused.getMessage should include("can still be published")
    // Nothing was changed, not even the generation: the writer, were it alive, would go on.
    registrationOf(lake) shouldBe before
    lake.list() shouldBe (0L to 2L).map(commit)
    lake.entry(2).value.complete shouldBe true
  }

  test("a table that is written every time it is examined is not adopted again") {
    val lake = new Lake().withCommits(last = 0)
    var next = 0L
    val busy = registryWhere(lake)(beforeChanging = () => {
      next += 1
      lake.commit(next)(append(next))
    })
    val before = registrationOf(lake)

    val refused = the[MaintenanceRefusedException] thrownBy
      new TableMaintenance(busy).readopt(hadoop(lake.directory), fs, None)

    refused.getMessage should include("is being written")
    registrationOf(lake) shouldBe before
    (0L to next).map(lake.entry(_).value.complete) shouldBe Seq.fill(next.toInt + 1)(true)
  }

  /** A process that outlived its table: it writes its temporary file and takes `version`. */
  private def takenByAWriterStillRunning(lake: Lake, version: Long): CommitEntry = {
    val claim = lake.claimBy(s"$version.json.still-running", version)
    Files.createDirectories(lake.log.resolve(".tmp"))
    Files.write(lake.log.resolve(claim.tempPath), append(version).asJava)
    lake.memory.take(claim) shouldBe TakeResult.Taken
    lake.entry(version).value
  }

  test("the registry of a dropped table is not reset while a writer of it can still publish") {
    val lake = new Lake().withCommits(last = 1)
    (0L to 1L).foreach(version => Files.delete(lake.commitFile(version)))
    lake.temps.foreach(Files.delete)
    val taken = takenByAWriterStillRunning(lake, version = 2)
    val before = registrationOf(lake)

    val refused = the[MaintenanceRefusedException] thrownBy
      maintenanceOf(lake).reset(hadoop(lake.directory), fs)

    refused.getMessage should include("version 2")
    refused.getMessage should include("still running")
    registrationOf(lake) shouldBe before
    lake.entry(2) shouldBe Some(taken)
  }

  test("nor if such a writer takes its version while the registry is being reset") {
    val lake = new Lake().withCommits(last = 1)
    (0L to 1L).foreach(version => Files.delete(lake.commitFile(version)))
    lake.temps.foreach(Files.delete)
    // The operator found no unfinished entry. One is made before the registry is changed.
    val registry = registryWhere(lake)(beforeChanging = once {
      takenByAWriterStillRunning(lake, version = 2): Unit
    })
    val before = registrationOf(lake)

    val refused = the[MaintenanceRefusedException] thrownBy
      new TableMaintenance(registry).reset(hadoop(lake.directory), fs)

    refused.getMessage should include("still running")
    registrationOf(lake) shouldBe before
    lake.entry(2).value.complete shouldBe false
  }

  // ---------------------------------------------------------------------------------------------
  // Collecting temporary files
  // ---------------------------------------------------------------------------------------------

  test("F1: the orphan of a writer that died before its version is collected once it is old") {
    val lake = new Lake().withCommits(last = 0)
    val tempsBefore = lake.temps
    lake.crashAt(FaultPoint.BeforeInsert, version = 1)
    val orphan = lake.temps.diff(tempsBefore).head
    val published = tempsBefore.head

    // Young, it may belong to a writer that is about to take a version: it is left alone.
    val first = maintenanceOf(lake).collectTemps(hadoop(lake.directory), fs)
    first shouldBe Map(
      orphan.getFileName.toString -> TempFate.KeptRecent,
      published.getFileName.toString -> TempFate.RemovedPublished
    )
    Files.exists(orphan) shouldBe true

    age(orphan, by = 2.days)
    maintenanceOf(lake).collectTemps(hadoop(lake.directory), fs) shouldBe
      Map(orphan.getFileName.toString -> TempFate.RemovedOrphan)

    lake.temps shouldBe empty
    // The commit lost a name, not its content.
    lake.committed(0) shouldBe Creation
    lake.list() shouldBe Seq(commit(0))
    lake.commit(1)(append(1))
  }

  test("the temporary file of a commit that is not published is kept, however old") {
    val lake = new Lake().withCommits(last = 0)
    val unfinished = lake.crashedAt(FaultPoint.AfterInsert, version = 1)
    age(lake.tempOf(unfinished), by = 30.days)

    val fates = maintenanceOf(lake).collectTemps(hadoop(lake.directory), fs)

    fates(lake.tempOf(unfinished).getFileName.toString) shouldBe TempFate.KeptUnfinished
    // It is the commit: the next reader publishes it from there.
    lake.list() shouldBe Seq(commit(0), commit(1))
    lake.committed(1) shouldBe append(1)
  }

  test("the temporary file of a commit whose file is gone or replaced is the only copy, and kept") {
    val lake = new Lake().withCommits(last = 2)
    val replaced = lake.tempOf(lake.entry(1).value)
    val gone = lake.tempOf(lake.entry(2).value)
    // What the known limit of the detection looks like: a foreign file over a published commit.
    Files.delete(lake.commitFile(1))
    Files.write(lake.commitFile(1), ForeignActions.asJava)
    Files.delete(lake.commitFile(2))

    val fates = maintenanceOf(lake).collectTemps(hadoop(lake.directory), fs, 0.seconds)

    fates(replaced.getFileName.toString) shouldBe TempFate.KeptOnlyCopy
    fates(gone.getFileName.toString) shouldBe TempFate.KeptOnlyCopy
    Files.readAllLines(replaced).asScala shouldBe append(1)
    Files.readAllLines(gone).asScala shouldBe append(2)
  }

  /** The name of the temporary file of `version`, as the collection reports it. */
  private def tempNameOf(lake: Lake, version: Long): String =
    lake.tempOf(lake.entry(version).value).getFileName.toString

  /** Delta's pointer to a checkpoint of the table at `version`, written as Delta writes it. */
  private def checkpointAt(lake: Lake, version: Long): Unit =
    Files.write(
      lake.log.resolve("_last_checkpoint"),
      s"""{"version":$version,"size":4,"sizeInBytes":12345,"numOfAddFiles":2}""".getBytes(UTF_8)
    ): Unit

  test("the temporary file of a commit that Delta's log cleanup expired is collected") {
    val lake = new Lake().withCommits(last = 4)
    val temps = (0L to 4L).map(tempNameOf(lake, _))
    checkpointAt(lake, version = 3)
    // What the cleanup does a month later: the commits below the checkpoint leave the log.
    lake.temps.foreach(age(_, by = 40.days))
    (0L to 2L).foreach(version => Files.delete(lake.commitFile(version)))

    val fates = maintenanceOf(lake).collectTemps(hadoop(lake.directory), fs)

    temps.map(fates) shouldBe
      Seq.fill(3)(TempFate.RemovedExpired) ++ Seq.fill(2)(TempFate.RemovedPublished)
    lake.temps shouldBe empty
    // The table goes on from its checkpoint.
    lake.commit(5)(append(5))
    lake.list(from = 3).filter(_.endsWith(".json")) shouldBe (3L to 5L).map(commit)
  }

  test("a commit that is gone is not taken for expired unless a checkpoint is above it") {
    val lake = new Lake().withCommits(last = 3)
    val temps = (0L to 3L).map(tempNameOf(lake, _))
    lake.temps.foreach(age(_, by = 40.days))
    (1L to 3L).foreach(version => Files.delete(lake.commitFile(version)))
    def fates(): Map[String, TempFate] =
      maintenanceOf(lake).collectTemps(hadoop(lake.directory), fs)

    // No checkpoint: nothing explains a commit that is missing.
    temps.drop(1).map(fates()) shouldBe Seq.fill(3)(TempFate.KeptOnlyCopy)

    // A pointer that is not written the way Delta writes one says nothing.
    Files.write(lake.log.resolve("_last_checkpoint"), """{"size":4,"version":3}""".getBytes(UTF_8))
    temps.drop(1).map(fates()) shouldBe Seq.fill(3)(TempFate.KeptOnlyCopy)

    // Delta's cleanup removes what is below the checkpoint: not its own version, nor above.
    checkpointAt(lake, version = 2)
    temps.drop(1).map(fates()) shouldBe
      Seq(TempFate.RemovedExpired, TempFate.KeptOnlyCopy, TempFate.KeptOnlyCopy)
    Files.readAllLines(lake.log.resolve(".tmp").resolve(temps(3))).asScala shouldBe append(3)
  }

  test("below a checkpoint, a commit that went missing a moment ago or was replaced is kept") {
    val lake = new Lake().withCommits(last = 3)
    val temps = (0L to 3L).map(tempNameOf(lake, _))
    checkpointAt(lake, version = 3)
    // Not the cleanup's doing: that one removes commits a month old, and does not replace any.
    Files.delete(lake.commitFile(1))
    Files.delete(lake.commitFile(2))
    Files.write(lake.commitFile(2), ForeignActions.asJava)

    val young = maintenanceOf(lake).collectTemps(hadoop(lake.directory), fs)
    Seq(temps(1), temps(2)).map(young) shouldBe Seq.fill(2)(TempFate.KeptOnlyCopy)

    // A month later the first could be the cleanup's, and its bytes are in the checkpoint.
    lake.temps.foreach(age(_, by = 40.days))
    val old = maintenanceOf(lake).collectTemps(hadoop(lake.directory), fs)
    Seq(temps(1), temps(2)).map(old) shouldBe
      Seq(TempFate.RemovedExpired, TempFate.KeptOnlyCopy)
  }

  test("a writer that stopped after taking its version finds its commit finished and collected") {
    val lake = new Lake().withCommits(last = 0)
    // While the writer is stopped, a reader publishes its commit, and the temporary files of
    // published commits are collected: the writer's own among them.
    var collected = Set.empty[TempFate]
    val whileStopped = LogStores.at(FaultPoint.AfterInsert) {
      lake.list(): Unit
      collected = maintenanceOf(lake).collectTemps(hadoop(lake.directory), fs).values.toSet
    }

    // The commit is done, and its writer is told so: nothing was lost.
    lake.commit(1, lake.logStore(whileStopped))(append(1))

    collected shouldBe Set(TempFate.RemovedPublished)
    lake.committed(1) shouldBe append(1)
    lake.entry(1).value.complete shouldBe true
    lake.temps shouldBe empty
  }

  test("a reader that stopped before publishing a commit finds it finished and collected") {
    val lake = new Lake().withCommits(last = 0)
    lake.crashAt(FaultPoint.AfterInsert, version = 1)
    // This reader has read the unfinished entry. Before it publishes the commit, another
    // reader does, and the temporary files are collected.
    lake.store.afterNext(ReadLatestEntry) {
      lake.list(): Unit
      maintenanceOf(lake).collectTemps(hadoop(lake.directory), fs): Unit
    }

    lake.list() shouldBe Seq(commit(0), commit(1))

    lake.committed(1) shouldBe append(1)
    lake.temps shouldBe empty
  }

  test("a table without temporary files has nothing to collect") {
    val lake = new Lake
    maintenanceOf(lake).collectTemps(hadoop(lake.directory), fs) shouldBe empty
  }

  /** The entry of version 0, as a sign that nothing of the registry was touched. */
  private def lake0(lake: Lake) = lake.entry(0)
}

object TableMaintenanceSpec {

  private val ForeignActions = Seq(
    """{"commitInfo":{"operation":"WRITE","engineInfo":"another writer"}}"""
  )

  private def commit(version: Long): String = f"$version%020d.json"
}
