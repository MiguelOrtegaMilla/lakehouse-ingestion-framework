package lakehouse.ingestion.logstore

import java.io.IOException
import java.nio.file.FileAlreadyExistsException
import java.util.UUID

import lakehouse.ingestion.logstore.coordination._
import lakehouse.ingestion.logstore.storage.{LogStorage, PublishResult}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

/**
 * A coordinated table as one call sees it: its key, and its `_delta_log` on the storage.
 *
 * @param directory the table's `_delta_log`, qualified
 */
final private[logstore] class TableLog(
    val table: TableUri,
    val directory: Path,
    fs: FileSystem,
    val storage: LogStorage
) {

  def commitFile(version: Long): Path = new Path(directory, f"$version%020d.json")

  def exists(file: Path): Boolean = fs.exists(file)

  /** The temporary file of an entry, whose path is kept relative to `_delta_log`. */
  def tempFile(tempPath: String): Path = new Path(directory, tempPath)
}

/**
 * The commit protocol (ADR-0004 §3.2), a port of the one in Delta's `BaseExternalLogStore`:
 *
 *  1. '''Prepare.''' The commit's bytes go to a temporary file, whole and on disk.
 *  1. '''Take the version.''' One row inserted in the coordination store. This is where the
 *     commit happens: from then on the version is this writer's, even if it dies.
 *  1. '''Publish.''' The temporary file gets the commit's name, at once.
 *  1. '''Acknowledge.''' The entry is marked complete.
 *
 * A writer that dies after step 2 leaves an entry that is not complete. Whoever finds it
 * finishes the commit with `finish`, which is steps 3 and 4 again: both can be repeated, by
 * anybody, any number of times. Readers find it too: `bringUpToDate` runs before every
 * listing of the log.
 *
 * Before any of that the registry must allow the write (§3.5, layer 2), and the commit before
 * this one must be published: a version is never written on top of one that is not visible.
 *
 * A commit that some writer made without this protocol is detected in three places, and taints
 * the table each time (§3.5, layers 3 and 4): in the log without an entry, when a reader lists
 * it or when a writer is about to write after it; and under the name of a commit that is being
 * published. The first leaves readers alone. The other two stop the writer, or the reader that
 * was finishing the commit: the registry and the log then disagree on what the version is.
 *
 * @param writerInstance who this writer is, for the entries it makes
 */
final private[logstore] class CommitProtocol(
    store: CoordinationStore,
    hooks: FaultHooks,
    retry: RetryPolicy,
    writerInstance: String
) {

  import CommitProtocol._

  /**
   * Writes commit `version` of the table. When it returns, the commit is visible under `target`.
   *
   * @param target the commit file, as Delta named it
   * @param context the run the calling thread commits for, if it said so: its id goes into the
   *   entry, and it is told of each version lost (ADR-0004 §3.7)
   * @throws java.nio.file.FileAlreadyExistsException the version belongs to another writer.
   *   It is the one exception Delta answers by checking for conflicts and trying the next
   *   version (ADR-0004 §8, item 2).
   * @throws WriteRefusedException the registry does not allow the write
   * @throws CommitPublicationPendingException the version is this writer's, and its file
   *   could not be published (ADR-0017)
   * @throws java.io.IOException anything else, the coordination store not answering included:
   *   nothing is written without it (fail closed)
   */
  @throws[IOException]
  def commit(
      log: TableLog,
      version: Long,
      target: Path,
      actions: Iterator[String],
      context: Option[CommitContext]
  ): Unit = {
    // Not the mutual exclusion, which is the entry. It spares a temporary file and an INSERT
    // when the version is plainly gone.
    if (log.exists(target)) throw taken(target, context)

    val lines = admit(log, version, actions, context)

    val claim =
      Claim(log.table, version, tempPathFor(target), writerInstance, context.map(_.runId))
    val temp = log.tempFile(claim.tempPath)
    log.storage.writeTemp(temp, lines)
    hooks.at(FaultPoint.BeforeInsert)

    // Repeated when it fails, because a lost reply says nothing about the INSERT. The claim's
    // temporary path is this attempt's alone, so the answer to a repeated claim is the truth:
    // `Taken` if the row is this writer's, whichever call inserted it.
    val taking = retry(s"take version $version of ${log.table}")(store.take(claim))
    taking match {
      case TakeResult.Taken =>
        hooks.at(FaultPoint.AfterInsert)
        val entry = CommitEntry(
          log.table,
          version,
          claim.tempPath,
          complete = false,
          writerInstance,
          claim.runId
        )
        val published =
          try publish(log, entry, target)
          catch {
            // Delta reads the commit file as soon as this returns, so it cannot return. The
            // commit is durable all the same: the caller is told exactly that.
            case e: IOException =>
              throw new CommitPublicationPendingException(log.table, version, e)
          }
        settle(published, entry, target)
        hooks.at(FaultPoint.AfterPublish)
        acknowledge(entry)

      case TakeResult.TakenBy(_) =>
        discard(log, temp)
        throw taken(target, context)

      // Registered a moment ago: only maintenance removes a registration.
      case TakeResult.NotRegistered =>
        discard(log, temp)
        throw new TableNotRegisteredException(log.table)
    }
  }

  /**
   * Makes the log on the storage agree with the registry before the log is listed (ADR-0004
   * §3.3). A commit exists from the moment its version is taken, so a reader that looked at
   * the storage alone could miss the latest one: that of a writer that died, or of one that is
   * about to publish.
   *
   *  - If the latest entry is not complete, its commit is published and acknowledged.
   *  - If it is complete, its file must be in the log. Otherwise the reader would be given an
   *    older table for the current one.
   *
   * A table without entries is left as it is: it is not coordinated, or has no commit yet.
   *
   * @return the version of the latest entry, if the table has entries
   * @throws CommitMissingException the file of the latest commit is not in the log
   */
  @throws[IOException]
  def bringUpToDate(log: TableLog): Option[Long] = {
    val latest = store.latestEntry(log.table)
    latest.foreach { entry =>
      if (!entry.complete) finish(log, entry)
      else if (!log.exists(log.commitFile(entry.version)))
        throw new CommitMissingException(entry.table, entry.version)
    }
    latest.map(_.version)
  }

  /**
   * Looks, in what a listing of the log returned, for a commit that no writer of this protocol
   * made, and taints the table if there is one (ADR-0004 §3.3, step 5; §3.5, layer 3). The
   * reader goes on: the log can still be read, and somebody has to look at it.
   *
   * A coordinated commit has its entry before it has its file. A commit file above the latest
   * entry is therefore a foreign one, or one whose entry was made after `latestKnown` was
   * read. So the entry is looked for again, now that the listing is done, and only its absence
   * counts. Commits below the latest entry are not examined: maintenance may prune their
   * entries, and a commit can only be taken after one that has an entry.
   *
   * @param latestKnown the version of the latest entry before the listing (`bringUpToDate`)
   * @param highestListed the highest commit version in the listing
   */
  @throws[IOException]
  def detectForeignCommit(log: TableLog, latestKnown: Option[Long], highestListed: Long): Unit =
    if (latestKnown.forall(_ < highestListed))
      // A table that is not registered is not coordinated: nothing is expected of its log.
      store.registration(log.table).foreach { registration =>
        val foreign =
          !registration.isTainted &&
            highestListed > registration.registeredVersion &&
            store.entry(log.table, highestListed).isEmpty
        if (foreign) detected(log.table, highestListed, NoEntry): Unit
      }

  /**
   * Publishes and acknowledges the commit of an entry that is not complete. It is what the
   * writer of the entry was about to do when it stopped, and any number of readers and writers
   * may do it at once: the publish replaces nothing, and an entry is completed only once.
   *
   * Here the acknowledgement must succeed. Unfinished work is looked for at the latest entry
   * only, which is right as long as nothing is ever written after an entry that is not
   * complete. (A writer's own acknowledgement may fail: its entry is the latest.)
   *
   * @throws UncoordinatedWriteException another file is under the commit's name
   * @throws UnpublishableCommitException the entry's temporary file is gone
   */
  @throws[IOException]
  def finish(log: TableLog, entry: CommitEntry): Unit = {
    val target = log.commitFile(entry.version)
    settle(publish(log, entry, target), entry, target)
    val found = retry(s"mark version ${entry.version} of ${entry.table} complete") {
      store.complete(entry.table, entry.version, entry.tempPath)
    }
    if (!found) warnOfMissingEntry(entry)
  }

  /**
   * Whether the registry lets this writer write `version`, and whether the log is ready for it.
   *
   * @return the actions to write
   */
  private def admit(
      log: TableLog,
      version: Long,
      actions: Iterator[String],
      context: Option[CommitContext]
  ): Iterator[String] = {
    val table = log.table
    val (registration, lines) = store.registration(table) match {
      case Some(found) => (found, actions)

      // The table is created through this LogStore: that is its registration. Its first
      // commit is read once more than the others, for the table's id.
      case None if version == 0 =>
        val all = actions.toVector
        val id = DeltaTableId.in(all)
        val registered = store.register(table, Registration.NotYetCreated, id) match {
          case RegisterResult.Registered =>
            Registration(table, Registration.NotYetCreated, id, taintReason = None)
          case RegisterResult.AlreadyRegistered(byAnother) => byAnother
        }
        (registered, all.iterator)

      case None => throw new TableNotRegisteredException(table)
    }

    registration.taintReason.foreach(reason => throw new TableTaintedException(table, reason))
    if (version == 0) requireNewTable(log, registration, context)
    else requirePreviousCommit(log, registration, version)
    lines
  }

  /**
   * Version 0 is being written and is not on the storage, so the table is new there. The
   * registry must know no commit of it either, or its entries belong to an earlier table at
   * the same path. (The base class's "old entries for table still exist".)
   *
   * Unless the table is on the storage by now. The registry is read after the log was looked
   * at, and a writer that created the table between the two looks is in both: its commits are
   * recorded and version 0 is in the log. That is a version this writer lost, not an earlier
   * table, and Delta is told so.
   */
  private def requireNewTable(
      log: TableLog,
      registration: Registration,
      context: Option[CommitContext]
  ): Unit = {
    val known =
      if (registration.registeredVersion != Registration.NotYetCreated)
        Some(s"the table was adopted at version ${registration.registeredVersion}")
      else
        // An entry of version 0 that is not complete is a creation in flight, not an old table.
        store
          .latestEntry(log.table)
          .filter(latest => latest.version > 0 || latest.complete)
          .map(latest => s"its commits up to version ${latest.version} are recorded")
    known.foreach { what =>
      val creation = log.commitFile(0)
      if (log.exists(creation)) throw taken(creation, context)
      throw new StaleRegistrationException(log.table, what)
    }
  }

  /** The commit before `version` must be in the log, and be one this protocol knows of. */
  private def requirePreviousCommit(
      log: TableLog,
      registration: Registration,
      version: Long
  ): Unit = {
    val table = log.table
    val adoptedAt = registration.registeredVersion
    if (version <= adoptedAt)
      throw new StaleRegistrationException(table, s"the table was adopted at version $adoptedAt")

    val previous = version - 1
    store.entry(table, previous) match {
      // Its writer stopped after taking the version. The commit exists, so it is published
      // before anything is written after it.
      case Some(unfinished) if !unfinished.complete => finish(log, unfinished)
      case Some(_) => ()
      case None =>
        val inLog = log.exists(log.commitFile(previous))
        if (inLog && previous > adoptedAt) throw detected(table, previous, NoEntry)
        // Otherwise it is the last commit made before the table was coordinated.
        if (!inLog)
          throw new IOException(
            s"cannot write version $version of $table: version $previous is not in its log"
          )
    }
  }

  /** Gives the commit its name, trying again if the storage fails. */
  private def publish(log: TableLog, entry: CommitEntry, target: Path): PublishResult =
    retry(s"publish version ${entry.version} of ${entry.table}") {
      log.storage.publish(log.tempFile(entry.tempPath), target)
    }

  /** What the outcome of a publish means for the commit. */
  private def settle(published: PublishResult, entry: CommitEntry, target: Path): Unit =
    published match {
      case PublishResult.Published | PublishResult.AlreadyPublished => ()
      case PublishResult.ForeignFile =>
        throw detected(
          entry.table,
          entry.version,
          s"$target is not the commit that took the version (its bytes are in " +
            s"${entry.tempPath}), so a writer without this LogStore made it"
        )
      case PublishResult.TempFileLost =>
        throw new UnpublishableCommitException(entry.table, entry.version, entry.tempPath)
    }

  /**
   * Marks the entry complete. If that fails the commit is no less visible, so the failure is
   * logged and goes no further: whoever finds the entry incomplete finishes it (ADR-0017).
   */
  private def acknowledge(entry: CommitEntry): Unit =
    try if (!store.complete(entry.table, entry.version, entry.tempPath)) warnOfMissingEntry(entry)
    catch {
      case e: IOException =>
        logger.warn(
          s"Version ${entry.version} of ${entry.table} is published. Marking its entry " +
            "complete failed, and is left to the next reader or writer of the table",
          e
        )
    }

  /**
   * Taints the table: a commit that this protocol did not make is in its log, so every write
   * is refused from now on, until an operator has verified the table and adopted it again
   * (ADR-0004 §3.5). It is said at ERROR, and recorded with who found it.
   *
   * If the taint cannot be recorded, the detection stands: a writer stops on it all the same,
   * and the next reader or writer that meets the commit records it.
   *
   * @return the detection, for a writer to throw
   */
  private def detected(
      table: TableUri,
      version: Long,
      found: String
  ): UncoordinatedWriteException = {
    val detection = new UncoordinatedWriteException(table, version, found)
    logger.error(
      "{}. The table is tainted: it refuses writes until an operator verifies and re-adopts it.",
      detection.getMessage
    )
    try store.taint(table, s"version $version: $found (found by $writerInstance)")
    catch {
      case e: IOException =>
        detection.addSuppressed(e)
        logger.error(s"The taint of $table could not be recorded", e)
    }
    detection
  }

  /** Only maintenance removes an entry, and not while the table is being written. */
  private def warnOfMissingEntry(entry: CommitEntry): Unit =
    logger.warn(
      "Version {} of {} is published, and its coordination entry is gone",
      entry.version,
      entry.table
    )

  /** Removes a temporary file that no entry points at. If it stays, it is only litter. */
  private def discard(log: TableLog, temp: Path): Unit =
    try log.storage.discard(temp)
    catch {
      case e: IOException =>
        logger.warn(s"Could not remove the temporary file $temp, which nothing uses", e)
    }
}

private[logstore] object CommitProtocol {

  private val logger = LoggerFactory.getLogger(classOf[CommitProtocol])

  /** What is wrong with a commit that is in the log and has no entry above the adoption. */
  private val NoEntry =
    "the commit has no coordination entry, so a writer without this LogStore made it"

  /** The directory of temporary files, inside `_delta_log`. */
  val TempDirectory = ".tmp"

  /** A name for a temporary file of `file` that no other attempt has, relative to its directory. */
  def tempPathFor(file: Path): String = s"$TempDirectory/${file.getName}.${UUID.randomUUID()}"

  /**
   * The version went to another writer, and the run is told. The writer learns it from the
   * commit file or from the entry, depending on how far the winner had got: that says nothing
   * about the contention, so both count.
   */
  private def taken(target: Path, context: Option[CommitContext]): FileAlreadyExistsException = {
    context.foreach(_.versionLost())
    new FileAlreadyExistsException(target.toString)
  }
}

/**
 * The table's id (`metaData.id`), read from the actions of its first commit, for the registry.
 *
 * The module has no JSON parser and does not need one for this. Delta writes `id` as the first
 * field of `metaData`; a line of any other shape gives no id, which the registry allows.
 */
private[logstore] object DeltaTableId {

  private val MetaDataWithId = """^\{"metaData":\{"id":"([0-9A-Za-z_-]{1,64})"[,}].*""".r

  def in(actions: Seq[String]): Option[String] =
    actions.collectFirst { case MetaDataWithId(id) => id }
}
