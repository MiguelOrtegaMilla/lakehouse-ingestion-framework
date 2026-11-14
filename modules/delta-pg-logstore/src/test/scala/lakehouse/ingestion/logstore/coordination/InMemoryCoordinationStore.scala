package lakehouse.ingestion.logstore.coordination

import java.io.IOException

/**
 * A `CoordinationStore` in one JVM's memory, for tests of the protocol that need no database.
 *
 * Every operation runs under one lock, which is the simplest way to be atomic and linearised.
 * It passes the same `CoordinationStoreContract` as the PostgreSQL implementation: that is what
 * makes a protocol test written against it worth trusting.
 */
final class InMemoryCoordinationStore extends CoordinationStore {

  private var registrations = Map.empty[TableUri, Registration]
  private var entries = Map.empty[(TableUri, Long), CommitEntry]

  override def registration(table: TableUri): Option[Registration] =
    synchronized(registrations.get(table))

  override def register(
      table: TableUri,
      registeredVersion: Long,
      deltaTableId: Option[String]
  ): RegisterResult = {
    require(
      registeredVersion >= Registration.NotYetCreated,
      s"not a registered version: $registeredVersion"
    )
    synchronized {
      registrations.get(table) match {
        case Some(existing) => RegisterResult.AlreadyRegistered(existing)
        case None =>
          val registration =
            Registration(table, registeredVersion, deltaTableId, taintReason = None)
          registrations += table -> registration
          RegisterResult.Registered
      }
    }
  }

  override def entry(table: TableUri, version: Long): Option[CommitEntry] =
    synchronized(entries.get((table, version)))

  override def latestEntry(table: TableUri): Option[CommitEntry] =
    synchronized(entries.values.filter(_.table == table).maxByOption(_.version))

  override def take(claim: Claim): TakeResult = synchronized {
    val key = (claim.table, claim.version)
    if (!registrations.contains(claim.table)) TakeResult.NotRegistered
    else
      entries.get(key) match {
        case Some(owner) if owner.tempPath == claim.tempPath => TakeResult.Taken
        case Some(owner) => TakeResult.TakenBy(owner)
        case None =>
          entries += key -> CommitEntry(
            claim.table,
            claim.version,
            claim.tempPath,
            complete = false,
            claim.writerInstance,
            claim.runId
          )
          TakeResult.Taken
      }
  }

  override def complete(table: TableUri, version: Long, tempPath: String): Boolean =
    synchronized {
      entries.get((table, version)).filter(_.tempPath == tempPath) match {
        case Some(owned) =>
          entries += (table, version) -> owned.copy(complete = true)
          true
        case None => false
      }
    }

  override def taint(table: TableUri, reason: String): Unit = synchronized {
    registrations.get(table) match {
      case Some(registration) if registration.isTainted => ()
      case Some(registration) =>
        registrations += table -> registration.copy(taintReason = Some(reason))
      case None => throw new IOException(s"cannot taint $table: it is not registered")
    }
  }
}
