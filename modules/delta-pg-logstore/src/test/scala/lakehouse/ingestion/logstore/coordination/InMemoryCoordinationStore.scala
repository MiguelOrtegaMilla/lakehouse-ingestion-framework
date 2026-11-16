package lakehouse.ingestion.logstore.coordination

import java.io.IOException

/**
 * A `CoordinationStore` in one JVM's memory, for tests of the protocol that need no database,
 * and the `RegistryMaintenance` of the same registry, for tests of an operator's work.
 *
 * Every operation runs under one lock, which is the simplest way to be atomic and linearised.
 * It passes the same `CoordinationStoreContract` and `RegistryMaintenanceContract` as the
 * PostgreSQL implementations: that is what makes a test written against it worth trusting.
 */
final class InMemoryCoordinationStore extends CoordinationStore with RegistryMaintenance {

  private var registrations = Map.empty[TableUri, Registration]
  private var entries = Map.empty[(TableUri, Long), CommitEntry]
  private var lastGeneration = 0L

  /** A generation that no registration of this registry has had. Called under the lock. */
  private def nextGeneration(): Long = {
    lastGeneration += 1
    lastGeneration
  }

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
            Registration(table, registeredVersion, deltaTableId, None, nextGeneration())
          registrations += table -> registration
          RegisterResult.Registered(registration)
      }
    }
  }

  override def entry(table: TableUri, version: Long): Option[CommitEntry] =
    synchronized(entries.get((table, version)))

  override def latestEntry(table: TableUri): Option[CommitEntry] =
    synchronized(entries.values.filter(_.table == table).maxByOption(_.version))

  override def take(claim: Claim): TakeResult = synchronized {
    val key = (claim.table, claim.version)
    registrations.get(claim.table) match {
      case None => TakeResult.NotRegistered
      case Some(registration) =>
        entries.get(key) match {
          case Some(owner) if owner.tempPath == claim.tempPath => TakeResult.Taken
          case Some(owner) => TakeResult.TakenBy(owner)
          case None if registration.generation != claim.generation =>
            TakeResult.RegistrationChanged
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

  override def unfinished(table: TableUri): Seq[CommitEntry] = synchronized {
    entries.values.filter(entry => entry.table == table && !entry.complete).toSeq.sortBy(_.version)
  }

  override def contradicted(table: TableUri, version: Long): Seq[CommitEntry] = synchronized {
    entries.values
      .filter(entry => entry.table == table && (entry.version > version || !entry.complete))
      .toSeq
      .sortBy(_.version)
  }

  override def adopt(table: TableUri, version: Long, deltaTableId: Option[String]): Boolean = {
    require(version >= 0, s"a table is adopted at a version it has, not at $version")
    synchronized {
      val isNew = !registrations.contains(table)
      if (isNew)
        registrations += table ->
          Registration(table, version, deltaTableId, taintReason = None, nextGeneration())
      isNew
    }
  }

  override def readopt(
      table: TableUri,
      version: Long,
      deltaTableId: Option[String],
      examined: Seq[CommitEntry]
  ): RegistryChange[Readoption] = {
    require(version >= 0, s"a table is adopted at a version it has, not at $version")
    synchronized {
      registrations.get(table) match {
        case None => RegistryChange.NotRegistered
        case Some(registered) =>
          val now = contradicted(table, version)
          if (now.toSet != examined.toSet) RegistryChange.EntriesChanged(now)
          else {
            entries --= now.map(entry => (entry.table, entry.version))
            registrations += table -> Registration(
              table,
              version,
              deltaTableId.orElse(registered.deltaTableId),
              taintReason = None,
              nextGeneration()
            )
            RegistryChange.Applied(Readoption(registered.taintReason, now))
          }
      }
    }
  }

  override def forget(table: TableUri, examined: Seq[CommitEntry]): RegistryChange[Unit] =
    synchronized {
      if (!registrations.contains(table)) RegistryChange.NotRegistered
      else {
        val now = unfinished(table)
        if (now.toSet != examined.toSet) RegistryChange.EntriesChanged(now)
        else {
          registrations -= table
          entries = entries.filterNot { case ((owner, _), _) => owner == table }
          RegistryChange.Applied(())
        }
      }
    }
}
