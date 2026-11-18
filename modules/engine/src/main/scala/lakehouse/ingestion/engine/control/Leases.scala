package lakehouse.ingestion.engine.control

import java.sql.{Connection, SQLException}
import java.time.{Instant, OffsetDateTime}
import java.util.UUID

import scala.concurrent.duration.FiniteDuration
import scala.util.Using

import lakehouse.ingestion.core.control.{Epoch, InstanceId, LeaseKey, RunId}

/**
 * The leases of sources (ADR-0006): who may advance the state of a source in a layer, now.
 *
 * A lease is a row. It is taken with a time at which it expires, and renewed by whoever holds
 * it; a holder that dies renews nothing, and its lease is there to be taken when it expires.
 * Every time is the database's: no two instances have to agree on a clock.
 *
 * Each taking counts one more epoch, and what a holder does to its lease it does on condition
 * of its epoch. A holder whose lease was taken by another changes nothing, and is told so.
 *
 * Every method runs on the connection it is given, so that the caller decides what is one
 * transaction: a lease is taken together with the run that takes it (`LeasedRun`).
 */
object Leases {

  /** Who holds a lease, and until when. */
  final case class Holder(run: RunId, instance: InstanceId, acquiredAt: Instant, expiresAt: Instant)

  /**
   * A lease as the database has it.
   *
   * @param epoch how many times it has been taken; 0 for one that never was
   * @param holder none, for one that was released or never taken. A holder is still there after
   *   its lease expired, until another takes it
   */
  final case class Lease(key: LeaseKey, epoch: Long, holder: Option[Holder])

  sealed trait Acquisition

  object Acquisition {

    /**
     * The lease is the run's, under this epoch.
     *
     * @param tookOver the run that held it and let it expire, if one did: it died, or stopped
     *   renewing, and ended halfway
     */
    final case class Acquired(epoch: Epoch, expiresAt: Instant, tookOver: Option[RunId])
        extends Acquisition

    /** Another run holds the lease, and it has not expired. Nothing was changed. */
    final case class Busy(holder: Holder) extends Acquisition
  }

  /**
   * Takes the lease of `key` for `run`, if it is free or has expired.
   *
   * The row of the lease is locked from the moment it is looked at to the end of the
   * transaction, so of the runs that ask at the same moment one is answered `Acquired`, and
   * the others wait for it and are answered `Busy`.
   *
   * Whether a lease has expired is judged by the time at which the transaction started. A
   * transaction that waited for the row then judges by an earlier time than the real one, and
   * may find busy a lease that has just expired: it errs on the side of not taking it.
   *
   * To be called in a transaction that also writes the run: the holder of a lease is a run that
   * exists when the transaction ends.
   */
  @throws[SQLException]
  def acquire(
      connection: Connection,
      key: LeaseKey,
      run: RunId,
      instance: InstanceId,
      ttl: FiniteDuration
  ): Acquisition = {
    // The row is made the first time a key is asked for, by whoever asks first.
    Using.resource(
      connection.prepareStatement(
        "INSERT INTO control.source_lease (lease_key) VALUES (?) ON CONFLICT (lease_key) DO NOTHING"
      )
    ) { statement =>
      statement.setString(1, key.value)
      statement.executeUpdate(): Unit
    }

    val (free, holder) = Using.resource(
      connection.prepareStatement(
        s"""SELECT $Columns, (holder_run_id IS NULL OR expires_at < now()) AS free
           |  FROM control.source_lease WHERE lease_key = ? FOR UPDATE""".stripMargin
      )
    ) { statement =>
      statement.setString(1, key.value)
      Using.resource(statement.executeQuery()) { row =>
        if (!row.next())
          throw new IllegalStateException(s"the lease $key is gone from under its lock")
        (row.getBoolean("free"), lease(row).holder)
      }
    }

    holder match {
      case Some(other) if !free => Acquisition.Busy(other)
      case previous =>
        Using.resource(
          connection.prepareStatement(
            """UPDATE control.source_lease
              |   SET holder_run_id = ?, instance_id = ?, epoch = epoch + 1,
              |       acquired_at = now(), expires_at = now() + (? * interval '1 millisecond')
              | WHERE lease_key = ?
              |RETURNING epoch, expires_at""".stripMargin
          )
        ) { statement =>
          statement.setObject(1, run.uuid)
          statement.setString(2, instance.value)
          statement.setLong(3, ttl.toMillis)
          statement.setString(4, key.value)
          Using.resource(statement.executeQuery()) { row =>
            if (!row.next())
              throw new IllegalStateException(s"the lease $key is gone from under its lock")
            Acquisition.Acquired(
              epoch = known(key, "epoch", Epoch.of(row.getLong("epoch"))),
              expiresAt = row.getObject("expires_at", classOf[OffsetDateTime]).toInstant,
              tookOver = previous.map(_.run)
            )
          }
        }
    }
  }

  /**
   * Holds the lease for its time to live again, counted from now, if it is still `run`'s under
   * `epoch`.
   *
   * @return false if it is not: it expired and another run took it, or it was released. The
   *   run that asked is a zombie, and is told so
   */
  @throws[SQLException]
  def renew(
      connection: Connection,
      key: LeaseKey,
      run: RunId,
      epoch: Epoch,
      ttl: FiniteDuration
  ): Boolean =
    Using.resource(
      connection.prepareStatement(
        """UPDATE control.source_lease SET expires_at = now() + (? * interval '1 millisecond')
          | WHERE lease_key = ? AND holder_run_id = ? AND epoch = ?""".stripMargin
      )
    ) { statement =>
      statement.setLong(1, ttl.toMillis)
      statement.setString(2, key.value)
      statement.setObject(3, run.uuid)
      statement.setLong(4, epoch.value)
      statement.executeUpdate() == 1
    }

  /**
   * Gives the lease up, if it is still `run`'s under `epoch`: the next run has it at once, and
   * does not wait for it to expire. Its epoch stays, for the next taking to count from.
   *
   * @return false if it was not this run's to give up, and nothing was changed
   */
  @throws[SQLException]
  def release(connection: Connection, key: LeaseKey, run: RunId, epoch: Epoch): Boolean =
    Using.resource(
      connection.prepareStatement(
        """UPDATE control.source_lease
          |   SET holder_run_id = NULL, instance_id = NULL, acquired_at = NULL, expires_at = NULL
          | WHERE lease_key = ? AND holder_run_id = ? AND epoch = ?""".stripMargin
      )
    ) { statement =>
      statement.setString(1, key.value)
      statement.setObject(2, run.uuid)
      statement.setLong(3, epoch.value)
      statement.executeUpdate() == 1
    }

  /** The lease of `key`, if it was ever asked for. */
  @throws[SQLException]
  def find(connection: Connection, key: LeaseKey): Option[Lease] =
    Using.resource(
      connection.prepareStatement(s"SELECT $Columns FROM control.source_lease WHERE lease_key = ?")
    ) { statement =>
      statement.setString(1, key.value)
      Using.resource(statement.executeQuery())(row => Option.when(row.next())(lease(row)))
    }

  private val Columns = "lease_key, epoch, holder_run_id, instance_id, acquired_at, expires_at"

  private def lease(row: java.sql.ResultSet): Lease = {
    val key = LeaseKey
      .parse(row.getString("lease_key"))
      .fold(
        reason => throw new IllegalStateException(s"control.source_lease.lease_key $reason"),
        identity
      )
    def instant(column: String) = row.getObject(column, classOf[OffsetDateTime]).toInstant

    // Held whole or not at all: the table sees to it.
    val holder = Option(row.getObject("holder_run_id", classOf[UUID])).map { run =>
      Holder(
        run = known(key, "holder_run_id", RunId.of(run)),
        instance = known(key, "instance_id", InstanceId.parse(row.getString("instance_id"))),
        acquiredAt = instant("acquired_at"),
        expiresAt = instant("expires_at")
      )
    }
    Lease(key, row.getLong("epoch"), holder)
  }

  private def known[A](key: LeaseKey, column: String, value: Either[String, A]): A =
    value.fold(
      reason => throw new IllegalStateException(s"control.source_lease.$column of $key $reason"),
      identity
    )
}
