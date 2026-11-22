package lakehouse.ingestion.engine

import java.io.IOException

import lakehouse.ingestion.core.control.RunStart
import lakehouse.ingestion.logstore.WriterEpochs
import lakehouse.ingestion.logstore.coordination.WriterEpoch
import org.apache.hadoop.conf.Configuration

/**
 * Says to the LogStore that the epoch of a run is the one in force for its lease (ADR-0019), for
 * every run that holds a lease: one that ingests, and an operator's (ADR-0021 §1). Once it has, a
 * commit of a run under an earlier epoch is in its table, where the run finds it, or will never
 * be.
 *
 * A later epoch in force has one of two authors. A run of this control plane that took the lease
 * over: it ended this run, as `ABANDONED`, in the transaction that took the lease. Or another
 * control plane, whose epochs the coordination store remembers, as it does when this one was
 * made anew over it (ADR-0019): then the lease is still this run's, nobody ended it, and every
 * run of the source will be told the same until an operator forgets those epochs. The control
 * plane tells the two apart: the run asks it to record that it failed for good, with the message
 * that this gives. Taken over, the run was ended already, and it has lost its lease (3); not
 * taken over, the run is recorded as failed, its lease is given up, and it reports a failure that
 * running again does not mend (4).
 */
private[engine] object EpochAnnouncement {

  /**
   * @return why the run cannot go on, if a later epoch is in force: what it is to fail for good
   *   with, unless the control plane says that it was ended already
   * @throws RunFailedException a transient failure, if the coordination store could not be told
   */
  def announce(hadoop: Configuration, start: RunStart, turn: WriterEpoch): Either[String, Unit] = {
    val inForce =
      try WriterEpochs.announce(hadoop, turn)
      catch {
        case e: IOException =>
          throw RunFailedException.transient(
            s"the epoch of the lease of ${start.leaseKey} could not be announced to the " +
              s"coordination store, and nothing was read or written: ${e.getMessage}",
            e
          )
      }
    Either.cond(
      inForce == turn.epoch,
      (),
      s"the coordination store has the epoch $inForce in force for ${start.leaseKey}, and " +
        s"this run holds its lease under ${turn.epoch}, which no run of this control plane " +
        "gave up to a later one: the store remembers the epochs of another control plane, " +
        "as it does when this one was made anew over it. Every run of the source is " +
        "refused until an operator forgets them with the maintenance role (ADR-0019): " +
        "DELETE FROM delta_coordination.writer_epoch WHERE epoch_key = " +
        s"'${start.leaseKey}'. Nothing was read or written"
    )
  }
}
