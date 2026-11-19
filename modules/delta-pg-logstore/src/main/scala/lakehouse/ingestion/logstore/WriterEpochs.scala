package lakehouse.ingestion.logstore

import java.io.IOException

import lakehouse.ingestion.logstore.coordination.WriterEpoch
import org.apache.hadoop.conf.Configuration

/**
 * Where an application says which of its writers has the turn at some work (ADR-0019).
 *
 * A writer that takes the work over announces its epoch here before it asks a table anything,
 * and then commits inside a context that names the epoch (`CommitContext.forRun`). From the
 * announcement on, a writer under an earlier epoch of the same key commits nothing: what the
 * tables say of its work no longer changes behind whoever asks them.
 *
 * {{{
 * val epoch = WriterEpoch(leaseKey, leaseEpoch)
 * if (WriterEpochs.announce(spark.sessionState.newHadoopConf(), epoch) != epoch.epoch) stop()
 * val commits = CommitContext.forRun(runId, epoch)
 * commits.attribute { rows.write.format("delta").mode("append").save(table) }
 * }}}
 */
object WriterEpochs {

  /**
   * Announces `epoch` in the coordination store that the LogStores of this JVM with the
   * settings of `conf` commit through.
   *
   * Once it has returned, every commit under an earlier epoch of the key either took its
   * version before, and is in the log for whoever lists it from now on, or is refused.
   *
   * @param conf the Hadoop configuration that Delta hands the LogStore: that of the session
   * @return the epoch in force under the key: that of `epoch`, or a later one that was
   *   announced before, which says that this writer has lost its turn already
   * @throws java.io.IOException the settings of the store are not usable, or the store did not
   *   answer: nothing is known to have been announced
   */
  @throws[IOException]
  def announce(conf: Configuration, epoch: WriterEpoch): Long =
    PostgresLogStore.sharedCoordination(conf).announce(epoch)
}
