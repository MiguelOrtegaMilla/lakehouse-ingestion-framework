package lakehouse.ingestion.engine

import lakehouse.ingestion.core.control.RunId
import org.apache.spark.sql.SparkSession

/**
 * The jobs of Spark that one run starts, marked as its own, so that they can be stopped the
 * moment the run loses its lease (ADR-0006) and no other job of the process is.
 *
 * '''A tag, and not a job group.''' A thread has one job group, which whatever runs on it may
 * set again, and any number of tags. The tag is the run's id, so stopping the jobs of a run
 * stops no job of another run of the same process.
 *
 * '''What is stopped is what is running.''' A job that the run starts afterwards is not: a run
 * asks for its lease before each step that starts one (`LeasedRun.verify`).
 *
 * '''No thread is interrupted.''' Spark stops a task between two of its records, which is soon
 * enough, and a task that is interrupted in the middle of what it does may leave more than
 * itself broken: a log line, or a file that others read. A query of Spark SQL would have its
 * tasks interrupted (`spark.sql.execution.interruptOnCancel` is on unless a session says
 * otherwise), so the thread of the run says that its jobs are not to be, which Spark SQL then
 * leaves as it is.
 *
 * What is assumed of Spark here is pinned by a test of its own, against the version that the
 * build has (`SparkJobCancellationContractSpec`).
 */
final class RunJobs(spark: SparkSession, run: RunId) {
  import RunJobs._

  private val context = spark.sparkContext

  /** What every job of the run is marked with. */
  val tag: String = s"$TagPrefix$run"

  /**
   * Runs `body` with every job that it starts marked as the run's. The mark is the thread's:
   * `body` is the run, on the thread that the run works on. Spark hands it on to the threads
   * that work for that one.
   */
  def during[T](body: => T): T = {
    val interrupting = context.getLocalProperty(InterruptOnCancel)
    context.addJobTag(tag)
    context.setInterruptOnCancel(false)
    try body
    finally {
      context.removeJobTag(tag)
      context.setLocalProperty(InterruptOnCancel, interrupting)
    }
  }

  /**
   * Stops the jobs of the run that are running, from any thread. It waits for nothing: it is
   * asked on the thread that found the lease lost, which has nothing else to wait for.
   *
   * @param reason what the job that is stopped fails with, for whoever reads its error
   */
  def stop(reason: String): Unit = context.cancelJobsWithTag(tag, reason)
}

object RunJobs {

  /** Before the id of a run, in the tag of its jobs. */
  val TagPrefix = "run-"

  /** The property of a thread that says whether the tasks of its jobs are interrupted. */
  private val InterruptOnCancel = "spark.job.interruptOnCancel"
}
