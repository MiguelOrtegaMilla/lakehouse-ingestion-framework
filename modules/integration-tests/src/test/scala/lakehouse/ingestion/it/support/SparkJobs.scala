package lakehouse.ingestion.it.support

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.jdk.CollectionConverters._

import org.apache.spark.scheduler.{
  JobSucceeded,
  SparkListener,
  SparkListenerJobEnd,
  SparkListenerJobStart
}
import org.apache.spark.sql.SparkSession

/**
 * Records that take their time, for the tests that stop a job of Spark that is slow for real,
 * and that then ask what its tasks did. The tasks of a local session run in the JVM of the
 * test, so they count here; each test counts under a pace of its own.
 */
object Pace {

  private val begun = new ConcurrentHashMap[String, AtomicLong]
  private val finished = new ConcurrentHashMap[String, AtomicLong]
  private val broken = new ConcurrentHashMap[String, AtomicLong]

  private def count(in: ConcurrentHashMap[String, AtomicLong], pace: String): AtomicLong =
    in.computeIfAbsent(pace, _ => new AtomicLong)

  /** A pace that no other test counts under. */
  def fresh(): String = s"pace-${UUID.randomUUID()}"

  /** The records of a pace that a task started on. */
  def started(pace: String): Long = count(begun, pace).get()

  /** The records of a pace that a task got through. */
  def done(pace: String): Long = count(finished, pace).get()

  /** The records of a pace that a task was interrupted in the middle of. */
  def interrupted(pace: String): Long = count(broken, pace).get()

  /** How many records the tasks of a pace got through in the second that follows. */
  def doneInTheNextSecond(pace: String): Long = {
    val before = done(pace)
    Thread.sleep(1000)
    done(pace) - before
  }

  /** A record that takes `millis`, and says under `pace` how it went. */
  def paced(pace: String, millis: Long): Int => Int = { record =>
    count(begun, pace).incrementAndGet()
    try Thread.sleep(millis)
    catch {
      case e: InterruptedException =>
        count(broken, pace).incrementAndGet()
        throw e
    }
    count(finished, pace).incrementAndGet()
    record
  }
}

/**
 * The jobs of a session, as Spark says them to a listener: each with its tags, and how it
 * ended. A listener hears of a job some time after it happened, and in the order in which
 * things happened: `settle` waits until what happened so far has been heard.
 */
final class HeardJobs(spark: SparkSession) {
  import HeardJobs._

  private val starts = new ConcurrentLinkedQueue[Started]
  private val ends = new ConcurrentHashMap[Int, Boolean]

  spark.sparkContext.addSparkListener(new SparkListener {
    override def onJobStart(job: SparkListenerJobStart): Unit = {
      val tags = Option(job.properties).flatMap(p => Option(p.getProperty(JobTags)))
      starts.add(Started(job.jobId, tags.fold(Set.empty[String])(_.split(',').toSet))): Unit
    }
    override def onJobEnd(job: SparkListenerJobEnd): Unit =
      ends.put(job.jobId, job.jobResult == JobSucceeded): Unit
  })

  /** The jobs that started, in the order they did. The jobs of `settle` are not among them. */
  def started: Seq[Started] =
    starts.asScala.toSeq.filterNot(_.tags.exists(_.startsWith(OwnTag)))

  /** The jobs that started with this tag, in the order they did. */
  def tagged(tag: String): Seq[Started] = started.filter(_.tags.contains(tag))

  /** Whether a job ended well; none, if it has not been heard to end. */
  def succeeded(job: Int): Option[Boolean] = Option(ends.get(job)).map(_.booleanValue)

  /**
   * Waits until every job that ended before this was asked has been heard of. A job of its own
   * is run and heard to end: what happened before it was said before it.
   */
  def settle(): Unit = {
    val tag = s"$OwnTag${UUID.randomUUID()}"
    spark.sparkContext.addJobTag(tag)
    try spark.sparkContext.parallelize(Seq(1), 1).count(): Unit
    finally spark.sparkContext.removeJobTag(tag)

    val deadline = System.nanoTime() + SettleNanos
    def heard: Boolean = starts.asScala.exists(job => job.tags(tag) && ends.containsKey(job.id))
    while (!heard) {
      if (System.nanoTime() > deadline)
        throw new IllegalStateException("the listener did not hear of a job that ended")
      Thread.sleep(10)
    }
  }
}

object HeardJobs {

  /** @param tags the tags of the thread that started the job */
  final case class Started(id: Int, tags: Set[String])

  /** The property of a job that has its tags, as Spark writes them: one text, with commas. */
  val JobTags = "spark.job.tags"

  private val OwnTag = "heard-"
  private val SettleNanos = 30L * 1000 * 1000 * 1000
}
