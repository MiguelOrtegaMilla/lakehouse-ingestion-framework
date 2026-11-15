package lakehouse.ingestion.logstore

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

/**
 * A point of the commit protocol where a test makes a writer stop, wait or fail (ADR-0004
 * §7.1). Each is the border between two of the states a crash can leave behind (§3.8).
 *
 * @param key its name in a configuration key
 */
sealed abstract class FaultPoint(val key: String)

object FaultPoint {

  /** The temporary file is written and the version is still free: nothing has happened yet. */
  case object BeforeInsert extends FaultPoint("beforeInsert")

  /** The version is taken: the commit is durable, and not visible yet. */
  case object AfterInsert extends FaultPoint("afterInsert")

  /** The commit is visible, and its entry is not marked complete yet. */
  case object AfterPublish extends FaultPoint("afterPublish")

  val All: Seq[FaultPoint] = Seq(BeforeInsert, AfterInsert, AfterPublish)
}

/** What a writer does when it reaches a fault point. Outside tests: nothing. */
trait FaultHooks {
  def at(point: FaultPoint): Unit
}

object FaultHooks {

  private val logger = LoggerFactory.getLogger(classOf[FaultHooks])

  val Off: FaultHooks = _ => ()

  /** The setting that makes a writer wait at `point`; under Spark, with `spark.hadoop.` first. */
  def delayKey(point: FaultPoint): String = s"lakehouse.logstore.fault.${point.key}.delayMs"

  /**
   * The hooks a configuration selects. A configuration can only make a writer wait, which is
   * how the stress test widens the windows between the steps of a commit in writers that are
   * separate JVMs (ADR-0004 §7.3). Stopping or failing a writer takes a test's own hooks.
   */
  @throws[IOException]
  def from(conf: Configuration): FaultHooks = {
    val delays = FaultPoint.All.flatMap { point =>
      Option(conf.get(delayKey(point))).map(_.trim).filter(_.nonEmpty).map { text =>
        val millis = text.toLongOption.filter(_ >= 0).getOrElse {
          throw new IOException(s"${delayKey(point)} must be a number of milliseconds, 0 or more")
        }
        point -> millis
      }
    }.toMap

    if (delays.isEmpty) Off
    else {
      logger.warn(
        "Every commit waits at {}: fault hooks are configured, which is for tests only",
        delays.map { case (point, millis) => s"${point.key} (${millis} ms)" }.mkString(", ")
      )
      point => delays.get(point).foreach(Thread.sleep)
    }
  }
}
