package lakehouse.ingestion.logstore

import java.io.IOException
import java.util.concurrent.atomic.{AtomicIntegerArray, AtomicLongArray}

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

  val Off: FaultHooks = _ => ()

  /** The setting that makes a writer wait at `point`; under Spark, with `spark.hadoop.` first. */
  def delayKey(point: FaultPoint): String = s"lakehouse.logstore.fault.${point.key}.delayMs"

  /**
   * The hooks a configuration selects. A configuration can only make a writer wait, which is
   * how the stress test widens the windows between the steps of a commit in writers that are
   * separate JVMs (ADR-0004 §7.3), and how a test holds a process at a point to kill it there
   * (P7.10). Stopping or failing a writer takes a test's own hooks.
   *
   * The first wait at each point is said, as it begins: a test that kills a writer in it knows
   * from what the process printed that it is there. The waits after it are not, so that the many
   * commits of a stress run do not fill its logs; a LogStore has hooks of its own.
   */
  @throws[IOException]
  def from(conf: Configuration): FaultHooks = {
    val delays = delaysIn(conf).toMap
    val said = new AtomicIntegerArray(FaultPoint.All.size)
    if (delays.isEmpty) Off
    else
      point =>
        delays.get(point).foreach { millis =>
          if (said.compareAndSet(FaultPoint.All.indexOf(point), 0, 1))
            logger.warn(
              "A writer waits {} ms at {}, as {} asks; the waits after it at that point are not " +
                "said. Fault hooks are for tests only.",
              millis,
              point.key,
              delayKey(point)
            )
          Thread.sleep(millis)
          waits.incrementAndGet(FaultPoint.All.indexOf(point)): Unit
        }
  }

  private val logger = LoggerFactory.getLogger(classOf[FaultHooks])

  private val waits = new AtomicLongArray(FaultPoint.All.size)

  /**
   * How many times writers of this JVM have waited at `point` because a configuration asked for
   * it. A run that widens a window with these hooks reports the count, to show that the wait
   * was in the path of the commits it measured: one that was not would prove nothing.
   */
  def waitsAt(point: FaultPoint): Long = waits.get(FaultPoint.All.indexOf(point))

  /**
   * The waits a configuration asks for, in milliseconds, in the order of the protocol. A
   * LogStore announces them when it is created, so that none stays on unnoticed.
   */
  @throws[IOException]
  def delaysIn(conf: Configuration): Seq[(FaultPoint, Long)] =
    FaultPoint.All.flatMap { point =>
      Option(conf.get(delayKey(point))).map(_.trim).filter(_.nonEmpty).map { text =>
        val millis = text.toLongOption.filter(_ >= 0).getOrElse {
          throw new IOException(s"${delayKey(point)} must be a number of milliseconds, 0 or more")
        }
        point -> millis
      }
    }
}
