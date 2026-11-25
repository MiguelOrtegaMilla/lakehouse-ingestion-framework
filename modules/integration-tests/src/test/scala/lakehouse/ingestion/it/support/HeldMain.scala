package lakehouse.ingestion.it.support

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration._

import lakehouse.ingestion.cli.{Main, Stop}
import lakehouse.ingestion.engine.{FaultHooks, FaultPoint}
import lakehouse.ingestion.logstore.{FaultPoint => LogStorePoint}

/**
 * The application, held at a fault point to be killed there (discovery §17, P7.10): what a test
 * then does to it, `kill -9`, it does to a process in the middle of a run.
 *
 *  - `HeldMain --hold <point> -- <command line>`, at a point of the engine: the first time a run
 *    of the command reaches it, the process says so on its standard output (`heldAt`), and
 *    waits there for ever.
 *  - `HeldMain --wait-in <point> -- <command line>`, at a point of the LogStore, inside a commit:
 *    a writer waits there for `WaitInside`, and the LogStore says so the first time
 *    (`waitingIn`).
 *
 * Everything else is `Main`'s, signals included: the hooks and the waits alone are this
 * launcher's, and no argument of `Main` gives them.
 */
object HeldMain {

  /** What the process says, and then the batch, once it is held: a test waits for it. */
  def heldAt(point: FaultPoint): String = s"held at $point of"

  /** How long a writer waits at its point of the LogStore: far longer than a test takes. */
  val WaitInside: FiniteDuration = 10.minutes

  /** What the LogStore says the first time that a writer waits at the point. */
  def waitingIn(point: LogStorePoint): String =
    s"A writer waits ${WaitInside.toMillis} ms at ${point.key},"

  def main(args: Array[String]): Unit = args.toList match {
    case "--wait-in" :: name :: "--" :: command =>
      val point = LogStorePoint.All
        .find(_.key == name)
        .getOrElse(
          throw new IllegalArgumentException(
            s"no point of the LogStore is named $name: ${LogStorePoint.All.map(_.key).mkString(", ")} are"
          )
        )
      sys.exit(
        Main.run(
          command,
          System.out,
          System.err,
          sys.env.get,
          Stop.onSignals(System.err),
          waits = Map(point -> WaitInside)
        )
      )
    case "--hold" :: name :: "--" :: command =>
      val point = FaultPoint.All
        .find(_.name == name)
        .getOrElse(
          throw new IllegalArgumentException(
            s"no fault point is named $name: ${FaultPoint.All.mkString(", ")} are"
          )
        )
      sys.exit(
        Main.run(
          command,
          System.out,
          System.err,
          sys.env.get,
          Stop.onSignals(System.err),
          hooks = holding(point)
        )
      )
    case _ =>
      throw new IllegalArgumentException(
        "usage: HeldMain --hold <point> -- <command line>, or --wait-in <point> -- <command line>"
      )
  }

  /** Hooks that hold the run the first time it reaches `point`, and do nothing else. */
  private def holding(point: FaultPoint): FaultHooks = {
    val held = new AtomicBoolean(false)
    (reached, batch) =>
      if (reached == point && held.compareAndSet(false, true)) {
        System.out.println(s"${heldAt(point)} ${batch.id}")
        System.out.flush()
        // Counted down by nobody: the run stays here until the process is killed.
        new CountDownLatch(1).await()
      }
  }
}
