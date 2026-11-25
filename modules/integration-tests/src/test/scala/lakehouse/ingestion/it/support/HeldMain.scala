package lakehouse.ingestion.it.support

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean

import lakehouse.ingestion.cli.{Main, Stop}
import lakehouse.ingestion.engine.{FaultHooks, FaultPoint}

/**
 * The application, held at a fault point of the engine to be killed there (discovery §17,
 * P7.10): `HeldMain --hold <point> -- <command line>`. The first time a run of the command
 * reaches the point, the process says so on its standard output (`heldAt`), and waits there for
 * ever: what a test then does to it, `kill -9`, it does to a process in the middle of a run.
 *
 * Everything else is `Main`'s, signals included: the hooks alone are this launcher's, and no
 * argument of `Main` sets them.
 */
object HeldMain {

  /** What the process says, and then the batch, once it is held: a test waits for it. */
  def heldAt(point: FaultPoint): String = s"held at $point of"

  def main(args: Array[String]): Unit = args.toList match {
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
      throw new IllegalArgumentException("usage: HeldMain --hold <point> -- <command line>")
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
