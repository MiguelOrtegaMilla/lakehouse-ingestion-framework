package lakehouse.ingestion.cli

import java.io.PrintStream
import java.util.concurrent.atomic.AtomicReference

import sun.misc.{Signal, SignalHandler}

/**
 * Whether whoever started the process has asked it to stop, and by which signal (discovery
 * §17, P6.10). `run` asks before each run of a chain: the run in course finishes its batch,
 * and no other starts.
 */
trait Stop {

  /** The number of the signal that asked, if one did. */
  def requested: Option[Int]
}

object Stop {

  /** What a process that is never asked to stop has: a test, which runs commands in its JVM. */
  val Never: Stop = new Stop {
    override def requested: Option[Int] = None
  }

  /** The name of a signal that the process stops for, as a shell writes it. */
  def name(signal: Int): String = signal match {
    case 2 => "SIGINT"
    case 15 => "SIGTERM"
    case other => s"signal $other"
  }

  /**
   * Handles `SIGTERM` and `SIGINT` from now on, for the rest of the process. The first raises
   * the flag that `requested` reads, and says so; a second ends the process at once, in
   * 128 + its number, as the signal itself would.
   *
   * It is a handler of the signal, and not a shutdown hook: a JVM that shuts down stops the
   * session of Spark by a hook of Spark's own, and the batch in course with it. A process that
   * is ended at once leaves its batch planned, for the next run to finish (ADR-0005).
   */
  def onSignals(err: PrintStream): Stop = {
    val asked = new AtomicReference[Option[Int]](None)
    val handler = new SignalHandler {
      override def handle(signal: Signal): Unit = {
        val number = signal.getNumber
        if (asked.compareAndSet(None, Some(number)))
          err.println(
            s"warning: ${name(number)} received: the batch in course finishes, and no other " +
              "starts. Another signal ends the process at once."
          )
        else Runtime.getRuntime.halt(128 + number)
      }
    }
    Seq("TERM", "INT").foreach(signal => Signal.handle(new Signal(signal), handler): Unit)
    new Stop {
      override def requested: Option[Int] = asked.get
    }
  }
}
