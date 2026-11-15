package lakehouse.ingestion.logstore

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/** Calls that tests make at the same moment, each on a thread of its own. */
object Concurrently {

  private val WaitSeconds = 60L

  /**
   * Runs `call` for every input on its own thread, all released together.
   *
   * @return the results, in the order of the inputs; the first failure of a call fails this
   */
  def atOnce[A, B](inputs: Seq[A])(call: A => B): Seq[B] = {
    val pool = Executors.newFixedThreadPool(inputs.size)
    implicit val threads: ExecutionContext = ExecutionContext.fromExecutor(pool)
    val start = new CountDownLatch(1)
    try {
      val calls = inputs.map { input =>
        Future {
          start.await(WaitSeconds, TimeUnit.SECONDS)
          call(input)
        }
      }
      start.countDown()
      Await.result(Future.sequence(calls), WaitSeconds.seconds)
    } finally pool.shutdownNow(): Unit
  }
}
