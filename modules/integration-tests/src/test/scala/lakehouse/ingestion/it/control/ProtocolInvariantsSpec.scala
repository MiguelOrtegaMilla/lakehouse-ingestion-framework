package lakehouse.ingestion.it.control

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Random
import scala.util.control.NonFatal

import io.circe.Json
import lakehouse.ingestion.core.config.{PathTableLocator, Source}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.control._
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import lakehouse.ingestion.logstore.Concurrently
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The whole protocol of a source (ADR-0005 §3, ADR-0006), with nothing arranged: six instances
 * go at one source for a while, each with a pool of its own. A run takes the lease, recovers or
 * plans a batch, writes it and commits it; at any step it may die instead, and what is left of
 * it may wake up later and do the next thing it was about to do, as a zombie does.
 *
 * What is checked is what must hold whatever happened: every batch is in the table once and in
 * its order, the batches are committed one after another with none left out, the state is at
 * the last of them, and the audit trail agrees with all of it.
 *
 * The table is a Delta that is simulated; the control plane is a real PostgreSQL.
 */
final class ProtocolInvariantsSpec extends AnyFunSuite with Matchers with ControlPlaneSuite {

  private val Instances = 6
  private val For = 20.seconds
  private val RowsInABatch = 5
  private val timing = LeaseTiming(2.minutes)

  /** What is left of a run that died: where it was, and what it knew. */
  final private class Zombie(val leased: LeasedRun, val batch: Option[PlannedBatch], val from: Int)

  private def range(from: Int): Json =
    Json.obj("from" -> Json.fromInt(from), "to" -> Json.fromInt(from + RowsInABatch))
  private def read(plan: Json): Seq[String] =
    (plan.hcursor.get[Int]("from").value until plan.hcursor.get[Int]("to").value).map(i =>
      s"row-$i"
    )
  private def watermarkAfter(plan: Json): Json =
    Json.obj("row" -> plan.hcursor.get[Json]("to").value)

  test("whatever the runs of a source do, and wherever they die, each batch is in its table once") {
    val orders: Source = source(newSourceId().value)
    val table = new PathTableLocator(profile.lake.uri).locate(orders.targets.bronze.table)
    val app = TxnAppId.of(orders.id, Layer.Bronze)
    val key = LeaseKey.of(orders.id, Layer.Bronze)
    val version = versionOf(orders)
    val delta = new SimulatedDelta
    val seed = System.nanoTime()
    val failures = new ConcurrentLinkedQueue[String]()
    val (deaths, wakings, commits) = (new AtomicInteger, new AtomicInteger, new AtomicInteger)
    val until = System.nanoTime() + For.toNanos

    def start(): RunStart = RunStart(
      RunId.generate(),
      orders.id,
      Layer.Bronze,
      RunKind.Regular,
      Application.instance,
      Application.version,
      version.hash
    )

    /** Steps 5 to 8 for a batch, by a run that may be the holder or may only think so. */
    def writeAndCommit(leased: LeasedRun, batch: PlannedBatch): Boolean = {
      Recovery.of(Some(batch), table, delta) match {
        case Recovery.Replay(_) => delta.write(table, batch.id, read(batch.plan)): Unit
        case _ => ()
      }
      val committed = leased.commit(
        batch.id,
        Some(watermarkAfter(batch.plan)),
        Seq(BatchOutput(orders.targets.bronze.table, delta.committedAt(table, batch.id).value))
      )
      if (committed) commits.incrementAndGet()
      committed
    }

    def stateRow(on: ControlDatabase): Int =
      on.run(SourceStates.find(_, orders.id, Layer.Bronze))
        .flatMap(_.watermark)
        .map(_.hcursor.get[Int]("row").value)
        .getOrElse(0)

    /** The process of a run dies where it is: nothing renews its lease, and the lease runs out. */
    def dies(leased: LeasedRun): Unit = {
      leased.close()
      asSuperuser { c =>
        update(
          c,
          "UPDATE control.source_lease SET expires_at = now() - interval '1 second' " +
            "WHERE lease_key = ? AND holder_run_id = ?",
          key.value,
          leased.start.id.uuid
        ): Unit
      }
      deaths.incrementAndGet(): Unit
    }

    /** A zombie does the next thing it was about to do. Whatever it is told, nothing may break. */
    def wakes(zombie: Zombie, random: Random): Unit = {
      wakings.incrementAndGet()
      try
        (zombie.batch, random.nextInt(4)) match {
          case (Some(batch), 0 | 1) => writeAndCommit(zombie.leased, batch): Unit
          case (None, 0 | 1) => zombie.leased.plan(range(zombie.from)): Unit
          case (_, 2) =>
            zombie.leased.finish(RunEnd.Failed(FailureClass.Transient, "it woke up")): Unit
          case _ => zombie.leased.finish(RunEnd.Succeeded()): Unit
        }
      catch {
        // Stopped, or refused: what a zombie is to be told.
        case _: LeaseLostException | _: IllegalStateException => ()
        // A batch is planned already: it planned one itself before, or another did.
        case e: java.sql.SQLException if e.getSQLState == "23505" => ()
      }
    }

    val instances = Seq.fill(Instances)(anotherInstance())
    try
      Concurrently.atOnce(instances.zipWithIndex) { case (instance, index) =>
        val random = new Random(seed + index)
        var zombies = List.empty[Zombie]
        try
          while (System.nanoTime() < until) {
            if (zombies.nonEmpty && random.nextInt(3) == 0) {
              wakes(zombies.head, random)
              zombies = zombies.tail
            }
            LeasedRun.open(instance, start(), version, timing) match {
              case Left(_) => Thread.sleep(random.nextInt(15).toLong)
              case Right(leased) =>
                val found = leased.planned()
                val from = stateRow(instance)
                if (random.nextInt(10) == 0) {
                  dies(leased)
                  zombies ::= new Zombie(leased, found, from)
                } else {
                  val batch = found.getOrElse(leased.plan(range(from)))
                  if (random.nextInt(8) == 0) {
                    dies(leased)
                    zombies ::= new Zombie(leased, Some(batch), from)
                  } else if (random.nextInt(8) == 0) {
                    // After its data, and before its state.
                    delta.write(table, batch.id, read(batch.plan))
                    dies(leased)
                    zombies ::= new Zombie(leased, Some(batch), from)
                  } else if (!writeAndCommit(leased, batch))
                    failures.add(
                      s"the run ${leased.start.id} held its lease and was refused the commit of ${batch.id}"
                    )
                }
            }
          }
        catch {
          case NonFatal(e) => failures.add(s"instance $index: $e"): Unit
        } finally zombies.foreach(_.leased.close())
      }
    finally instances.foreach(_.close())

    // The last of them may have left a batch planned, and a lease that nothing renews: one more
    // run, that dies nowhere.
    asSuperuser { c =>
      update(
        c,
        "UPDATE control.source_lease SET expires_at = now() - interval '1 second' " +
          "WHERE lease_key = ? AND holder_run_id IS NOT NULL",
        key.value
      ): Unit
    }
    val last = LeasedRun.open(database, start(), version, timing).value
    last.planned() match {
      case Some(batch) => writeAndCommit(last, batch) shouldBe true
      case None => last.finish(RunEnd.Succeeded()) shouldBe true
    }

    info(
      s"seed $seed: ${commits.get} batches committed, ${deaths.get} runs died, ${wakings.get} of them woke up"
    )
    withClue(s"seed $seed: ")(failures.asScala.toSeq shouldBe empty)
    commits.get should be > 20
    deaths.get should be > 5

    asSuperuser { c =>
      val batches = query(
        c,
        """SELECT batch_seq, status, committed_by_run::text, (outputs -> 0 ->> 'version')
          |  FROM control.ingestion_batch WHERE txn_app_id = ? ORDER BY batch_seq""".stripMargin,
        app.value
      )
      val count = batches.size

      withClue(s"seed $seed: ") {
        // Every row of every batch, once, in the order of the batches.
        delta.rows(table) shouldBe (0 until count * RowsInABatch).map(i => s"row-$i")
        // One batch after another, none left out, all of them committed, each where it says.
        batches.map(_.head.toInt) shouldBe (1 to count)
        batches.map(_(1)).distinct shouldBe Seq("COMMITTED")
        batches.foreach { row =>
          val id = BatchId(app, BatchSeq.of(row.head.toLong).value)
          delta.committedAt(table, id) shouldBe Some(row(3).toLong)
        }
        // The state is at the last of them.
        query(
          c,
          "SELECT last_committed_batch_seq, watermark ->> 'row' FROM control.source_state WHERE source_id = ?",
          orders.id.value
        ) shouldBe Seq(Seq(count.toString, (count * RowsInABatch).toString))

        // The audit trail agrees. No run is left running; a run succeeded with a batch only by
        // committing it; and each batch was committed by a run that says so.
        query(
          c,
          "SELECT count(*) FROM control.ingestion_run WHERE source_id = ? AND status = 'RUNNING'",
          orders.id.value
        ) shouldBe
          Seq(Seq("0"))
        query(
          c,
          """SELECT count(*) FROM control.ingestion_run r
            |  JOIN control.ingestion_batch b ON b.txn_app_id = r.txn_app_id AND b.batch_seq = r.batch_seq
            | WHERE r.source_id = ? AND r.status = 'SUCCEEDED' AND b.committed_by_run <> r.run_id""".stripMargin,
          orders.id.value
        ) shouldBe Seq(Seq("0"))
        query(
          c,
          """SELECT count(*) FROM control.ingestion_batch b
            |  JOIN control.ingestion_run r ON r.run_id = b.committed_by_run
            | WHERE b.txn_app_id = ? AND (r.status <> 'SUCCEEDED' OR r.batch_seq <> b.batch_seq)""".stripMargin,
          app.value
        ) shouldBe Seq(Seq("0"))
        // Every run that took the lease took it under an epoch of its own, one after another.
        val epochs = query(
          c,
          "SELECT lease_epoch FROM control.ingestion_run WHERE source_id = ? AND lease_epoch IS NOT NULL ORDER BY 1",
          orders.id.value
        ).map(_.head.toLong)
        epochs shouldBe (1L to epochs.size.toLong)
        query(c, "SELECT epoch FROM control.source_lease WHERE lease_key = ?", key.value) shouldBe
          Seq(Seq(epochs.size.toString))
      }
    }
  }
}
