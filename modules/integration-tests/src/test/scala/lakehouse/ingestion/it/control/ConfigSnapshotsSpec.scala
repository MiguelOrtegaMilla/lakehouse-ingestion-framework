package lakehouse.ingestion.it.control

import lakehouse.ingestion.core.config.{ConfigVersion, Source}
import lakehouse.ingestion.core.control._
import lakehouse.ingestion.engine.control.{ConfigSnapshots, Runs}
import lakehouse.ingestion.it.support.ControlPlaneSchema._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The configurations that runs ran with, stored by the engine's role on a real PostgreSQL, and
 * the one that the configuration of now is compared with to find a breaking change (ADR-0007).
 */
final class ConfigSnapshotsSpec extends AnyFunSuite with Matchers with ControlPlaneSuite {

  private def store(source: Source): Boolean =
    database.run(ConfigSnapshots.store(_, source.id, versionOf(source)))

  private def ofState(source: Source, layer: Layer = Layer.Bronze): Option[ConfigSnapshots.Stored] =
    database.run(ConfigSnapshots.ofState(_, source.id, layer))

  /** Writes the state of a source as advanced by `run`: what the commit of a batch does (P4.6). */
  private def advancedBy(run: RunStart, seq: Int): Unit = database.run { c =>
    update(
      c,
      """INSERT INTO control.source_state
        |  (source_id, layer, watermark, last_committed_batch_seq, updated_by_run)
        |VALUES (?, ?, '{}'::jsonb, ?, ?)
        |ON CONFLICT (source_id, layer) DO UPDATE
        |  SET last_committed_batch_seq = EXCLUDED.last_committed_batch_seq,
        |      updated_by_run = EXCLUDED.updated_by_run, updated_at = now()""".stripMargin,
      run.source.value,
      run.layer.name,
      Int.box(seq),
      run.id.uuid
    ) shouldBe 1: Unit
  }

  /** A run of `source` that ran and ended as `end` says; if it says nothing, that was skipped. */
  private def ran(source: Source, end: Option[RunEnd]): RunStart = {
    val start = starting(source)
    end match {
      case Some(how) =>
        database.run(Runs.open(_, start, epoch(1)))
        database.run(Runs.end(_, start.id, how)) shouldBe true
      case None => database.run(Runs.skip(_, start))
    }
    start
  }

  private val otherWatermark: String => String =
    _.replace("column: updated_at", "column: changed_at")

  test("a configuration is stored once, and read back is the one that has its hash") {
    val orders = source(newSourceId().value)
    val version = versionOf(orders)

    store(orders) shouldBe true
    val firstSeen = database.run(c =>
      query(
        c,
        "SELECT first_seen_at FROM control.config_snapshot WHERE config_hash = ?",
        version.hash
      )
    )

    // The same configuration, from another run or another instance.
    store(orders) shouldBe false
    database.run { c =>
      query(
        c,
        "SELECT source_id, first_seen_at FROM control.config_snapshot WHERE config_hash = ?",
        version.hash
      )
    } shouldBe Seq(Seq(orders.id.value, firstSeen.head.head))

    val stored = database.run(ConfigSnapshots.find(_, version.hash)).value
    stored.hash shouldBe version.hash
    stored.content shouldBe version.content
    // Through jsonb and back, which keeps neither the order of the keys nor the spaces: what
    // is read is still the configuration that the hash is of.
    ConfigVersion.hashOf(stored.content) shouldBe version.hash
  }

  test("what is stored says where a password is, and never the password") {
    val orders = source(newSourceId().value)
    store(orders)

    val text = database
      .run { c =>
        query(
          c,
          "SELECT content::text FROM control.config_snapshot WHERE config_hash = ?",
          versionOf(orders).hash
        )
      }
      .head
      .head
    text should include("env:INGEST_CONTROL_PASSWORD")
    environment(pg).values.foreach(password => text should not include password)
  }

  test("two configurations are two snapshots, and one that was never stored is not found") {
    val orders = source(newSourceId().value)
    val changed = source(orders.id.value, otherWatermark)

    versionOf(changed).hash should not be versionOf(orders).hash
    store(orders) shouldBe true
    store(changed) shouldBe true
    database.run(ConfigSnapshots.find(_, versionOf(changed).hash)).value.content shouldBe
      versionOf(changed).content
    database.run(ConfigSnapshots.find(_, "e" * 64)) shouldBe None
  }

  test("a source that has committed nothing has no configuration to be compared with") {
    val orders = source(newSourceId().value)
    ofState(orders) shouldBe None

    // It ran, and failed before it committed anything: still nothing.
    ran(orders, Some(RunEnd.Failed(FailureClass.Transient, "the source did not answer")))
    ofState(orders) shouldBe None
  }

  test("the configuration to compare with is that of the run that last advanced the state") {
    val orders = source(newSourceId().value)
    val changed = source(orders.id.value, otherWatermark)

    advancedBy(ran(orders, Some(RunEnd.Succeeded())), seq = 1)
    ofState(orders).value.hash shouldBe versionOf(orders).hash

    // Runs with another configuration that advanced nothing: one that failed, one that was
    // skipped. Were theirs the one compared with, the next run would find no change.
    ran(changed, Some(RunEnd.Failed(FailureClass.Permanent, "refused: a breaking change")))
    ran(changed, None)
    ofState(orders).value.hash shouldBe versionOf(orders).hash

    // The state is advanced with the other configuration, as after a reset: it is the one now.
    advancedBy(ran(changed, Some(RunEnd.Succeeded())), seq = 2)
    ofState(orders).value.hash shouldBe versionOf(changed).hash
    ofState(orders).value.content shouldBe versionOf(changed).content
    // Each layer has its own state, and its own configuration to compare with.
    ofState(orders, Layer.Silver) shouldBe None
  }

  test("a breaking change is found against what is stored, and goes on being found") {
    val orders = source(newSourceId().value)
    val changed = source(orders.id.value, otherWatermark)
    advancedBy(ran(orders, Some(RunEnd.Succeeded())), seq = 1)

    def changes(now: Source): Vector[ConfigVersion.BreakingChange] =
      ConfigVersion.breakingChanges(ofState(orders).value.content, versionOf(now))

    changes(orders) shouldBe empty
    // A change that the state follows: the owner.
    changes(source(orders.id.value, _.replace("owner: ", "owner: another-"))) shouldBe empty

    val found = changes(changed)
    found.map(_.field) shouldBe Vector("load.watermark.column")
    found.head.was shouldBe "updated_at"
    found.head.is shouldBe "changed_at"

    // The run that is refused for it is recorded, and the next one is refused the same.
    ran(changed, Some(RunEnd.Failed(FailureClass.Permanent, found.head.message)))
    changes(changed).map(_.field) shouldBe Vector("load.watermark.column")
  }
}
