package lakehouse.ingestion.it.stress

import java.nio.file.Files

import scala.annotation.tailrec
import scala.concurrent.duration._

import lakehouse.ingestion.it.stress.StressHarness.{Chaos, HarnessFailure}
import lakehouse.ingestion.it.stress.WriterKiller.InFlight
import lakehouse.ingestion.it.support.{
  BootstrappedPostgres,
  CoordinationSchema,
  CoordinationStores,
  LocalSparkSuite,
  ProxiedPostgres
}
import lakehouse.ingestion.logstore.TableIdentity
import lakehouse.ingestion.logstore.coordination.{
  CoordinationSettings,
  PostgresCoordinationStore,
  TableUri
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.Suite

import scala.util.Using

/**
 * A stress run on `PostgresLogStore`: the orchestrator's session and every writer JVM commit
 * through it, coordinated by one PostgreSQL that the suite starts with the schema migrated.
 *
 * Every process reaches PostgreSQL through the proxy that a run can break, at an address that
 * stays the same when the server is stopped and started again. The suite itself looks at the
 * registry on the path that is never broken.
 */
trait CoordinatedStressSuite extends LocalSparkSuite { self: Suite =>

  import CoordinatedStressSuite.{BatchCommit, Look, Outage}

  protected lazy val pg: ProxiedPostgres = ProxiedPostgres.start()

  /** How every process of a run reaches the coordination database. */
  protected lazy val coordination: CoordinationSettings =
    CoordinationStores.settings(pg.faultyJdbcUrl(CoordinationSchema.Database), pg.postgres)

  /** The registry as the commit path sees it, read by the suite to check what a run left. */
  protected lazy val registry = new PostgresCoordinationStore(coordination)

  /** The orchestrator creates the table and reads it back through the LogStore, like a writer. */
  override protected def sparkConf: Map[String, String] =
    CoordinationStores.sparkSettings(coordination)

  /**
   * The same settings for a writer JVM, which takes its Spark configuration from `-Dspark.*`
   * options: what makes it commit through `PostgresLogStore`, and to this database.
   */
  protected def coordinatedJvmOptions: Seq[String] =
    CoordinationStores.sparkSettings(coordination).toSeq.sorted.map { case (key, value) =>
      s"-D$key=$value"
    }

  /** The key a stress table is coordinated under. */
  protected def keyOf(run: StressRun): TableUri =
    TableIdentity.of(new Path(run.tableUri), FileSystem.getLocal(new Configuration()))

  /** What the database holds about the table of `run`, read on the path that is never broken. */
  protected def registryView(run: StressRun): LogModel.RegistryView =
    Using.resource(pg.observe(CoordinationSchema.CommitPathRole, CoordinationSchema.Database)) {
      RegistryReader.view(_, keyOf(run).value)
    }

  /**
   * The entries of the table of `run` now, and for the ones that are not complete, whether
   * their commit file is in the log.
   */
  protected def look(run: StressRun): Look = {
    val logDir = run.tableDir.resolve("_delta_log")
    val entries = registryView(run).entries
    Look(
      entries.map(_.version).toSet,
      entries.filterNot(_.complete).map { entry =>
        InFlight(entry.version, Files.exists(logDir.resolve(f"${entry.version}%020d.json")))
      }
    )
  }

  /**
   * Waits for a version of the table of `run` that is taken and whose commit is not in the
   * log: a commit that exists in the coordination database alone.
   *
   * @throws HarnessFailure there was none within `within`
   */
  protected def awaitTakenAndNotPublished(run: StressRun, within: FiniteDuration): Look = {
    val deadline = within.fromNow
    @tailrec
    def await(): Look = {
      val found = look(run)
      if (found.unfinished.exists(!_.published)) found
      else if (deadline.isOverdue())
        throw new HarnessFailure(s"no version was seen taken and not published within $within")
      else {
        Thread.sleep(10)
        await()
      }
    }
    await()
  }

  /**
   * Kills PostgreSQL in the middle of a run, as `docker kill` does, leaves it down for
   * `downFor`, and starts it again: a crash of the coordination database, which it recovers
   * from out of its write-ahead log. Whatever happens, PostgreSQL is started again.
   *
   * @param before the registry at the last look before the kill
   */
  protected def killPostgres(
      run: StressRun,
      chaos: Chaos,
      writers: Int,
      downFor: FiniteDuration,
      before: Look
  ): Outage = {
    val durability = Seq("fsync", "synchronous_commit", "full_page_writes").map { setting =>
      setting -> serverSetting(setting)
    }
    val atKill = (0 until writers).map(chaos.acknowledged(_).size)
    val killed = System.nanoTime()
    pg.killPostgres()
    val whileDown =
      try {
        Thread.sleep(downFor.toMillis)
        (0 until writers).map(writer => chaos.acknowledged(writer).size - atKill(writer))
      } finally pg.startPostgres()
    val down = (System.nanoTime() - killed).nanos
    Outage(durability, before, look(run), down, atKill, whileDown)
  }

  private def serverSetting(name: String): String =
    Using.resource(pg.observe(BootstrappedPostgres.Superuser, "postgres")) { connection =>
      Using.resource(connection.createStatement()) { statement =>
        // A fixed name of this file, never input.
        Using.resource(statement.executeQuery(s"SHOW $name")) { rows =>
          rows.next()
          rows.getString(1)
        }
      }
    }

  /** What kills the writers of `run`, looking at them from outside. The caller closes it. */
  protected def killerOf(run: StressRun): WriterKiller =
    new WriterKiller(
      pg.observe(CoordinationSchema.CommitPathRole, CoordinationSchema.Database),
      keyOf(run).value,
      run.tableDir.resolve("_delta_log")
    )

  /**
   * The commit of a writer's batch, read after the run is over: its version in the log, and
   * the run that its entry in the registry names. `None` if the batch is not in the log.
   */
  protected def commitOfBatch(run: StressRun, writer: Int, batch: Long): Option[BatchCommit] = {
    val entries = registryView(run).entries.map(e => e.version -> e.runId).toMap
    LogReader
      .logView(spark, run.tableDir)
      .commits
      .find(_.txns.contains(LogModel.Txn(StressTable.writerId(writer), batch)))
      .map(commit => BatchCommit(commit.version, entries.get(commit.version).flatten))
  }

  /**
   * Verifies a finished run as a coordinated one: what every run is held to, and what
   * `PostgresLogStore` adds, with the registry read after the readers have read the table.
   */
  protected def verify(
      run: StressRun,
      settings: StressHarness.Settings,
      outcomes: Seq[StressHarness.WriterOutcome]
  ): Verifier.Report =
    LogReader.verify(
      spark,
      run,
      Verifier.Expected.of(settings),
      outcomes,
      Some(() => registryView(run)),
      Option.when(settings.epochs)(() => epochsView(run, outcomes))
    )

  /**
   * The epochs of a run whose writers commit under them: what its processes wrote down of
   * their turns, and what the database has in force under their keys, read with plain SQL.
   */
  protected def epochsView(
      run: StressRun,
      outcomes: Seq[StressHarness.WriterOutcome]
  ): LogModel.EpochsView = {
    val keys = outcomes.map(o => StressTable.writerId(o.writer) -> run.epochKey(o.writer)).toMap
    val inForce =
      Using.resource(pg.observe(CoordinationSchema.CommitPathRole, CoordinationSchema.Database)) {
        RegistryReader.epochsInForce(_, keys.values.toSeq)
      }
    LogModel.EpochsView(keys, outcomes.flatMap(_.turns), inForce)
  }

  /**
   * Holds, from now on, every claim of a version under `epoch` of `key` that takes its version,
   * for `held`, between its `INSERT` and the end of its transaction: the claim has read the
   * epoch in force, which was its own, and has its entry, which nobody else sees yet. It is
   * the window in which an announcement of the next epoch must wait: one that did not would be
   * followed by a look at a registry that does not have the commit, and then by the commit.
   *
   * The entry is there while the claim is held, so the version is the claim's: another writer
   * that wants it waits, and is told that it is taken. A hold before the `INSERT` would hold a
   * claim that then loses its version to another writer, and commits nothing.
   *
   * Nothing of the LogStore knows of it: it is a trigger of the test's own in the database
   * (`installClaimHold`), which sleeps for the claims of that one epoch and of no other.
   */
  protected def holdClaims(key: String, epoch: Long, held: FiniteDuration): Unit =
    asSuperuser { connection =>
      Using.resource(
        connection.prepareStatement(
          "INSERT INTO stress_hold.held (epoch_key, epoch, millis) VALUES (?, ?, ?) " +
            "ON CONFLICT (epoch_key) DO UPDATE SET epoch = EXCLUDED.epoch, millis = EXCLUDED.millis"
        )
      ) { statement =>
        statement.setString(1, key)
        statement.setLong(2, epoch)
        statement.setLong(3, held.toMillis)
        statement.executeUpdate(): Unit
      }
    }

  /** Lets the claims of `key` go as they come. */
  protected def releaseClaims(key: String): Unit = asSuperuser { connection =>
    Using.resource(
      connection.prepareStatement("DELETE FROM stress_hold.held WHERE epoch_key = ?")
    ) { statement =>
      statement.setString(1, key)
      statement.executeUpdate(): Unit
    }
  }

  /**
   * Waits until a claim is being held (`holdClaims`): a session of a LogStore is asleep with
   * its entry inserted and its transaction open, seen from outside.
   *
   * @throws HarnessFailure none was within `within`
   */
  protected def awaitHeldClaim(within: FiniteDuration): Unit = {
    val deadline = within.fromNow
    def held: Boolean = asSuperuser { connection =>
      Using.resource(
        connection.prepareStatement(
          "SELECT count(*) FROM pg_stat_activity WHERE application_name = ? AND wait_event = 'PgSleep'"
        )
      ) { statement =>
        statement.setString(1, PostgresCoordinationStore.ApplicationName)
        Using.resource(statement.executeQuery()) { rows =>
          rows.next()
          rows.getInt(1) > 0
        }
      }
    }
    while (!held) {
      if (deadline.isOverdue())
        throw new HarnessFailure(s"no claim was seen held with its version taken within $within")
      Thread.sleep(5)
    }
  }

  private def asSuperuser[T](body: java.sql.Connection => T): T =
    Using.resource(pg.observe(BootstrappedPostgres.Superuser, CoordinationSchema.Database))(body)

  /**
   * Makes the trigger that holds claims, and the table that says which. Before the writers of
   * a run are started: a trigger is not made on a table that is being written.
   */
  protected def installClaimHold(): Unit = asSuperuser { connection =>
    Using.resource(connection.createStatement()) { statement =>
      // Fixed text of this file, never input.
      Seq(
        "CREATE SCHEMA IF NOT EXISTS stress_hold",
        """CREATE TABLE IF NOT EXISTS stress_hold.held (
          |  epoch_key text PRIMARY KEY, epoch bigint NOT NULL, millis bigint NOT NULL)""".stripMargin,
        // As its owner: the role of the commit path has nothing in this schema.
        """CREATE OR REPLACE FUNCTION stress_hold.hold_claim() RETURNS trigger
          |LANGUAGE plpgsql SECURITY DEFINER AS $$
          |DECLARE pause bigint;
          |BEGIN
          |  SELECT millis INTO pause FROM stress_hold.held
          |   WHERE epoch_key = NEW.epoch_key AND epoch = NEW.epoch;
          |  IF FOUND THEN PERFORM pg_sleep(pause / 1000.0); END IF;
          |  RETURN NEW;
          |END $$""".stripMargin,
        "DROP TRIGGER IF EXISTS stress_hold_claim ON delta_coordination.commit_entry",
        // After the row: it fires for a claim that took its version, and for no other.
        """CREATE TRIGGER stress_hold_claim AFTER INSERT ON delta_coordination.commit_entry
          |FOR EACH ROW WHEN (NEW.epoch_key IS NOT NULL)
          |EXECUTE FUNCTION stress_hold.hold_claim()""".stripMargin
      ).foreach(statement.execute(_): Unit)
    }
  }

  override def beforeAll(): Unit = {
    CoordinationSchema.migrate(pg.postgres): Unit
    super.beforeAll()
  }

  override def afterAll(): Unit =
    try super.afterAll()
    finally
      try registry.close()
      finally pg.stop()
}

object CoordinatedStressSuite {

  /** Where a writer's batch is in the log, and the run that the entry of that version names. */
  final case class BatchCommit(version: Long, run: Option[String])

  /**
   * The registry of a table at one moment.
   *
   * @param versions the versions that have an entry
   * @param unfinished of them, the ones whose entry is not complete, and whether their commit
   *   file was in the log at that moment
   */
  final case class Look(versions: Set[Long], unfinished: Seq[InFlight]) {

    def describe: String =
      if (unfinished.isEmpty) s"${versions.size} entries, all complete"
      else
        s"${versions.size} entries, with " + unfinished
          .map(e => s"version ${e.version} ${if (e.published) "in the log" else "not in the log"}")
          .mkString(", ") + " not complete"
  }

  /**
   * An outage of PostgreSQL in the middle of a run: killed, down for a time, started again.
   *
   * @param durability the settings of the server that say whether a commit it confirmed
   *   outlives its crash, as it ran with them
   * @param before the registry at the last look before the kill
   * @param after the registry at the first look after the restart
   * @param down from the kill to the first connection that the server accepted again
   * @param acknowledgedAtKill how many batches each writer had acknowledged at the kill
   * @param acknowledgedWhileDown how many more each acknowledged before the server was started
   */
  final case class Outage(
      durability: Seq[(String, String)],
      before: Look,
      after: Look,
      down: FiniteDuration,
      acknowledgedAtKill: Seq[Int],
      acknowledgedWhileDown: Seq[Int]
  ) {

    /** The versions that had an entry before the kill and have none after the restart. */
    def gone: Seq[Long] = (before.versions -- after.versions).toSeq.sorted
  }
}
