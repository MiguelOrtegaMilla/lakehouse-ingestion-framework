package lakehouse.ingestion.core.config

import scala.collection.immutable.SortedMap
import scala.concurrent.duration._

import cats.data.{Chain, NonEmptyChain}
import cats.syntax.all._
import io.circe.Json
import lakehouse.ingestion.core.config.Read.{Node, Obj, Problem, Result}

/**
 * Where an application runs: its lake, its landing zone, its databases, how long it waits and
 * how long it goes on trying (`conf/app/<profile>.yaml`; ADR-0007, ADR-0011). The sources say
 * what is ingested, and are the same in every profile; the profile says where and on what.
 *
 * A profile is validated from its text alone. No connection is opened and no name is looked up:
 * a database that is down costs seconds for each attempt (5 s by default, P2b.5), and whether a
 * database answers is not a property of a file.
 */
final case class Profile(
    lake: Profile.Lake,
    landing: Profile.Landing,
    databases: Profile.Databases,
    retries: Profile.Retries,
    leases: Profile.Leases,
    spark: Profile.Spark
)

object Profile {

  sealed abstract class Deployment(val name: String)

  object Deployment {

    /** Every process of the application is on one machine, and sees its disk. */
    case object SingleMachine extends Deployment("single-machine")

    /** Processes on several machines: drivers, executors, or both. */
    case object Cluster extends Deployment("cluster")
  }

  /**
   * @param sharedFilesystem for a `file://` lake in a cluster: the path is storage that every
   *   node mounts (NFS). Allowed, and said at startup: its renames and listings are not the
   *   local ones, and need a verification of their own (ADR-0011 §2)
   */
  final case class Lake(uri: RootUri, deployment: Deployment, sharedFilesystem: Boolean)

  /** The root that the paths of file sources are relative to (ADR-0013 §2). */
  final case class Landing(root: RootUri)

  /**
   * @param control the control plane: runs, batches, leases, state (ADR-0003)
   * @param coordination what the LogStore claims the versions of a table in (ADR-0004)
   */
  final case class Databases(control: DatabaseConnection, coordination: DatabaseConnection)

  /**
   * For how long the engine goes on with a step that fails with an error that passes, and how
   * far apart its attempts are. The policy itself is the engine's (P8); the numbers are here
   * because one rule about them can be checked before anything runs (`rules`).
   *
   * A budget of time, not a number of attempts: an attempt against a database that is down
   * takes the connect timeout to fail, so a count says little of how long an outage is ridden.
   */
  final case class Retries(
      budget: FiniteDuration,
      firstBackoff: FiniteDuration,
      maxBackoff: FiniteDuration
  )

  object Retries {
    val Default: Retries =
      Retries(budget = 2.minutes, firstBackoff = 1.second, maxBackoff = 30.seconds)
  }

  /** @param settings Spark settings that the profile adds, by key */
  final case class Spark(master: Master, settings: SortedMap[String, String])

  sealed trait Master {
    def value: String
  }

  object Master {

    /** `local`, `local[4]`, `local[*]`: Spark inside the process. */
    final case class Local(value: String) extends Master

    /** `spark://host:port`: a Spark Standalone cluster. */
    final case class Standalone(value: String) extends Master

    private val LocalForm = """local(\[(\*|[1-9][0-9]{0,3})\])?""".r
    private val Node = """[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?:[0-9]{1,5}"""
    private val StandaloneForm = s"""spark://$Node(,$Node)*""".r

    def parse(text: String): Either[String, Master] =
      if (LocalForm.matches(text)) Right(Local(text))
      else if (StandaloneForm.matches(text)) Right(Standalone(text))
      else Left("must be local, local[N], local[*] or spark://host:port")
  }

  /**
   * For how long a run holds the lease of its source without renewing it (ADR-0006). A run
   * renews its lease every third of it. One that dies renews nothing, and its source is taken
   * that long before another run may have it: the time to live is how long a crash is waited
   * out, and a short one is no better than a long one. It must outlast a renewal that takes as
   * long as it may (`rules`).
   */
  final case class Leases(ttl: FiniteDuration)

  object Leases {

    /** ADR-0006: two minutes, with a renewal every forty seconds. */
    val Default: Leases = Leases(ttl = 2.minutes)

    /** A lease is renewed every third of its time to live. */
    val RenewalsInATtl = 3
  }

  /**
   * How many times the LogStore tries the `INSERT` that takes a version before a commit fails
   * (`RetryPolicy.Default` in the LogStore's module, which `core` may not depend on; a test
   * holds the two to each other).
   */
  val LogStoreAttempts = 3

  /**
   * Reads a profile from the text of its file.
   *
   * @param file the name of the file, for the errors
   * @return the profile with its warnings, or everything that is wrong in the file
   */
  def load(file: String, text: String): Either[NonEmptyChain[ConfigError], Loaded[Profile]] = {
    def located(problem: Problem): ConfigError =
      ConfigError(file, Option(problem.field).filter(_ != "the file"), problem.problem)

    SafeYaml.parse(text) match {
      case Left(refusal) => Left(NonEmptyChain.one(ConfigError(file, None, refusal.message)))
      case Right(json) =>
        read(json).toEither.leftMap(_.map(located)).flatMap { profile =>
          val (errors, warnings) = rules(profile)
          NonEmptyChain
            .fromChain(errors.map(located))
            .toLeft(Loaded(profile, warnings.map(located).toVector))
        }
    }
  }

  // -------------------------------------------------------------------------------------------
  // The file, field by field
  // -------------------------------------------------------------------------------------------

  private[config] def read(json: Json): Result[Profile] = Read.document(json) { top =>
    (
      top.required("lake")(_.obj(lake)),
      top.required("landing")(
        _.obj(landing => landing.required("root")(_.text(RootUri.parse)).map(Landing))
      ),
      top.required("databases")(_.obj { databases =>
        (
          databases.required("control")(_.obj(DatabaseConnection.read(Timeouts.Default))),
          databases.required("coordination")(_.obj(DatabaseConnection.read(Timeouts.Default)))
        ).mapN(Databases)
      }),
      top.orElse("retries", Retries.Default)(_.obj(retries)),
      top.orElse("leases", Leases.Default)(
        _.obj(_.orElse("ttl", Leases.Default.ttl)(_.duration(10.seconds, 1.day)).map(Leases.apply))
      ),
      top.required("spark")(_.obj(spark))
    ).mapN(Profile.apply)
  }

  private def lake(fields: Obj): Result[Lake] =
    (
      fields.required("uri")(_.text(RootUri.parse)),
      fields.required("deployment")(
        _.oneOf(
          Deployment.SingleMachine.name -> Deployment.SingleMachine,
          Deployment.Cluster.name -> Deployment.Cluster
        )
      ),
      fields.orElse("shared_filesystem", false)(_.boolean)
    ).mapN(Lake)

  private def retries(fields: Obj): Result[Retries] =
    (
      fields.orElse("budget", Retries.Default.budget)(_.duration(1.second, 1.day)),
      fields.orElse("first_backoff", Retries.Default.firstBackoff)(_.duration(100.millis, 1.hour)),
      fields.orElse("max_backoff", Retries.Default.maxBackoff)(_.duration(100.millis, 1.hour))
    ).mapN(Retries.apply)

  private def spark(fields: Obj): Result[Spark] =
    (
      fields.required("master")(_.text(Master.parse)),
      fields.orElse("settings", Vector.empty[(String, String)])(_.entries(sparkSetting))
    ).mapN((master, settings) => Spark(master, SortedMap.from(settings)))

  private def sparkSetting(key: String, value: Node): Result[(String, String)] = {
    val lower = key.toLowerCase(java.util.Locale.ROOT)
    if (!key.startsWith("spark."))
      value.fail("is not a Spark setting: its key must start with spark.")
    else if (lower == "spark.master") value.fail("is set by the field master of this profile")
    else
      refusedSparkSetting(key) match {
        case Some(reason) => value.fail(reason)
        case None => credentialOrValue(key, lower, value)
      }
  }

  private def credentialOrValue(key: String, lower: String, value: Node): Result[(String, String)] =
    if (CredentialWords.exists(lower.contains))
      value.fail(
        "is a credential, and no credential is written in a profile: object storage takes its " +
          "own from the environment or from a Hadoop credential provider (ADR-0011)"
      )
    else value.scalarText.map(key -> _)

  private val CredentialWords = Seq("password", "secret", "token", "access.key")

  /**
   * The Spark settings that the engine gives the session of every run, whatever its profile
   * says (the command `run`). A profile may set none of them: its value would be overridden
   * without a word, or would be the engine's undoing. A test of the command holds every key
   * that it sets to these.
   */
  object EngineSpark {

    /** What a session is called: after the source that it runs. */
    val AppName = "spark.app.name"

    /** Delta's extension to Spark SQL, which every session of the engine has. */
    val Extensions = "spark.sql.extensions"

    /** Delta's catalog in place of Spark's own. */
    val Catalog = "spark.sql.catalog.spark_catalog"

    /** Which LogStore commits to a table, by scheme or for every scheme (ADR-0004). */
    val LogStorePrefix = "spark.delta.logStore."

    /**
     * The settings of the LogStore, made from `databases.coordination` (ADR-0004 §3.9), its
     * fault hooks among them, which are for tests alone. Under `spark.hadoop.`, which Spark
     * copies into the Hadoop configuration that Delta hands a LogStore.
     */
    val LogStoreSettingsPrefix = "spark.hadoop.lakehouse.logstore."
  }

  /**
   * What gives every table that Delta makes `delta.setTransactionRetentionDuration`
   * (`DeltaConfigs`: `spark.databricks.delta.properties.defaults.` and the key of the property
   * without `delta.`). Delta then forgets the transaction of a batch that is older than that,
   * and a replay of the batch writes it again: no table of the engine may have the property
   * (P5.4, P5.5).
   */
  val TransactionRetentionDefault =
    "spark.databricks.delta.properties.defaults.setTransactionRetentionDuration"

  /**
   * Why a Spark setting is not a profile's to set, if it is not: one that the engine sets, or
   * one that would break what the engine stands on. Keys are compared with case aside: one
   * under another case is no setting of Spark's, and is a mistake, or a way round this rule.
   *
   * `spark.master` is not here: it is a field of the profile, said in its own way.
   */
  def refusedSparkSetting(key: String): Option[String] = {
    val lower = key.toLowerCase(java.util.Locale.ROOT)
    def is(engine: String): Boolean = lower == engine.toLowerCase(java.util.Locale.ROOT)
    def under(prefix: String): Boolean =
      lower.startsWith(prefix.toLowerCase(java.util.Locale.ROOT))

    if (under(EngineSpark.LogStorePrefix))
      Some(
        "is set by the engine: which LogStore commits to a table is not a setting of a profile (ADR-0004)"
      )
    else if (under(EngineSpark.LogStoreSettingsPrefix))
      Some(
        "is set by the engine, from databases.coordination: how the LogStore reaches its " +
          "database is said there, and its fault hooks are for tests alone (ADR-0004)"
      )
    else if (is(EngineSpark.Extensions) || is(EngineSpark.Catalog))
      Some(
        "is set by the engine: every session of a run has the extension and the catalog of Delta"
      )
    else if (is(EngineSpark.AppName))
      Some("is set by the engine: a session is named after the source that it runs")
    else if (is(TransactionRetentionDefault))
      Some(
        "would give every table that Delta makes the property delta.setTransactionRetentionDuration. " +
          "Delta would then forget the batches written to a table before that time, and a replay " +
          "would write such a batch again: no table of the engine may have it"
      )
    else None
  }

  // -------------------------------------------------------------------------------------------
  // What is about more than one field
  // -------------------------------------------------------------------------------------------

  /** The errors and the warnings of a profile whose every field is valid by itself. */
  private[config] def rules(profile: Profile): (Chain[Problem], Chain[Problem]) = {
    val Profile(lake, landing, databases, retries, leases, spark) = profile
    val local = lake.uri.isLocal
    val cluster = lake.deployment == Deployment.Cluster
    val clusterMaster = spark.master match {
      case _: Master.Standalone => true
      case _: Master.Local => false
    }
    val logStoreWait = databases.coordination.timeouts.connect * LogStoreAttempts.toLong
    // The longest a renewal of a lease can take: a connection is waited for, and then an answer.
    val renewal = databases.control.timeouts.connect + databases.control.timeouts.statement
    val shortestTtl = renewal * Leases.RenewalsInATtl.toLong

    val errors = Chain.fromSeq(
      Seq(
        // ADR-0011 §2
        Option.when(local && cluster && !lake.sharedFilesystem)(
          Problem(
            "lake.uri",
            "is on a local disk, and lake.deployment is cluster: processes on other machines " +
              "would each write to a disk of their own. Use shared storage, or set " +
              "lake.shared_filesystem to true if every machine mounts this path"
          )
        ),
        Option.when(local && !cluster && clusterMaster)(
          Problem(
            "spark.master",
            "is a cluster, and lake.deployment is single-machine with a lake on a local disk: " +
              "every worker would write data files to its own disk"
          )
        ),
        Option.when(lake.sharedFilesystem && !(local && cluster))(
          Problem(
            "lake.shared_filesystem",
            "is true, and it only says something of a file:// lake in a cluster deployment"
          )
        ),
        // ADR-0013 §2: files that land must never be read as tables, nor tables as files.
        Option.when(lake.uri.contains(landing.root) || landing.root.contains(lake.uri))(
          Problem("landing.root", "is the lake, or inside it, or around it: the two must be apart")
        ),
        // ADR-0013 §3
        Option.when(databases.control.user == databases.coordination.user)(
          Problem(
            "databases.coordination.user",
            "is the role of databases.control as well: each has the privileges of its own work and no more"
          )
        ),
        Option.when(retries.firstBackoff > retries.maxBackoff)(
          Problem(
            "retries.first_backoff",
            s"is ${Durations.render(retries.firstBackoff)}, longer than retries.max_backoff, " +
              Durations.render(retries.maxBackoff)
          )
        ),
        Option.when(retries.maxBackoff > retries.budget)(
          Problem(
            "retries.max_backoff",
            s"is ${Durations.render(retries.maxBackoff)}, longer than retries.budget, " +
              Durations.render(retries.budget)
          )
        ),
        // P2b.5: the LogStore waits this long by itself before a commit fails.
        Option.when(retries.budget <= logStoreWait)(
          Problem(
            "retries.budget",
            s"is ${Durations.render(retries.budget)}, and must be longer than " +
              s"${Durations.render(logStoreWait)}: the LogStore itself waits that long for the " +
              "coordination database before a commit fails " +
              s"(databases.coordination.timeouts.connect, ${Durations.render(databases.coordination.timeouts.connect)}, " +
              s"$LogStoreAttempts times), and a shorter budget gives up on outages that would have passed"
          )
        ),
        // ADR-0006: a lease is renewed every third of its time to live.
        Option.when(leases.ttl < shortestTtl)(
          Problem(
            "leases.ttl",
            s"is ${Durations.render(leases.ttl)}, and must be at least ${Durations.render(shortestTtl)}: " +
              s"a lease is renewed every third of it, and a renewal can take " +
              s"${Durations.render(renewal)} (databases.control.timeouts: connect, " +
              s"${Durations.render(databases.control.timeouts.connect)}, and statement, " +
              s"${Durations.render(databases.control.timeouts.statement)}). A shorter lease would be " +
              "lost to a renewal that was still waiting for its answer"
          )
        )
      ).flatten
    )

    val warnings = Chain.fromSeq(
      Seq(
        // The registry outlives such a lake: its tables would be gone, and their commits not.
        Option.when(local && lake.uri.segments.headOption.contains("tmp"))(
          Problem(
            "lake.uri",
            "is under /tmp, which is emptied when the machine restarts. The coordination " +
              "database would still have the commits of the lake's tables, and the LogStore " +
              "would find them missing and refuse to write"
          )
        ),
        Option.when(lake.sharedFilesystem && local && cluster)(
          Problem(
            "lake.shared_filesystem",
            "is true: the lake is a path that every machine mounts. Renames and listings on " +
              "such storage are not those of a local disk, and need a verification of their own"
          )
        )
      ).flatten ++
        DatabaseConnection.warnings("databases.control", databases.control) ++
        DatabaseConnection.warnings("databases.coordination", databases.coordination)
    )

    (errors, warnings)
  }
}
