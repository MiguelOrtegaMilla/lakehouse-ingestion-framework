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
  final case class Databases(control: Database, coordination: Database)

  /** @param user the role to connect as; a name like any other that configuration gives */
  final case class Database(
      url: PostgresUrl,
      user: SqlIdentifier,
      password: SecretRef,
      tls: Tls,
      timeouts: Timeouts
  )

  /** TLS to a database (ADR-0013 §5). Two modes, and none in between that encrypts and trusts anyone. */
  sealed trait Tls

  object Tls {

    /** No TLS: for a database on this machine. */
    case object Disabled extends Tls

    /** `sslmode=verify-full`: the server proves it is the host that was named, to this root. */
    final case class VerifyFull(rootCertificate: String) extends Tls
  }

  /**
   * @param connect how long a connection is waited for. It is how long a call takes to fail
   *   when the database is down
   * @param statement how long one statement may run
   */
  final case class Timeouts(connect: FiniteDuration, statement: FiniteDuration)

  object Timeouts {

    /** The LogStore's own defaults (`CoordinationSettings`); a test holds the two to each other. */
    val Default: Timeouts = Timeouts(connect = 5.seconds, statement = 10.seconds)
  }

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
          databases.required("control")(_.obj(database)),
          databases.required("coordination")(_.obj(database))
        ).mapN(Databases)
      }),
      top.orElse("retries", Retries.Default)(_.obj(retries)),
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

  private def database(fields: Obj): Result[Database] =
    (
      fields.required("url")(_.text(PostgresUrl.parse)),
      fields.required("user")(_.text(SqlIdentifier.parse)),
      fields.required("password")(_.secret),
      fields.required("tls")(_.obj(tls)),
      fields.orElse("timeouts", Timeouts.Default)(_.obj(timeouts))
    ).mapN(Database)

  private def tls(fields: Obj): Result[Tls] = {
    val certificate = fields.optional("root_certificate")(_.text(absolutePath))
    val mode = fields.required("mode")(_.oneOf("disable" -> false, "verify-full" -> true))
    (mode, certificate).tupled.andThen {
      case (true, Some(path)) => Read.valid(Tls.VerifyFull(path))
      case (true, None) =>
        Read.invalid(
          fields.path("root_certificate"),
          "is missing: verify-full needs the certificate of the authority that signed the server's"
        )
      case (false, Some(_)) =>
        Read.invalid(
          fields.path("root_certificate"),
          "is set, and mode is disable: nothing would be verified with it"
        )
      case (false, None) => Read.valid(Tls.Disabled)
    }
  }

  private def timeouts(fields: Obj): Result[Timeouts] =
    (
      // The connection pool takes no shorter wait; a longer one is a hang, not a timeout.
      fields.orElse("connect", Timeouts.Default.connect)(_.duration(250.millis, 5.minutes)),
      fields.orElse("statement", Timeouts.Default.statement)(_.duration(1.second, 1.hour))
    ).mapN(Timeouts.apply)

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
    val lower = key.toLowerCase
    if (!key.startsWith("spark."))
      value.fail("is not a Spark setting: its key must start with spark.")
    else if (key == "spark.master") value.fail("is set by the field master of this profile")
    else if (key.startsWith("spark.delta.logStore."))
      value.fail(
        "is set by the engine: which LogStore commits to a table is not a setting of a profile (ADR-0004)"
      )
    else if (CredentialWords.exists(lower.contains))
      value.fail(
        "is a credential, and no credential is written in a profile: object storage takes its " +
          "own from the environment or from a Hadoop credential provider (ADR-0011)"
      )
    else value.scalarText.map(key -> _)
  }

  private val CredentialWords = Seq("password", "secret", "token", "access.key")

  private def absolutePath(text: String): Either[String, String] =
    if (!text.startsWith("/")) Left("must be an absolute path")
    else RelativePath.parse(text.drop(1)).map(_ => text)

  // -------------------------------------------------------------------------------------------
  // What is about more than one field
  // -------------------------------------------------------------------------------------------

  /** The errors and the warnings of a profile whose every field is valid by itself. */
  private[config] def rules(profile: Profile): (Chain[Problem], Chain[Problem]) = {
    val Profile(lake, landing, databases, retries, spark) = profile
    val local = lake.uri.isLocal
    val cluster = lake.deployment == Deployment.Cluster
    val clusterMaster = spark.master match {
      case _: Master.Standalone => true
      case _: Master.Local => false
    }
    val logStoreWait = databases.coordination.timeouts.connect * LogStoreAttempts.toLong

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
      ).flatten ++ Seq("control" -> databases.control, "coordination" -> databases.coordination)
        .collect {
          case (name, database) if database.tls == Tls.Disabled && !database.url.isLoopback =>
            Problem(
              s"databases.$name.tls.mode",
              "is disable, and the database is on another machine: its traffic, the password " +
                "included, is not encrypted"
            )
        }
    )

    (errors, warnings)
  }
}
