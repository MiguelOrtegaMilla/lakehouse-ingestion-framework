import Dependencies._
import ModuleBoundaries._

// Reload the build automatically when build.sbt or project/ changes (no stale build in the shell).
Global / onChangedBuildSource := ReloadOnSourceChanges

// The publisher identity: the only place the GitHub handle appears (ADR-0012).
ThisBuild / organization := "io.github.miguelortegamilla"
ThisBuild / licenses := Seq(License.Apache2)
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := Versions.scala

// `-Xlint` plus selective fatal warnings (ADR-0002): every warning fails the build except
// deprecations, which Spark and Delta upgrades can introduce and which must be reported, not
// block. `-Wunused:params` is left out: implementations of a trait often ignore a parameter.
ThisBuild / scalacOptions ++= Seq(
  "-release",
  "17",
  "-encoding",
  "utf8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint:_",
  "-Wdead-code",
  "-Wnumeric-widen",
  "-Wvalue-discard",
  "-Wunused:imports,privates,locals,implicits,patvars,nowarn",
  "-Wconf:any:e,cat=deprecation:w"
)

// One log4j version in every classpath, Spark's, and one Scala library, the build's and Spark's
// (see Dependencies.log4jAlignment and Dependencies.scalaAlignment).
ThisBuild / dependencyOverrides ++= log4jAlignment ++ scalaAlignment

lazy val root = (project in file("."))
  .aggregate(core, deltaPgLogstore, engine, cli, integrationTests)
  .settings(
    name := "lakehouse-ingestion",
    publish / skip := true,
    // `checkUnconnected`: no code of production that only the tests reach, in any module.
    Unconnected.rules
  )

/**
 * Pure domain: configuration model, validation, planning. No Spark, no JDBC, and no cycle among
 * its packages (PackageCycles).
 */
lazy val core = module("core", artifact = "core")
  .settings(
    rules(forbidden = Seq(Forbid.spark, Forbid.anyDelta, Forbid.hadoop) ++ Forbid.jdbc),
    PackageCycles.rules,
    libraryDependencies ++= Seq(catsCore, circeCore, snakeyamlEngine, scalatest)
  )

/**
 * PostgreSQL-coordinated Delta LogStore (ADR-0004). Depends on no other project module, so it
 * can be extracted as a library; sees only the LogStore API, never Spark, and brings neither
 * Unity Catalog's client libraries nor a logging backend with it (ADR-0016).
 */
lazy val deltaPgLogstore = module("delta-pg-logstore", artifact = "pg-logstore")
  .settings(
    rules(
      forbidden = Seq(Forbid.spark, Forbid.deltaSpark, Forbid.unityCatalog, Forbid.log4j),
      allowProjectDependencies = false
    ),
    libraryDependencies ++= Seq(
      deltaStorage,
      hadoopClientApi,
      postgresJdbc,
      hikari,
      slf4jApi,
      scalatest,
      scalaReflectTest,
      hadoopClientRuntimeTest
    ),
    // The tests use Hadoop's local file system, which keeps JVM-wide state and a shutdown hook:
    // they get a JVM of their own, not sbt's.
    Test / fork := true,
    // The module brings no logging backend (ADR-0016). slf4j says so on stderr at every start.
    Test / javaOptions += "-Dslf4j.internal.verbosity=ERROR"
  )

/**
 * Connectors, writers, control-plane repositories, engine service. No cycle among its packages
 * (PackageCycles).
 */
lazy val engine = module("engine", artifact = "engine")
  .dependsOn(core, deltaPgLogstore)
  .settings(
    // The engine checks the version of its schema, and never migrates it. And it reads JSON
    // with the safe reader of `core`, not with circe's parser.
    rules(forbidden = Seq(Forbid.flyway, Forbid.jawn)),
    PackageCycles.rules,
    libraryDependencies ++=
      Seq(sparkSql, deltaSpark, jacksonCore, postgresJdbc, hikari, slf4jApi, scalatest),
    // What a run records as the version of the application that ran it: the version of the
    // build and the commit it was built from (project/ApplicationVersion.scala).
    Compile / resourceGenerators += Def.task {
      val file =
        (Compile / resourceManaged).value / "lakehouse-ingestion" / "application.properties"
      val built = ApplicationVersion.of(version.value, (LocalRootProject / baseDirectory).value)
      val content = s"version=$built\n"
      if (!file.exists || IO.read(file) != content) IO.write(file, content)
      Seq(file)
    }.taskValue
  )

/** Entry point, argument parsing, wiring, exit codes. */
lazy val cli = module("cli", artifact = "cli")
  .dependsOn(engine)
  .settings(
    rules(forbidden = Nil),
    // Spark and Delta are `provided`, as in the engine: `run` builds the session of a run.
    libraryDependencies ++= flyway ++ Seq(sparkSql, deltaSpark, scalatest),
    Compile / mainClass := Some("lakehouse.ingestion.cli.Main"),
    // `sbt "cli/run validate …"`: a process of its own, so that its exit code is the command's
    // and not the end of sbt, started where sbt was, so that paths are those of the repository.
    run / fork := true,
    run / baseDirectory := (LocalRootProject / baseDirectory).value,
    // And with its two outputs as they are, not as lines of the log of sbt: what a command writes
    // to its standard output may be for another program to read, as the records of `quarantine`.
    run / outputStrategy := Some(StdoutOutput),
    // `sbt "cli/run run …"` runs Spark in that process (discovery §17, P5): with the classpath
    // that has what is `provided`, which `spark-submit` would supply, and the options of a JVM
    // that embeds Spark, as the tests have them (project/SparkTestJvm.scala).
    Compile / run := Defaults
      .runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner)
      .evaluated,
    run / javaOptions ++= SparkTestJvm.moduleOptions ++ Seq("-Duser.timezone=UTC"),
    run / envVars += "SPARK_LOCAL_IP" -> "127.0.0.1"
  )

/**
 * Stress runs (the LogStore spikes, ADR-0004 §7.3): tests tagged `StressTest`. They take minutes
 * and start several JVMs, so `test` leaves them out and `sbt stress` runs only them. The
 * configuration extends `Test`: same sources, classpath and forked JVM settings.
 */
lazy val Stress = config("stress").extend(Test)
lazy val stressTag = "lakehouse.ingestion.it.Stress"
lazy val excludeStress = Tests.Argument(TestFrameworks.ScalaTest, "-l", stressTag)

addCommandAlias("stress", "integration-tests/Stress/test")

/** Testcontainers, end-to-end, fault injection, stress harness and verifier. Never published. */
lazy val integrationTests = module("integration-tests", artifact = "integration-tests")
  // `test->test`: the LogStore module's contract suites and test doubles are reused here against
  // real services.
  .dependsOn(core, deltaPgLogstore % "compile->compile;test->test", engine, cli)
  .configs(Stress)
  .settings(SparkTestJvm.settings)
  .settings(inConfig(Stress)(Defaults.testTasks))
  .settings(
    Test / testOptions += excludeStress,
    Stress / testOptions := (Test / testOptions).value.filterNot(_ == excludeStress) :+
      Tests.Argument(TestFrameworks.ScalaTest, "-n", stressTag),
    rules(forbidden = Nil),
    publish / skip := true,
    libraryDependencies ++= Seq(
      scalatest,
      testcontainers,
      testcontainersToxiproxy,
      postgresJdbc % Test,
      sparkSqlTest,
      deltaSparkTest,
      log4jSlf4j,
      log4jCore
    ),
    // The suites use repository files (compose file, init scripts) through this path.
    Test / javaOptions += s"-Drepo.root=${(LocalRootProject / baseDirectory).value}"
  )

def module(id: String, artifact: String): Project =
  Project(id, file(s"modules/$id"))
    .settings(
      name := s"lakehouse-ingestion-$artifact",
      // The REPL is for exploration; unused imports there are not errors.
      Compile / console / scalacOptions ~= (_.filterNot(o =>
        o.startsWith("-Wunused") || o.startsWith("-Wconf")
      ))
    )
