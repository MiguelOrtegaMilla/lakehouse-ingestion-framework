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

// One log4j version in every classpath: Spark's (see Dependencies.log4jAlignment).
ThisBuild / dependencyOverrides ++= log4jAlignment

lazy val root = (project in file("."))
  .aggregate(core, deltaPgLogstore, engine, cli, integrationTests)
  .settings(
    name := "lakehouse-ingestion",
    publish / skip := true
  )

/** Pure domain: configuration model, validation, planning. No Spark, no JDBC. */
lazy val core = module("core", artifact = "core")
  .settings(rules(forbidden = Seq(Forbid.spark, Forbid.anyDelta, Forbid.hadoop) ++ Forbid.jdbc))

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

/** Connectors, writers, control-plane repositories, engine service. */
lazy val engine = module("engine", artifact = "engine")
  .dependsOn(core, deltaPgLogstore)
  .settings(
    rules(forbidden = Nil),
    libraryDependencies ++= Seq(sparkSql, deltaSpark)
  )

/** Entry point, argument parsing, wiring, exit codes. */
lazy val cli = module("cli", artifact = "cli")
  .dependsOn(engine)
  .settings(rules(forbidden = Nil))

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
    ) ++ flywayTest,
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
