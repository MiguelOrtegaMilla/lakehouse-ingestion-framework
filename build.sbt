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
 * can be extracted as a library; sees only the LogStore API, never Spark.
 */
lazy val deltaPgLogstore = module("delta-pg-logstore", artifact = "pg-logstore")
  .settings(
    rules(forbidden = Seq(Forbid.spark, Forbid.deltaSpark), allowProjectDependencies = false),
    libraryDependencies ++= Seq(deltaStorage, hadoopClientApi)
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

/** Testcontainers, end-to-end, fault injection, stress harness and verifier. Never published. */
lazy val integrationTests = module("integration-tests", artifact = "integration-tests")
  .dependsOn(core, deltaPgLogstore, engine, cli)
  .settings(
    rules(forbidden = Nil),
    publish / skip := true,
    libraryDependencies ++= Seq(
      scalatest,
      testcontainers,
      postgresJdbc % Test,
      log4jSlf4j,
      log4jCore
    ),
    // Own JVM for the suites: isolated heap and system properties (Spark-ready for P1.3).
    Test / fork := true,
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
