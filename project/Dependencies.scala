import sbt._

/**
 * Single source of truth for library versions (ADR-0002, versions pinned by ADR-0014).
 *
 * A library is added here when the first module that uses it is written, not before.
 */
object Dependencies {

  object Versions {
    // Must equal the Scala patch Apache Spark is built with (spark-parent `scala.version`).
    val scala = "2.13.17"

    val spark = "4.1.4"
    val delta = "4.4.1"
    // The Hadoop line bundled with Spark 4.1.4 (spark-parent `hadoop.version`).
    val hadoop = "3.4.2"
    // The log4j line bundled with Spark 4.1.4 (spark-parent `log4j.version`).
    val log4j = "2.24.3"

    val postgresJdbc = "42.7.14"
    val hikari = "7.1.0"
    val flyway = "13.12.2"
    val scalatest = "3.2.20"
    // Upgrading: re-pin Ryuk in integration-tests' testcontainers.properties (ADR-0013 §5).
    val testcontainers = "2.0.5"
  }

  // Spark and Delta are `provided`: the Spark runtime (local or Standalone) supplies them.
  val sparkSql = "org.apache.spark" %% "spark-sql" % Versions.spark % Provided
  val deltaSpark = "io.delta" % "delta-spark_4.1_2.13" % Versions.delta % Provided

  // The LogStore API only; `delta-pg-logstore` must not see Spark or delta-spark (ADR-0012).
  // `provided`, like the Hadoop API: the Delta runtime that loads the LogStore supplies it. Its
  // dependencies on Unity Catalog's client libraries are left out: the LogStore API does not use
  // them, and they bring a logging backend (ADR-0016).
  val deltaStorage = ("io.delta" % "delta-storage" % Versions.delta % Provided)
    .excludeAll(ExclusionRule(organization = "io.unitycatalog"))
  val hadoopClientApi = "org.apache.hadoop" % "hadoop-client-api" % Versions.hadoop % Provided

  // The classes behind the Hadoop API, for the LogStore module's own tests on Hadoop's local
  // file system. At run time Spark supplies them. reload4j, a logging backend the artifact
  // declares, is left out: the module has none, in its tests either (ADR-0016).
  val hadoopClientRuntimeTest =
    ("org.apache.hadoop" % "hadoop-client-runtime" % Versions.hadoop % Test)
      .exclude("ch.qos.reload4j", "reload4j")

  val postgresJdbc = "org.postgresql" % "postgresql" % Versions.postgresJdbc

  // The LogStore's connection pool (ADR-0004 §3.9). Its one dependency is slf4j-api.
  val hikari = "com.zaxxer" % "HikariCP" % Versions.hikari

  /**
   * Pins every log4j2 module to Spark's version. log4j modules must all have one version, and
   * Delta's Unity Catalog client dependencies (`unitycatalog-client`, `unitycatalog-hadoop`
   * 0.6.0) pull `log4j-api` and `log4j-slf4j2-impl` 2.25.3. Without this override, eviction
   * would mix 2.25.3 and 2.24.3. Under `spark-submit` the runtime is Spark's 2.24.3; tests match
   * it.
   */
  val log4jAlignment: Seq[ModuleID] =
    Seq(
      "log4j-api",
      "log4j-core",
      "log4j-slf4j2-impl",
      "log4j-1.2-api",
      "log4j-layout-template-json"
    )
      .map("org.apache.logging.log4j" % _ % Versions.log4j)

  // `provided` dependencies are not inherited through `dependsOn`, so a module that runs Spark in
  // its tests declares the runtime itself, in the Test configuration.
  val sparkSqlTest = sparkSql.withConfigurations(Some(Test.name))
  val deltaSparkTest = deltaSpark.withConfigurations(Some(Test.name))

  // Test-only. log4j2 is the only logging backend, as in Spark (ADR-0002): Testcontainers logs
  // through SLF4J, which this binding routes to log4j2.
  val scalatest = "org.scalatest" %% "scalatest" % Versions.scalatest % Test
  // For a test that asks a compiled class how it was declared. ScalaTest brings it anyway.
  val scalaReflectTest = "org.scala-lang" % "scala-reflect" % Versions.scala % Test
  val testcontainers = "org.testcontainers" % "testcontainers" % Versions.testcontainers % Test

  // A TCP proxy in a container, to cut, delay or stall real connections in tests. Its image is
  // pinned in integration-tests' testcontainers.properties.
  val testcontainersToxiproxy =
    "org.testcontainers" % "testcontainers-toxiproxy" % Versions.testcontainers % Test

  // Migrations (ADR-0002). Test-only until a module applies them itself (the control plane, P4):
  // the LogStore ships its migrations as SQL and never runs them (ADR-0004, deviation 10).
  val flywayTest: Seq[ModuleID] =
    Seq("flyway-core", "flyway-database-postgresql").map(
      "org.flywaydb" % _ % Versions.flyway % Test
    )
  val log4jSlf4j = "org.apache.logging.log4j" % "log4j-slf4j2-impl" % Versions.log4j % Test
  val log4jCore = "org.apache.logging.log4j" % "log4j-core" % Versions.log4j % Test
}
