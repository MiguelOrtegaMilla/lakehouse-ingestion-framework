// Formatting gate (ADR-0002). Further plugins arrive with the phase that needs them:
// sbt-assembly with the CLI fat jar, sbt-scoverage with the coverage report.
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.6.2")
