// Formatting gate (ADR-0002). Further plugins arrive with the phase that needs them:
// sbt-assembly with the CLI fat jar, sbt-scoverage with the coverage report.
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.6.2")

// Resolves the dependency graph that CI submits to GitHub (ADR-0013 §4,
// scripts/dependency-snapshot.sh). It is one of the build's plugins, and not added to the build
// by the job that uses it, so that the job that runs the tests downloads it too: that job is the
// one that saves the cache of CI, and what it does not download is downloaded again every time.
addSbtPlugin("ch.epfl.scala" % "sbt-github-dependency-submission" % "3.2.2")
