package lakehouse.ingestion.it.support

import org.scalatest.Tag

/**
 * Marks a test as a stress run: minutes long, several JVMs, results written to `target/stress`.
 * `sbt test` excludes it; `sbt stress` runs only these (see `build.sbt`, configuration `Stress`).
 */
object StressTest extends Tag("lakehouse.ingestion.it.Stress")
