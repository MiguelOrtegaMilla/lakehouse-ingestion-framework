package lakehouse.ingestion.it.support

import scala.util.control.ControlThrowable

/**
 * What a test throws at a fault point of the engine, for a run that dies there, in the JVM of
 * the test, as the process of a run dies when it is killed: nothing after the point runs, and
 * the run says nothing of how it ended. Its lease runs out, and the run that takes it over
 * finds what the dead one left.
 *
 * It is a `ControlThrowable`, as the `SimulatedCrash` of the LogStore's tests is, and so not a
 * failure that the engine catches (`NonFatal` takes none). A run that fails with what nothing
 * classified says so, as `internal`, and gives its lease up (ADR-0022 §3): a death that the
 * engine could catch would be that, and not a death.
 */
final class Died extends ControlThrowable("the run died here")
