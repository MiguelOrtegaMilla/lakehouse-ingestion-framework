import sbt._
import sbt.Keys._

/**
 * Enforces the module dependency rules of ADR-0012 on the *resolved* classpath.
 *
 * The build graph already stops `core` from declaring Spark. What it does not stop is a library
 * added to `core` that pulls Spark, Hadoop or a JDBC driver in transitively: the import would
 * then compile. This check inspects every module on the compile classpath, `provided` included,
 * and fails the build on a forbidden one.
 */
object ModuleBoundaries {

  val checkBoundaries =
    taskKey[Unit]("Fails if the resolved compile classpath breaks the ADR-0012 module rules")

  /** Forbids every module of `organization` whose name starts with `namePrefix`. */
  final case class Forbidden(organization: String, namePrefix: String = "") {
    def matches(m: ModuleID): Boolean =
      m.organization == organization && m.name.startsWith(namePrefix)

    override def toString: String = s"$organization:${namePrefix}*"
  }

  /** The forbidden groups the rules of ADR-0012 are written in. */
  object Forbid {
    val spark: Forbidden = Forbidden("org.apache.spark")
    val deltaSpark: Forbidden = Forbidden("io.delta", "delta-spark")
    val anyDelta: Forbidden = Forbidden("io.delta")
    val hadoop: Forbidden = Forbidden("org.apache.hadoop")
    val jdbc: Seq[Forbidden] =
      Seq(Forbidden("org.postgresql"), Forbidden("com.zaxxer"), Forbidden("org.flywaydb"))
  }

  /**
   * @param forbidden external modules that must not appear on the compile classpath
   * @param allowProjectDependencies false for a module that must stay extractable as a library
   */
  def rules(
      forbidden: Seq[Forbidden],
      allowProjectDependencies: Boolean = true
  ): Seq[Setting[_]] = Seq(
    checkBoundaries := {
      val log = streams.value.log
      val id = thisProject.value.id
      val external = (Compile / dependencyClasspath).value.flatMap(_.get(moduleID.key))

      val forbiddenModules = for {
        m <- external
        rule <- forbidden.find(_.matches(m))
      } yield s"$m (rule $rule)"

      val forbiddenProjects =
        if (allowProjectDependencies) Nil
        else
          thisProject.value.dependencies.map(d => s"project dependency on '${d.project.project}'")

      val violations = forbiddenModules ++ forbiddenProjects
      if (violations.nonEmpty)
        sys.error(
          violations.mkString(s"[$id] violates ADR-0012 module boundaries:\n  - ", "\n  - ", "")
        )
      log.info(s"[$id] module boundaries OK (${external.size} external modules checked)")
    }
  )
}
