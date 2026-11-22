import sbt._
import sbt.Keys._
import sbt.internal.inc.Analysis

/**
 * Fails a module whose packages depend on each other in a cycle.
 *
 * A package that depends on another, which depends on it back, cannot be understood, tested or
 * moved alone: the two are one package under two names. The dependencies are those that the
 * compiler recorded between the classes of the module's main sources (zinc's analysis), so a
 * name written in full counts as an import does, and a comment does not.
 *
 * The package of a class is the part of its name before the first segment that starts with an
 * upper-case letter: `a.b.Load.Incremental` is of `a.b`. A package object is of its package.
 */
object PackageCycles {

  /** The cycles among the packages of `analysis`: for each, the edges in it, each with a cause. */
  def of(analysis: Analysis): Seq[Seq[String]] = {
    def packageOf(className: String): String =
      className
        .split('.')
        .takeWhile(s => s.nonEmpty && !s.head.isUpper && s != "package")
        .mkString(".")

    val edges: Map[(String, String), (String, String)] =
      analysis.relations.internalClassDep.all.toSeq
        .map { case (from, to) => (packageOf(from), packageOf(to)) -> (from, to) }
        .filter { case ((a, b), _) => a != b && a.nonEmpty && b.nonEmpty }
        .groupBy(_._1)
        .map { case (edge, causes) => edge -> causes.map(_._2).min }

    val next: Map[String, Set[String]] =
      edges.keys.groupBy(_._1).map { case (from, out) => from -> out.map(_._2).toSet }

    def reach(from: String): Set[String] = {
      @annotation.tailrec
      def go(todo: List[String], seen: Set[String]): Set[String] = todo match {
        case Nil => seen
        case p :: rest =>
          val fresh = next.getOrElse(p, Set.empty) -- seen
          go(fresh.toList ++ rest, seen ++ fresh)
      }
      go(List(from), Set.empty)
    }

    val reachable = (edges.keys.map(_._1) ++ edges.keys.map(_._2)).map(p => p -> reach(p)).toMap
    // A strongly connected set: the packages that each reaches the other.
    val components = reachable.keys.toSeq.sorted
      .map(p => (reachable(p).filter(q => reachable(q).contains(p)) + p).toSeq.sorted)
      .filter(_.size > 1)
      .distinct

    components.map { members =>
      edges.toSeq
        .filter { case ((a, b), _) => members.contains(a) && members.contains(b) }
        .sortBy(_._1)
        .map { case ((a, b), (from, to)) => s"$a -> $b ($from uses $to)" }
    }
  }

  /** Makes `checkBoundaries` of the module fail on a cycle among its packages as well. */
  def rules: Seq[Setting[_]] = Seq(
    ModuleBoundaries.checkBoundaries := {
      ModuleBoundaries.checkBoundaries.value
      val log = streams.value.log
      val id = thisProject.value.id
      val analysis = (Compile / compile).value match {
        case a: Analysis => a
        case other => sys.error(s"[$id] the analysis of the compiler is not one: $other")
      }
      val cycles = of(analysis)
      if (cycles.nonEmpty)
        throw new MessageOnlyException(
          cycles
            .map(_.mkString("\n    "))
            .mkString(
              s"[$id] has packages that depend on each other in a cycle:\n  - ",
              "\n  - ",
              ""
            )
        )
      log.info(s"[$id] no cycle among its packages")
    }
  )
}
