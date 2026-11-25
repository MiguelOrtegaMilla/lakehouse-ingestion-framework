package sbt.internal.inc

/**
 * Two relations of the analysis of the compiler that zinc shows to its own package alone, which
 * is why this object is in it: the names that each class uses, and which class extends which.
 * They are what zinc itself decides by, when a member changes, which classes to compile again.
 *
 * Both are internals of zinc, of the version that `project/build.properties` pins through sbt's:
 * one that renames them fails the load of the build, and nothing else.
 */
object ZincRelations {

  /** For each class of the analysis, the names that its code uses: of members, types, packages. */
  def usedNames(analysis: Analysis): Map[String, Set[String]] =
    analysis.relations.names.toMultiMap.map { case (user, names) =>
      user -> names.map(_.name).toSet
    }.toMap

  /** Each class of the analysis with each class or trait of the build that it extends. */
  def inheritance(analysis: Analysis): Seq[(String, String)] = {
    val inheritance = analysis.relations.inheritance
    (inheritance.internal.all ++ inheritance.external.all).toSeq
  }
}
