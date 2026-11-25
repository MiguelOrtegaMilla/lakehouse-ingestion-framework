import java.net.URLClassLoader

import scala.collection.mutable
import scala.reflect.NameTransformer

import sbt._
import sbt.Keys._
import sbt.internal.inc.{Analysis, ZincRelations}

/**
 * Fails the build on code of production that no code of production reaches: what only the tests
 * call, and what nothing calls.
 *
 * Code that the tests alone keep alive is the hardest to see by reading: it compiles, it is
 * covered, and it does nothing in the application. The rule reads what the compiler recorded of
 * the main sources and of the tests of every module (zinc's analysis, as `PackageCycles` does),
 * so a name in a comment or in a string counts for nothing.
 *
 * '''Classes.''' A class is reached when a class that is reached depends on it, starting from
 * the classes of the list (`project/unconnected.txt`): the entry point of the command line, and
 * whatever is left unconnected on purpose, each with its reason. The dependencies are those that
 * the compiler recorded. It records none between two classes of one file, which it always
 * compiles together: there, a class depends on those whose name it uses. A class and its
 * companion are one; a class inside another is a class of its own.
 *
 * '''Members, as closely as zinc tells them.''' A member of a class that is reached is used when
 * a class that is reached uses its name and depends on the class, on one that it extends or on
 * one that extends it. It is the question zinc asks to decide what to compile again. A name that
 * a class takes from one that it extends is judged where it is first declared, and one that it
 * takes from outside the build (`toString`, `close`, what Delta asks of a LogStore) is not
 * judged: its callers are not in the build.
 *
 * What the rule does not see, so that nobody takes its silence for more than it is:
 *   - a member that nothing calls, in a class one of whose users calls another member of the same
 *     name, of its own or of another class;
 *   - a member that only a member that nothing calls uses: it is found once that one is gone;
 *   - a `val`, which its own accessor uses, and so the fields of a case class; and what the
 *     compiler writes for a case class (`apply`, `copy`);
 *   - members that are private, which the compiler warns of (`-Wunused:privates`).
 *
 * An entry of the list that is reached by now, or that names nothing, fails the build as well, so
 * the list cannot outlive its reasons.
 */
object Unconnected {

  val checkUnconnected =
    taskKey[Unit]("Fails on code of production that no code of production reaches")

  /**
   * What the analyses of a build say of its classes, by the names of the sources
   * (`a.b.Outer.Inner`).
   *
   * @param production the classes of the main sources of every module
   * @param tests the classes of the tests of every module
   * @param uses each class with the classes of the build that it depends on
   * @param file each class of production with the source file that it is in
   * @param parents each class of production with those of production that it extends
   * @param names each class with the names that its code uses
   * @param defined each class of production with the names of its members, its own and inherited
   * @param outside each class of production with the names of the members of what it extends
   *   outside the build; `None` for a class whose ancestors could not be read
   */
  final case class Facts(
      production: Set[String],
      tests: Set[String],
      uses: Map[String, Set[String]],
      file: Map[String, String],
      parents: Map[String, Set[String]],
      names: Map[String, Set[String]],
      defined: Map[String, Set[String]],
      outside: Map[String, Option[Set[String]]]
  )

  /** A class (`a.b.C`) or a member of one (`a.b.C#member`) that the list leaves unconnected. */
  final case class Entry(id: String, reason: String)

  /** A class or a member that production does not reach, with the tests that use it. */
  final case class Finding(id: String, tests: Seq[String]) {
    def show: String = {
      val by =
        if (tests.isEmpty) "nothing uses it"
        else {
          val some = tests.take(3).map(_.split('.').last).mkString(", ")
          val more = if (tests.size > 3) s" and ${tests.size - 3} more" else ""
          s"only tests use it: $some$more"
        }
      s"$id ($by)"
    }
  }

  /**
   * @param unconnected what is not reached and not in the list
   * @param connected the entries of the list that production reaches without them
   * @param missing the entries of the list that name no class, or no member, of production
   */
  final case class Report(unconnected: Seq[Finding], connected: Seq[String], missing: Seq[String]) {
    def clean: Boolean = unconnected.isEmpty && connected.isEmpty && missing.isEmpty
  }

  // Members that every class and trait has, from the language and from `java.lang.Object`.
  private val ofEveryClass =
    Set("==", "!=", "##", "eq", "ne", "synchronized", "asInstanceOf", "isInstanceOf") ++
      classOf[Object].getDeclaredMethods.map(_.getName)

  // What the compiler writes for a case class, and what the JVM calls by its name.
  private val ofACaseClass = Set("apply", "unapply", "copy")
  private val calledByName = Set("main")

  private def isSynthetic(name: String): Boolean =
    name.contains("$") || name.contains(";") || name.startsWith("<")

  def of(facts: Facts, entries: Seq[Entry]): Report = {
    import facts._

    val (memberEntries, classEntries) = entries.map(_.id).partition(_.contains("#"))

    def enclosing(c: String): Seq[String] =
      c.split('.').inits.map(_.mkString(".")).toSeq.drop(1).filter(production)
    def inside(c: String, outer: String): Boolean = c == outer || c.startsWith(outer + ".")

    def simple(c: String): String = c.split('.').last
    val inFile: Map[String, Set[String]] =
      file.toSeq.groupBy(_._2).map { case (f, classes) => f -> classes.map(_._1).toSet }
    def beside(c: String): Set[String] = file.get(c).flatMap(inFile.get).getOrElse(Set.empty) - c

    // The compiler records no dependency between two classes of one file, which it always
    // compiles together: there, a class depends on those whose name it uses.
    val usesOfProduction: Map[String, Set[String]] =
      production.toSeq.map { from =>
        val used = names.getOrElse(from, Set.empty)
        from -> (uses.getOrElse(from, Set.empty).filter(production) ++
          beside(from).filter(to => used(simple(to))))
      }.toMap

    def reachedFrom(roots: Set[String]): Set[String] = {
      @annotation.tailrec
      def go(todo: List[String], seen: Set[String]): Set[String] = todo match {
        case Nil => seen
        case c :: rest =>
          val fresh = usesOfProduction.getOrElse(c, Set.empty) -- seen
          go(fresh.toList ++ rest, seen ++ fresh)
      }
      go(roots.toList, roots)
    }

    // An entry of a class is of everything inside the class as well.
    def rootsOf(listed: Seq[String]): Set[String] =
      production.filter(c => listed.exists(inside(c, _)))

    val reached = reachedFrom(rootsOf(classEntries))

    def testsOf(classes: Set[String], name: Option[String]): Seq[String] =
      tests.toSeq.sorted.filter { t =>
        uses.getOrElse(t, Set.empty).exists(classes) &&
        name.forall(names.getOrElse(t, Set.empty))
      }

    val unreached = (production -- reached).toSeq.sorted
      .filterNot(c => enclosing(c).exists(outer => !reached(outer)))
      .map(c => Finding(c, testsOf(production.filter(inside(_, c)), None)))

    def closure(from: String, next: Map[String, Set[String]]): Set[String] = {
      @annotation.tailrec
      def go(todo: List[String], seen: Set[String]): Set[String] = todo match {
        case Nil => seen
        case c :: rest =>
          val fresh = next.getOrElse(c, Set.empty) -- seen
          go(fresh.toList ++ rest, seen ++ fresh)
      }
      go(List(from), Set.empty)
    }
    val children: Map[String, Set[String]] =
      parents.toSeq
        .flatMap { case (child, ps) => ps.map(_ -> child) }
        .groupBy(_._1)
        .map { case (parent, pairs) => parent -> pairs.map(_._2).toSet }

    final case class Member(owner: String, name: String) {
      val id = s"$owner#$name"
      private val family = closure(owner, parents) ++ closure(owner, children) + owner
      private val besideIt = family.flatMap(beside)
      private def usedBy(users: Set[String]): Boolean =
        users.exists { c =>
          names.getOrElse(c, Set.empty)(name) &&
          (family(c) || besideIt(c) || uses.getOrElse(c, Set.empty).exists(family))
        }
      def usedByProduction: Boolean = usedBy(reached)
      def tests: Seq[String] = testsOf(family, Some(name))
    }

    // The members that a class declares first: not those of what it extends, in the build or
    // outside it, nor the classes inside it, which are judged as classes.
    def membersOf(c: String): Seq[Member] = outside.getOrElse(c, None) match {
      case None => Nil
      case Some(fromOutside) =>
        val all = defined.getOrElse(c, Set.empty)
        val inherited = closure(c, parents).flatMap(defined.getOrElse(_, Set.empty))
        val ofTheCompiler = if (all("productArity")) ofACaseClass else Set.empty[String]
        (all -- inherited -- fromOutside -- ofEveryClass -- ofTheCompiler -- calledByName)
          .filterNot(isSynthetic)
          .filterNot(_ == simple(c))
          .filterNot(name => production(s"$c.$name"))
          .toSeq
          .sorted
          .map(Member(c, _))
    }

    val listedWhole = rootsOf(classEntries)
    val members = reached.toSeq.sorted.flatMap(membersOf)
    val unused = members
      .filterNot(_.usedByProduction)
      .filterNot(m => memberEntries.contains(m.id) || listedWhole(m.owner))
      .map(m => Finding(m.id, m.tests))

    val missing =
      classEntries.filterNot(production) ++
        memberEntries.filterNot(id => members.exists(_.id == id))
    val connected =
      classEntries.filter(production).filter { c =>
        reachedFrom(rootsOf(classEntries.filterNot(_ == c)))(c)
      } ++ members.filter(m => memberEntries.contains(m.id) && m.usedByProduction).map(_.id)

    Report(unreached ++ unused, connected.sorted, missing.sorted)
  }

  /** The list: an entry a line, its id and then its reason; `#` starts a line of comment. */
  def entries(file: File): Seq[Entry] =
    IO.readLines(file).map(_.trim).filterNot(l => l.isEmpty || l.startsWith("#")).map { line =>
      line.split("\\s+", 2) match {
        case Array(id, reason) if reason.trim.nonEmpty => Entry(id, reason.trim)
        case _ =>
          sys.error(s"$file: an entry is an id and the reason for it, and this is not: $line")
      }
    }

  /** The facts of a build, from the analyses of its modules and the class files on their paths. */
  def facts(main: Seq[Analysis], test: Seq[Analysis], classpath: Seq[File]): Facts = {
    val all = main ++ test
    val production = main.flatMap(_.apis.internal.keys).toSet
    val tests = test.flatMap(_.apis.internal.keys).toSet
    val known = production ++ tests

    // A dependency on a class of another module is recorded by the name of its class file.
    val sourceName: Map[String, String] =
      all
        .flatMap(_.relations.productClassName.all.map { case (source, binary) => binary -> source })
        .toMap
    def ofTheBuild(name: String): Option[String] =
      sourceName.get(name).orElse(Some(name).filter(known))

    def grouped(pairs: Seq[(String, String)]): Map[String, Set[String]] =
      pairs.groupBy(_._1).map { case (from, to) => from -> to.map(_._2).toSet }

    val uses = grouped(all.flatMap { a =>
      a.relations.internalClassDep.all.toSeq ++
        a.relations.externalClassDep.all.toSeq.flatMap { case (from, to) =>
          ofTheBuild(to).map(from -> _)
        }
    })
    val parents = grouped(main.flatMap { a =>
      ZincRelations.inheritance(a).flatMap { case (child, parent) =>
        ofTheBuild(parent).filter(production).map(child -> _)
      }
    })
    val names = all.flatMap(ZincRelations.usedNames).groupBy(_._1).map { case (user, used) =>
      user -> used.flatMap(_._2).toSet
    }
    val defined = main
      .flatMap(_.apis.internal.map { case (c, analyzed) =>
        c -> analyzed.nameHashes().map(_.name()).toSet
      })
      .toMap
    val file =
      main.flatMap(_.relations.classes.all.map { case (source, c) => c -> source.id }).toMap
    val classFiles = grouped(main.flatMap(_.relations.productClassName.all.toSeq))

    Facts(
      production,
      tests,
      uses,
      file,
      parents,
      names,
      defined,
      extendedOutside(classFiles, classpath)
    )
  }

  /**
   * For each class, the names of the members of every class and interface that it extends
   * outside the build, read from the class files with nothing initialised. `None` where an
   * ancestor could not be read, as one whose own dependencies are not on the path.
   */
  private def extendedOutside(
      classFiles: Map[String, Set[String]],
      classpath: Seq[File]
  ): Map[String, Option[Set[String]]] = {
    val ours = classFiles.values.flatten.toSet
    val loader =
      new URLClassLoader(classpath.map(_.toURI.toURL).toArray, ClassLoader.getPlatformClassLoader)
    val members = mutable.Map.empty[Class[_], Set[String]]

    def extended(c: Class[_]): Seq[Class[_]] = Option(c.getSuperclass).toSeq ++ c.getInterfaces
    def membersOf(c: Class[_]): Set[String] = members.getOrElseUpdate(
      c,
      (c.getDeclaredMethods.map(_.getName) ++ c.getDeclaredFields.map(_.getName))
        .map(NameTransformer.decode)
        .toSet ++ extended(c).flatMap(membersOf)
    )
    def fromOutside(c: Class[_]): Set[String] =
      extended(c).flatMap(p => if (ours(p.getName)) fromOutside(p) else membersOf(p)).toSet

    try
      classFiles.map { case (source, binaries) =>
        source -> (
          try Some(binaries.flatMap(b => fromOutside(Class.forName(b, false, loader))))
          catch { case _: LinkageError | _: ClassNotFoundException => None }
        )
      }
    finally loader.close()
  }

  private val ofAModule = Def.task {
    def analysis(compiled: xsbti.compile.CompileAnalysis): Analysis = compiled match {
      case a: Analysis => a
      case other => sys.error(s"the analysis of the compiler is not one: $other")
    }
    (
      analysis((Compile / compile).value),
      analysis((Test / compile).value),
      (Compile / fullClasspath).value.map(_.data)
    )
  }

  /** `checkUnconnected`, for the project that has every module of the build under it. */
  def rules: Seq[Setting[_]] = Seq(
    checkUnconnected := {
      val log = streams.value.log
      val list = (LocalRootProject / baseDirectory).value / "project" / "unconnected.txt"
      val modules = ofAModule.all(ScopeFilter(inAnyProject)).value
      val known = facts(modules.map(_._1), modules.map(_._2), modules.flatMap(_._3).distinct)
      val listed = entries(list)
      val report = of(known, listed)

      val unread = known.outside.collect { case (c, None) => c }.toSeq.sorted
      if (unread.nonEmpty)
        log.warn(
          unread.mkString(
            "Classes whose members are not judged, since what they extend could not be read:\n  - ",
            "\n  - ",
            ""
          )
        )

      def section(title: String, lines: Seq[String]): Seq[String] =
        if (lines.isEmpty) Nil else title +: lines.map("  - " + _)
      if (!report.clean)
        throw new MessageOnlyException(
          (section(
            s"Code of production that no code of production reaches. Connect it, remove it, or " +
              s"give it a line in ${list.getName} with the reason for leaving it:",
            report.unconnected.map(_.show)
          ) ++ section(
            s"In ${list.getName}, and reached by production without it. Take it out of the list:",
            report.connected
          ) ++ section(
            s"In ${list.getName}, and no class or member of production:",
            report.missing
          )).mkString("\n")
        )
      log.info(
        s"every class and member of production is reached, but for the ${listed.size} of " +
          s"${list.getName} (${known.production.size} classes)"
      )
    }
  )
}
