package lakehouse.ingestion.core.config

import cats.data.{Chain, NonEmptyChain}

/**
 * Everything an application is told: where it runs, and what it ingests. It exists only whole
 * and valid: a profile and every source, each valid by itself, and all of them together
 * (ADR-0007: the whole configuration is validated before any data is touched).
 *
 * @param sources by their id
 */
final case class Configuration(profile: Profile, sources: Vector[Source])

object Configuration {

  /** A file of configuration: its name, for the errors, and what is written in it. */
  final case class File(name: String, text: String)

  /** The column that the engine adds to every row: the source it came from. */
  val SourceIdColumn = "_source_id"

  /**
   * Reads and validates a configuration. Every file is read whatever is wrong in the others, so
   * the errors are those of all of them, and then the rules that are about more than one source
   * are applied to the sources that are valid by themselves.
   *
   * @param kinds the kinds of connector that the application has
   */
  def load(
      profile: File,
      sources: Seq[File],
      kinds: Seq[ConnectorKind] = ConnectorKind.BuiltIn
  ): Either[NonEmptyChain[ConfigError], Loaded[Configuration]] =
    load(Some(profile), sources, Chain.empty, kinds)

  /**
   * As `load`, for files of which some could not be read at all. Those that could are read and
   * validated all the same, so that what is wrong in them is said with what could not be read.
   *
   * @param profile `None` if the file of the profile could not be read
   * @param unread why each file that could not be read was not
   */
  private[config] def load(
      profile: Option[File],
      sources: Seq[File],
      unread: Chain[ConfigError],
      kinds: Seq[ConnectorKind]
  ): Either[NonEmptyChain[ConfigError], Loaded[Configuration]] = {
    val readProfile = profile.map(file => Profile.load(file.name, file.text))
    val readSources =
      sources.sortBy(_.name).map(file => file.name -> Source.load(file.name, file.text, kinds))
    val valid = readSources.collect { case (name, Right(loaded)) => name -> loaded.value }.toVector

    val errors =
      unread ++
        Chain.fromSeq(readProfile.toSeq.flatMap(_.left.toSeq.flatMap(_.toChain.toList))) ++
        Chain.fromSeq(readSources.flatMap(_._2.left.toSeq.flatMap(_.toChain.toList))) ++
        across(valid)

    (readProfile, NonEmptyChain.fromChain(errors)) match {
      case (Some(Right(loaded)), None) =>
        val warnings = loaded.warnings ++ readSources.flatMap(_._2.toSeq.flatMap(_.warnings))
        Right(Loaded(Configuration(loaded.value, valid.map(_._2).sortBy(_.id.value)), warnings))
      case (_, Some(found)) => Left(found)
      // Not reached: a profile that is missing or not valid has its error among those above.
      case (Some(Left(found)), None) => Left(found)
      case (None, None) =>
        Left(NonEmptyChain.one(ConfigError("the profile", None, "was not read")))
    }
  }

  // -------------------------------------------------------------------------------------------
  // What is about more than one source
  // -------------------------------------------------------------------------------------------

  /** A table that a source writes: in which file, by which field, in which layer. */
  final private case class Written(file: String, field: String, layer: String, table: RelativePath)

  /** The errors of sources that are each valid, in the order of their files. */
  private[config] def across(sources: Vector[(String, Source)]): Chain[ConfigError] =
    Chain.fromSeq(
      (named(sources) ++ repeated(sources) ++ layers(sources) ++ fanIn(sources))
        .sortBy(error => (error.file, error.field))
    )

  /** A source is in the file of its name: one file for each, and none to look for. */
  private def named(sources: Vector[(String, Source)]): Seq[ConfigError] =
    sources.collect {
      case (file, source) if baseName(file) != s"${source.id}.yaml" =>
        ConfigError(
          file,
          Some("id"),
          s"is ${source.id}, and a source is in the file of its name, ${source.id}.yaml"
        )
    }

  private def repeated(sources: Vector[(String, Source)]): Seq[ConfigError] =
    sources.groupBy(_._2.id.value).values.filter(_.size > 1).toSeq.flatMap { same =>
      same.map { case (file, source) =>
        ConfigError(
          file,
          Some("id"),
          s"is ${source.id}, which is the id of ${others(file, same.map(_._1))} as well"
        )
      }
    }

  /** A table has one layer, and no table is inside another. */
  private def layers(sources: Vector[(String, Source)]): Seq[ConfigError] = {
    val written = sources.flatMap { case (file, source) =>
      Written(file, "targets.bronze.table", "Bronze", source.targets.bronze.table) +:
        source.targets.silver.toVector.map(silver =>
          Written(file, "targets.silver.table", "Silver", silver.table)
        )
    }
    for {
      one <- written
      other <- written
      // Within one source it is the source's own rule.
      if one.file != other.file
      problem <-
        if (one.table == other.table && one.layer != other.layer)
          Some(
            s"is ${one.table}, which ${other.file} writes as its ${other.layer} table: a table has one layer"
          )
        else if (one.table != other.table && one.table.segments.startsWith(other.table.segments))
          Some(
            s"is ${one.table}, inside ${other.table}, which is a table of ${other.file}: no table is inside another"
          )
        else None
    } yield ConfigError(one.file, Some(one.field), problem)
  }.distinct

  /**
   * A Silver table that several sources merge into (ADR-0010 §3). It is partitioned by the
   * column that separates its writers, the same for all of them, and that column is part of
   * the key of each: a row is found by its key within its partition, so two sources that write
   * at once read parts of the table that do not meet. `_source_id` is the engine's own, and is
   * part of every key without being written in one.
   */
  private def fanIn(sources: Vector[(String, Source)]): Seq[ConfigError] = {
    val merged = for {
      (file, source) <- sources
      silver <- source.targets.silver
    } yield (file, source, silver)

    merged.groupBy(_._3.table).values.filter(_.size > 1).toSeq.flatMap { writers =>
      val files = writers.map(_._1)
      val partitionings = writers.map(_._3.partitionBy).distinct

      writers.flatMap { case (file, source, silver) =>
        def at(field: String, problem: String): ConfigError =
          ConfigError(file, Some(field), problem)
        val table = s"${silver.table} is written by ${others(file, files)} as well"

        if (partitionings.size > 1)
          Seq(
            at(
              "targets.silver.partition_by",
              s"is [${silver.partitionBy.mkString(", ")}], and $table, partitioned another way: a table is partitioned one way"
            )
          )
        else if (silver.partitionBy.isEmpty)
          Seq(
            at(
              "targets.silver.partition_by",
              s"is empty, and $table: a table that several sources merge into is partitioned by " +
                s"the column that separates them, as $SourceIdColumn (ADR-0010)"
            )
          )
        else
          silver.partitionBy.zipWithIndex.collect {
            case (column, index)
                if column.value != SourceIdColumn && !source.keys.contains(column) =>
              at(
                s"targets.silver.partition_by[$index]",
                s"is $column, which is not one of keys, and $table: there a row is found by " +
                  "its key within its partition, so the partition is part of the key (ADR-0010)"
              )
          }
      }
    }
  }

  private def baseName(file: String): String = file.substring(file.lastIndexOf('/') + 1)

  private def others(file: String, all: Seq[String]): String =
    all.filterNot(_ == file).sorted.mkString(", ")
}
