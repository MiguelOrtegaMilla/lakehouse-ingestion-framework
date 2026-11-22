package lakehouse.ingestion.core.config

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

import scala.concurrent.duration.FiniteDuration

import io.circe.{ACursor, Json}

/**
 * The configuration that a run of a source used, as something that can be stored and compared:
 * its content, written one way, and the SHA-256 of that (ADR-0007). Every run records the hash,
 * and the content is stored once for each hash (`control.config_snapshot`, P4).
 *
 * '''The resolved configuration, defaults included.''' What is hashed is what ran, not what
 * was typed. Two files that differ in the order of their keys, in their comments, in `120s`
 * for `2m`, or in a default that one of them writes out, are one configuration and have one
 * hash. A new field with a default changes every hash when the application is upgraded, and a
 * run records the version of the application next to its hash to tell that apart.
 *
 * '''The profile is in it.''' The same source in another lake, or with another retry budget,
 * did not run with the same configuration.
 *
 * '''Nothing in it is a secret.''' A configuration holds references to secrets and never their
 * values, in the types of the model themselves: there is nothing to redact, because there was
 * never anything to leak.
 *
 * The content has the names of the fields of the files, so a snapshot reads as a configuration
 * and is one: read back, it gives the same profile and the same source.
 *
 * @param hash the SHA-256 of `text`, in hexadecimal
 * @param content `{ schema_version, profile, source }`
 * @param origin the paths in `content` of the fields that say where the rows of the source
 *   come from, as its connector names them. They are not content, and not in the hash
 */
final case class ConfigVersion(hash: String, content: Json, origin: Vector[String]) {

  /** The content as it was hashed: keys in order, no space, no field that is null. */
  def text: String = ConfigVersion.print(content)
}

object ConfigVersion {

  /**
   * The version of the shape of `content`. It changes when the way a configuration is written
   * down here changes, so that two hashes that differ for that alone can be told from two
   * configurations that differ.
   */
  val SchemaVersion = 1

  def of(profile: Profile, source: Source): ConfigVersion = {
    val content = Json
      .obj(
        "schema_version" -> Json.fromInt(SchemaVersion),
        "profile" -> canonical(profile),
        "source" -> canonical(source)
      )
      .deepDropNullValues
    val origin =
      "source.connector.type" +: source.connector.origin.map(field => s"source.connector.$field")
    ConfigVersion(hashOf(content), content, origin.toVector)
  }

  /**
   * The hash that a content has. A snapshot that is read back from where it was stored has the
   * hash it was stored under, or it is not the configuration that the run was told.
   */
  def hashOf(content: Json): String = sha256(print(content))

  private def print(content: Json): String = content.noSpacesSortKeys

  private def sha256(text: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(text.getBytes(UTF_8))
      .map(byte => f"$byte%02x")
      .mkString

  private def text(value: String): Json = Json.fromString(value)

  private def duration(length: FiniteDuration): Json = text(Durations.render(length))

  private def some[A](value: Option[A])(encode: A => Json): Json = value.fold(Json.Null)(encode)

  /** A profile with the names of the fields of its file. */
  def canonical(profile: Profile): Json = Json.obj(
    "lake" -> Json.obj(
      "uri" -> text(profile.lake.uri.value),
      "deployment" -> text(profile.lake.deployment.name),
      "shared_filesystem" -> Json.fromBoolean(profile.lake.sharedFilesystem)
    ),
    "landing" -> Json.obj("root" -> text(profile.landing.root.value)),
    "databases" -> Json.obj(
      "control" -> profile.databases.control.canonical,
      "coordination" -> profile.databases.coordination.canonical
    ),
    "retries" -> Json.obj(
      "budget" -> duration(profile.retries.budget),
      "first_backoff" -> duration(profile.retries.firstBackoff),
      "max_backoff" -> duration(profile.retries.maxBackoff)
    ),
    "leases" -> Json.obj("ttl" -> duration(profile.leases.ttl)),
    "spark" -> Json.obj(
      "master" -> text(profile.spark.master.value),
      "settings" -> Json.fromFields(profile.spark.settings.map { case (key, value) =>
        key -> text(value)
      })
    )
  )

  /** A source with the names of the fields of its file. */
  def canonical(source: Source): Json = Json.obj(
    "id" -> text(source.id.value),
    "enabled" -> Json.fromBoolean(source.enabled),
    "owner" -> some(source.owner)(text),
    "connector" -> Json
      .obj("type" -> text(source.connector.kind))
      .deepMerge(source.connector.canonical),
    "load" -> (source.load match {
      case Load.Incremental(watermark, range) =>
        Json.obj(
          "mode" -> text(source.load.mode.name),
          "watermark" -> Json.obj(
            "column" -> text(watermark.column.value),
            "lookback" -> duration(watermark.lookback),
            "safety_margin" -> duration(watermark.safetyMargin),
            "start" -> some(watermark.start)(start => text(Timestamps.render(start)))
          ),
          "max_batch_range" -> some(range)(duration)
        )
      case other => Json.obj("mode" -> text(other.mode.name))
    }),
    "keys" -> Json.fromValues(source.keys.map(key => text(key.value))),
    "deletes" -> some(source.deletes) {
      case Deletes.KeyReconciliation => Json.obj("strategy" -> text("key_reconciliation"))
      case Deletes.DeletedFlag(column) =>
        Json.obj("strategy" -> text("deleted_flag"), "column" -> text(column.value))
    },
    "schema" -> Json.obj(
      "on_new_column" -> text(source.schema.onNewColumn.name),
      "on_missing_column" -> text(source.schema.onMissingColumn.name),
      "on_type_change" -> text(source.schema.onTypeChange.name),
      "renames" -> Json.fromFields(source.schema.renames.map { case (old, to) =>
        old -> text(to.value)
      })
    ),
    "columns" -> Json.fromFields(source.columns.map { case (name, spec) =>
      name -> Json.obj(
        "pii" -> some(spec.pii)(text),
        "silver" -> some(spec.mask)(mask => Json.obj("mask" -> text(mask.name)))
      )
    }),
    "quality" -> Json.obj(
      "max_rejected_ratio" -> Json.fromDoubleOrNull(source.quality.maxRejectedRatio)
    ),
    "targets" -> Json.obj(
      "bronze" -> Json.obj("table" -> text(source.targets.bronze.table.value)),
      "silver" -> some(source.targets.silver) { silver =>
        Json.obj(
          "table" -> text(silver.table.value),
          "partition_by" -> Json.fromValues(silver.partitionBy.map(column => text(column.value)))
        )
      }
    ),
    "freshness_sla" -> some(source.freshnessSla)(duration)
  )

  // -------------------------------------------------------------------------------------------
  // A change that what is already ingested cannot follow
  // -------------------------------------------------------------------------------------------

  /**
   * A change to a source that its state and its tables cannot follow (ADR-0007): what was
   * ingested was ingested under the other value.
   *
   * @param of whose field it is: the `profile` or the `source`
   * @param field the path of the field in that file
   * @param was the value of the last run
   * @param is the value now
   * @param consequence what would go wrong if the source simply ran on
   * @param breaksBronze whether it is what Bronze has of the source that cannot follow it: the
   *   state that says how far the source was read and what was taken of it, and the table that
   *   was written. A run into Bronze is refused for these, and for no other. The rest are of
   *   what is made of Bronze afterwards, and are for a run into Silver to refuse (P10)
   */
  final case class BreakingChange(
      of: String,
      field: String,
      was: String,
      is: String,
      consequence: String,
      breaksBronze: Boolean
  ) {
    def message: String = s"$field of the $of was $was and is $is: $consequence"

    /**
     * Whether what is ingested from now on goes to another table than the one that has what
     * was: the lake was moved, or the Bronze table is another.
     */
    def movesBronze: Boolean =
      (of, field) == ("profile", "lake.uri") || (of, field) == ("source", "targets.bronze.table")

    /** Whether the watermark is now a value of another column. */
    def ofWatermarkColumn: Boolean = (of, field) == ("source", "load.watermark.column")
  }

  /**
   * The breaking changes from the configuration of the last run of a source to the one it would
   * run with now. An empty answer says that the source can run on.
   *
   * Only a few fields are looked at, each by its path in the content, so a snapshot that an
   * older version of the application wrote can still be compared: what it does not have is no
   * change.
   *
   * Where the rows come from is one of them only for a source that was loaded by a watermark
   * and still is: the kind of its connector, and the fields that its connector names as its
   * origin, a database and a table for JDBC.
   *
   * Each says whether it is Bronze that cannot follow it (`BreakingChange.breaksBronze`): the
   * lake, the Bronze table, the watermark column and where the rows come from. A key, the
   * Silver table and its partitioning change nothing of what was ingested.
   *
   * @param last the content of the snapshot of the last run
   */
  def breakingChanges(last: Json, now: ConfigVersion): Vector[BreakingChange] = {
    def at(content: Json, path: String*): Option[Json] =
      path.foldLeft(content.hcursor: ACursor)(_.downField(_)).focus.filterNot(_.isNull)
    def shown(value: Json): String = value.asString.getOrElse(value.noSpaces)

    /** A field that both have, with two values that are not the `same`. */
    def differs(
        same: (Json, Json) => Boolean,
        breaksBronze: Boolean
    )(consequence: String, path: String*): Option[BreakingChange] =
      for {
        was <- at(last, path: _*)
        is <- at(now.content, path: _*)
        if !same(was, is)
      } yield BreakingChange(
        path.head,
        path.tail.mkString("."),
        shown(was),
        shown(is),
        consequence,
        breaksBronze
      )

    /** A change that what Bronze has of the source cannot follow. */
    def ofBronze(consequence: String, path: String*): Option[BreakingChange] =
      differs(_ == _, breaksBronze = true)(consequence, path: _*)

    /** A change that Bronze follows, and that what is made of Bronze afterwards cannot. */
    def afterBronze(consequence: String, path: String*): Option[BreakingChange] =
      differs(_ == _, breaksBronze = false)(consequence, path: _*)

    /** A key is its columns, in whatever order a file lists them. */
    def sameColumns(was: Json, is: Json): Boolean =
      was.asArray.map(_.toSet) == is.asArray.map(_.toSet)

    val incremental = Some(Json.fromString(Load.Mode.Incremental.name))
    val byWatermark =
      at(last, "source", "load", "mode") == incremental && at(
        now.content,
        "source",
        "load",
        "mode"
      ) == incremental

    val silver = Seq("source", "targets", "silver", "table")
    val sameSilver =
      at(last, silver: _*).nonEmpty && at(last, silver: _*) == at(now.content, silver: _*)
    val mergedBefore = at(last, silver: _*).nonEmpty && at(now.content, silver: _*).nonEmpty

    Vector(
      ofBronze(
        "every table of the source is in another place, and its state says what was written to the old one",
        "profile",
        "lake",
        "uri"
      ),
      ofBronze(
        "the Bronze table is another, and the state of the source says what was written to the old one",
        "source",
        "targets",
        "bronze",
        "table"
      ),
      afterBronze(
        "the Silver table is another, and what was merged is in the old one",
        silver: _*
      ),
      Option
        .when(sameSilver)(
          afterBronze(
            "a table is partitioned one way, and this one already is",
            "source",
            "targets",
            "silver",
            "partition_by"
          )
        )
        .flatten,
      Option
        .when(mergedBefore)(
          differs(sameColumns, breaksBronze = false)(
            "the rows already in Silver were merged by the old key, and would not be found by the new one",
            "source",
            "keys"
          )
        )
        .flatten,
      ofBronze(
        "the watermark that is stored is a value of the old column",
        "source",
        "load",
        "watermark",
        "column"
      )
    ).flatten ++
      // A watermark says how far one table of one database was read. Of another, it says
      // nothing, and a full load takes everything whatever it is.
      now.origin.filter(_ => byWatermark).flatMap { field =>
        ofBronze(
          "the watermark that is stored is of what was read before, and going on from it would " +
            "skip rows of what is read now without a word",
          field.split('.').toIndexedSeq: _*
        )
      }
  }
}
