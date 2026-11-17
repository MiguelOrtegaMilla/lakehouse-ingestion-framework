package lakehouse.ingestion.core.config

/**
 * One thing that is wrong in a configuration: the file, the field, and the problem (ADR-0007).
 * A configuration is validated whole, so there are as many of these as there are mistakes.
 *
 * @param field the path of the field from the top of the file, as `load.watermark.column` or
 *   `keys[1]`; `None` for a problem of the file as a whole
 * @param problem what is wrong, in words that follow the name of the field
 */
final case class ConfigError(file: String, field: Option[String], problem: String) {

  def message: String = field.fold(s"$file: $problem")(name => s"$file: $name $problem")

  override def toString: String = message
}

/**
 * A configuration that is valid, with what is worth a second look in it: settings that are
 * allowed and unusual, said once at startup.
 */
final case class Loaded[A](value: A, warnings: Vector[ConfigError])
