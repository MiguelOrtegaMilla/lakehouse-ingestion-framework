package lakehouse.ingestion.engine.connector

import lakehouse.ingestion.core.config.{ConnectorConfig, ConnectorKind, Source}

/**
 * The connectors that an application has, by the name that `connector.type` gives them: the one
 * place where a kind of source is added to it (RNF-04).
 *
 * It is made where the application is put together, and gives both halves of a kind from one
 * list: the kinds, for a configuration to be read with (`ConfigLoader.load`), and the
 * connectors, for the sources of that configuration to be planned and read with. A source of a
 * kind that the configuration was read with is then a source that the engine has a connector
 * for.
 */
final class ConnectorRegistry private (connectors: Vector[SourceConnector[_ <: ConnectorConfig]]) {

  private val byName = connectors.map(connector => connector.kind.name -> connector).toMap

  /** The kinds of connector, in the order they were registered. */
  def kinds: Seq[ConnectorKind] = connectors.map(_.kind)

  /**
   * The connector of a source.
   *
   * @throws IllegalArgumentException if the source is of a kind that this application has no
   *   connector for, or its configuration is not of the class that its kind reads: a
   *   configuration read with other kinds than these, which is a mistake of whoever put the
   *   application together
   */
  def bind(source: Source): BoundSource = {
    val name = source.connector.kind
    val connector = byName.getOrElse(
      name,
      throw new IllegalArgumentException(
        s"the source ${source.id} is of kind '$name', and this application has no connector " +
          s"for it: it has ${kinds.map(kind => s"'${kind.name}'").mkString(", ")}"
      )
    )
    connector
      .bind(source)
      .getOrElse(
        throw new IllegalArgumentException(
          s"the configuration of the source ${source.id} is a " +
            s"${source.connector.getClass.getName}, which is not what the connector of kind " +
            s"'$name' reads: it was read with another kind of that name"
        )
      )
  }
}

object ConnectorRegistry {

  /** @throws IllegalArgumentException if two connectors are of kinds of one name */
  def apply(connectors: SourceConnector[_ <: ConnectorConfig]*): ConnectorRegistry = {
    val twice = connectors.groupBy(_.kind.name).collect {
      case (name, same) if same.size > 1 => name
    }
    require(
      twice.isEmpty,
      s"two connectors of one kind: ${twice.toSeq.sorted.map(name => s"'$name'").mkString(", ")}"
    )
    new ConnectorRegistry(connectors.toVector)
  }
}
