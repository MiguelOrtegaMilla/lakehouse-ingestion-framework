package lakehouse.ingestion.logstore.coordination

/** The in-memory registry keeps the contract of `RegistryMaintenance`. */
final class InMemoryRegistryMaintenanceSpec extends RegistryMaintenanceContract {

  private val registry = new InMemoryCoordinationStore

  override protected def store: CoordinationStore = registry
  override protected def maintenance: RegistryMaintenance = registry
}
