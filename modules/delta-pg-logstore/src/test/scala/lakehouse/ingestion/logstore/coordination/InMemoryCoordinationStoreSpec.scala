package lakehouse.ingestion.logstore.coordination

/** The in-memory store keeps the contract the protocol's unit tests assume of it. */
final class InMemoryCoordinationStoreSpec extends CoordinationStoreContract {
  override protected val store: CoordinationStore = new InMemoryCoordinationStore
}
