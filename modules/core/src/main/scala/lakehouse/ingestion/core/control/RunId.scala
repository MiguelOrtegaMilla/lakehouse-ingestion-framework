package lakehouse.ingestion.core.control

import java.security.SecureRandom
import java.time.{Clock, Instant}
import java.util.UUID

/**
 * The identity of a run: one attempt to ingest from a source (ADR-0005 §1). A run is not a
 * batch. A batch is a unit of data, and may take several runs, one that died and one that
 * recovered what it left; idempotence is anchored to the batch, never to the run.
 *
 * It is a UUID of version 7 (RFC 9562): 48 bits of the time in milliseconds, and 74 that are
 * random. Each instance makes its own, with no coordination and no round trip to a database:
 *
 *  - '''no two instances make the same one'''. The time narrows a collision to the ids of one
 *    millisecond, and 74 random bits make it one that does not happen;
 *  - '''ids sort by the time they were made''', as text and as PostgreSQL sorts a `uuid`, so
 *    the runs of an audit trail read in order and an index on them grows at one end.
 *
 * The time is that of the instance, and only says roughly when: the times that matter are the
 * database's (`started_at`, the expiry of a lease).
 */
final class RunId private (val uuid: UUID) extends AnyVal {

  /** When the id was made, to the millisecond, by the clock of the instance that made it. */
  def madeAt: Instant = Instant.ofEpochMilli(uuid.getMostSignificantBits >>> 16)

  override def toString: String = uuid.toString
}

object RunId {

  private val Version = 7
  // The variant of RFC 9562, which `UUID.variant` calls 2.
  private val Variant = 2
  private val Written =
    """[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}""".r

  private val Random = new SecureRandom()

  /** Ids in the order they were made, to the millisecond. */
  implicit val ordering: Ordering[RunId] = Ordering.by(_.toString)

  def generate(): RunId = generate(Clock.systemUTC(), Random)

  /**
   * @param clock where the time comes from
   * @param random where the other 74 bits come from
   */
  def generate(clock: Clock, random: java.util.Random): RunId = {
    val millis = clock.millis()
    require(millis >= 0 && millis < (1L << 48), s"the time $millis does not fit an id")
    val most = (millis << 16) | (Version.toLong << 12) | (random.nextLong() & 0xfffL)
    val least = (Variant.toLong << 62) | (random.nextLong() & 0x3fffffffffffffffL)
    new RunId(new UUID(most, least))
  }

  /** A UUID that is one of version 7, as one read back from the database. */
  def of(uuid: UUID): Either[String, RunId] =
    if (uuid.version == Version && uuid.variant == Variant) Right(new RunId(uuid))
    else Left(s"is a UUID of version ${uuid.version}, and a run id is one of version $Version")

  /** The one way an id is written: 36 characters, lower case. */
  def parse(text: String): Either[String, RunId] =
    if (Written.matches(text)) Right(new RunId(UUID.fromString(text)))
    else Left("is not a run id: a UUID of version 7, as 0193a5f0-7c2e-7b1a-9f3e-2d4c6a8b0e1f")
}
