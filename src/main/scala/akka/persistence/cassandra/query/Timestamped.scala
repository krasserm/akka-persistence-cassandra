package akka.persistence.cassandra.query

import java.time.Instant

/**
 * Marks a akka-persistence data structure as carrying a creation timestamp.
 *
 * - Timestamps of consecutive events emitted by the same persistenceId must never decrease.
 * - Timestamps must roughly follow clock time of when they are emitted (i.e. they must be
 *   the actual time at which the event was emitted / created by the persistenceId)
 */
trait Timestamped {
  def timestamp: Instant
}
