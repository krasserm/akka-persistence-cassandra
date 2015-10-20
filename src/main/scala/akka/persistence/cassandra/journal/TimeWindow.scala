package akka.persistence.cassandra.journal

import java.util.Calendar
import TimeWindow._
import akka.persistence.cassandra.query.Timestamped
import scala.collection.immutable.SortedMap
import java.time.Instant

/**
 * Determines the time window to classify stored {@link Timestamped} events into.
 *
 * @param windowDurationMillis The duration of a time window. Events are only logged once per time
 * window per persistenceId.
 */
class TimeWindow(windowDurationMillis: Long) {
  // how long do we remember "old" events to see if new events arrive for the same time window
  private val extendedTimeWindow = Math.min(windowDurationMillis * 4, 10000)

  private var windows = Map.empty[String, Long]
  private var persistenceIds = SortedMap.empty[Long,Vector[String]]

  /**
   * @return A Placement instance describing into which time window the given event should be placed,
   * and whether it's the first seen event for that persistenceId in that time window
   */
  def place(persistenceId: String, event: Timestamped): Placement = {
    val timestamp = event.timestamp.toEpochMilli
    val window = timestamp / windowDurationMillis * windowDurationMillis

    val placement = windows.get(persistenceId) match {
      case Some(last) if last == window =>
        // already known, and still in the same time window
        Placement(window, false)

      case Some(last) if last < window =>
        // already known, but for an old time window
        Placement(window, true)

      case Some(last) =>
        throw new IllegalArgumentException("Timestamps for " + persistenceId + " have decreased: seen time window " +
            last + " but now receiving event at " + timestamp + ": " + event)

      case None =>
        // not known yet
        Placement(window, true)
    }

    // store new window is it's the first occurrence
    if (placement.isFirstOccurrenceInWindow) {
      windows += persistenceId -> window
      persistenceIds += window -> (persistenceIds.getOrElse(window, Vector.empty) :+ persistenceId)
    }

    // remove old values
    val threshold = persistenceIds.head._1 - extendedTimeWindow
    for (id <- persistenceIds.to(threshold).values.flatten) {
      windows -= id
    }
    persistenceIds = persistenceIds.from(threshold)

    placement
  }

  /**
   * @return The number of persistence IDs for which the latest time window has been remembered
   */
  def cacheUsage:Int = windows.size
}

object TimeWindow {
  case class Placement(windowStart: Long, isFirstOccurrenceInWindow: Boolean)
}
