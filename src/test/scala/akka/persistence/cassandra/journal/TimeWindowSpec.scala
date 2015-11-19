package akka.persistence.cassandra.journal

import org.scalatest.WordSpec
import org.scalatest.Matchers
import akka.persistence.cassandra.query.Timestamped
import java.time.Instant
import akka.persistence.cassandra.journal.TimeWindow.Placement

class TimeWindowSpec extends WordSpec with Matchers {

  case class Event(timestamp: Instant) extends Timestamped

  "A TimeWindow instance" must {
    "place events with similar timestamps into the same time window" in {
      val window = new TimeWindow(1000)

      window.place("doc", Event(Instant.ofEpochMilli(1000))) should be (Placement(1000, true))
      window.place("doc", Event(Instant.ofEpochMilli(1001))) should be (Placement(1000, false))
    }

    "forget about old instances after 4 * time window length has passed" in {
      val window = new TimeWindow(1000)

      window.place("doc", Event(Instant.ofEpochMilli(1000))) should be (Placement(1000, true))
      window.place("doc", Event(Instant.ofEpochMilli(5000))) should be (Placement(5000, true))
      window.cacheUsage should be (1)
    }
  }
}
