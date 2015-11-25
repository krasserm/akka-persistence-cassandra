package akka.persistence.cassandra.query.journal

import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.TestSink

class EventsByTagSpec extends CassandraReadJournalSpecBase {

  override def systemName: String = "EventsByTagSpec"

  "Cassandra query EventsByTag" must {
    "find existing events" in {
      val ref = setup("a", 15)

      val src = queries.currentEventsByTag("one", 0L)
      src.map(_.event).runWith(Sink.foreach(println))
    }
  }
}
