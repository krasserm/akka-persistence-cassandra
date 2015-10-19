package akka.persistence.cassandra.query.journal

import scala.concurrent.duration._
import akka.stream.testkit.scaladsl.TestSink

class EventsByPersistenceIdSpec extends CassandraReadJournalSpecBase {

  "Cassandra query EventsByPersistenceId" must {
    "find existing events" in {
      val ref = setup("a", 3)

      val src = queries.currentEventsByPersistenceId("a", 0L, Long.MaxValue)
      src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("a-1", "a-2")
        .expectNoMsg(500.millis)
        .request(2)
        .expectNext("a-3")
        .expectComplete()
    }

    "not see new events if the stream starts after current latest event" in {
      val ref = setup("b", 3)
      val src = queries.currentEventsByPersistenceId("b", 5L, Long.MaxValue)
      src.map(_.event).runWith(TestSink.probe[Any])
      src.map(_.event).runWith(TestSink.probe[Any])
        .request(5)
        .expectNoMsg(1000.millis)
    }

    "find existing events up to a sequence number" in {
      val ref = setup("c", 3)
      val src = queries.currentEventsByPersistenceId("c", 0L, 2L)
      src.map(_.event).runWith(TestSink.probe[Any])
        .request(5)
        .expectNext("c-1", "c-2")
        .expectComplete()
    }

    "not see new events after demand request" in {
      val ref = setup("d", 3)

      val src = queries.currentEventsByPersistenceId("d", 0L, Long.MaxValue)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("d-1", "d-2")
        .expectNoMsg(100.millis)

      ref ! "d-4"
      expectMsg("d-4-done")

      probe
        .expectNoMsg(100.millis)
        .request(5)
        .expectNext("d-3")
        .expectComplete() // f-4 not seen
    }

    "only deliver what requested if there is more in the buffer" in {
      val ref = setup("e", 1000)

      val src = queries.currentEventsByPersistenceId("e", 0L, Long.MaxValue)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("e-1", "e-2")
        .expectNoMsg(1000.millis)

      probe
        .expectNoMsg(1000.millis)
        .request(5)
        .expectNext("e-3", "e-4", "e-5", "e-6", "e-7")
        .expectNoMsg(1000.millis)

      probe
        .request(5)
        .expectNext("e-8", "e-9", "e-10", "e-11", "e-12")
        .expectNoMsg(1000.millis)
    }
  }

  "Cassandra live query EventsByPersistenceId" must {
    "find new events" in {
      val ref = setup("f", 3)
      val src = queries.eventsByPersistenceId("f", 0L, Long.MaxValue)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(5)
        .expectNext("f-1", "f-2", "f-3")

      ref ! "f-4"
      expectMsg("f-4-done")

      probe.expectNext("f-4")
    }

    "find new events if the stream starts after current latest event" in {
      val ref = setup("g", 4)
      val src = queries.eventsByPersistenceId("g", 5L, Long.MaxValue)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(5)
        .expectNoMsg(1000.millis)

      ref ! "g-5"
      expectMsg("g-5-done")
      ref ! "g-6"
      expectMsg("g-6-done")

      probe.expectNext("g-6")

      ref ! "g-7"
      expectMsg("g-7-done")

      probe.expectNext("g-7")
    }

    "find new events up to a sequence number" in {
      val ref = setup("h", 3)
      val src = queries.eventsByPersistenceId("h", 0L, 4L)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(5)
        .expectNext("h-1", "h-2", "h-3")

      ref ! "h-4"
      expectMsg("h-4-done")

      probe.expectNext("h-4").expectComplete()
    }

    "find new events after demand request" in {
      val ref = setup("i", 3)
      val src = queries.eventsByPersistenceId("i", 0L, Long.MaxValue)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("i-1", "i-2")
        .expectNoMsg(100.millis)

      ref ! "i-4"
      expectMsg("i-4-done")

      probe
        .expectNoMsg(100.millis)
        .request(5)
        .expectNext("i-3")
        .expectNext("i-4")
    }

    "only deliver what requested if there is more in the buffer" in {
      val ref = setup("j", 1000)

      val src = queries.eventsByPersistenceId("j", 0L, Long.MaxValue)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("j-1", "j-2")
        .expectNoMsg(1000.millis)

      probe
        .expectNoMsg(1000.millis)
        .request(5)
        .expectNext("j-3", "j-4", "j-5", "j-6", "j-7")
        .expectNoMsg(1000.millis)

      probe
        .request(5)
        .expectNext("j-8", "j-9", "j-10", "j-11", "j-12")
        .expectNoMsg(1000.millis)
    }
  }
}
