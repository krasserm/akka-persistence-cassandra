/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query.journal

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.journal.TestActor
import akka.persistence.cassandra.server.CassandraServer
import akka.persistence.query.EventsByPersistenceId
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.RefreshInterval
import akka.stream.ActorMaterializer
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import akka.persistence.query.NoRefresh
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, WordSpecLike}

object EventsByPersistenceIdSpec {
  val config = """
    akka.loglevel = INFO
    akka.persistence.journal.plugin = "cassandra-journal"
    akka.test.single-expect-default = 10s
    cassandra-journal.port = 9142
    cassandra-query-journal.cassandra-journal.port = 9142
    cassandra-query-journal.max-buffer-size = 10
               """
}

class EventsByPersistenceIdSpec
  extends TestKit(ActorSystem("EventsByPersistenceIdSpec", ConfigFactory.parseString(EventsByPersistenceIdSpec.config)))
  with ImplicitSender //TODO: with Cleanup
  with WordSpecLike
  with CassandraLifecycle
  with Matchers {

  implicit val mat = ActorMaterializer()(system)

  val refreshInterval = RefreshInterval(1.second)

  val queries = PersistenceQuery(system).readJournalFor(CassandraReadJournal.Identifier)

  override protected def afterAll(): Unit = {
    Await.result(system.terminate(), Timeout(1.second).duration)
    super.afterAll()
  }

  def setup(persistenceId: String, n: Long): ActorRef = {
    val ref = system.actorOf(TestActor.props(persistenceId))
    for(i <- 1l to n) {
      ref ! s"$persistenceId-$i"
      expectMsg(s"$persistenceId-$i-done")
    }

    ref
  }

  "Cassandra query EventsByPersistenceId" must {
    "find existing events" in {
      val ref = setup("a", 3)

      val src = queries.query(EventsByPersistenceId("a", 0L, Long.MaxValue), NoRefresh)
      src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("a-1", "a-2")
        .expectNoMsg(500.millis)
        .request(2)
        .expectNext("a-3")
        .expectComplete()
    }

    "find existing events up to a sequence number" in {
      val ref = setup("b", 3)
      val src = queries.query(EventsByPersistenceId("b", 0L, 2L), NoRefresh)
      src.map(_.event).runWith(TestSink.probe[Any])
        .request(5)
        .expectNext("b-1", "b-2")
        .expectComplete()
    }

    "not see new events after demand request" in {
      val ref = setup("c", 3)
      val src = queries.query(EventsByPersistenceId("c", 0L, Long.MaxValue), NoRefresh)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("c-1", "c-2")
        .expectNoMsg(100.millis)

      ref ! "c-4"
      expectMsg("c-4-done")

      probe
        .expectNoMsg(100.millis)
        .request(5)
        .expectNext("c-3")
        .expectComplete() // f-4 not seen
    }

    "only deliver what requested if there is more in the buffer" in {
      val ref = setup("d", 1000)

      val src = queries.query(EventsByPersistenceId("d", 0L, Long.MaxValue), NoRefresh)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("d-1", "d-2")
        .expectNoMsg(1000.millis)

      probe
        .expectNoMsg(1000.millis)
        .request(5)
        .expectNext("d-3", "d-4", "d-5", "d-6", "d-7")
        .expectNoMsg(1000.millis)

      probe
        .request(5)
        .expectNext("d-8", "d-9", "d-10", "d-11", "d-12")
        .expectNoMsg(1000.millis)
    }
  }

  "Cassandra live query EventsByPersistenceId" must {
    "find new events" in {
      val ref = setup("e", 3)
      val src = queries.query(EventsByPersistenceId("e", 0L, Long.MaxValue), refreshInterval)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(5)
        .expectNext("e-1", "e-2", "e-3")

      ref ! "e-4"
      expectMsg("e-4-done")

      probe.expectNext("e-4")
    }

    "find new events up to a sequence number" in {
      val ref = setup("f", 3)
      val src = queries.query(EventsByPersistenceId("f", 0L, 4L), refreshInterval)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(5)
        .expectNext("f-1", "f-2", "f-3")

      ref ! "f-4"
      expectMsg("f-4-done")

      probe.expectNext("f-4").expectComplete()
    }

    "find new events after demand request" in {
      val ref = setup("g", 3)
      val src = queries.query(EventsByPersistenceId("g", 0L, Long.MaxValue), refreshInterval)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("g-1", "g-2")
        .expectNoMsg(100.millis)

      ref ! "g-4"
      expectMsg("g-4-done")

      probe
        .expectNoMsg(100.millis)
        .request(5)
        .expectNext("g-3")
        .expectNext("g-4")
    }

    "only deliver what requested if there is more in the buffer" in {
      val ref = setup("h", 1000)

      val src = queries.query(EventsByPersistenceId("h", 0L, Long.MaxValue), refreshInterval)
      val probe = src.map(_.event).runWith(TestSink.probe[Any])
        .request(2)
        .expectNext("h-1", "h-2")
        .expectNoMsg(1000.millis)

      probe
        .expectNoMsg(1000.millis)
        .request(5)
        .expectNext("h-3", "h-4", "h-5", "h-6", "h-7")
        .expectNoMsg(1000.millis)

      probe
        .request(5)
        .expectNext("h-8", "h-9", "h-10", "h-11", "h-12")
        .expectNoMsg(1000.millis)
    }
  }
}
