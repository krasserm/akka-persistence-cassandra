package akka.persistence.cassandra.query.journal.scaladsl

import scala.concurrent.ExecutionContext.Implicits.global

import akka.stream.scaladsl.Sink
import org.scalatest.concurrent.ScalaFutures

import akka.persistence.cassandra.query.journal.CassandraReadJournalSpecBase

class CassandraReadJournalSpec
  extends CassandraReadJournalSpecBase
  with ScalaFutures {

  "Cassandra Read Journal Scala API" must {
    "start eventsByPersistenceId query" in {
      setup("a", 1)
      val src = queries.eventsByPersistenceId("a", 0L, Long.MaxValue)
      src.runWith(Sink.head).map(_.persistenceId).futureValue.shouldEqual("a")
    }

    "start current eventsByPersistenceId query" in {
      setup("b", 1)
      val src = queries.currentEventsByPersistenceId("b", 0L, Long.MaxValue)
      src.runWith(Sink.head).map(_.persistenceId).futureValue.shouldEqual("b")
    }
  }
}
