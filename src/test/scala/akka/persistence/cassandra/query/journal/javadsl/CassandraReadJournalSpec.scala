package akka.persistence.cassandra.query.journal.javadsl

import scala.concurrent.ExecutionContext.Implicits.global

import akka.persistence.query.PersistenceQuery
import akka.stream.scaladsl.Sink
import org.scalatest.concurrent.ScalaFutures

import akka.persistence.cassandra.query.journal.CassandraReadJournalSpecBase
import akka.persistence.cassandra.query.journal.javadsl
import akka.persistence.cassandra.query.journal.scaladsl

class CassandraReadJournalSpec
  extends CassandraReadJournalSpecBase
  with ScalaFutures {

  lazy val javaQueries = PersistenceQuery(system)
    .getReadJournalFor(classOf[javadsl.CassandraReadJournal], scaladsl.CassandraReadJournal.Identifier)

  "Cassandra Read Journal Java API" must {
    "start eventsByPersistenceId query" in {
      setup("a", 1)
      val src = javaQueries.eventsByPersistenceId("a", 0L, Long.MaxValue)
      src.runWith(Sink.head, mat).map(_.persistenceId).futureValue.shouldEqual("a")
    }

    "start current eventsByPersistenceId query" in {
      setup("b", 1)
      val src = javaQueries.currentEventsByPersistenceId("b", 0L, Long.MaxValue)
      src.runWith(Sink.head, mat).map(_.persistenceId).futureValue.shouldEqual("b")
    }
  }
}
