package akka.persistence.cassandra.query.journal

import scala.concurrent.ExecutionContext.Implicits.global

import akka.persistence.query.{EventEnvelope, Query, NoRefresh, EventsByPersistenceId}
import akka.stream.scaladsl.Sink
import org.scalatest.concurrent.ScalaFutures

class CassandraReadJournalSpec
  extends CassandraReadJournalSpecBase
  with ScalaFutures {

  "Cassandra Read Journal" must {
    "start EventsByPersistenceId query" in {
      setup("a", 1)
      val src = queries.query(EventsByPersistenceId("a", 0L, Long.MaxValue), NoRefresh)
      src.runWith(Sink.head).map(_.persistenceId).futureValue.shouldEqual("a")
    }

    "should throw an exception if the query is not supported" in {
      case object CassandraReadJournalSpecUnsupportedQuery extends Query[EventEnvelope, Unit]

      intercept[IllegalArgumentException] {
        queries.query(CassandraReadJournalSpecUnsupportedQuery)
      }
    }
  }
}
