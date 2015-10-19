package akka.persistence.cassandra.query.journal

import scala.concurrent.ExecutionContext.Implicits.global

import akka.stream.scaladsl.Sink
import org.scalatest.concurrent.ScalaFutures

class CassandraReadJournalSpec
  extends CassandraReadJournalSpecBase
  with ScalaFutures {

  "Cassandra Read Journal" must {
    "start EventsByPersistenceId query" in {
      setup("a", 1)
      val src = queries.currentEventsByPersistenceId("a", 0L, Long.MaxValue)
      src.runWith(Sink.head).map(_.persistenceId).futureValue.shouldEqual("a")
    }
  }
}
