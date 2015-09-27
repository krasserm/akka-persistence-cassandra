package akka.persistence.cassandra.query.journal

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.{ActorRef, ActorSystem}
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.journal.TestActor
import akka.persistence.query.PersistenceQuery
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, WordSpecLike}

object CassandraReadJournalSpecBase {
  val config = """
    akka.loglevel = OFF
    akka.persistence.journal.plugin = "cassandra-journal"
    cassandra-journal.port = 9142
    cassandra-query-journal.port = 9142
    cassandra-query-journal.max-buffer-size = 10
               """
}

class CassandraReadJournalSpecBase
  extends TestKit(ActorSystem("EventsByPersistenceIdSpec", ConfigFactory.parseString(CassandraReadJournalSpecBase.config)))
  with ImplicitSender
  with WordSpecLike
  with CassandraLifecycle
  with Matchers {

  implicit val mat = ActorMaterializer()(system)
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
}
