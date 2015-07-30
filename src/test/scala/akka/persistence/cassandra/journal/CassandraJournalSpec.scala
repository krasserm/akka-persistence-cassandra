package akka.persistence.cassandra.journal

import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.TestConfig
import akka.persistence.journal.JournalPerfSpec

import scala.concurrent.duration._

class CassandraJournalSpec extends JournalPerfSpec(TestConfig.config) with CassandraLifecycle {

  override def awaitDurationMillis: Long = 20.seconds.toMillis
}
