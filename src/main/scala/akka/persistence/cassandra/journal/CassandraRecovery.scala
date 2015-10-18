package akka.persistence.cassandra.journal

import akka.actor.ActorLogging

trait CassandraRecovery extends ActorLogging {
  this: CassandraJournal =>
  import config._

  implicit lazy val replayDispatcher = context.system.dispatchers.lookup(replayDispatcherId)
}
