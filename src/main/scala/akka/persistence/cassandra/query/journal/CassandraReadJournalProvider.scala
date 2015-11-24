package akka.persistence.cassandra.query.journal

import akka.actor.ExtendedActorSystem
import akka.persistence.query.ReadJournalProvider
import com.typesafe.config.Config

import scala.util.control.NonFatal

class CassandraReadJournalProvider(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {

  override val scaladslReadJournal: scaladsl.CassandraReadJournal =
    new scaladsl.CassandraReadJournal(system, config)

  override val javadslReadJournal: javadsl.CassandraReadJournal =
    new javadsl.CassandraReadJournal(scaladslReadJournal)
}
