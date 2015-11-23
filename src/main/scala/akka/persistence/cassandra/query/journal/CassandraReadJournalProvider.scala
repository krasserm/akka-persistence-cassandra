package akka.persistence.cassandra.query.journal

import akka.actor.ExtendedActorSystem
import akka.persistence.query.ReadJournalProvider
import com.typesafe.config.Config

import scala.util.control.NonFatal

class CassandraReadJournalProvider(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {

  override val scaladslReadJournal: scaladsl.CassandraReadJournal =
    try {
      new scaladsl.CassandraReadJournal(system, config)
    } catch {
        case NonFatal(e) =>
          println("FAILED FAILED")
          println(e.getMessage)
          println(e.getStackTrace)
          throw e
  }

  override val javadslReadJournal: javadsl.CassandraReadJournal =
    new javadsl.CassandraReadJournal(scaladslReadJournal)
}
