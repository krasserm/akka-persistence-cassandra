package akka.persistence.cassandra.query.journal

import scala.concurrent.duration.FiniteDuration

import akka.actor.ExtendedActorSystem
import akka.persistence.query._
import akka.persistence.query.scaladsl.ReadJournal
import akka.stream.scaladsl.Source
import com.typesafe.config.Config

object CassandraReadJournal {
  final val Identifier = "cassandra-query-journal"
}

class CassandraReadJournal(system: ExtendedActorSystem, config: Config) extends ReadJournal {
  val readJournalConfig = new CassandraReadJournalConfig(config)

  override def query[T, M](q: Query[T, M], hints: Hint*): Source[T, M] = q match {
    case EventsByPersistenceId(pid, from, to) => eventsByPersistenceId(pid, from, to, hints, readJournalConfig)
    case AllPersistenceIds                    => ??? //allPersistenceIds(hints)
    case EventsByTag(tag, offset)             => ??? //eventsByTag(tag, offset, hints)
    case unknown                              => unsupportedQueryType(unknown)
  }

  //TODO: Do we want to use materialized values?
  //TODO: http://doc.akka.io/docs/akka/snapshot/scala/persistence-query.html#Materialized_values_of_queries
  //TODO: It makes the use of streams less convenient as own Query's must be used.
  def eventsByPersistenceId(
      persistenceId: String,
      fromSeqNr: Long,
      toSeqNr: Long,
      hints: Seq[Hint],
      readJournalConfig: CassandraReadJournalConfig): Source[EventEnvelope, Unit] = {

    val refreshInterval = refreshIntervalFromHints(hints, readJournalConfig.refreshInterval)
    val name = s"eventsByPersistenceId-$persistenceId"

    Source.actorPublisher[EventEnvelope](
      EventsByPersistenceIdPublisher.props(
        persistenceId,
        fromSeqNr,
        toSeqNr,
        refreshInterval,
        readJournalConfig.maxBufferSize,
        readJournalConfig))
        .mapMaterializedValue(_ => ())
        .named(name)
  }

  // TODO: Move hints to producer?
  private[this] def refreshIntervalFromHints(
      hints: Seq[Hint],
      defaultRefreshInterval: Option[FiniteDuration]): Option[FiniteDuration] =
    if (hints.contains(NoRefresh)) {
      None
    } else {
      hints.collectFirst { case RefreshInterval(interval) => interval }.orElse(defaultRefreshInterval)
    }

  private[this] def unsupportedQueryType[M, T](unknown: Query[T, M]): Nothing =
    throw new IllegalArgumentException(
      s"${getClass.getSimpleName} does not implement the ${unknown.getClass.getName} query type!")
}