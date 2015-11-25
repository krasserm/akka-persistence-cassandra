package akka.persistence.cassandra.query.journal.scaladsl

import java.util.UUID

import com.datastax.driver.core.utils.UUIDs

import scala.concurrent.duration.FiniteDuration

import akka.actor.ExtendedActorSystem
import akka.persistence.cassandra.ClusterBuilder
import akka.persistence.query._
import akka.persistence.query.scaladsl._
import akka.stream.scaladsl.Source
import com.typesafe.config.Config

import akka.persistence.cassandra.query.journal.{EventsByTagPublisher,
CassandraReadJournalConfig, EventsByPersistenceIdPublisher}

object CassandraReadJournal {
  final val Identifier = "cassandra-query-journal"
}

class CassandraReadJournal(system: ExtendedActorSystem, config: Config)
  extends ReadJournal
  with AllPersistenceIdsQuery
  with CurrentPersistenceIdsQuery
  with EventsByPersistenceIdQuery
  with CurrentEventsByPersistenceIdQuery
  with EventsByTagQuery
  with CurrentEventsByTagQuery {

  val readJournalConfig = new CassandraReadJournalConfig(config)

  val session = ClusterBuilder.cluster(readJournalConfig)

  override def allPersistenceIds(): Source[String, Unit] = ???

  override def currentPersistenceIds(): Source[String, Unit] = ???

  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, Unit] =
    currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr, readJournalConfig.refreshInterval)

  override def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, Unit] =
    currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr, None)

  private[this] def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      refreshInterval: Option[FiniteDuration]) = {
    val name = s"eventsByPersistenceId-$persistenceId"

    Source.actorPublisher[EventEnvelope](
      EventsByPersistenceIdPublisher.props(
        persistenceId,
        fromSequenceNr,
        toSequenceNr,
        refreshInterval,
        readJournalConfig.maxBufferSize,
        session,
        readJournalConfig))
      .mapMaterializedValue(_ => ())
      .named(name)
  }

  override def eventsByTag(tag: String, offset: Long): Source[EventEnvelope, Unit] =
    eventsByTag(tag, offset, readJournalConfig.refreshInterval)

  override def currentEventsByTag(tag: String, offset: Long): Source[EventEnvelope, Unit] =
    eventsByTag(tag, offset, None)

  private[this] def eventsByTag(
      tag: String,
      offset: Long,
      refreshInterval: Option[FiniteDuration]) = {
    val name = s"eventsByTag-$tag"

    Source.actorPublisher[EventEnvelope](
      EventsByTagPublisher.props(
        tag,
        offset,
        refreshInterval,
        readJournalConfig.maxBufferSize,
        session,
        readJournalConfig))
      .mapMaterializedValue(_ => ())
      .named(name)
  }
}