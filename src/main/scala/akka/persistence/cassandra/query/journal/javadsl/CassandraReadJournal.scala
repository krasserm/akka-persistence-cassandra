package akka.persistence.cassandra.query.journal.javadsl

import akka.persistence.query.{EventEnvelope, javadsl}
import akka.persistence.query.javadsl._
import akka.stream.javadsl.Source

import akka.persistence.cassandra.query.journal.scaladsl

class CassandraReadJournal(scaladslReadJournal: scaladsl.CassandraReadJournal)
  extends javadsl.ReadJournal
  with AllPersistenceIdsQuery
  with CurrentPersistenceIdsQuery
  with EventsByPersistenceIdQuery
  with CurrentEventsByPersistenceIdQuery
  with EventsByTagQuery
  with CurrentEventsByTagQuery {

  override def allPersistenceIds(): Source[String, Unit] = ???

  override def currentPersistenceIds(): Source[String, Unit] = ???

  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, Unit] =
    scaladslReadJournal.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, Unit] =
    scaladslReadJournal.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def eventsByTag(tag: String, offset: Long): Source[EventEnvelope, Unit] = ???

  override def currentEventsByTag(tag: String, offset: Long): Source[EventEnvelope, Unit] = ???
}
