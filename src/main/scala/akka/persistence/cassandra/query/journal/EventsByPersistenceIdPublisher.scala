package akka.persistence.cassandra.query.journal

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import com.datastax.driver.core.Session

import akka.actor.Props
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.EventsByPersistenceIdIterator
import akka.persistence.query.EventEnvelope
import akka.serialization.SerializationExtension

private[journal] object EventsByPersistenceIdPublisher {
  def props(
      persistenceId: String,
      fromSeqNr: Long,
      toSeqNr: Long,
      refreshInterval: Option[FiniteDuration],
      maxBufSize: Long,
      session: Session,
      config: CassandraReadJournalConfig): Props =
    Props(
      new EventsByPersistenceIdPublisher(
        persistenceId,
        fromSeqNr,
        toSeqNr,
        refreshInterval,
        maxBufSize,
        session,
        config))
}

// TODO: Decouple database.
// TODO: Query index tables instead.
// TODO: Generic message iterator to handle different tables etc.
private[journal] class EventsByPersistenceIdPublisher(
    persistenceId: String,
    fromSeqNr: Long,
    toSeqNr: Long,
    refreshInterval: Option[FiniteDuration],
    maxBufSize: Long,
    session: Session,
    override val config: CassandraReadJournalConfig)
  extends QueryActorPublisher[EventEnvelope, Long](refreshInterval, maxBufSize)
  with CassandraReadStatements {

  import config._

  private[this] val serialization = SerializationExtension(context.system)

  private[this] val step = 50l

  override protected def query(state: Long, max: Long): Future[Vector[EventEnvelope]] = {
    implicit val ec = context.dispatcher

    // TODO: Async?
    Future {
      val from = state
      val to = Math.min(Math.min(state + step, toSeqNr), state + max)
      val ret = (state to to)
        .zip(
          new EventsByPersistenceIdIterator(
            persistenceId,
            from,
            to,
            targetPartitionSize,
            maxBufSize)(session, config, serialization)
            .toVector)
        .map(r => toEventEnvelope(r._2, r._1 - 1))
        .toVector

      ret
    }
  }

  override protected def initialState: Long = Math.max(1, fromSeqNr)

  override def updateBuffer(
      buffer: Vector[EventEnvelope],
      newBuffer: Vector[EventEnvelope],
      state: Long): (Vector[EventEnvelope], Long) = {
    val newState = if (newBuffer.isEmpty) state else newBuffer.last.sequenceNr + 1
    (buffer ++ newBuffer, newState)
  }

  override protected def completionCondition(state: Long): Boolean = state > toSeqNr

  private[this] def toEventEnvelope(persistentRepr: PersistentRepr, offset: Long): EventEnvelope =
    EventEnvelope(offset, persistentRepr.persistenceId, persistentRepr.sequenceNr, persistentRepr.payload)
}
