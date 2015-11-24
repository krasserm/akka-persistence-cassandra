/*
package akka.persistence.cassandra.query.journal

import akka.persistence.cassandra.query.EventsByTagFetcher
import akka.persistence.cassandra.query.EventsByTagPublisher.TaggedEventEnvelope
import akka.persistence.cassandra.query.EventsByTagPublisher.TaggedEventEnvelope
import akka.persistence.query.EventEnvelope
import akka.pattern.ask
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.UUID

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.ActorLogging
import akka.actor.DeadLetterSuppression
import akka.actor.NoSerializationVerificationNeeded
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy
import akka.persistence.cassandra.journal.CassandraJournal
import akka.persistence.query.EventEnvelope
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.actor.ActorPublisherMessage.Request
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.datastax.driver.core.utils.UUIDs

class EventsByTagPublisher(
  tag: String,
  fromOffset: UUID,
  refreshInterval: Option[FiniteDuration],
  maxBufSize: Int,
  session: Session,
  preparedSelect: PreparedStatement)
  extends QueryActorPublisher[EventEnvelope, (String, UUID)](refreshInterval, maxBufSize) {

  override protected def query(
    state: (String, UUID),
    max: Long): Future[Vector[EventEnvelope]] = {

    val fetcher = context.actorOf(
      EventsByTagFetcher.props(
        tag,
        state._1,
        state._2,
        max.toInt,
        self,
        session,
        preparedSelect))

    // TODO: This is obviously wrong.
    (fetcher ? "hi").mapTo[TaggedEventEnvelope].map { te =>
      EventEnvelope(
        offset = UUIDs.unixTimestamp(te.offset),
        persistenceId = te.persistenceId,
        sequenceNr = te.sequenceNr,
        event = te.event)
    }
  }

  // Stream Completion condition
  override protected def completionCondition(state: (String, UUID)): Boolean = isTimeBucketTodayOrLater(state._1)

  // Starting state.
  override protected def initialState: (String, UUID) = (CassandraJournal.timeBucket(UUIDs.unixTimestamp(fromOffset)), fromOffset)

  // Received update
  override protected def updateBuffer(
    buf: Vector[EventEnvelope],
    newBuf: Vector[EventEnvelope],
    state: (String, UUID)): (Vector[EventEnvelope], (String, UUID)) = {

    // TODO: Update stream state for next data fetch.
  }

  private[this] def isTimeBucketBeforeToday(currTimeBucket: String): Boolean = {
    val today: LocalDate = LocalDate.now(ZoneOffset.UTC)
    val bucketDate: LocalDate = LocalDate.parse(currTimeBucket, CassandraJournal.timeBucketFormatter)

    bucketDate.isBefore(today)
  }

  private[this] def isTimeBucketTodayOrLater(currTimeBucket: String): Boolean = {
    val today: LocalDate = LocalDate.now(ZoneOffset.UTC)
    val bucketDate: LocalDate = LocalDate.parse(currTimeBucket, CassandraJournal.timeBucketFormatter)
    val tday = today
    val bucket = bucketDate
    bucket.isEqual(tday) || bucket.isAfter(tday)
  }
}
*/
