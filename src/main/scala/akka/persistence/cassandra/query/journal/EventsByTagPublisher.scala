package akka.persistence.cassandra.query.journal

import java.nio.ByteBuffer
import java.util.UUID

import akka.actor.Props
import akka.persistence.PersistentRepr
import akka.persistence.query.EventEnvelope
import akka.serialization.{Serialization, SerializationExtension}
import com.datastax.driver.core.{Row, Session}
import com.datastax.driver.core.utils.{Bytes, UUIDs}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.collection.JavaConverters._

import akka.persistence.cassandra.listenableFutureToFuture
import EventsByTagPublisher._

private[journal] object EventsByTagPublisher {
  def props(
    tag: String,
    offset: Long,
    refreshInterval: Option[FiniteDuration],
    maxBufSize: Long,
    session: Session,
    config: CassandraReadJournalConfig): Props =
    Props(
      new EventsByTagPublisher(
        tag,
        offset,
        refreshInterval,
        maxBufSize,
        session,
        config))

  def offsetUuid(timestamp: Long): UUID = UUIDs.startOf(timestamp)

  private[query] final case class TaggedEventEnvelope(offset: UUID, event: Any)
}

private[journal] class EventsByTagPublisher(
    tag: String,
    offset: Long,
    refreshInterval: Option[FiniteDuration],
    maxBufSize: Long,
    session: Session,
    override val config: CassandraReadJournalConfig)
  extends QueryActorPublisher[EventEnvelope, Long](refreshInterval, maxBufSize)
  with CassandraReadStatements {

  import config._

  private[this] val serialization = SerializationExtension(context.system)

  private[this] val step = 1000l

  val preparedSelectMessages = session.prepare(selectByTag).setConsistencyLevel(readConsistency)
  implicit val ec = context.dispatcher


  override protected def query(state: Long, max: Long): Future[Vector[EventEnvelope]] = {
    listenableFutureToFuture(
      session
        .executeAsync(preparedSelectMessages.bind(tag, offsetUuid(state), offsetUuid(state + step))))
      .map(f => f.all().asScala.toVector.map(extractor).map(r => toEventEnvelope(r._1, r._2)))
  }

  override protected def completionCondition(state: Long): Boolean = false

  override protected def initialState: Long = offset

  override protected def updateBuffer(
      buf: Vector[EventEnvelope],
      newBuf: Vector[EventEnvelope],
      state: Long): (Vector[EventEnvelope], Long) = {

    val newState = if (newBuf.isEmpty) state else newBuf.last.offset
    (buf ++ newBuf, newState)
  }

  private[this] def extractor(row: Row): (PersistentRepr, Long) =
    (persistentFromByteBuffer(serialization, row.getBytes("message")), UUIDs.unixTimestamp(row.getUUID("timestamp")))

  private[this] def persistentFromByteBuffer(
      serialization: Serialization,
      b: ByteBuffer): PersistentRepr =
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get

  private[this] def toEventEnvelope(persistentRepr: PersistentRepr, offset: Long): EventEnvelope =
    EventEnvelope(offset, persistentRepr.persistenceId, persistentRepr.sequenceNr, persistentRepr.payload)
}
