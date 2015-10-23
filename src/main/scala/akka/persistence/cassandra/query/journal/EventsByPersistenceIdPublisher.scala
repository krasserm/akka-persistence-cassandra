package akka.persistence.cassandra.query.journal

import java.nio.ByteBuffer
import java.lang.{Long => JLong}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.actor.Props
import akka.persistence.PersistentRepr
import akka.persistence.query.EventEnvelope
import akka.serialization.SerializationExtension
import com.datastax.driver.core.utils.Bytes
import com.datastax.driver.core.{Session, ResultSet, Row}

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

  private[this] val preparedSelectMessages = session.prepare(selectMessages).setConsistencyLevel(readConsistency)
  private[this] val preparedSelectDeletedTo = session.prepare(selectDeletedTo).setConsistencyLevel(readConsistency)
  private[this] val preparedCheckInUse = session.prepare(selectInUse).setConsistencyLevel(readConsistency)

  private[this] val step = 50l

  override protected def query(state: Long, max: Long): Future[Vector[EventEnvelope]] = {
    implicit val ec = context.dispatcher

    // TODO: Async?
    Future {
      val from = state
      val to = Math.min(Math.min(state + step, toSeqNr), state + max)
      val ret = (state to to)
        .zip(new MessageIterator(persistenceId, from, to, maxBufSize).toVector)
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







  // TODO: THE BELOW WAS BLINDLY COPIED FROM CASSANDRAJOURNAL AND RECOVERY
  // TODO: Decouple and export

  /**
   * Iterator over messages, crossing partition boundaries.
   */
  class MessageIterator(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long) extends Iterator[PersistentRepr] {

    import PersistentRepr.Undefined

    private val initialFromSequenceNr = math.max(highestDeletedSequenceNumber(persistenceId) + 1, fromSequenceNr)
    log.debug("Starting message scan from {}", initialFromSequenceNr)

    private val iter = new RowIterator(persistenceId, initialFromSequenceNr, toSequenceNr)
    private var mcnt = 0L

    private var c: PersistentRepr = null
    private var n: PersistentRepr = PersistentRepr(Undefined)

    fetch()

    def hasNext: Boolean =
      n != null && mcnt < max

    def next(): PersistentRepr = {
      fetch()
      mcnt += 1
      c
    }

    /**
     * Make next message n the current message c, complete c
     * and pre-fetch new n.
     */
    private def fetch(): Unit = {
      c = n
      n = null
      while (iter.hasNext && n == null) {
        val row = iter.next()
        val snr = row.getLong("sequence_nr")
        val m = persistentFromByteBuffer(row.getBytes("message"))
        // there may be duplicates returned by iter
        // (on scan boundaries within a partition)
        if (snr == c.sequenceNr) c = m else n = m
      }
    }
  }


  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
  }

  def partitionNr(sequenceNr: Long): Long =
    (sequenceNr - 1L) / targetPartitionSize

  private def highestDeletedSequenceNumber(persistenceId: String): Long = {
    Option(session.execute(preparedSelectDeletedTo.bind(persistenceId)).one())
      .map(_.getLong("deleted_to")).getOrElse(0)
  }

  /**
   * Iterates over rows, crossing partition boundaries.
   */
  class RowIterator(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long) extends Iterator[Row] {
    var currentPnr = partitionNr(fromSequenceNr)
    var currentSnr = fromSequenceNr

    var fromSnr = fromSequenceNr
    var toSnr = toSequenceNr

    var iter = newIter()

    def newIter() = {
      session.execute(preparedSelectMessages.bind(persistenceId, currentPnr: JLong, fromSnr: JLong, toSnr: JLong)).iterator
    }

    def inUse: Boolean = {
      val execute: ResultSet = session.execute(preparedCheckInUse.bind(persistenceId, currentPnr: JLong))
      if (execute.isExhausted) false
      else execute.one().getBool("used")
    }

    @annotation.tailrec
    final def hasNext: Boolean = {
      if (iter.hasNext) {
        // more entries available in current resultset
        true
      } else if (!inUse) {
        // partition has never been in use so stop
        false
      } else {
        // all entries consumed, try next partition
        currentPnr += 1
        fromSnr = currentSnr
        iter = newIter()
        hasNext
      }
    }

    def next(): Row = {
      val row = iter.next()
      currentSnr = row.getLong("sequence_nr")
      row
    }

    private def sequenceNrMin(partitionNr: Long): Long =
      partitionNr * targetPartitionSize + 1L

    private def sequenceNrMax(partitionNr: Long): Long =
      (partitionNr + 1L) * targetPartitionSize
  }
}
