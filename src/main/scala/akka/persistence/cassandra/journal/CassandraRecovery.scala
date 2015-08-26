package akka.persistence.cassandra.journal

import java.lang.{ Long => JLong }

import scala.concurrent._

import com.datastax.driver.core.Row

import akka.persistence.PersistentRepr

trait CassandraRecovery { this: CassandraJournal =>
  import config._

  implicit lazy val replayDispatcher = context.system.dispatchers.lookup(replayDispatcherId)

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] =
    Future { replayMessages(persistenceId, fromSequenceNr, toSequenceNr, max)(replayCallback) }

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    Future {
      val result = readHighestSequenceNr(persistenceId, fromSequenceNr)
      if (fromSequenceNr != 1) Math.max(fromSequenceNr, result)
      else result
    }
  }

  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long = {
    Option(session.execute(preparedHighestSequenceNr.bind(persistenceId, fromSequenceNr: JLong)).one).map(_.getLong("sequence_nr")).getOrElse(0L)
  }

  def readLowestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long =
    Option(session.execute(preparedLowestSequenceNr.bind(persistenceId, fromSequenceNr: JLong)).one).map(_.getLong("sequence_nr")).getOrElse(0L)

  def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Unit =
    new MessageIterator(persistenceId, fromSequenceNr, toSequenceNr, max).foreach(replayCallback)

  /**
   * Iterator over messages, crossing partition boundaries.
   */
  class MessageIterator(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long) extends Iterator[PersistentRepr] {
    import PersistentRepr.Undefined
    private val iter = new RowIterator(persistenceId, fromSequenceNr, toSequenceNr)
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
     * (ignoring orphan markers) and pre-fetch new n.
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

  /**
   * Iterates over rows, crossing partition boundaries.
   */
  class RowIterator(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long) extends Iterator[Row] {
    var nextSnr = fromSequenceNr
    var toSnr = toSequenceNr

    var emptyIter: Boolean = false
    var iter = newIter()

    def newIter() = {
      val it = session.execute(preparedSelectMessages.bind(persistenceId, nextSnr: JLong, toSnr: JLong)).iterator
      emptyIter = !it.hasNext
      it
    }

    @annotation.tailrec
    final def hasNext: Boolean = {
      if (emptyIter) {
        false
      } else if (iter.hasNext) {
        true
      } else {
        iter = newIter()
        hasNext
      }
    }

    def next(): Row = {
      val row = iter.next()
      nextSnr = row.getLong("sequence_nr") + 1
      row
    }
  }
}
