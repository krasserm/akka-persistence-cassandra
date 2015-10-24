package akka.persistence.cassandra

import java.nio.ByteBuffer

import akka.persistence.PersistentRepr
import com.datastax.driver.core.{Row, Session}

/**
 * Iterator over messages, crossing partition boundaries.
 */
private class MessageIterator(
    partitionKey: String,
    fromSequenceNr: Long,
    toSequenceNr: Long,
    targetPartitionSize: Int,
    max: Long,
    persistentFromByteBuffer: ByteBuffer => PersistentRepr,
    select: (String, Long, Long, Long) => Iterator[Row],
    inUse: (String, Long) => Boolean,
    sequenceNumberColumn: String) extends Iterator[PersistentRepr] {

  import akka.persistence.PersistentRepr.Undefined

  private val initialFromSequenceNr = math.max(highestDeletedSequenceNumber(partitionKey) + 1, fromSequenceNr)
  // log.debug("Starting message scan from {}", initialFromSequenceNr)

  private val iter = new RowIterator(
    partitionKey,
    initialFromSequenceNr,
    toSequenceNr,
    targetPartitionSize,
    select,
    inUse,
    sequenceNumberColumn)

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

  // TODO: FIX
  private def highestDeletedSequenceNumber(persistenceId: String): Long = 0
}
