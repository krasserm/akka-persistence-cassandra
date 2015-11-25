package akka.persistence.cassandra

import com.datastax.driver.core.Row

/**
 * Iterator over messages, crossing partition boundaries.
 */
class MessageIterator[T >: Null](
    partitionKey: String,
    fromSequenceNr: Long,
    toSequenceNr: Long,
    targetPartitionSize: Int,
    max: Long,
    extractor: Row => T,
    default: T,
    sequenceNumber: T => Long,
    select: (String, Long, Long, Long) => Iterator[Row],
    inUse: (String, Long) => Boolean,
    highestDeletedSequenceNumber: String => Long,
    sequenceNumberColumn: String) extends Iterator[T] {

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

  private var c: T = null
  // TODO: FIX
  private var n: T = default

  fetch()

  def hasNext: Boolean =
    n != null && mcnt < max

  def next(): T = {
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
      val snr = row.getLong(sequenceNumberColumn)
      val m = extractor(row)
      // there may be duplicates returned by iter
      // (on scan boundaries within a partition)
      if (snr == sequenceNumber(c)) c = m else n = m
    }
  }
}
