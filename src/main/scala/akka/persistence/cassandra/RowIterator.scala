package akka.persistence.cassandra

import com.datastax.driver.core.Row

import JournalFunctions._

/**
 * Iterates over rows, crossing partition boundaries.
 */
private class RowIterator(
    partitionKey: String,
    fromSequenceNr: Long,
    toSequenceNr: Long,
    targetPartitionSize: Int,
    select: (String, Long, Long, Long) => Iterator[Row],
    inUse: (String, Long) => Boolean,
    sequenceNumberColumn: String) extends Iterator[Row] {

  private[this] var currentPnr = partitionNr(fromSequenceNr, targetPartitionSize)
  private[this] var currentSnr = fromSequenceNr

  private[this] var fromSnr = fromSequenceNr
  private[this] val toSnr = toSequenceNr

  private[this] var iter = newIter()

  private[this] def newIter(): Iterator[Row] = select(partitionKey, currentPnr, fromSnr, toSnr)

  @annotation.tailrec
  final def hasNext: Boolean = {
    if (iter.hasNext) {
      // more entries available in current resultset
      true
    } else if (!inUse(partitionKey, currentPnr)) {
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
    currentSnr = row.getLong(sequenceNumberColumn)
    row
  }

  private def sequenceNrMin(partitionNr: Long): Long =
    partitionNr * targetPartitionSize + 1L

  private def sequenceNrMax(partitionNr: Long): Long =
    (partitionNr + 1L) * targetPartitionSize
}
