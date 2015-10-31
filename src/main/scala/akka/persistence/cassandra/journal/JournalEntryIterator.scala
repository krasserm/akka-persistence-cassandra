package akka.persistence.cassandra.journal

import java.lang.{Long => JLong}

import scala.collection.JavaConverters._

import akka.persistence.cassandra.MessageIterator
import akka.persistence.cassandra.journal.StreamMerger.{PersistenceId, JournalId, JournalEntry}
import com.datastax.driver.core.{Session, ResultSet, Row}

final class JournalEntryIterator (
    partitionId: String,
    fromSequenceNr: Long,
    toSequenceNr: Long,
    targetPartitionSize: Int,
    max: Long)(session: Session, override val config: CassandraJournalConfig)
  extends Iterator[JournalEntry]
  with CassandraStatements {

  import config._

  private[this] val preparedSelectMessages =
    session.prepare(selectMessages).setConsistencyLevel(readConsistency)
  private[this] val preparedCheckInUse=
    session.prepare(selectInUse).setConsistencyLevel(readConsistency)

  private[this] val iterator = new MessageIterator[JournalEntry] (
    partitionId,
    fromSequenceNr,
    toSequenceNr,
    targetPartitionSize,
    max,
    extractor,
    default,
    sequenceNumber,
    select,
    inUse,
    highestDeletedSequenceNumber,
    sequenceNumberColumn)

  override def hasNext: Boolean = iterator.hasNext
  override def next(): JournalEntry = iterator.next()

  private[this] def extractor(row: Row): JournalEntry =
    JournalEntry(
      JournalId(row.getString("journal_id")),
      row.getLong("journal_sequence_nr"),
      PersistenceId(row.getString("persistence_id")),
      row.getLong("sequence_nr"),
      row.getBytes("message"))

  private[this] def select(
      partitionKey: String,
      currentPnr: Long,
      fromSnr: Long,
      toSnr: Long): Iterator[Row] =
    session.execute(preparedSelectMessages.bind(
      partitionKey,
      currentPnr: JLong,
      fromSnr: JLong,
      toSnr: JLong)).iterator.asScala

  private[this] def inUse(partitionKey: String, currentPnr: Long): Boolean = {
    val execute: ResultSet =
      session.execute(preparedCheckInUse.bind(partitionKey, currentPnr: JLong))
    if (execute.isExhausted) false
    else execute.one().getBool("used")
  }

  private[this] def default: JournalEntry =
    JournalEntry(JournalId(""), -1l, PersistenceId(""), 0, null)

  private[this] def sequenceNumberColumn: String = "journal_sequence_nr"

  private[this] def sequenceNumber(element: JournalEntry): Long = element.journalSequenceNr

  // TODO: Fix.
  private[this] def highestDeletedSequenceNumber(partitionKey: String): Long = -1l
}
