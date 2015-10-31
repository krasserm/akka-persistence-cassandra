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
  extends MessageIterator[JournalEntry] (
    partitionId,
    fromSequenceNr,
    toSequenceNr,
    targetPartitionSize,
    max)
  with CassandraStatements {

  import config._

  private[this] val preparedSelectMessages =
    session.prepare(selectMessages).setConsistencyLevel(readConsistency)

  private[this] val preparedCheckInUse=
    session.prepare(selectInUse).setConsistencyLevel(readConsistency)

  protected def extractor(row: Row): JournalEntry =
    JournalEntry(
      JournalId(row.getString("journal_id")),
      row.getLong("journal_sequence_nr"),
      PersistenceId(row.getString("persistence_id")),
      row.getLong("sequence_nr"),
      row.getBytes("message"))

  override protected def select(
      partitionKey: String,
      currentPnr: Long,
      fromSnr: Long,
      toSnr: Long): Iterator[Row] =
    session.execute(preparedSelectMessages.bind(
      partitionKey,
      currentPnr: JLong,
      fromSnr: JLong,
      toSnr: JLong)).iterator.asScala

  override protected def inUse(partitionKey: String, currentPnr: Long): Boolean = {
    val execute: ResultSet =
      session.execute(preparedCheckInUse.bind(partitionKey, currentPnr: JLong))
    if (execute.isExhausted) false
    else execute.one().getBool("used")
  }

  override protected def default: JournalEntry =
    JournalEntry(JournalId(""), -1l, PersistenceId(""), 0, null)

  override protected def sequenceNumberColumn: String = "journal_sequence_nr"

  override protected def sequenceNumber(element: JournalEntry): Long = element.journalSequenceNr

  // TODO: Fix.
  override protected def highestDeletedSequenceNumber(partitionKey: String): Long = -1l
}
