package akka.persistence.cassandra

import java.lang.{Long => JLong}

import scala.collection.JavaConverters._

import akka.persistence.PersistentRepr
import akka.persistence.PersistentRepr._
import akka.persistence.cassandra.JournalFunctions._
import akka.persistence.cassandra.journal.CassandraStatements
import akka.persistence.cassandra.query.journal.{CassandraReadStatements,
CassandraReadJournalConfig}
import akka.serialization.Serialization
import com.datastax.driver.core.{ResultSet, Row, Session}

final class EventsByPersistenceIdIterator(
    partitionId: String,
    fromSequenceNr: Long,
    toSequenceNr: Long,
    targetPartitionSize: Int,
    max: Long)(
    session: Session,
    override val config: CassandraReadJournalConfig,
    serialization: Serialization)
  extends MessageIterator[PersistentRepr] (
    partitionId,
    fromSequenceNr,
    toSequenceNr,
    targetPartitionSize,
    max)
  with CassandraReadStatements {

  import config._

  private[this] val preparedSelectMessages = session.prepare(selectMessages).setConsistencyLevel(readConsistency)
  private[this] val preparedCheckInUse = session.prepare(selectInUse).setConsistencyLevel(readConsistency)

  override protected def extractor(row: Row): PersistentRepr =
    persistentFromByteBuffer(serialization, row.getBytes("message"))

  override protected def inUse(partitionKey: String, currentPnr: Long): Boolean = {
    val execute: ResultSet = session.execute(preparedCheckInUse.bind(partitionId, currentPnr: JLong))
    if (execute.isExhausted) false
    else execute.one().getBool("used")
  }

  override protected def default: PersistentRepr = PersistentRepr(Undefined)

  override protected def sequenceNumberColumn: String = "sequence_nr"

  override protected def sequenceNumber(element: PersistentRepr): Long = element.sequenceNr

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

  // TODO: Fix.
  override protected def highestDeletedSequenceNumber(partitionKey: String): Long = 0l
}
