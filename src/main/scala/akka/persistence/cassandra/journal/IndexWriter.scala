package akka.persistence.cassandra.journal

import java.lang.{Long => JLong}

import akka.persistence.cassandra.journal.StreamMerger.{PersistenceId, JournalEntry}
import com.datastax.driver.core.BoundStatement

trait IndexWriter extends CassandraStatements with BatchWriter {

  session.execute(createEventsByPersistenceIdTable)

  val preparedWriteMessage = session.prepare(super.writeEventsByPersistenceId)
  val preparedWriteInUse = session.prepare(writeEventsByPersistenceIdInUse)

  // TODO: Store progress of all persistenceJournals so we can resume?
  def writeIndexProgress(stream: Seq[JournalEntry]): Unit = {
    val byPersistenceId = stream.groupBy(_.persistenceId)

    val boundJournalEntry: (JournalEntry, Long) => BoundStatement =
      (entry, maxPnr) => preparedWriteMessage.bind(
        entry.persistenceId.id,
        maxPnr: JLong,
        entry.sequenceNr: JLong,
        entry.serialized)

    writeBatch[PersistenceId, JournalEntry](
      byPersistenceId,
      _._2.head.sequenceNr,
      _._2.last.sequenceNr,
      boundJournalEntry,
      x => x._2.head.persistenceId.id,
      Some((persistenceId, minPnr) => preparedWriteInUse.bind(persistenceId, minPnr: JLong)))
  }
}
