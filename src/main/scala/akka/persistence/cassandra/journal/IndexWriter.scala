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

    val boundJournalEntries: (PersistenceId, Seq[JournalEntry]) => Seq[BoundStatement] =
      (persistenceId, entries) => {

        val maxPnr = partitionNr(entries.last.sequenceNr)

        entries.map{ e =>
          preparedWriteMessage.bind(
            persistenceId.id,
            maxPnr: JLong,
            e.sequenceNr: JLong,
            e.serialized)
        }
      }

    writeBatch[PersistenceId, JournalEntry](
      byPersistenceId,
      boundJournalEntries,
      _.journalSequenceNr,
      (persistenceId, minPnr) => preparedWriteInUse.bind(persistenceId.id, minPnr: JLong))
  }
}
