package akka.persistence.cassandra.journal

import java.lang.{Integer => JInt, Long => JLong}

import scala.collection.JavaConverters._

import akka.persistence.cassandra.journal.StreamMerger._
import com.datastax.driver.core.BoundStatement

trait ProgressWriter extends CassandraStatements with BatchWriter {

  session.execute(createJournalIdProgressTable)
  session.execute(createPersistenceIdProgressTable)

  val preparedWritePersistenceIdProgress = session.prepare(writePersistenceIdProgress)
  val preparedSelectPersistenceIdProgress = session.prepare(selectPersistenceIdProgress)

  val preparedWriteJournalIdProgress  = session.prepare(writeJournalIdProgress)
  val preparedSelectJournalIdProgress = session.prepare(selectJournalIdProgress)

  def writeProgress(
      journalIdIdProgress: Progress[JournalId],
      persistenceIdProgress: Progress[PersistenceId]): Unit = {

    // TODO: The two progress updates are not batched.
    // TODO: Verify its ok.
    val boundJournalIdProgress: (Int, Seq[(JournalId, Long)]) => Seq[BoundStatement] =
      (partitionKey, journalIds) => journalIds.map { e =>
        preparedWriteJournalIdProgress
          .bind(partitionKey: JInt, e._1.id, e._2: JLong)
      }

    writeBatchSingle[Int, (JournalId, Long)](
      0 -> journalIdIdProgress.toList, boundJournalIdProgress)

    val boundPersistenceIdProgress: (Int, Seq[(PersistenceId, Long)]) => Seq[BoundStatement] =
      (partitionKey, persistenceIds) => persistenceIds.map{ e =>
        preparedWritePersistenceIdProgress
          .bind(partitionKey: JInt, e._1.id, e._2: JLong)
      }

    writeBatchSingle[Int, (PersistenceId, Long)](
      0 -> persistenceIdProgress.toList, boundPersistenceIdProgress)
  }

  def readPersistenceIdProgress(): Progress[PersistenceId] = {
    val persistenceIdProgress =
      session.execute(preparedSelectPersistenceIdProgress.bind())
        .all()
        .asScala
        .map { row =>
          (PersistenceId(row.getString("persistence_id")), row.getLong("progress"))
        }
        .toMap

    persistenceIdProgress
  }
}
