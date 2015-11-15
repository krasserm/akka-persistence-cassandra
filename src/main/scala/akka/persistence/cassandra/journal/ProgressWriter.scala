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
    val boundJournalIdProgress: ((JournalId, Long)) => BoundStatement = journalId =>
        preparedWriteJournalIdProgress.bind(0: JInt, journalId._1.id, journalId._2: JLong)

    // TODO: We don't need subpartitioning here. Fix the method.
    writeBatchSingle[String, (JournalId, Long)](
      "" -> journalIdIdProgress.toList, boundJournalIdProgress)

    val boundPersistenceIdProgress: ((PersistenceId, Long)) => BoundStatement = persistenceId =>
        preparedWritePersistenceIdProgress
          .bind(0: JInt, persistenceId._1.id, persistenceId._2: JLong)

    writeBatchSingle[String, (PersistenceId, Long)](
      "" -> persistenceIdProgress.toList, boundPersistenceIdProgress)
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
