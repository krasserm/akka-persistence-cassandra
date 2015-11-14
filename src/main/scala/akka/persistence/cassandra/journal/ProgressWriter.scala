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
    // TODO: Verify that is ok.
    val boundJournalIdProgress: ((JournalId, Long), Long) => BoundStatement =
      (journalId, maxPnr) =>
        preparedWriteJournalIdProgress.bind(0: JInt, journalId._1.id, journalId._2: JLong)

    // TODO: We don't need subpartitioning here. Fix the method.
    writeBatch[String, (JournalId, Long)](
      Map("" -> journalIdIdProgress.toList),
      _ => 0,
      _ => Long.MaxValue,
      boundJournalIdProgress,
      _ => "",
      None)

    val boundPersistenceIdProgress: ((PersistenceId, Long), Long) => BoundStatement =
      (persistenceId, maxPnr) =>
        preparedWritePersistenceIdProgress.bind(0: JInt, persistenceId._1.id, persistenceId._2: JLong)

    writeBatch[String, (PersistenceId, Long)](
      Map("" -> persistenceIdProgress.toList),
      _ => 0,
      _ => Long.MaxValue,
      boundPersistenceIdProgress,
      _ => "",
      None)
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
