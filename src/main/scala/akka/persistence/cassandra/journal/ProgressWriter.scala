package akka.persistence.cassandra.journal

import scala.collection.JavaConverters._

import akka.persistence.cassandra.journal.StreamMerger._
import com.datastax.driver.core.Session

trait ProgressWriter extends CassandraStatements {
  this: StreamMergerActor =>

  def config: CassandraJournalConfig
  def session: Session

  val preparedWritePersistenceIdProgress = session.prepare(writePersistenceIdProgress)
  val preparedSelectPersistenceIdProgress = session.prepare(selectPersistenceIdProgress)

  val preparedWriteJournalIdProgress  = session.prepare(writeJournalIdProgress)
  val preparedSelectJournalIdProgress = session.prepare(selectJournalIdProgress)

  def writeProgress(
      journalIdIdProgress: Progress[JournalId],
      persistenceIdProgress: Progress[PersistenceId]) = {

    // TODO: Batch?
    // TODO: Or a map?
    val boundPersistenceIdProgress = preparedWritePersistenceIdProgress.bind()
  }

  def persistenceIdProgress(): Progress[PersistenceId] = {
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
