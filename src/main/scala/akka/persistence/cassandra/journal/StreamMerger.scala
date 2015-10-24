package akka.persistence.cassandra.journal

import java.lang.{Long => JLong}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

import akka.actor.Actor
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.MessageIterator
import akka.persistence.cassandra.JournalFunctions._
import akka.persistence.cassandra.journal.StreamMerger.{PersistenceId, JournalId, Progress}
import akka.serialization.SerializationExtension
import com.datastax.driver.core.{ResultSet, Row, Session}

object StreamMerger {
  case class JournalId(id: String) extends AnyVal
  case class PersistenceId(id: String) extends AnyVal

  type Progress[T] = Map[T, Long]
}

class StreamMerger(
    val config: CassandraJournalConfig,
    session: Session) extends Actor with CassandraStatements {

  private[this] case object Continue

  import config._

  val serialization = SerializationExtension(context.system)

  private[this] val refreshInterval = FiniteDuration(1, SECONDS)
  private[this] val step = 50l

  private[this] val tickTask =
    context.system.scheduler.schedule(refreshInterval, refreshInterval, self, Continue)(context.dispatcher)

  override def postStop(): Unit = {
    tickTask.cancel()
    super.postStop()
  }

  private[this] val preparedSelectMessages =
    session.prepare(selectMessages).setConsistencyLevel(readConsistency)
  private[this] val preparedSelectDistinctJournalId =
    session.prepare(selectDistinctJournalId).setConsistencyLevel(readConsistency)
  private[this] val preparedCheckInUse=
    session.prepare(selectInUse).setConsistencyLevel(readConsistency)

  override def receive: Receive = merging(initialJournalIdProgress, initialPersistenceIdProgress)

  private[this] def merging(
      journalIdProgress: Progress[JournalId],
      persistenceIdProgress: Progress[PersistenceId]): Receive = {

    case Continue =>
      val currentJournalIds = journalIds()
      val updatedProgress = currentJournalIds
        .map(journalId => (JournalId(journalId), journalIdProgress.getOrElse(JournalId(journalId), 0l)))
        .toMap

      val independentStreams = updatedProgress.map{ journalProgress =>
        new MessageIterator(
          journalProgress._1.id,
          journalProgress._2,
          journalProgress._2 + step,
          targetPartitionSize,
          Long.MaxValue,
          persistentFromByteBuffer(serialization, _),
          select,
          inUse,
          "journal_sequence_nr")
      }

      val (newJournalIdProgress, newPersistenceIdProgress, mergedStream) =
        merge(updatedProgress, persistenceIdProgress, independentStreams, Seq(), Seq())

      println(mergedStream)
      context.become(merging(newJournalIdProgress, newPersistenceIdProgress))
  }

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
    val execute: ResultSet = session.execute(preparedCheckInUse.bind(partitionKey, currentPnr: JLong))
    if (execute.isExhausted) false
    else execute.one().getBool("used")
  }

  // TODO: FIX Recovery case
  private[this] def initialJournalIdProgress: Progress[JournalId] = Map[JournalId, Long]()

  // TODO: FIX Recovery case
  private[this] def initialPersistenceIdProgress: Progress[PersistenceId] = Map[PersistenceId, Long]()

  private[this] def journalIds(): Seq[String] =
    session.execute(preparedSelectDistinctJournalId.bind()).all().asScala.map(_.getString("journal_id"))

  @tailrec
  private[this] def merge(
      journalIdProgress: Progress[JournalId],
      persistenceIdProgress: Progress[PersistenceId],
      independentStreams: Iterable[Iterator[PersistentRepr]],
      buffer: Seq[PersistentRepr],
      mergedStream: Seq[PersistentRepr]
    ): (Progress[JournalId], Progress[PersistenceId], Seq[PersistentRepr]) = {


    merge(journalIdProgress, persistenceIdProgress, independentStreams, Seq(), Seq())
  }
}
