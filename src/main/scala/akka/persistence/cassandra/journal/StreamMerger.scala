package akka.persistence.cassandra.journal

import java.lang.{Long => JLong}

import scala.Option
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

// TODO: Are sequenceNrs for a given persistenceId within one physical stream ordered?
// TODO: If not we will need to add a buffer larger than 1 element.
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

      val independentStreams =
        updatedProgress
          .map{ journalProgress =>
            Stream(
              journalProgress._1,
              new MessageIterator(
                journalProgress._1.id,
                journalProgress._2,
                journalProgress._2 + step,
                targetPartitionSize,
                Long.MaxValue,
                persistentFromByteBuffer(serialization, _),
                select,
                inUse,
                "journal_sequence_nr"))
          }
          .toSeq

      val mergeResult =
        merge(MergeState(updatedProgress, persistenceIdProgress, independentStreams, 0, Seq(), Set()))

      val nextState =
      mergeResult match {
        case None =>
          // TODO: Fetch more next time.
          merging(journalIdProgress, persistenceIdProgress)
        case Some((newJournalIdProgress, newPersistenceIdProgress, mergedStream)) =>
          println(mergedStream)
          merging(newJournalIdProgress, newPersistenceIdProgress)
      }

      context.become(nextState)
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

  case class Stream(journalId: JournalId, elements: Iterator[PersistentRepr])

  case class MergeState(
    journalIdProgress: Progress[JournalId],
    persistenceIdProgress: Progress[PersistenceId],
    independentStreams: Seq[Stream],
    independentStreamPointer: Int,
    mergedStream: Seq[PersistentRepr],
    buffer: Set[PersistentRepr])

  @tailrec
  private[this] def merge(
      state: MergeState): Option[(Progress[JournalId], Progress[PersistenceId], Seq[PersistentRepr])] = {

    import state._

    def allEmpty(independentStreams: Seq[Stream]) =
      !independentStreams.exists(_.elements.hasNext)

    if(allEmpty(independentStreams))
      return Some((journalIdProgress, persistenceIdProgress, mergedStream))

    merge(mergeInternal(state))
  }

  private[this] def mergeInternal(state: MergeState): MergeState = {
    import state._

    if(buffer.exists(isExpectedSequenceNr(_, persistenceIdProgress))) {
      val head = buffer.find(isExpectedSequenceNr(_, persistenceIdProgress)).get
      val persistenceId = PersistenceId(head.persistenceId)
      val newPersistenceIdProgress = persistenceIdProgress
        .updated(persistenceId, persistenceIdProgress.getOrElse(persistenceId, 0l) + 1l)

      MergeState(
        journalIdProgress,
        newPersistenceIdProgress,
        independentStreams,
        independentStreamPointer,
        mergedStream :+ head,
        buffer - head)
    } else if (state.independentStreams(independentStreamPointer).elements.hasNext) {
      val stream = independentStreams(independentStreamPointer)
      val head = stream.elements.next()
      val newIndependentStreamPointer = independentStreamPointer + 1 % independentStreams.size
      val newJournalIdProgress = journalIdProgress
        .updated(stream.journalId, journalIdProgress.getOrElse(stream.journalId, 0l) + 1l)

      if(isExpectedSequenceNr(head, persistenceIdProgress)) {
        val persistenceId = PersistenceId(head.persistenceId)
        val newPersistenceIdProgress = persistenceIdProgress
          .updated(persistenceId, persistenceIdProgress.getOrElse(persistenceId, 0l) + 1l)

        MergeState(
          newJournalIdProgress,
          newPersistenceIdProgress,
          independentStreams,
          newIndependentStreamPointer,
          mergedStream :+ head,
          buffer)
      } else {
        MergeState(
          newJournalIdProgress,
          persistenceIdProgress,
          independentStreams,
          newIndependentStreamPointer,
          mergedStream,
          buffer + head)
      }
    } else {
      // TODO: Ensure we don't get stuck in a loop here.
      state
    }
  }

  // TODO: Handle situation when new persistenceId is encountered, but its sequenceNr is not 0
  private[this] def isExpectedSequenceNr(event: PersistentRepr, persistenceIdProgress: Progress[PersistenceId]) =
    persistenceIdProgress.getOrElse(PersistenceId(event.persistenceId), event.sequenceNr) == event.sequenceNr
}
