package akka.persistence.cassandra.journal

import java.lang.{Long => JLong}
import java.nio.ByteBuffer

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

import akka.actor.{Props, Actor}
import akka.persistence.cassandra.MessageIterator
import akka.persistence.cassandra.journal.StreamMerger._
import akka.serialization.SerializationExtension
import com.datastax.driver.core.{ResultSet, Row, Session}

object StreamMerger {

  def props(config: CassandraJournalConfig, session: Session): Props =
    Props(new StreamMerger(config, session))

  case class JournalEntry(
    journalId: JournalId,
    journalSequenceNr: Long,
    persistenceId: PersistenceId,
    sequenceNr: Long,
    serialized: ByteBuffer)

  case class Stream(journalId: JournalId, elements: Iterator[JournalEntry])

  case class MergeState(
    journalIdProgress: Progress[JournalId],
    persistenceIdProgress: Progress[PersistenceId],
    independentStreams: Seq[Stream],
    independentStreamPointer: Int,
    mergedStream: Seq[JournalEntry],
    buffer: Set[JournalEntry],
    noActionCounter: Int)

  case object Continue

  case class JournalId(id: String) extends AnyVal
  case class PersistenceId(id: String) extends AnyVal

  type Progress[T] = Map[T, Long]

  // TODO: Likely terribly inefficient and memory intensive. Refactor, mutable state instead etc.
  // TODO: Simply .sort by persistenceId and check sequenceNr?
  // TODO: It could work well. But do we want to achieve some "fairness" between streams?
  /**
   * Merge returns either single merged stream of events with preserved causality per persistenceId
   * or does not return a result.
   *
   * @param state the state of the merging progress.
   * @return
   */
  @tailrec
  def merge(
    state: MergeState): Option[(Progress[JournalId], Progress[PersistenceId], Seq[JournalEntry])] = {

    import state._

    def allEmpty(independentStreams: Seq[Stream]) =
      !independentStreams.exists(_.elements.hasNext)

    def mergeFailed(independentStreams: Seq[Stream], noActionCounter: Int) =
      independentStreams.size == noActionCounter

    if(allEmpty(independentStreams) && buffer.isEmpty)
      return Some((journalIdProgress, persistenceIdProgress, mergedStream))

    if(mergeFailed(independentStreams, noActionCounter))
      return None

    merge(mergeInternal(state))
  }

  private[this] def mergeInternal(state: MergeState): MergeState = {
    import state._

    def incrementPointer: Int = (independentStreamPointer + 1) % independentStreams.size

    def newPersistenceIdProgress(entry: JournalEntry): Progress[PersistenceId] =
      persistenceIdProgress
        .updated(
          entry.persistenceId,
          persistenceIdProgress.getOrElse(entry.persistenceId, entry.sequenceNr - 1) + 1l)

    def newJournalIdProgress(entry: JournalEntry): Progress[JournalId] =
      journalIdProgress
        .updated(
          entry.journalId,
          journalIdProgress.getOrElse(entry.journalId, entry.journalSequenceNr - 1) + 1l)

    if(buffer.exists(isExpectedSequenceNr(_, persistenceIdProgress))) {
      val head = buffer.find(isExpectedSequenceNr(_, persistenceIdProgress)).get

      state.copy(
        journalIdProgress = newJournalIdProgress(head),
        persistenceIdProgress = newPersistenceIdProgress(head),
        mergedStream = mergedStream :+ head,
        buffer = buffer - head,
        noActionCounter = 0)
    } else if (independentStreams(independentStreamPointer).elements.hasNext) {
      val head = independentStreams(independentStreamPointer).elements.next()

      if(isExpectedSequenceNr(head, persistenceIdProgress)) {
        state.copy(
          journalIdProgress = newJournalIdProgress(head),
          persistenceIdProgress = newPersistenceIdProgress(head),
          independentStreamPointer = incrementPointer,
          mergedStream = mergedStream :+ head,
          noActionCounter = 0)
      } else {
        state.copy(independentStreamPointer = incrementPointer, buffer = buffer + head, noActionCounter = 0)
      }
    } else {
      state.copy(independentStreamPointer = incrementPointer, noActionCounter = noActionCounter + 1)
    }
  }

  // TODO: Handle situation when new persistenceId is encountered, but its sequenceNr is not 0
  // TODO: Represent progress for not known persistenceIds properly across merger.
  private[this] def isExpectedSequenceNr(event: JournalEntry, persistenceIdProgress: Progress[PersistenceId]) =
    persistenceIdProgress.getOrElse(event.persistenceId, event.sequenceNr - 1) + 1 == event.sequenceNr
}

/**
 * Merges n independent physical streams into a single logical stream.
 * Preserves causal order per persistenceId.
 *
 * The intent of current functionality is to work in batches. The merger retrieves data from all
 * streams up to a maximum of configured step. Then creates a single logical stream out of the
 * n independent streams. There is no ordering between the independent streams and the only
 * requirement is to maintain causal order per persistenceId. When the whole batch can be merged
 * the stream is emitted. If not (e.g. causality violation in given batch) then the merger
 * requests again for more data.
 *
 * The emmited logical stream can be stored into database in a batch. This grants atomicity, but not
 * isolation which is what we need. Part of the batch is not perceived as complete due to eventual
 * consistency should be fine and reconciled during replay.
 *
 * Other options were considered, e.g. storing each element as it arrives. This would require a more
 * granular control of processed elements. E.g. updating the master table with a "processed" flag
 * or storing current state of a buffer in database using a batch. The reason is that maintaining
 * causal order requires a buffer to store out of sequence elements until they are expected in
 * sequence. This buffer must be persistent and recovered in case of failure otherwise the elements
 * could remain unprocessed. Creating a batch does not require any additional data structure and
 * is similar to how CassandraJournal works.
 *
 * @param config CassandraJournalConfig.
 * @param session Session.
 */
class StreamMerger(
    val config: CassandraJournalConfig,
    session: Session) extends Actor with CassandraStatements {

  import config._

  val serialization = SerializationExtension(context.system)

  private[this] val refreshInterval = FiniteDuration(1, SECONDS)

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

  override def receive: Receive = merging(initialJournalIdProgress, initialPersistenceIdProgress, 50l)

  private[this] def merging(
      journalIdProgress: Progress[JournalId],
      persistenceIdProgress: Progress[PersistenceId],
      step: Long): Receive = {

    case Continue =>
      println("CONTINUE")
      val currentJournalIds = journalIds()
      val updatedProgress = currentJournalIds
        .map(journalId => (JournalId(journalId), journalIdProgress.getOrElse(JournalId(journalId), 0l)))
        .toMap

      val independentStreams =
        updatedProgress
          .map{ journalProgress =>
            Stream(
              journalProgress._1,
              new MessageIterator[JournalEntry](
                journalProgress._1.id,
                journalProgress._2,
                journalProgress._2 + step,
                targetPartitionSize,
                Long.MaxValue,
                extractor,
                JournalEntry(JournalId(""), 0l, PersistenceId(""), 0, null),
                _.journalSequenceNr,
                select,
                inUse,
                "journal_sequence_nr"))
          }
          .toSeq

      val mergeResult =
        merge(MergeState(updatedProgress, persistenceIdProgress, independentStreams, 0, Seq(), Set(), 0))

      val nextState =
      mergeResult match {
        case None =>
          merging(journalIdProgress, persistenceIdProgress, step + 50l)
        case Some((newJournalIdProgress, newPersistenceIdProgress, mergedStream)) =>
          merging(newJournalIdProgress, newPersistenceIdProgress, step)
      }

      context.become(nextState)
  }

  private[this] def extractor(row: Row): JournalEntry =
    JournalEntry(
      JournalId(row.getString("journal_id")),
      row.getLong("journal_sequence_nr"),
      PersistenceId(row.getString("persistence_id")),
      row.getLong("sequence_nr"),
      row.getBytes("message"))

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
    session
      .execute(preparedSelectDistinctJournalId.bind()).all().asScala.map(_.getString("journal_id"))
      .distinct
}
