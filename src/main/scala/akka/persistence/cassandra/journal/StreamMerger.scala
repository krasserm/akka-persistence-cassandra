package akka.persistence.cassandra.journal

import java.nio.ByteBuffer

import scala.annotation.tailrec

object StreamMerger {

  final case class JournalEntry(
    journalId: JournalId,
    journalSequenceNr: Long,
    persistenceId: PersistenceId,
    sequenceNr: Long,
    serialized: ByteBuffer)

  final case class Stream(journalId: JournalId, elements: Iterator[JournalEntry])

  final case class MergeState(
    journalIdProgress: Progress[JournalId],
    persistenceIdProgress: Progress[PersistenceId],
    independentStreams: Seq[Stream],
    independentStreamPointer: Int,
    mergedStream: Seq[JournalEntry],
    buffer: Set[JournalEntry],
    noActionCounter: Int)

  final case class JournalId(id: String) extends AnyVal
  final case class PersistenceId(id: String) extends AnyVal

  type Progress[T] = Map[T, Long]

  // TODO: Do we want to represent the result more genericly to be able to substitute different
  // TODO: stream merging approach?
  sealed trait MergeResult

  final case class MergeSuccess(
      journalIdProgress: Progress[JournalId],
      persistenceIdProgress: Progress[PersistenceId],
      mergedStream: Seq[JournalEntry]) extends MergeResult

  final case class MergeFailure(
      journalIdProgress: Progress[JournalId],
      persistenceIdProgress: Progress[PersistenceId],
      mergedStream: Seq[JournalEntry],
      unmerged: Set[JournalEntry]) extends MergeResult

  // TODO: Likely terribly inefficient and memory intensive. Refactor, mutable state instead etc.
  // TODO: Simply .sort by persistenceId and check sequenceNr?
  // TODO: It could work well. But do we want to achieve some "fairness" between streams?
  /**
    * Merge returns single merged stream of events with preserved causality per persistenceId.
    *
    * @param journalIdProgress Progress of processing in journal instances.
    * @param persistenceIdProgress Progress of processing of each persistenceId.
    * @param independentStreams The independent unmerged streams.
    * @return
    */
  def merge(
      journalIdProgress: Progress[JournalId],
      persistenceIdProgress: Progress[PersistenceId],
      independentStreams: Seq[Stream]): MergeResult =
    merge(
      MergeState(journalIdProgress, persistenceIdProgress, independentStreams, 0, Seq(), Set(), 0))

  @tailrec
  private[this] def merge(state: MergeState): MergeResult = {

    import state._

    def allEmpty(independentStreams: Seq[Stream]) =
      !independentStreams.exists(_.elements.hasNext)

    def mergeFailed(independentStreams: Seq[Stream], noActionCounter: Int) =
      independentStreams.size == noActionCounter

    if(allEmpty(independentStreams) && buffer.isEmpty)
      return MergeSuccess(journalIdProgress, persistenceIdProgress, mergedStream)

    if(mergeFailed(independentStreams, noActionCounter))
      return MergeFailure(journalIdProgress, persistenceIdProgress, mergedStream, buffer)

    merge(mergeInternal(state))
  }

  private[this] def mergeInternal(state: MergeState): MergeState = {
    import state._

    def incrementPointer: Int = (independentStreamPointer + 1) % independentStreams.size

    def newPersistenceIdProgress(entry: JournalEntry): Progress[PersistenceId] =
      persistenceIdProgress + (entry.persistenceId -> entry.sequenceNr)

    def newJournalIdProgress(entry: JournalEntry): Progress[JournalId] =
      journalIdProgress +
        (entry.journalId -> (journalIdProgress.getOrElse(entry.journalId, -1l) + 1))

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
        state.copy(
          independentStreamPointer = incrementPointer,
          buffer = buffer + head,
          noActionCounter = 0)
      }
    } else {
      state.copy(independentStreamPointer = incrementPointer, noActionCounter = noActionCounter + 1)
    }
  }

  // TODO: Handle situation when new persistenceId is encountered, but its sequenceNr is not 0
  private[this] def isExpectedSequenceNr(
      event: JournalEntry,
      persistenceIdProgress: Progress[PersistenceId]) =
    persistenceIdProgress.getOrElse(event.persistenceId, 0l) + 1 == event.sequenceNr
}
