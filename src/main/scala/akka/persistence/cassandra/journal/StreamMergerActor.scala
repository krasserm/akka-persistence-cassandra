package akka.persistence.cassandra.journal

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import akka.actor.{Props, Actor}
import akka.persistence.cassandra.journal.StreamMerger._
import akka.serialization.SerializationExtension
import com.datastax.driver.core.Session

object StreamMergerActor {
  def props(config: CassandraJournalConfig, session: Session): Props =
    Props(new StreamMergerActor(config, session))
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
 * isolation which should be what we need. We can recover with RYOW consistency from the persisted
 * index table similarly to original functionality. But if we store the progress in all independent
 * streams we should be able to rerun the merging and idempotency shoud ensure we don't emit
 * duplicates. If part of the batch is not perceived as complete due to eventual
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
private class StreamMergerActor(
  val config: CassandraJournalConfig,
  session: Session) extends Actor with CassandraStatements {

  import config._

  private[this] case object Continue

  val serialization = SerializationExtension(context.system)

  private[this] val refreshInterval = FiniteDuration(1, SECONDS)

  private[this] val tickTask =
    context.system.scheduler.schedule(refreshInterval, refreshInterval, self, Continue)(context.dispatcher)

  override def postStop(): Unit = {
    tickTask.cancel()
    super.postStop()
  }

  private[this] val preparedSelectDistinctJournalId =
    session.prepare(selectDistinctJournalId).setConsistencyLevel(readConsistency)

  override def receive: Receive = merging(initialJournalIdProgress, initialPersistenceIdProgress, 50l)

  private[this] def merging(
    journalIdProgress: Progress[JournalId],
    persistenceIdProgress: Progress[PersistenceId],
    step: Long): Receive = {

    case Continue =>
      val currentJournalIds = journalIds()
      val updatedProgress = currentJournalIds
        .map(journalId => (JournalId(journalId), journalIdProgress.getOrElse(JournalId(journalId), -1l)))
        .toMap

      val independentStreams =
        updatedProgress
          .map{ progress =>
          Stream(
            progress._1,
            new JournalEntryIterator(
              progress._1.id,
              progress._2 + 1,
              progress._2 + step + 1,
              targetPartitionSize,
              Long.MaxValue)(session, config))
          }
          .toSeq

      val mergeResult = merge(updatedProgress, persistenceIdProgress, independentStreams)

      /**
       * We now have a merged stream with the desired attributes or a stream that failed to merge.
       * So the storage approach can be applied.
       * 1) We can store the stream as a whole only if it merges cleanly.
       * 2) We can store each element independently/part of the stream and in case of failure
       *   preserve the state (e.g. elements that were not merged, elements that were merged etc.)
       */
      val nextState =
        mergeResult match {
          case MergeFailure(_, _, _, _) =>
            merging(journalIdProgress, persistenceIdProgress, step + 50l)
          case MergeSuccess(newJournalIdProgress, newPersistenceIdProgress, mergedStream) =>
            merging(newJournalIdProgress, newPersistenceIdProgress, step)
        }

      context.become(nextState)
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