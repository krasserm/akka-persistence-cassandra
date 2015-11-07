package akka.persistence.cassandra.journal

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.control.Exception._

import akka.actor.{Props, Actor}
import akka.persistence.cassandra.journal.StreamMerger._
import akka.serialization.SerializationExtension
import com.datastax.driver.core.Session
import com.datastax.driver.core.exceptions.DriverException

object StreamMergerActor {
  def props(config: CassandraJournalConfig, session: Session): Props =
    Props(new StreamMergerActor(config, session))

  val name = "StreamMerger"
}

/**
 * Merges n independent physical streams into a single logical stream.
 * Preserves causal order per persistenceId.
 *
 * The intent of current functionality is to work in batches. The merger retrieves data from all
 * streams up to a maximum of configured step. Then creates a single logical stream out of the
 * n independent streams. There is no ordering between the independent streams and the only
 * requirement is to maintain causal order per persistenceId. When the whole batch can be merged
 * the stream is emitted. If not (e.g. unresolvable causality violation in given batch)
 * the merger returns that information and more data can be requested or another approach taken.
 *
 * The emitted logical stream can be stored into database in a batch, which grants atomicity,
 * It does not grant isolation, but in case of failure the stream merging can be replayed from
 * last known state. It may cause duplicated emission so the index table update must be idempotent.
 * Part of a batch may not be perceived as complete due to eventual consistency, but this can be
 * reconciled during replay. Such approach should achieve required causal consistency per key and
 * correctness during replay and failures.
 *
 * Other options were considered, e.g. storing each element independently. This would require a more
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
class StreamMergerActor(
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

  override def receive: Receive =
    merging(initialJournalIdProgress, initialPersistenceIdProgress, 50l)

  private[this] def merging(
    journalIdProgress: Progress[JournalId],
    persistenceIdProgress: Progress[PersistenceId],
    step: Long): Receive = {

    case Continue =>
      val currentJournalIds =
        catching(classOf[DriverException]).withTry(journalIds()).getOrElse(Seq())

      val updatedProgress = currentJournalIds
        .map{ journalId =>
          (JournalId(journalId), journalIdProgress.getOrElse(JournalId(journalId), -1l))
        }
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
       * An index table and progress update approach can be applied.
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