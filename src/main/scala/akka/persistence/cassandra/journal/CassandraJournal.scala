package akka.persistence.cassandra.journal

import java.lang.{Long => JLong}
import java.nio.ByteBuffer

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.util.{Success, Try}

import akka.cluster.Cluster
import akka.cluster.singleton.{ClusterSingletonManagerSettings, ClusterSingletonManager}
import akka.cluster.singleton.ClusterSingletonManager.Internal.End
import akka.persistence._
import akka.persistence.cassandra._
import akka.persistence.journal.AsyncWriteJournal
import akka.serialization.SerializationExtension
import com.datastax.driver.core._
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.core.policies.RetryPolicy

class CassandraJournal
  extends AsyncWriteJournal
  with CassandraRecovery
  with CassandraConfigChecker
  with CassandraStatements
  with BatchWriter {

  override lazy val executionContext = replayDispatcher
  val config =
    new CassandraJournalConfig(context.system.settings.config.getConfig("cassandra-journal"))

  import config._

  val serialization = SerializationExtension(context.system)

  private[this] var journalSequenceNr = 0L

  val cluster = ClusterBuilder.cluster(config)
  val session = cluster.connect()

  case class MessageId(persistenceId: String, sequenceNr: Long)

  if (config.keyspaceAutoCreate) {
    retry(config.keyspaceAutoCreateRetries) {
      session.execute(createKeyspace)
    }
  }

  session.execute(createTable)
  session.execute(createMetatdataTable)
  session.execute(createConfigTable)

  val persistentConfig: Map[String, String] = initializePersistentConfig

  // TODO: Figure out how to sensibly run the merging process.
  Cluster(context.system).join(Cluster(context.system).selfAddress)

  val merger = context.system.actorOf(ClusterSingletonManager.props(
    StreamMergerActor.props(config, session),
    End,
    ClusterSingletonManagerSettings(context.system)),
    StreamMergerActor.name)

  val preparedWriteMessage = session.prepare(writeMessage)
  val preparedWriteInUse = session.prepare(writeInUse)

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    // we need to preserve the order / size of this sequence even though we don't map
    // AtomicWrites 1:1 with a C* insert

    val flattenedMessages = messages.flatMap(_.payload)

    val newJournalSequenceNr = journalSequenceNr + flattenedMessages.size

    val serialized = (journalSequenceNr to newJournalSequenceNr)
      .zip(flattenedMessages)
      .map(aw => Try {
        Serialized(aw._1, aw._2.sequenceNr, aw._2.persistenceId, persistentToByteBuffer(aw._2))
      })
    journalSequenceNr = newJournalSequenceNr

    val byPersistenceId = serialized
      .collect({ case Success(caw) => caw })

    val boundJournalEntries: (String, scala.Seq[Serialized]) => scala.Seq[BoundStatement] =
      (persistenceId, entries) => {

        val maxPnr = partitionNr(entries.last.journalSequenceNr)

        entries.map{ e =>
          preparedWriteMessage.bind(
            journalId,
            maxPnr: JLong,
            e.journalSequenceNr: JLong,
            e.persistenceId,
            e.sequenceNr: JLong,
            e.serialized)
        }
      }

    writeBatch[String, Serialized](
      Map(journalId -> byPersistenceId),
      boundJournalEntries,
      _.journalSequenceNr,
      (persistenceId, minPnr) => preparedWriteInUse.bind(journalId, minPnr: JLong))
      .map(_.to[scala.collection.immutable.Seq])
  }

  // TODO: FIX
  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    Future(())

  // TODO: FIX
  override def asyncReadHighestSequenceNr(
    persistenceId: String,
    fromSequenceNr: Long): Future[Long] = Future(0l)

  // TODO: FIX
  override def asyncReplayMessages(
    persistenceId: String,
    fromSequenceNr: Long,
    toSequenceNr: Long,
    max: Long)(recoveryCallback: (PersistentRepr) => Unit): Future[Unit] = Future(())

  private def minSequenceNr(partitionNr: Long): Long =
    partitionNr * targetPartitionSize + 1

  private def persistentToByteBuffer(p: PersistentRepr): ByteBuffer =
    ByteBuffer.wrap(serialization.serialize(p).get)

  override def postStop(): Unit = {
    session.close()
    cluster.close()
  }

  private case class SerializedAtomicWrite(persistenceId: String, payload: Seq[Serialized])
  private case class Serialized(journalSequenceNr: Long, sequenceNr: Long, persistenceId: String, serialized: ByteBuffer)
  private case class PartitionInfo(partitionNr: Long, minSequenceNr: Long, maxSequenceNr: Long)
}

class FixedRetryPolicy(number: Int) extends RetryPolicy {
  def onUnavailable(statement: Statement, cl: ConsistencyLevel, requiredReplica: Int, aliveReplica: Int, nbRetry: Int): RetryDecision = retry(cl, nbRetry)
  def onWriteTimeout(statement: Statement, cl: ConsistencyLevel, writeType: WriteType, requiredAcks: Int, receivedAcks: Int, nbRetry: Int): RetryDecision = retry(cl, nbRetry)
  def onReadTimeout(statement: Statement, cl: ConsistencyLevel, requiredResponses: Int, receivedResponses: Int, dataRetrieved: Boolean, nbRetry: Int): RetryDecision = retry(cl, nbRetry)

  private def retry(cl: ConsistencyLevel, nbRetry: Int): RetryDecision = {
    if (nbRetry < number) RetryDecision.retry(cl) else RetryDecision.rethrow()
  }
}
