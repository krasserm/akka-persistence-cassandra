package akka.persistence.cassandra.journal

import java.lang.{Long => JLong}
import java.nio.ByteBuffer
import java.util.UUID

import scala.collection.JavaConversions._
import scala.collection.immutable.Seq
import scala.concurrent._
import scala.util.{Failure, Success, Try}

import akka.cluster.Cluster
import akka.cluster.singleton.{ClusterSingletonManagerSettings, ClusterSingletonManager}
import akka.cluster.singleton.ClusterSingletonManager.Internal.End
import akka.persistence._
import akka.persistence.cassandra._
import akka.persistence.journal.AsyncWriteJournal
import akka.serialization.SerializationExtension
import com.datastax.driver.core._
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.core.policies.{LoggingRetryPolicy, RetryPolicy}
import com.datastax.driver.core.utils.Bytes

class CassandraJournal extends AsyncWriteJournal with CassandraRecovery with CassandraConfigChecker with CassandraStatements {

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
  /*Cluster(context.system).join(Cluster(context.system).selfAddress)

  val merger = context.system.actorOf(ClusterSingletonManager.props(
    StreamMergerActor.props(config, session),
    End,
    ClusterSingletonManagerSettings(context.system)),
    StreamMergerActor.name)*/

  val preparedWriteMessage = session.prepare(writeMessage)
  val preparedWriteInUse = session.prepare(writeInUse)

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    // we need to preserve the order / size of this sequence even though we don't map
    // AtomicWrites 1:1 with a C* insert
    val newJournalSequenceNr = journalSequenceNr + messages.size

    val serialized = (journalSequenceNr to newJournalSequenceNr)
      .zip(messages)
      .map(aw => Try { SerializedAtomicWrite(
        aw._2.payload.head.persistenceId,
        aw._2.payload.map(pr => Serialized(aw._1, pr.sequenceNr, persistentToByteBuffer(pr))))
      })
    journalSequenceNr = newJournalSequenceNr

    val result = serialized.map(a => a.map(_ => ()))

    val byPersistenceId = serialized.collect({ case Success(caw) => caw }).groupBy(_.persistenceId).values
    val boundStatements = byPersistenceId.map(statementGroup)

    val batchStatements = boundStatements.map({ unit =>
      executeBatch(batch => unit.foreach(batch.add))
    })
    val promise = Promise[Seq[Try[Unit]]]()

    Future.sequence(batchStatements).onComplete {
      case Success(_) => promise.complete(Success(result))
      case Failure(e) => promise.failure(e)
    }

    promise.future
  }

  private def statementGroup(atomicWrites: Seq[SerializedAtomicWrite]): Seq[BoundStatement] = {
    val firstJournalSequenceNr = atomicWrites.head.payload.head.journalSequenceNr
    val lastJournalSequenceNr = atomicWrites.last.payload.last.journalSequenceNr

    val maxPnr = partitionNr(firstJournalSequenceNr)
    val minPnr = partitionNr(lastJournalSequenceNr)
    val persistenceId: String = atomicWrites.head.persistenceId
    val all = atomicWrites.flatMap(_.payload)

    // reading assumes sequence numbers are in the right partition or partition + 1
    // even if we did allow this it would perform terribly as large C* batches are not good
    require(maxPnr - minPnr <= 1, "Do not support AtomicWrites that span 3 partitions. Keep AtomicWrites <= max partition size.")

    val writes: Seq[BoundStatement] = all.map { m =>
      preparedWriteMessage.bind(journalId, maxPnr: JLong, m.journalSequenceNr: JLong, persistenceId, m.sequenceNr: JLong, m.serialized)
    }
    // in case we skip an entire partition we want to make sure the empty partition has in in-use flag so scans
    // keep going when they encounter it
    if (partitionNew(firstJournalSequenceNr) && minPnr != maxPnr) writes :+ preparedWriteInUse.bind(journalId, minPnr: JLong)
    else writes
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

  private def executeBatch(body: BatchStatement ⇒ Unit, retries: Option[Int] = None): Future[Unit] = {
    val batch = new BatchStatement().setConsistencyLevel(writeConsistency).asInstanceOf[BatchStatement]
    retries.foreach(times => batch.setRetryPolicy(new LoggingRetryPolicy(new FixedRetryPolicy(times))))
    body(batch)
    session.executeAsync(batch).map(_ => ())
  }

  def partitionNr(sequenceNr: Long): Long =
    (sequenceNr - 1L) / targetPartitionSize

  private def partitionNew(sequenceNr: Long): Boolean =
    (sequenceNr - 1L) % targetPartitionSize == 0L

  private def minSequenceNr(partitionNr: Long): Long =
    partitionNr * targetPartitionSize + 1

  private def persistentToByteBuffer(p: PersistentRepr): ByteBuffer =
    ByteBuffer.wrap(serialization.serialize(p).get)

  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
  }

  override def postStop(): Unit = {
    session.close()
    cluster.close()
  }

  private case class SerializedAtomicWrite(persistenceId: String, payload: Seq[Serialized])
  private case class Serialized(journalSequenceNr: Long, sequenceNr: Long, serialized: ByteBuffer)
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

