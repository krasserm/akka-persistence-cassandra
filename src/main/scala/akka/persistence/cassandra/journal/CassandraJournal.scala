package akka.persistence.cassandra.journal

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer

import scala.collection.immutable.Seq
import scala.concurrent._

import akka.persistence.journal.AsyncWriteJournal
import akka.persistence._
import akka.persistence.cassandra._
import akka.serialization.SerializationExtension

import com.datastax.driver.core._
import com.datastax.driver.core.utils.Bytes

class CassandraJournal extends AsyncWriteJournal with CassandraRecovery with CassandraStatements {
  val config = new CassandraJournalConfig(context.system.settings.config.getConfig("cassandra-journal"))
  val serialization = SerializationExtension(context.system)
  val persistence = Persistence(context.system)

  import config._

  val cluster = clusterBuilder.build
  val session = cluster.connect()

  if (config.keyspaceAutoCreate) session.execute(createKeyspace)
  session.execute(createTable)

  val preparedWriteHeader = session.prepare(writeHeader)
  val preparedWriteMessage = session.prepare(writeMessage)
  val preparedConfirmMessage = session.prepare(confirmMessage)
  val preparedDeleteLogical = session.prepare(deleteMessageLogical)
  val preparedDeletePermanent = session.prepare(deleteMessagePermanent)
  val preparedSelectHeader = session.prepare(selectHeader).setConsistencyLevel(readConsistency)
  val preparedSelectMessages = session.prepare(selectMessages).setConsistencyLevel(readConsistency)

  def asyncWriteMessages(messages: Seq[PersistentRepr]): Future[Unit] = executeBatch { batch =>
    messages.foreach { m =>
      val pnr = partitionNr(m.sequenceNr)
      if (partitionNew(m.sequenceNr)) batch.add(preparedWriteHeader.bind(m.persistenceId, pnr: JLong))
      batch.add(preparedWriteMessage.bind(m.persistenceId, pnr: JLong, m.sequenceNr: JLong, persistentToByteBuffer(m)))
    }
  }

  def asyncDeleteMessages(messageIds: Seq[PersistentId], permanent: Boolean): Future[Unit] = executeBatch { batch =>
    messageIds.foreach { mid =>
      val stmt =
        if (permanent) preparedDeletePermanent.bind(mid.persistenceId, partitionNr(mid.sequenceNr): JLong, mid.sequenceNr: JLong)
        else preparedDeleteLogical.bind(mid.persistenceId, partitionNr(mid.sequenceNr): JLong, mid.sequenceNr: JLong)
      batch.add(stmt)
    }
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long, permanent: Boolean): Future[Unit] = {
    val fromSequenceNr = readLowestSequenceNr(persistenceId, 1L)
    val asyncDeletions = (fromSequenceNr to toSequenceNr).grouped(persistence.settings.journal.maxDeletionBatchSize).map { group =>
      asyncDeleteMessages(group map (PersistentIdImpl(persistenceId, _)), permanent)
    }
    Future.sequence(asyncDeletions).map(_ => ())
  }

  def executeBatch(body: BatchStatement ⇒ Unit): Future[Unit] = {
    val batch = new BatchStatement().setConsistencyLevel(writeConsistency).asInstanceOf[BatchStatement]
    body(batch)
    session.executeAsync(batch).map(_ => ())
  }

  def partitionNr(sequenceNr: Long): Long =
    (sequenceNr - 1L) / maxPartitionSize

  def partitionNew(sequenceNr: Long): Boolean =
    (sequenceNr - 1L) % maxPartitionSize == 0L

  def persistentToByteBuffer(p: PersistentRepr): ByteBuffer =
    ByteBuffer.wrap(serialization.serialize(p).get)

  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
  }

  override def postStop(): Unit = {
    session.close()
    cluster.close()
  }
}
