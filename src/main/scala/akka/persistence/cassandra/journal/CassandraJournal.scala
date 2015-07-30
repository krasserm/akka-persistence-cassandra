package akka.persistence.cassandra.journal

import java.io.NotSerializableException
import java.lang.{Long => JLong}
import java.nio.ByteBuffer

import akka.persistence._
import akka.persistence.cassandra._
import akka.persistence.journal.AsyncWriteJournal
import akka.serialization.SerializationExtension
import com.datastax.driver.core._
import com.datastax.driver.core.utils.Bytes

import scala.collection.JavaConversions.asJavaIterable
import scala.collection.immutable.Seq
import scala.concurrent._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class CassandraJournal extends AsyncWriteJournal with CassandraRecovery with CassandraStatements {
  override val config = new CassandraJournalConfig(context.system.settings.config.getConfig("cassandra-journal"))
  val serialization = SerializationExtension(context.system)
  override lazy val persistence = Persistence(context.system)

  import config._

  val cluster = clusterBuilder.build
  val session = cluster.connect()

  if (config.keyspaceAutoCreate) {
    retry(config.keyspaceAutoCreateRetries) {
      session.execute(createKeyspace)
    }
  }
  session.execute(createTable)

  val preparedWriteHeader = session.prepare(writeHeader)
  val preparedWriteMessage = session.prepare(writeMessage)
  val preparedConfirmMessage = session.prepare(confirmMessage)
  val preparedDeleteLogical = session.prepare(deleteMessageLogical)
  val preparedDeletePermanent = session.prepare(deleteMessagePermanent)
  val preparedSelectHeader = session.prepare(selectHeader).setConsistencyLevel(readConsistency)
  val preparedSelectMessages = session.prepare(selectMessages).setConsistencyLevel(readConsistency)

  private def batchStatement() = new BatchStatement().setConsistencyLevel(writeConsistency).asInstanceOf[BatchStatement]

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {

    val batch = batchStatement()
    Future.traverse(messages) { m =>
      batch.clear()
      try {
        val preparedMessages = m.payload.flatMap { p =>
          val pnr = partitionNr(p.sequenceNr)
          val message = preparedWriteMessage.bind(p.persistenceId, pnr: JLong, p.sequenceNr: JLong, persistentToByteBuffer(p))

          if (partitionNew(p.sequenceNr))
            Seq(preparedWriteHeader.bind(p.persistenceId, pnr: JLong), message)
          else
            Seq(message)
        }

        batch.addAll(preparedMessages)
        session.executeAsync(batch).map(_ => Success(())).recover { case f => Failure(f) }
      } catch {
        case t: NotSerializableException => Future.successful(Failure(t))
        case e: Throwable => Future.failed(e)
      }
    }
  }

  override def asyncDeleteMessagesTo(processorId: String, toSequenceNr: Long): Future[Unit] = executeBatch { batch =>
    val fromSequenceNr = readLowestSequenceNr(processorId, 1L)
    (fromSequenceNr to toSequenceNr).grouped(persistence.settings.journal.maxDeletionBatchSize).foreach { group =>
        group.foreach { sequenceNr =>
          batch.add(preparedDeletePermanent.bind(processorId, partitionNr(sequenceNr): JLong, sequenceNr: JLong))
      }
    }
  }

  private def executeBatch(body: BatchStatement ⇒ Unit): Future[Unit] = {
    val batch = batchStatement()
    body(batch)
    session.executeAsync(batch).map(_ => ())
  }

  def partitionNr(sequenceNr: Long): Long =
    (sequenceNr - 1L) / maxPartitionSize

  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
  }

  override def postStop(): Unit = {
    session.close()
    cluster.close()
  }

  private def partitionNew(sequenceNr: Long): Boolean =
    (sequenceNr - 1L) % maxPartitionSize == 0L

  private def persistentToByteBuffer(p: PersistentRepr): ByteBuffer =
    ByteBuffer.wrap(serialization.serialize(p).get)

}
