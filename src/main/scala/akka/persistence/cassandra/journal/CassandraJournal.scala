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

import scala.util.{Success, Failure, Try}

class CassandraJournal extends AsyncWriteJournal with CassandraRecovery with CassandraStatements {
  val config = new CassandraJournalConfig(
    context.system.settings.config.getConfig("cassandra-journal").withFallback(
      context.system.settings.config.getConfig("akka.persistence.journal-plugin-fallback")))
  val serialization = SerializationExtension(context.system)

  import config._

  val cluster = clusterBuilder.build
  val session = cluster.connect()

  case class MessageId(persistenceId: String, sequenceNr: Long)

  if (config.keyspaceAutoCreate) {
    retry(config.keyspaceAutoCreateRetries) {
      session.execute(createKeyspace)
    }
  }
  session.execute(createTable)

  val preparedWriteMessage = session.prepare(writeMessage)
  val preparedDeletePermanent = session.prepare(deleteMessage)
  val preparedSelectMessages = session.prepare(selectMessages).setConsistencyLevel(readConsistency)
  val preparedHighestSequenceNr = session.prepare(preparedHighestSequenceNrMessage)

  def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    val groupedStatements = messages.map(statementGroup)
    val batchStatements = groupedStatements.map({
      case Success(atomicWrite) =>
        executeBatch(batch => atomicWrite.foreach(batch.add)).map(_ => Success(()))
      case Failure(e) =>
        Future.successful(Failure[Unit](e))
    })

    Future.sequence(batchStatements)
  }

  private def statementGroup(atomicWrite: AtomicWrite): Try[Seq[BoundStatement]] = Try {
    atomicWrite.payload.flatMap { m =>
      Seq(preparedWriteMessage.bind(m.persistenceId, m.sequenceNr: JLong, persistentToByteBuffer(m)))
    }
  }

  private def asyncDeleteMessages(messageIds: Seq[MessageId]): Future[Unit] = executeBatch { batch =>
    messageIds.foreach { mid =>
      val stmt =
        preparedDeletePermanent.bind(mid.persistenceId, mid.sequenceNr: JLong)
      batch.add(stmt)
    }
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val fromSequenceNr = readLowestSequenceNr(persistenceId, 1)
    val asyncDeletions = (fromSequenceNr to toSequenceNr).grouped(maxMessageBatchSize).map { group =>
      asyncDeleteMessages(group map (MessageId(persistenceId, _)))
    }
    Future.sequence(asyncDeletions).map(_ => ())
  }

  private def executeBatch(body: BatchStatement ⇒ Unit): Future[Unit] = {
    val batch = new BatchStatement().setConsistencyLevel(writeConsistency).asInstanceOf[BatchStatement]
    body(batch)
    session.executeAsync(batch).map(_ => ())
  }

  private def persistentToByteBuffer(p: PersistentRepr): ByteBuffer =
    ByteBuffer.wrap(serialization.serialize(p).get)

  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
  }

  override def postStop(): Unit = {
    session.close()
    cluster.close()
  }
}
