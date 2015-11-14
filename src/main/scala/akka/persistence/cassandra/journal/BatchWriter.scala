package akka.persistence.cassandra.journal


import scala.concurrent._

import scala.util.{Failure, Success, Try}

import akka.persistence.cassandra._
import com.datastax.driver.core.policies.LoggingRetryPolicy
import com.datastax.driver.core.{Session, BatchStatement, BoundStatement}

trait BatchWriter {

  def config: CassandraJournalConfig
  def session: Session

  implicit def executionContext: ExecutionContext

  def writeBatch[G, T](
      data: Map[G, Seq[T]],
      firstInBatch: ((G, Seq[T])) => Long,
      lastInBatch: ((G, Seq[T])) => Long,
      boundStatement: (T, Long) => BoundStatement,
      partitionKey: ((G, Seq[T])) => String,
      writeInUse: Option[(String, Long) => BoundStatement]): Unit = {

    val boundStatements =
      data.map(x => statementGroup(x, firstInBatch, lastInBatch, boundStatement, partitionKey, writeInUse))

    val batchStatements = boundStatements.map({ unit =>
      executeBatch(batch => unit.foreach(batch.add))
    })
    val promise = Promise[Seq[Try[Unit]]]()

    Future.sequence(batchStatements).onComplete {
      case Success(_) => promise.complete(Success(Seq()))
      case Failure(e) => promise.failure(e)
    }

    promise.future
  }

  private[this] def statementGroup[G, T](
      atomicWrites: (G, Seq[T]),
      firstInBatch: ((G, Seq[T])) => Long,
      lastInBatch: ((G, Seq[T])) => Long,
      boundStatement: (T, Long) => BoundStatement,
      partitionKey: ((G, Seq[T])) => String,
      writeInUse: Option[(String, Long) => BoundStatement]): Seq[BoundStatement] = {

    val firstSequenceNr = firstInBatch(atomicWrites)
    val lastSequenceNr = lastInBatch(atomicWrites)

    val maxPnr = partitionNr(firstSequenceNr)
    val minPnr = partitionNr(lastSequenceNr)
    val persistenceId: String = partitionKey(atomicWrites)
    val all = atomicWrites._2

    // reading assumes sequence numbers are in the right partition or partition + 1
    // even if we did allow this it would perform terribly as large C* batches are not good
    require(maxPnr - minPnr <= 1, "Do not support AtomicWrites that span 3 partitions. Keep AtomicWrites <= max partition size.")

    val writes: Seq[BoundStatement] = all.map(boundStatement(_, maxPnr))

    // in case we skip an entire partition we want to make sure the empty partition has in in-use flag so scans
    // keep going when they encounter it
    writeInUse match {
      case Some(write) =>
        if (partitionNew(firstSequenceNr) && minPnr != maxPnr) writes :+ write(persistenceId, minPnr)
        else writes
      case None =>
        writes
    }
  }

  private[this] def executeBatch(body: BatchStatement ⇒ Unit, retries: Option[Int] = None): Future[Unit] = {
    val batch = new BatchStatement().setConsistencyLevel(config.writeConsistency).asInstanceOf[BatchStatement]
    retries.foreach(times => batch.setRetryPolicy(new LoggingRetryPolicy(new FixedRetryPolicy(times))))
    body(batch)
    session.executeAsync(batch).map(_ => ())
  }

  private[this] def partitionNr(sequenceNr: Long): Long =
    (sequenceNr - 1L) / config.targetPartitionSize

  private[this] def partitionNew(sequenceNr: Long): Boolean =
    (sequenceNr - 1L) % config.targetPartitionSize == 0L

}
