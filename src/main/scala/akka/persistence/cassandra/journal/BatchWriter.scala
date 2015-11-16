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

  def writeBatchSingle[G, T](data: (G, Seq[T]), boundStatement: (G, Seq[T]) => Seq[BoundStatement]) =
    executeBatches(Seq(boundStatement(data._1, data._2)))

  def writeBatch[G, T](
      data: Map[G, Seq[T]],
      boundStatement: (G, Seq[T]) => Seq[BoundStatement],
      sequenceNr: T => Long,
      writeInUse: (G, Long) => BoundStatement): Future[Seq[Try[Unit]]] = {

    val boundStatements = data.map(statementGroup(_, boundStatement, sequenceNr, writeInUse))

    executeBatches(boundStatements)
  }

  private[this] def statementGroup[G, T](
      atomicWrites: (G, Seq[T]),
      boundStatement: (G, Seq[T]) => Seq[BoundStatement],
      sequenceNr: T => Long,
      writeInUse: (G, Long) => BoundStatement): Seq[BoundStatement] = {

    val firstSequenceNr = sequenceNr(atomicWrites._2.head)
    val lastSequenceNr = sequenceNr(atomicWrites._2.last)

    val maxPnr = partitionNr(lastSequenceNr)
    val minPnr = partitionNr(firstSequenceNr)

    // reading assumes sequence numbers are in the right partition or partition + 1
    // even if we did allow this it would perform terribly as large C* batches are not good
    require(maxPnr - minPnr <= 1,
      "Do not support AtomicWrites that span 3 partitions. Keep AtomicWrites <= max partition size.")

    val writes: Seq[BoundStatement] = boundStatement(atomicWrites._1, atomicWrites._2)

    // in case we skip an entire partition we want to make sure the empty partition has in in-use flag so scans
    // keep going when they encounter it
    if (partitionNew(firstSequenceNr) && minPnr != maxPnr) writes :+ writeInUse(atomicWrites._1, minPnr)
    else writes
  }

  private[this] def executeBatches(
      boundStatements: Iterable[Seq[BoundStatement]]): Future[Seq[Try[Unit]]] = {

    val result = boundStatements.flatten.toSeq.map(_ => Try(()))

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

  private[this] def executeBatch(body: BatchStatement ⇒ Unit, retries: Option[Int] = None): Future[Unit] = {
    val batch = new BatchStatement().setConsistencyLevel(config.writeConsistency).asInstanceOf[BatchStatement]
    retries.foreach(times => batch.setRetryPolicy(new LoggingRetryPolicy(new FixedRetryPolicy(times))))
    body(batch)
    session.executeAsync(batch).map(_ => ())
  }

  def partitionNr(sequenceNr: Long): Long =
    (sequenceNr - 1L) / config.targetPartitionSize

  private[this] def partitionNew(sequenceNr: Long): Boolean =
    (sequenceNr - 1L) % config.targetPartitionSize == 0L

}
