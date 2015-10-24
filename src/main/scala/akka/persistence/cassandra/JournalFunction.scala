package akka.persistence.cassandra

object JournalFunction {

  def partitionNr(sequenceNr: Long, targetPartitionSize: Long): Long =
    (sequenceNr - 1L) / targetPartitionSize
}
