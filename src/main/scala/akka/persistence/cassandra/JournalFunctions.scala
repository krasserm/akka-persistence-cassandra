package akka.persistence.cassandra

import java.nio.ByteBuffer

import akka.persistence.PersistentRepr
import akka.serialization.Serialization
import com.datastax.driver.core.utils.Bytes

object JournalFunctions {

  def partitionNr(sequenceNr: Long, targetPartitionSize: Int): Long =
    (sequenceNr - 1L) / targetPartitionSize

  def persistentToByteBuffer(serialization: Serialization, p: PersistentRepr): ByteBuffer =
    ByteBuffer.wrap(serialization.serialize(p).get)

  def persistentFromByteBuffer(serialization: Serialization, b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(Bytes.getArray(b), classOf[PersistentRepr]).get
  }
}
