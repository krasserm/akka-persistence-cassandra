package akka.persistence.cassandra.journal

import com.typesafe.config.Config

class CassandraJournalConfig(config: Config) extends CassandraWriteJournalConfig(config) {
  val replayDispatcherId: String = config.getString("replay-dispatcher")
  val targetPartitionSize: Int = config.getInt(CassandraJournalConfig.TargetPartitionProperty)
  val maxResultSize: Int = config.getInt("max-result-size")
  val gc_grace_seconds: Long = config.getLong("gc-grace-seconds")
  val maxMessageBatchSize = config.getInt("max-message-batch-size")
  val deleteRetries: Int = config.getInt("delete-retries")
}

object CassandraJournalConfig {
  val TargetPartitionProperty: String = "target-partition-size"
}
