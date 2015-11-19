package akka.persistence.cassandra.journal

import com.typesafe.config.Config
import scala.collection.JavaConverters._

import akka.persistence.cassandra.CassandraPluginConfig

class CassandraJournalConfig(config: Config) extends CassandraPluginConfig(config) {
  val replayDispatcherId: String = config.getString("replay-dispatcher")
  val targetPartitionSize: Int = config.getInt(CassandraJournalConfig.TargetPartitionProperty)
  val maxResultSize: Int = config.getInt("max-result-size")
  val gc_grace_seconds: Long = config.getLong("gc-grace-seconds")
  val maxMessageBatchSize = config.getInt("max-message-batch-size")
  val deleteRetries: Int = config.getInt("delete-retries")
  val timeIndexTable: String = config.getString("time-index-table")
  val timeWindowLength: Long = config.getLong("time-window-length")
}

object CassandraJournalConfig {
  val TargetPartitionProperty: String = "target-partition-size"
}
