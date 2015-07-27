package akka.persistence.cassandra.journal

import com.typesafe.config.Config

import akka.persistence.cassandra.CassandraPluginConfig

class CassandraJournalConfig(config: Config) extends CassandraPluginConfig(config) {
  val replayDispatcherId: String = config.getString("replay-dispatcher")
  val targetPartitionSize: Int = config.getInt("target-partition-size") // TODO: make persistent
  val maxResultSize: Int = config.getInt("max-result-size")
  val gc_grace_seconds: Long = config.getLong("gc-grace-seconds")
}
