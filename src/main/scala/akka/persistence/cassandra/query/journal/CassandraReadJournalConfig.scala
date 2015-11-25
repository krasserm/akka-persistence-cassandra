package akka.persistence.cassandra.query.journal

import scala.concurrent.duration._

import com.typesafe.config.Config

import akka.persistence.cassandra.CassandraPluginConfig

class CassandraReadJournalConfig(config: Config) extends CassandraPluginConfig(config) {
  val refreshInterval: Option[FiniteDuration] = Some(config.getDuration("refresh-interval", MILLISECONDS).millis)
  val maxBufferSize: Long = config.getLong("max-buffer-size")
  val targetPartitionSize: Int = config.getInt("target-partition-size")
}
