package akka.persistence.cassandra.query.journal

import scala.concurrent.duration._

import akka.persistence.cassandra.CassandraPluginConfig
import com.typesafe.config.Config

class CassandraReadJournalConfig(config: Config) extends CassandraPluginConfig(config) {

  val refreshInterval: Option[FiniteDuration] = Some(config.getDuration("refresh-interval", MILLISECONDS).millis)
  val maxBufferSize: Long = config.getLong("max-buffer-size")

  //TODO: Duplicated in CassandraJournalConfig
  val targetPartitionSize: Int = config.getInt("target-partition-size")
}
