package akka.persistence.cassandra.journal

import akka.persistence.cassandra.CassandraPluginConfig
import akka.persistence.cassandra.CassandraPluginConfig._
import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.config.Config

import scala.collection.JavaConverters._

class CassandraWriteJournalConfig(config: Config) extends CassandraPluginConfig(config) {
  val configTable: String = validateTableName(config.getString("config-table"))
  val keyspaceAutoCreate: Boolean = config.getBoolean("keyspace-autocreate")
  val keyspaceAutoCreateRetries: Int = config.getInt("keyspace-autocreate-retries")
  val replicationStrategy: String = getReplicationStrategy(
    config.getString("replication-strategy"),
    config.getInt("replication-factor"),
    config.getStringList("data-center-replication-factors").asScala)
  val writeConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(config.getString("write-consistency"))
}
