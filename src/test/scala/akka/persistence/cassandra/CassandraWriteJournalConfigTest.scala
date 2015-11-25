package akka.persistence.cassandra

import com.typesafe.config.ConfigFactory
import org.scalatest.{MustMatchers, WordSpec}

import akka.persistence.cassandra.journal.CassandraWriteJournalConfig

class CassandraWriteJournalConfigTest extends WordSpec with MustMatchers{
  lazy val defaultConfig = ConfigFactory.parseString(
    """
      |keyspace-autocreate = true
      |keyspace-autocreate-retries = 1
      |keyspace = test-keyspace
      |connect-retries = 3
      |connect-retry-delay = 5s
      |table = test-table
      |table-compaction-strategy { class = "SizeTieredCompactionStrategy" }
      |metadata-table = test-metadata-table
      |config-table = config
      |replication-strategy = "SimpleStrategy"
      |replication-factor = 1
      |data-center-replication-factors = []
      |read-consistency = QUORUM
      |write-consistency = QUORUM
      |contact-points = ["127.0.0.1"]
      |port = 9142
      |max-result-size = 50
      |delete-retries = 4
    """.stripMargin)

  "parse config with SimpleStrategy as default for replication-strategy" in {
    val config = new CassandraWriteJournalConfig(defaultConfig)
    config.replicationStrategy must be("'SimpleStrategy','replication_factor':1")
  }

  "parse config with a list of datacenters configured for NetworkTopologyStrategy" in {
    lazy val configWithNetworkStrategy = ConfigFactory.parseString(
      """
        |replication-strategy = "NetworkTopologyStrategy"
        |data-center-replication-factors = ["dc1:3", "dc2:2"]
      """.stripMargin).withFallback(defaultConfig)
    val config = new CassandraWriteJournalConfig(configWithNetworkStrategy)
    config.replicationStrategy must be("'NetworkTopologyStrategy','dc1':3,'dc2':2")
  }

  "parse keyspace-autocreate parameter" in {
    val configWithFalseKeyspaceAutocreate = ConfigFactory.parseString( """keyspace-autocreate = false""").withFallback(defaultConfig)

    val config = new CassandraWriteJournalConfig(configWithFalseKeyspaceAutocreate)
    config.keyspaceAutoCreate must be(false)
  }
}
