package akka.persistence.cassandra.snapshot

import com.typesafe.config.Config

import akka.persistence.cassandra.journal.CassandraWriteJournalConfig

class CassandraSnapshotStoreConfig(config: Config) extends CassandraWriteJournalConfig(config) {
  val maxMetadataResultSize = config.getInt("max-metadata-result-size")
}
