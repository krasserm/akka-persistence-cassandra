package akka.persistence.cassandra.query.journal

trait CassandraReadStatements {
  def config: CassandraReadJournalConfig

  def selectMessages = s"""
      SELECT * FROM ${tableName} WHERE
        persistence_id = ? AND
        partition_nr = ? AND
        sequence_nr >= ? AND
        sequence_nr <= ?
    """

  // TODO: THE BELOW WAS COPIED FROM CASSANDRASTATEMENTS

  def selectDeletedTo = s"""
      SELECT deleted_to FROM ${metadataTableName} WHERE
        persistence_id = ?
    """

  def selectInUse = s"""
     SELECT used from ${tableName} WHERE
      persistence_id = ? AND
      partition_nr = ?
   """

  private def tableName = s"${config.keyspace}.${config.table}"
  private def metadataTableName = s"${config.keyspace}.${config.metadataTable}"
}
