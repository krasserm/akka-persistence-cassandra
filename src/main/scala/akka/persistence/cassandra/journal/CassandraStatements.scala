package akka.persistence.cassandra.journal

trait CassandraStatements {
  def config: CassandraJournalConfig

  def createKeyspace = s"""
      CREATE KEYSPACE IF NOT EXISTS ${config.keyspace}
      WITH REPLICATION = { 'class' : ${config.replicationStrategy} }
    """

  def createTable = s"""
      CREATE TABLE IF NOT EXISTS ${tableName} (
        persistence_id text,
        sequence_nr bigint,
        message blob,
        PRIMARY KEY (persistence_id, sequence_nr))
        WITH COMPACT STORAGE
         AND gc_grace_seconds =${config.gc_grace_seconds}
    """

  def writeMessage = s"""
      INSERT INTO ${tableName} (persistence_id, sequence_nr, message)
      VALUES (?, ?, ?)
    """

  def deleteMessage = s"""
      DELETE FROM ${tableName} WHERE
        persistence_id = ? AND
        sequence_nr = ?
    """

  def selectMessages = s"""
      SELECT * FROM ${tableName} WHERE
        persistence_id = ? AND
        sequence_nr >= ? AND
        sequence_nr <= ?
        LIMIT ${config.maxResultSize}
    """

  def preparedLowestSequenceNrMessage = s"""
      SELECT * FROM ${tableName} WHERE
        persistence_id = ? AND
        sequence_nr >= ?
        ORDER BY sequence_nr
        LIMIT 1
     """

  def preparedHighestSequenceNrMessage = s"""
      SELECT * FROM ${tableName} WHERE
        persistence_id = ? AND
        sequence_nr >= ?
        ORDER BY sequence_nr DESC
        LIMIT 1
     """

  private def tableName = s"${config.keyspace}.${config.table}"
}
