package akka.persistence.cassandra.journal

trait CassandraStatements {
  def config: CassandraJournalConfig

  def createKeyspace = s"""
      CREATE KEYSPACE IF NOT EXISTS ${config.keyspace}
      WITH REPLICATION = { 'class' : ${config.replicationStrategy} }
    """

  def createTable = s"""
      CREATE TABLE IF NOT EXISTS ${tableName} (
        used boolean static,
        persistence_id text,
        partition_nr bigint,
        sequence_nr bigint,
        message blob,
        PRIMARY KEY ((persistence_id, partition_nr), sequence_nr))
        WITH gc_grace_seconds =${config.gc_grace_seconds}
    """

  def writeMessage = s"""
      INSERT INTO ${tableName} (persistence_id, partition_nr, sequence_nr, message, used)
      VALUES (?, ?, ?, ?, true)
    """

  def confirmMessage = s"""
      INSERT INTO ${tableName} (persistence_id, partition_nr, sequence_nr, message)
      VALUES (?, ?, ?, 0x00)
    """

  def deleteMessage = s"""
      DELETE FROM ${tableName} WHERE
        persistence_id = ? AND
        partition_nr = ? AND
        sequence_nr = ?
    """

  def selectMessages = s"""
      SELECT * FROM ${tableName} WHERE
        persistence_id = ? AND
        partition_nr = ? AND
        sequence_nr >= ? AND
        sequence_nr <= ?
        LIMIT ${config.maxResultSize}
    """

  def selectInUse =
    s"""
       SELECT used from ${tableName}
       WHERE persistence_id = ? AND
       partition_nr = ?
     """

  def writeInUse =
    s"""
       INSERT INTO ${tableName} (persistence_id, partition_nr, used)
       VALUES(?, ?, true)
     """
  private def tableName = s"${config.keyspace}.${config.table}"
}
