package akka.persistence.cassandra.journal

trait CassandraStatements {

  def config: CassandraJournalConfig

  def createKeyspace = s"""
      CREATE KEYSPACE IF NOT EXISTS ${config.keyspace}
      WITH REPLICATION = { 'class' : ${config.replicationStrategy} }
    """

  def createConfigTable = s"""
      CREATE TABLE IF NOT EXISTS ${configTableName} (
        property text primary key, value text)
     """

  // TODO: Fix cluster columns based on deletes/query requirements
  def createTable = s"""
      CREATE TABLE IF NOT EXISTS ${tableName} (
        used boolean static,
        journal_id text,
        journal_sequence_nr bigint,
        partition_nr bigint,
        persistence_id text,
        sequence_nr bigint,
        message blob,
        PRIMARY KEY ((journal_id, partition_nr), journal_sequence_nr, persistence_id, sequence_nr))
        WITH gc_grace_seconds =${config.gc_grace_seconds}
    """

  def createMetatdataTable = s"""
      CREATE TABLE IF NOT EXISTS ${metadataTableName}(
        persistence_id text PRIMARY KEY,
        deleted_to bigint,
        properties map<text,text>
      );
   """

  def writeMessage = s"""
      INSERT INTO ${tableName} (journal_id, partition_nr, journal_sequence_nr, persistence_id, sequence_nr, message, used)
      VALUES (?, ?, ?, ?, ?, ?, true)
    """

  def selectConfig = s"""
      SELECT * FROM ${configTableName}
    """

  def writeConfig = s"""
      INSERT INTO ${configTableName}(property, value) VALUES(?, ?)
    """

  def writeInUse =
    s"""
       INSERT INTO ${tableName} (journal_id, partition_nr, used)
       VALUES(?, ?, true)
     """

  private def tableName = s"${config.keyspace}.${config.table}"
  private def configTableName = s"${config.keyspace}.${config.configTable}"
  private def metadataTableName = s"${config.keyspace}.${config.metadataTable}"
}
