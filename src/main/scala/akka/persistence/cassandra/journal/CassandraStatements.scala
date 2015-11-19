package akka.persistence.cassandra.journal

trait CassandraStatements {

  def config: CassandraJournalConfig

  def createKeyspace = s"""
      CREATE KEYSPACE IF NOT EXISTS ${config.keyspace}
      WITH REPLICATION = { 'class' : ${config.replicationStrategy} }
    """

  def createConfigTable = s"""
      CREATE TABLE IF NOT EXISTS ${configTableName} (
        property text primary key, value text
      )
     """

  def createTable = s"""
      CREATE TABLE IF NOT EXISTS ${tableName} (
        used boolean static,
        journal_id text,
        journal_sequence_nr bigint,
        partition_nr bigint,
        persistence_id text,
        sequence_nr bigint,
        message blob,
        PRIMARY KEY ((journal_id, partition_nr), journal_sequence_nr))
        WITH gc_grace_seconds =${config.gc_grace_seconds}
        AND compaction = ${config.tableCompactionStrategy.asCQL}
    """

  def createMetatdataTable = s"""
      CREATE TABLE IF NOT EXISTS ${metadataTableName}(
        persistence_id text PRIMARY KEY,
        deleted_to bigint,
        properties map<text,text>
      )
   """
  
  def createPersistenceIdProgressTable = s"""
      CREATE TABLE IF NOT EXISTS ${persistenceIdProgressTableName} (
        id int,
        persistence_id text,
        progress bigint,
        PRIMARY KEY (id, persistence_id)
      )
     """

  def selectPersistenceIdProgress = s"""
      SELECT * FROM ${persistenceIdProgressTableName}
    """

  def writePersistenceIdProgress = s"""
      INSERT INTO ${persistenceIdProgressTableName} (id, persistence_id, progress) VALUES (?, ?, ?)
    """

  def createJournalIdProgressTable = s"""
      CREATE TABLE IF NOT EXISTS ${journalIdProgressTableName} (
        id int,
        journal_id text,
        progress bigint,
        PRIMARY KEY (id, journal_id)
      )
    """

  def selectJournalIdProgress = s"""
      SELECT * FROM ${journalIdProgressTableName}
    """

  def writeJournalIdProgress = s"""
      INSERT INTO ${journalIdProgressTableName} (id, journal_id, progress) VALUES (?, ?, ?)
    """

  def writeMessage = s"""
      INSERT INTO ${tableName} (journal_id, partition_nr, journal_sequence_nr, persistence_id, sequence_nr, message, used)
      VALUES (?, ?, ?, ?, ?, ?, true)
    """

  def createEventsByPersistenceIdTable =
    s"""
      CREATE TABLE IF NOT EXISTS ${eventsByPersistenceIdTableName} (
        used boolean static,
        persistence_id text,
        partition_nr bigint,
        sequence_nr bigint,
        message blob,
        PRIMARY KEY ((persistence_id, partition_nr), sequence_nr))
        WITH gc_grace_seconds =${config.gc_grace_seconds}
        AND compaction = ${config.tableCompactionStrategy.asCQL}
     """.stripMargin

  def writeEventsByPersistenceId =
    s"""
      INSERT INTO ${eventsByPersistenceIdTableName} (persistence_id, partition_nr, sequence_nr, message, used)
      VALUES (?, ?, ?, ?, true)
     """

  def writeEventsByPersistenceIdInUse =
    s"""
       INSERT INTO ${eventsByPersistenceIdTableName} (persistence_id, partition_nr, used)
       VALUES(?, ?, true)
     """

  def selectMessages = s"""
      SELECT * FROM ${tableName} WHERE
        journal_id = ? AND
        partition_nr = ? AND
        journal_sequence_nr >= ? AND
        journal_sequence_nr <= ?
    """

  def selectInUse = s"""
     SELECT used from ${tableName} WHERE
      journal_id = ? AND
      partition_nr = ?
   """

  def selectDistinctJournalId = s"""
      SELECT DISTINCT journal_id, partition_nr FROM ${tableName}
    """

  def selectConfig = s"""
      SELECT * FROM ${configTableName}
    """

  def writeConfig = s"""
      INSERT INTO ${configTableName}(property, value) VALUES(?, ?) IF NOT EXISTS
    """

  def writeInUse =
    s"""
       INSERT INTO ${tableName} (journal_id, partition_nr, used)
       VALUES(?, ?, true)
     """

  private def eventsByPersistenceIdTableName =
    s"${config.keyspace}.${config.eventsByPersistenceIdTable}"
  private def persistenceIdProgressTableName =
    s"${config.keyspace}.${config.persistenceIdProgressTable}"
  private def journalIdProgressTableName =
    s"${config.keyspace}.${config.journalIdProgressTable}"
  private def tableName = s"${config.keyspace}.${config.table}"
  private def configTableName = s"${config.keyspace}.${config.configTable}"
  private def metadataTableName = s"${config.keyspace}.${config.metadataTable}"
}
