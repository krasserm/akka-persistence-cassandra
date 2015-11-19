package akka.persistence.cassandra.journal

trait CassandraStatements {
  val config: CassandraJournalConfig

  def createKeyspace = s"""
      CREATE KEYSPACE IF NOT EXISTS ${config.keyspace}
      WITH REPLICATION = { 'class' : ${config.replicationStrategy} }
    """

  def createConfigTable = s"""
      CREATE TABLE IF NOT EXISTS ${configTableName} (
        property text primary key, value text)
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
        AND compaction = ${config.tableCompactionStrategy.asCQL}
    """

  def createTimeIndexTable = s"""
      CREATE TABLE IF NOT EXISTS ${timeIndexTableName} (
        year_month_day int,
        window_start timestamp,
        persistence_id text,
        first_sequence_nr_in_window bigint,
        partition_nr bigint,

        primary key (year_month_day, window_start, persistence_id, first_sequence_nr_in_window));
    """

  def createMetatdataTable = s"""
      CREATE TABLE IF NOT EXISTS ${metadataTableName}(
        persistence_id text PRIMARY KEY,
        deleted_to bigint,
        properties map<text,text>)
   """

  def writeMessage = s"""
      INSERT INTO ${tableName} (persistence_id, partition_nr, sequence_nr, message, used)
      VALUES (?, ?, ?, ?, true)
    """

  def writeMessageTimeIndex = s"""
      INSERT INTO ${timeIndexTableName} (year_month_day, window_start, first_sequence_nr_in_window, persistence_id, partition_nr)
      VALUES (?, ?, ?, ?, ?)
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
    """

  def selectInUse = s"""
     SELECT used from ${tableName} WHERE
      persistence_id = ? AND
      partition_nr = ?
   """
  def selectConfig = s"""
      SELECT * FROM ${configTableName}
    """

  def writeConfig = s"""
      INSERT INTO ${configTableName}(property, value) VALUES(?, ?) IF NOT EXISTS
    """

  def selectHighestSequenceNr = s"""
     SELECT sequence_nr, used FROM ${tableName} WHERE
       persistence_id = ? AND
       partition_nr = ?
       ORDER BY sequence_nr
       DESC LIMIT 1
   """

  def selectDeletedTo = s"""
      SELECT deleted_to FROM ${metadataTableName} WHERE
        persistence_id = ?
    """

  def insertDeletedTo = s"""
      INSERT INTO ${metadataTableName} (persistence_id, deleted_to)
      VALUES ( ?, ? )
    """

  def writeInUse =
    s"""
       INSERT INTO ${tableName} (persistence_id, partition_nr, used)
       VALUES(?, ?, true)
     """

  private def tableName = s"${config.keyspace}.${config.table}"
  private def configTableName = s"${config.keyspace}.${config.configTable}"
  private def metadataTableName = s"${config.keyspace}.${config.metadataTable}"
  private def timeIndexTableName = s"${config.keyspace}.${config.timeIndexTable}"
}
