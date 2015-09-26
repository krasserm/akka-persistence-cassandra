package akka.persistence.cassandra.query.journal

/**
 * Cassandra backed store of all PersistenceIds in the journal.
 * Keeps track of all the currently known PersistenceIds
 * to know what to deliver to subscribers.
 */
private[journal] trait BufferOperations[T, S] {
  def updateBuffer(buf: Vector[T], newBuf: Vector[T], state: S): (Vector[T], S)
}