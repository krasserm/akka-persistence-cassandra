package akka.persistence.cassandra.query.journal.javadsl

import akka.persistence.query.javadsl

import akka.persistence.cassandra.query.journal.scaladsl

class CassandraReadJournal(scaladslReadJournal: scaladsl.CassandraReadJournal) extends javadsl.ReadJournal {

}
