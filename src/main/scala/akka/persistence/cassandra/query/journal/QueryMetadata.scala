package akka.persistence.cassandra.query.journal

import scala.concurrent.duration.FiniteDuration

import akka.persistence.query.Hint

import QueryMetadata._

//TODO: Do we want to use for materialized values?
//TODO: http://doc.akka.io/docs/akka/snapshot/scala/persistence-query.html#Materialized_values_of_queries
//TODO: It makes the use of streams less convenient as own Query's must be used.
case class QueryMetadata(
  hints: Seq[Hint],
  finality: Finality,
  bufferSize: Long,
  order: Order)

object QueryMetadata {
  sealed trait Finality
  case object Finite extends Finality
  case class Infinite(refreshInterval: FiniteDuration) extends Finality

  sealed trait Order
  case object Ordered extends Order

  def apply(hints: Seq[Hint], refreshInterval: Option[FiniteDuration], bufferSize: Long): QueryMetadata = {
    val finality = refreshInterval.fold[Finality](Finite)(Infinite)
    QueryMetadata(hints, finality, bufferSize, Ordered)
  }
}
