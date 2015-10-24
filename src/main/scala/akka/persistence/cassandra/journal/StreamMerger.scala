package akka.persistence.cassandra.journal

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

import akka.actor.Actor
import akka.persistence.PersistentRepr

class StreamMerger(val config: CassandraJournalConfig) extends Actor with CassandraStatements {

  private[this] case object Continue

  val refreshInterval = FiniteDuration(1, SECONDS)

  private[this] val tickTask =
    context.system.scheduler.schedule(refreshInterval, refreshInterval, self, Continue)(context.dispatcher)

  override def receive: Receive = {
    case Continue => println("Continue")
  }

  def merge(progressPointer: Map[String, Long]): Receive = {
    case Continue => {

    }
  }

  def merge(streams: Seq[Iterator[PersistentRepr]]): Seq[PersistentRepr] = {
    Seq()
  }
}
