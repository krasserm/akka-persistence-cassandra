package akka.persistence.cassandra.query.journal

import akka.persistence.PersistentActor
import akka.actor.Props

object TestActor {
  def props(persistenceId: String): Props =
    Props(new TestActor(persistenceId))
}

class TestActor(override val persistenceId: String) extends PersistentActor {

  val receiveRecover: Receive = {
    case evt: String ⇒
  }

  val receiveCommand: Receive = {
    case cmd: String ⇒
      persist(cmd) { evt ⇒
        sender() ! evt + "-done"
      }
  }
}
