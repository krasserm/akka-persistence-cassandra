package akka.persistence.cassandra.journal

import akka.actor._
import akka.persistence._
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.TestConfig
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.scalatest._

import scala.concurrent.duration._
import scala.language.postfixOps

object CassandraLoadSpec {
  val config = ConfigFactory.parseString(
    """
      |cassandra-journal.replication-strategy = NetworkTopologyStrategy
      |cassandra-journal.data-center-replication-factors = ["dc1:1"]
    """.stripMargin).withFallback(TestConfig.config)

  trait Measure extends { this: Actor ⇒
    val NanoToSecond = 1000.0 * 1000 * 1000

    var startTime: Long = 0L
    var stopTime: Long = 0L

    var startSequenceNr = 0L
    var stopSequenceNr = 0L

    def startMeasure(): Unit = {
      startSequenceNr = lastSequenceNr
      startTime = System.nanoTime
    }

    def stopMeasure(): Unit = {
      stopSequenceNr = lastSequenceNr
      stopTime = System.nanoTime
      sender ! (NanoToSecond * (stopSequenceNr - startSequenceNr) / (stopTime - startTime))
    }

    def lastSequenceNr: Long
  }

  class ProcessorA(val persistenceId: String) extends PersistentActor with Measure {
    def receiveRecover: Receive = handle

    def receiveCommand: Receive = {
      case c @ "start" =>
        deferAsync(c) { _ => startMeasure(); sender ! "started" }
      case c @ "stop" =>
        deferAsync(c) { _ => stopMeasure() }
      case payload: String =>
        persistAsync(payload)(handle)
    }

    def handle: Receive = {
      case payload: String =>
    }
  }

  class ProcessorB(val persistenceId: String, replyTo: ActorRef) extends PersistentActor {
    def receiveRecover: Receive = handle

    def receiveCommand: Receive = {
      case payload: String => persistAsync(payload)(handle)
    }

    def handle: Receive = {
      case payload: String =>
        replyTo ! s"${payload}-${lastSequenceNr}"
    }
  }
}

import akka.persistence.cassandra.journal.CassandraLoadSpec._

class CassandraLoadSpec extends TestKit(ActorSystem("test", config)) with ImplicitSender with WordSpecLike with Matchers with CassandraLifecycle {
  "A Cassandra journal" should {
    "have some reasonable write throughput" in {
      val warmCycles = 100L
      val loadCycles = 1000L

      val processor1 = system.actorOf(Props(classOf[ProcessorA], "p1a"))
      1L to warmCycles foreach { i => processor1 ! "a" }
      processor1 ! "start"
      expectMsg("started")
      1L to loadCycles foreach { i => processor1 ! "a" }
      processor1 ! "stop"
      expectMsgPF(100 seconds) { case throughput: Double ⇒ println(f"\nthroughput = $throughput%.2f persistent commands per second") }
    }
    "work properly under load" in {
      val cycles = 1000L
      val listener1 = TestProbe()
      val listener2 = TestProbe()

      val processor1 = system.actorOf(Props(classOf[ProcessorB], "p1b", listener1.ref))
      1L to cycles foreach { i => processor1 ! "a" }
      1L to cycles foreach { i => listener1.expectMsg(s"a-${i}") }

      val processor2 = system.actorOf(Props(classOf[ProcessorB], "p1b", listener2.ref))
      1L to cycles foreach { i => listener2.expectMsg(s"a-${i}") }

      processor2 ! "b"
      listener2.expectMsg(s"b-${cycles + 1L}")
    }
  }
}
