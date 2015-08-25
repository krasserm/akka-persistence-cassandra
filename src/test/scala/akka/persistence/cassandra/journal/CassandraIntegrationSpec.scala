package akka.persistence.cassandra.journal

import scala.concurrent.duration._

import akka.actor._
import akka.persistence._
import akka.persistence.cassandra.CassandraLifecycle
import akka.testkit._

import com.typesafe.config.ConfigFactory

import org.scalatest._

object CassandraIntegrationSpec {
  val config = ConfigFactory.parseString(
    """
      |akka.persistence.snapshot-store.plugin = "cassandra-snapshot-store"
      |akka.persistence.journal.plugin = "cassandra-journal"
      |akka.persistence.journal.max-deletion-batch-size = 3
      |akka.persistence.publish-confirmations = on
      |akka.persistence.publish-plugin-commands = on
      |akka.test.single-expect-default = 10s
      |cassandra-journal.max-partition-size = 5
      |cassandra-journal.max-result-size = 3
      |cassandra-journal.port = 9142
      |cassandra-snapshot-store.port = 9142
    """.stripMargin)

  case class DeleteTo(snr: Long)

  class ProcessorA(val persistenceId: String, receiver: ActorRef) extends PersistentActor {
    def receiveRecover: Receive = handle

    def receiveCommand: Receive = {
      case DeleteTo(sequenceNr) =>
        deleteMessages(sequenceNr)
      case payload: String =>
        persist(payload)(handle)
    }

    def handle: Receive = {
      case payload: String =>
        receiver ! payload
        receiver ! lastSequenceNr
        receiver ! recoveryRunning
    }
  }

  class ProcessorC(val persistenceId: String, probe: ActorRef) extends PersistentActor {
    var last: String = _

    def receiveRecover: Receive = {
      case SnapshotOffer(_, snapshot: String) =>
        last = snapshot
        probe ! s"offered-${last}"
      case payload: String =>
        handle(payload)
    }

    def receiveCommand: Receive = {
      case "snap" =>
        saveSnapshot(last)
      case SaveSnapshotSuccess(_) =>
        probe ! s"snapped-${last}"
      case payload: String =>
        persist(payload)(handle)
      case DeleteTo(sequenceNr) =>
        deleteMessages(sequenceNr)
    }

    def handle: Receive = {
      case payload: String =>
        last = s"${payload}-${lastSequenceNr}"
        probe ! s"updated-${last}"
    }
  }

  class ProcessorCNoRecover(override val persistenceId: String, toSequenceNr: Long, probe: ActorRef) extends ProcessorC(persistenceId, probe) {
    override def recovery = Recovery.create(toSequenceNr = toSequenceNr)
  }

  class ViewA(val viewId: String, val persistenceId: String, probe: ActorRef) extends PersistentView {
    def receive = {
      case payload =>
        probe ! payload
    }

    override def autoUpdate: Boolean = false

    override def autoUpdateReplayMax: Long = 0
  }

}

import CassandraIntegrationSpec._

class CassandraIntegrationSpec extends TestKit(ActorSystem("test", config)) with ImplicitSender with WordSpecLike with Matchers with CassandraLifecycle {
  val firstSequenceNumber = 2L

  def subscribeToRangeDeletion(probe: TestProbe): Unit =
    system.eventStream.subscribe(probe.ref, classOf[JournalProtocol.DeleteMessagesTo])

  def awaitRangeDeletion(probe: TestProbe): Unit =
    probe.expectMsgType[JournalProtocol.DeleteMessagesTo]

  def testRangeDelete(persistenceId: String): Unit = {
    val deleteProbe = TestProbe()
    subscribeToRangeDeletion(deleteProbe)

    val processor1 = system.actorOf(Props(classOf[ProcessorA], persistenceId, self))
    firstSequenceNumber to 16L foreach { i =>
      processor1 ! s"a-${i}"
      expectMsgAllOf(s"a-${i}", i, false)
    }

    processor1 ! DeleteTo(firstSequenceNumber+2)
    awaitRangeDeletion(deleteProbe)

    system.actorOf(Props(classOf[ProcessorA], persistenceId, self))
    (firstSequenceNumber+3) to 16L foreach { i =>
      expectMsgAllOf(s"a-${i}", i, true)
    }

    processor1 ! DeleteTo(firstSequenceNumber+5)
    awaitRangeDeletion(deleteProbe)

    system.actorOf(Props(classOf[ProcessorA], persistenceId, self))
    (firstSequenceNumber+6) to 16L foreach { i =>
      expectMsgAllOf(s"a-${i}", i, true)
    }
  }

  "A Cassandra journal" should {
    "write and replay messages" in {
      val processor1 = system.actorOf(Props(classOf[ProcessorA], "p1", self))
      firstSequenceNumber to 16L foreach { i =>
        processor1 ! s"a-${i}"
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val processor2 = system.actorOf(Props(classOf[ProcessorA], "p1", self))
      firstSequenceNumber to 16L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, true)
      }

      processor2 ! "b"
      expectMsgAllOf("b", 17L, false)
    }

    "not replay range-deleted messages" in {
      testRangeDelete("p6")
    }

    "replay messages incrementally" in {
      val probe = TestProbe()
      val processor1 = system.actorOf(Props(classOf[ProcessorA], "p7", self))
      firstSequenceNumber to (firstSequenceNumber+6) foreach { i =>
        processor1 ! s"a-${i}"
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val view = system.actorOf(Props(classOf[ViewA], "p7-view", "p7", probe.ref))
      probe.expectNoMsg(200.millis)

      view ! Update(await = true, replayMax = 3L)
      probe.expectMsg(s"a-${firstSequenceNumber}")
      probe.expectMsg(s"a-${firstSequenceNumber+1}")
      probe.expectMsg(s"a-${firstSequenceNumber+2}")
      probe.expectNoMsg(200.millis)

      view ! Update(await = true, replayMax = 3L)
      probe.expectMsg(s"a-${firstSequenceNumber+3}")
      probe.expectMsg(s"a-${firstSequenceNumber+4}")
      probe.expectMsg(s"a-${firstSequenceNumber+5}")
      probe.expectNoMsg(200.millis)
    }
  }

  "A processor" should {
    "recover from a snapshot with follow-up messages" in {
      val processor1 = system.actorOf(Props(classOf[ProcessorC], "p10", testActor))
      processor1 ! "a"
      expectMsg(s"updated-a-${firstSequenceNumber}")
      processor1 ! "snap"
      expectMsg(s"snapped-a-${firstSequenceNumber}")
      processor1 ! "b"
      expectMsg(s"updated-b-${firstSequenceNumber+1}")

      system.actorOf(Props(classOf[ProcessorC], "p10", testActor))
      expectMsg(s"offered-a-${firstSequenceNumber}")
      expectMsg(s"updated-b-${firstSequenceNumber+1}")
    }

    "recover from a snapshot with follow-up messages and an upper bound" in {
      val processor1 = system.actorOf(Props(classOf[ProcessorCNoRecover], "p11", firstSequenceNumber, testActor))
      processor1 ! Recovery()
      processor1 ! "a"
      expectMsg(s"updated-a-${firstSequenceNumber}")
      processor1 ! "snap"
      expectMsg(s"snapped-a-${firstSequenceNumber}")
      (firstSequenceNumber+1) to (firstSequenceNumber+6) foreach { i =>
        processor1 ! "a"
        expectMsg(s"updated-a-${i}")
      }

      val processor2 = system.actorOf(Props(classOf[ProcessorCNoRecover], "p11", firstSequenceNumber+2, testActor))
      expectMsg(s"offered-a-${firstSequenceNumber}")
      expectMsg(s"updated-a-${firstSequenceNumber+1}")
      expectMsg(s"updated-a-${firstSequenceNumber+2}")
      processor2 ! "d"
      expectMsg(s"updated-d-${firstSequenceNumber+7}")
    }

    "recover from a snapshot without follow-up messages inside a partition" in {
      val processor1 = system.actorOf(Props(classOf[ProcessorC], "p12", testActor))
      processor1 ! "a"
      expectMsg(s"updated-a-${firstSequenceNumber}")
      processor1 ! "snap"
      expectMsg(s"snapped-a-${firstSequenceNumber}")

      val processor2 = system.actorOf(Props(classOf[ProcessorC], "p12", testActor))
      expectMsg(s"offered-a-${firstSequenceNumber}")
      processor2 ! "b"
      // CONT!!
      expectMsg(s"updated-b-${firstSequenceNumber+2}")
    }

    "recover from a snapshot without follow-up messages at a partition boundary (where next partition is invalid)" in {
      val processor1 = system.actorOf(Props(classOf[ProcessorC], "p13", testActor))
      firstSequenceNumber to (firstSequenceNumber+5) foreach { i =>
        processor1 ! "a"
        expectMsg(s"updated-a-${i}")
      }
      processor1 ! "snap"
      expectMsg(s"snapped-a-${firstSequenceNumber+5}")

      val processor2 = system.actorOf(Props(classOf[ProcessorC], "p13", testActor))
      expectMsg(s"offered-a-${firstSequenceNumber+5}")
      processor2 ! "b"
      // CONT!!
      expectMsg(s"updated-b-${firstSequenceNumber+7}")
    }

    "recover from a snapshot without follow-up messages at a partition boundary (where next partition contains a permanently deleted message)" in {
      val deleteProbe = TestProbe()
      subscribeToRangeDeletion(deleteProbe)

      val processor1 = system.actorOf(Props(classOf[ProcessorC], "p15", testActor))
      firstSequenceNumber to (firstSequenceNumber+5) foreach { i =>
        processor1 ! "a"
        expectMsg(s"updated-a-${i}")
      }
      processor1 ! "snap"
      expectMsg(s"snapped-a-${firstSequenceNumber+5}")

      processor1 ! "a"
      expectMsg(s"updated-a-${firstSequenceNumber+6}")

      processor1 ! DeleteTo(firstSequenceNumber+6)
      awaitRangeDeletion(deleteProbe)

      val processor2 = system.actorOf(Props(classOf[ProcessorC], "p15", testActor))
      expectMsg(s"offered-a-${firstSequenceNumber+5}")
      processor2 ! "b"
      // CONT!!
      expectMsg(s"updated-b-${firstSequenceNumber+7}") // sequence number of permanently deleted message can be re-used
    }

    "properly recover after all messages have been deleted" in {
      val deleteProbe = TestProbe()
      subscribeToRangeDeletion(deleteProbe)

      val p = system.actorOf(Props(classOf[ProcessorA], "p16", self))

      p ! "a"
      expectMsgAllOf("a", firstSequenceNumber, false)

      p ! DeleteTo(firstSequenceNumber)
      awaitRangeDeletion(deleteProbe)

      val r = system.actorOf(Props(classOf[ProcessorA], "p16", self))

      r ! "b"
      expectMsgAllOf("b", firstSequenceNumber, false)
    }
  }
}
