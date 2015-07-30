package akka.persistence.cassandra.snapshot

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer

import akka.persistence._
import akka.persistence.SnapshotProtocol._
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.TestConfig
import akka.persistence.snapshot.SnapshotStoreSpec
import akka.testkit.TestProbe

import com.datastax.driver.core._

class CassandraSnapshotStoreSpec extends SnapshotStoreSpec(TestConfig.config) with CassandraLifecycle {

  val storeConfig = new CassandraSnapshotStoreConfig(system.settings.config.getConfig("cassandra-snapshot-store"))
  val storeStatements = new CassandraStatements { def config = storeConfig }

  var cluster: Cluster = _
  var session: Session = _

  import storeConfig._
  import storeStatements._

  override def beforeAll(): Unit = {
    super.beforeAll()
    cluster = clusterBuilder.build()
    session = cluster.connect()
  }

  override def afterAll(): Unit = {
    session.close()
    cluster.close()
    super.afterAll()
  }

  "A Cassandra snapshot store" must {
    "make up to 3 snapshot loading attempts" in {
      val probe = TestProbe()

      // load most recent snapshot
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, Long.MaxValue), probe.ref)

      // get most recent snapshot
      val expected = probe.expectMsgPF() { case LoadSnapshotResult(Some(snapshot), _) => snapshot }

      // write two more snapshots that cannot be de-serialized.
      session.execute(writeSnapshot, pid, 17L: JLong, 123L: JLong, ByteBuffer.wrap("fail-1".getBytes("UTF-8")))
      session.execute(writeSnapshot, pid, 18L: JLong, 124L: JLong, ByteBuffer.wrap("fail-2".getBytes("UTF-8")))

      // load most recent snapshot, first two attempts will fail ...
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, Long.MaxValue), probe.ref)

      // third attempt succeeds
      probe.expectMsg(LoadSnapshotResult(Some(expected), Long.MaxValue))
    }
    "give up after 3 snapshot loading attempts" in {
      val probe = TestProbe()

      // load most recent snapshot
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, Long.MaxValue), probe.ref)

      // wait for most recent snapshot
      probe.expectMsgPF() { case LoadSnapshotResult(Some(snapshot), _) => snapshot }

      // write three more snapshots that cannot be de-serialized.
      session.execute(writeSnapshot, pid, 17L: JLong, 123L: JLong, ByteBuffer.wrap("fail-1".getBytes("UTF-8")))
      session.execute(writeSnapshot, pid, 18L: JLong, 124L: JLong, ByteBuffer.wrap("fail-2".getBytes("UTF-8")))
      session.execute(writeSnapshot, pid, 19L: JLong, 125L: JLong, ByteBuffer.wrap("fail-3".getBytes("UTF-8")))

      // load most recent snapshot, first three attempts will fail ...
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, Long.MaxValue), probe.ref)

      // no 4th attempt has been made
      probe.expectMsg(LoadSnapshotResult(None, Long.MaxValue))
    }
  }
}
