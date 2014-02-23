package com.syntaxjockey.smr

import akka.testkit.ImplicitSender
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import akka.actor.ActorRef
import akka.util.ByteString
import org.joda.time.DateTime
import scala.concurrent.duration._

import com.syntaxjockey.smr.namespace._
import com.syntaxjockey.smr.raft.RandomBoundedDuration

class WatchSpecMultiJvmNode1 extends WatchSpec
class WatchSpecMultiJvmNode2 extends WatchSpec
class WatchSpecMultiJvmNode3 extends WatchSpec
class WatchSpecMultiJvmNode4 extends WatchSpec
class WatchSpecMultiJvmNode5 extends WatchSpec

class WatchSpec extends SMRMultiNodeSpec(SMRMultiNodeConfig) with ImplicitSender {
  import SMRMultiNodeConfig._
  import PathConversions._

  def initialParticipants = roles.size

  "A Watch" must {

    val electionTimeout = RandomBoundedDuration(4500 milliseconds, 5000 milliseconds)
    val idleTimeout = 2.seconds
    val maxEntriesBatch = 10
    var rsm: ActorRef = ActorRef.noSender

    "notify requestor node about a data event" in {
      enterBarrier("starting-1")
      Cluster(system).subscribe(testActor, classOf[MemberUp])
      expectMsgClass(classOf[CurrentClusterState])
      Cluster(system).join(node(node1).address)
      for (_ <- 0.until(roles.size)) { expectMsgClass(classOf[MemberUp]) }
      enterBarrier("joined-cluster")
      rsm = system.actorOf(ReplicatedStateMachine.props(self, roles.size, None, electionTimeout, idleTimeout, maxEntriesBatch), "rsm")
      within(30 seconds) { expectMsg(SMRClusterReadyEvent) }
      runOn(node1) {
        within(30 seconds) {
          rsm ! CreateNamespace("foo")
          expectMsgClass(classOf[CreateNamespaceResult])
          rsm ! CreateNode("foo", "/node1", ByteString("hello, world"), DateTime.now())
          expectMsgClass(classOf[CreateNodeResult])
          rsm ! Watch(GetNodeData("foo", "/node1"), self)
          expectMsgClass(classOf[GetNodeDataResult])
          rsm ! SetNodeData("foo", "/node1", ByteString("node data changed"), None, DateTime.now())
          val notification = expectMsgClass(classOf[Notification])
          val expectedPath =  NamespacePath("foo", "/node1")
          notification.nspath must be === expectedPath
          notification.event must be(Notification.NodeDataChangedEvent)
          expectMsgClass(classOf[SetNodeDataResult])
        }
      }
      enterBarrier("finished-1")
    }

    "notify observer about a data event" in {
      enterBarrier("starting-2")
      runOn(node2) {
        within(30 seconds) {
          rsm ! CreateNode("foo", "/node2", ByteString("hello, world"), DateTime.now())
          expectMsgClass(classOf[CreateNodeResult])
          rsm ! Watch(GetNodeData("foo", "/node2"), self)
          expectMsgClass(classOf[GetNodeDataResult])
          enterBarrier("set-watch")
          val notification = expectMsgClass(classOf[Notification])
          val expectedPath =  NamespacePath("foo", "/node2")
          notification.nspath must be === expectedPath
          notification.event must be(Notification.NodeDataChangedEvent)
        }
      }
      runOn(node3) {
        within(30 seconds) {
          enterBarrier("set-watch")
          rsm ! SetNodeData("foo", "/node2", ByteString("node data changed"), None, DateTime.now())
          expectMsgClass(classOf[SetNodeDataResult])
        }
      }
      runOn(node1, node4, node5) { enterBarrier("set-watch")}
      enterBarrier("finished-2")
    }
  }
}
