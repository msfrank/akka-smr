package com.syntaxjockey.smr

import java.nio.file.Paths
import java.util.UUID

import akka.testkit.ImplicitSender
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import akka.actor.ActorRef
import akka.util.ByteString
import com.syntaxjockey.smr.world.{PathConversions, Path}
import org.joda.time.DateTime
import scala.concurrent.duration._

import com.syntaxjockey.smr.command._
import com.syntaxjockey.smr.raft.{RaftProcessorSettings, RandomBoundedDuration}

class NamespaceSpecMultiJvmNode1 extends NamespaceSpec
class NamespaceSpecMultiJvmNode2 extends NamespaceSpec
class NamespaceSpecMultiJvmNode3 extends NamespaceSpec
class NamespaceSpecMultiJvmNode4 extends NamespaceSpec
class NamespaceSpecMultiJvmNode5 extends NamespaceSpec

class NamespaceSpec extends SMRMultiNodeSpec(SMRMultiNodeConfig) with ImplicitSender {
  import SMRMultiNodeConfig._
  import PathConversions._

  def initialParticipants = roles.size

  "A ReplicatedStateMachine cluster" must {

    val electionTimeout = RandomBoundedDuration(4500.milliseconds, 5000.milliseconds)
    val idleTimeout = 2.seconds
    val maxEntriesBatch = 10
    var rsm: ActorRef = ActorRef.noSender

    "create a namespace" in {
      enterBarrier("starting-1")
      Cluster(system).subscribe(testActor, classOf[MemberUp])
      expectMsgClass(classOf[CurrentClusterState])
      Cluster(system).join(node(node1).address)
      for (_ <- 0.until(roles.size)) { expectMsgClass(classOf[MemberUp]) }
      enterBarrier("startup")
      val uuid = UUID.randomUUID()
      val logDirectory = Paths.get("test-raft-log.%s".format(uuid))
      val settings = RaftProcessorSettings(roles.size, electionTimeout, idleTimeout, maxEntriesBatch, logDirectory, 0)
      rsm = system.actorOf(ReplicatedStateMachine.props(self, settings, None), "rsm")
      within(30.seconds) { expectMsg(SMRClusterReadyEvent) }
      runOn(node1) {
        within(30.seconds) {
          rsm ! PingCommand(Some(uuid))
          val result = expectMsgClass(classOf[PongResult])
          result.correlationId should be(Some(uuid))
        }
      }
      enterBarrier("finished-1")
    }

    "create a namespace node" in {
      enterBarrier("starting-2")
      runOn(node2) {
        within(30.seconds) {
          rsm ! CreateNode("foo", "/node1", ByteString("hello, world"), DateTime.now())
          val result = expectMsgClass(classOf[CreateNodeResult])
          result.path.segments shouldEqual Path("/node1").segments
        }
      }
      enterBarrier("finished-2")
    }

    "update a namespace node" in {
      enterBarrier("starting-3")
      runOn(node3) {
        within(30.seconds) {
          rsm ! SetNodeData("foo", "/node1", ByteString("hello, world"), None, DateTime.now())
          val result = expectMsgClass(classOf[SetNodeDataResult])
        }
      }
      enterBarrier("finished-3")
    }

    "delete a namespace node" in {
      enterBarrier("starting-4")
      runOn(node4) {
        within(30.seconds) {
          rsm ! DeleteNode("foo", "/node1", None, DateTime.now())
          val result = expectMsgClass(classOf[DeleteNodeResult])
          result.path.segments shouldEqual Path("/node1").segments
        }
      }
      enterBarrier("finished-4")
    }

//    "delete a namespace" in {
//      enterBarrier("starting-5")
//      runOn(node5) {
//        within(30.seconds) {
//          rsm ! DeleteNamespace("foo")
//          val result = expectMsgClass(classOf[DeleteNamespaceResult])
//          result.name must be("foo")
//        }
//      }
//      enterBarrier("finished-5")
//    }
  }
}
