package com.syntaxjockey.smr

import akka.testkit.ImplicitSender
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import akka.actor.ActorRef
import akka.util.ByteString
import org.joda.time.DateTime
import scala.concurrent.duration._

import com.syntaxjockey.smr.namespace._

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

    var rsm: Option[ActorRef] = None

    "create a namespace" in {
      Cluster(system).subscribe(testActor, classOf[MemberUp])
      expectMsgClass(classOf[CurrentClusterState])
      Cluster(system).join(node(node1).address)
      for (_ <- 0.until(roles.size)) { expectMsgClass(classOf[MemberUp]) }
      enterBarrier("startup")
      rsm = Some(system.actorOf(ReplicatedStateMachine.props(self, roles.size), "rsm"))
      within(30 seconds) { expectMsg(RSMReady) }
      runOn(node1) {
        within(30 seconds) {
          rsm.get ! CreateNamespace("foo")
          val result = expectMsgClass(classOf[WorldStateResult])
          result.world.namespaces.keys must contain("foo")
        }
      }
      enterBarrier("finished-1")
    }

    "create a namespace node" in {
      runOn(node2) {
        within(30 seconds) {
          rsm.get ! CreateNode("foo", "/node1", ByteString("hello, world"), DateTime.now())
          val result = expectMsgClass(classOf[NodeResult])
        }
      }
      enterBarrier("finished-2")
    }

    "update a namespace node" in {
      runOn(node3) {
        within(30 seconds) {
          rsm.get ! SetNodeData("foo", "/node1", ByteString("hello, world"), None, DateTime.now())
          val result = expectMsgClass(classOf[NodeResult])
        }
      }
      enterBarrier("finished-3")
    }

    "delete a namespace node" in {
      runOn(node4) {
        within(30 seconds) {
          rsm.get ! DeleteNode("foo", "/node1", None, DateTime.now())
          val result = expectMsgClass(classOf[EmptyResult])
        }
      }
      enterBarrier("finished-4")
    }

    "delete a namespace" in {
      runOn(node5) {
        within(30 seconds) {
          rsm.get ! DeleteNamespace("foo")
          val result = expectMsgClass(classOf[WorldStateResult])
          result.world.namespaces.keys must not contain("foo")
        }
      }
      enterBarrier("finished-5")
    }
  }
}
