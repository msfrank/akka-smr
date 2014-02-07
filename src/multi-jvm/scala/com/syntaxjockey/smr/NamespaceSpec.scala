package com.syntaxjockey.smr

import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import scala.concurrent.duration._
import akka.util.Timeout

class NamespaceSpecMultiJvmNode1 extends NamespaceSpec
class NamespaceSpecMultiJvmNode2 extends NamespaceSpec
class NamespaceSpecMultiJvmNode3 extends NamespaceSpec
class NamespaceSpecMultiJvmNode4 extends NamespaceSpec
class NamespaceSpecMultiJvmNode5 extends NamespaceSpec

class NamespaceSpec extends SMRMultiNodeSpec(SMRMultiNodeConfig) with ImplicitSender {
  import SMRMultiNodeConfig._

  def initialParticipants = roles.size

  "A ReplicatedStateMachine cluster" must {

    "create a namespace" in {
      Cluster(system).subscribe(testActor, classOf[MemberUp])
      expectMsgClass(classOf[CurrentClusterState])
      Cluster(system).join(node(node1).address)
      for (_ <- 0.until(roles.size)) { expectMsgClass(classOf[MemberUp]) }
      enterBarrier("startup")
      val rsm = system.actorOf(ReplicatedStateMachine.props(self, roles.size), "rsm")
      within(30 seconds) { expectMsg(RSMReady) }
      runOn(node2) {
        within(30 seconds) {
          rsm ! CreateNamespace("foo")
          val result = expectMsgClass(classOf[WorldStateResult])
          result.world.namespaces.keys must contain("foo")
        }
      }
      enterBarrier("finished")
    }

//    "delete a namespace" in {
//
//    }
//
//    "create a namespace node" in {
//
//    }
//
//    "update a namespace node" in {
//
//    }
//
//    "delete a namespace node" in {
//
//    }
  }
}
