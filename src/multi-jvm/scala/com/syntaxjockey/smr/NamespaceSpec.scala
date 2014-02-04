package com.syntaxjockey.smr

import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import scala.concurrent.duration._

class NamespaceSpecMultiJvmNode1 extends ReplicatedStateMachineSpec
class NamespaceSpecMultiJvmNode2 extends ReplicatedStateMachineSpec
class NamespaceSpecMultiJvmNode3 extends ReplicatedStateMachineSpec

class NamespaceSpec extends MultiNodeSpec(SMRMultiNodeConfig) with SMRMultiNodeSpec with ImplicitSender {
  import SMRMultiNodeConfig._

  def initialParticipants = roles.size

  "A ReplicatedStateMachine cluster" must {

    "wait for all nodes to become ready" in {
      Cluster(system).subscribe(testActor, classOf[MemberUp])
      expectMsgClass(classOf[CurrentClusterState])
      Cluster(system).join(node(node1).address)
      for (_ <- 0.until(roles.size)) { expectMsgClass(classOf[MemberUp]) }
      enterBarrier("startup")
      system.actorOf(ReplicatedStateMachine.props(self, 3))
      within(1 minute) { expectMsg(RSMReady) }
    }

  }
}
