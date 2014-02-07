package com.syntaxjockey.smr

import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender
import scala.concurrent.duration._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}

class ReplicatedStateMachineSpecMultiJvmNode1 extends ReplicatedStateMachineSpec
class ReplicatedStateMachineSpecMultiJvmNode2 extends ReplicatedStateMachineSpec
class ReplicatedStateMachineSpecMultiJvmNode3 extends ReplicatedStateMachineSpec
class ReplicatedStateMachineSpecMultiJvmNode4 extends ReplicatedStateMachineSpec
class ReplicatedStateMachineSpecMultiJvmNode5 extends ReplicatedStateMachineSpec

class ReplicatedStateMachineSpec extends SMRMultiNodeSpec(SMRMultiNodeConfig) with ImplicitSender {
  import SMRMultiNodeConfig._

  def initialParticipants = roles.size

  "A ReplicatedStateMachine cluster" must {

    "wait for all nodes to become ready" in {
      Cluster(system).subscribe(testActor, classOf[MemberUp])
      expectMsgClass(classOf[CurrentClusterState])
      Cluster(system).join(node(node1).address)
      for (_ <- 0.until(roles.size)) { expectMsgClass(classOf[MemberUp]) }
      enterBarrier("startup")
      system.actorOf(ReplicatedStateMachine.props(self, roles.size))
      within(30 seconds) { expectMsg(RSMReady) }
      enterBarrier("finished")
    }

//    "reply with the cluster status" in {
//      Cluster(system).subscribe(testActor, classOf[MemberUp])
//      expectMsgClass(classOf[CurrentClusterState])
//      Cluster(system).join(node(node1).address)
//      for (_ <- 0.until(roles.size)) { expectMsgClass(classOf[MemberUp]) }
//      enterBarrier("startup")
//      val rsm = system.actorOf(ReplicatedStateMachine.props(self, 3))
//      within(1 minute) { expectMsg(RSMReady) }
//      rsm ! RSMStatusQuery
//      expectMsgClass(classOf[RSMStatusResult])
//    }
  }
}
