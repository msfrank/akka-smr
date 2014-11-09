package com.syntaxjockey.smr

import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import scala.concurrent.duration._
import java.nio.file.Paths
import java.util.UUID

import com.syntaxjockey.smr.raft.{RaftProcessorSettings, RandomBoundedDuration}

class ReplicatedStateMachineSpecMultiJvmNode1 extends ReplicatedStateMachineSpec
class ReplicatedStateMachineSpecMultiJvmNode2 extends ReplicatedStateMachineSpec
class ReplicatedStateMachineSpecMultiJvmNode3 extends ReplicatedStateMachineSpec
class ReplicatedStateMachineSpecMultiJvmNode4 extends ReplicatedStateMachineSpec
class ReplicatedStateMachineSpecMultiJvmNode5 extends ReplicatedStateMachineSpec

class ReplicatedStateMachineSpec extends SMRMultiNodeSpec(SMRMultiNodeConfig) with ImplicitSender {
  import SMRMultiNodeConfig._

  def initialParticipants = roles.size

  "A ReplicatedStateMachine cluster" must {

    val electionTimeout = RandomBoundedDuration(4500 milliseconds, 5000 milliseconds)
    val idleTimeout = 2.seconds
    val maxEntriesBatch = 10

    "wait for all nodes to become ready" in {
      Cluster(system).subscribe(testActor, classOf[MemberUp])
      expectMsgClass(classOf[CurrentClusterState])
      Cluster(system).join(node(node1).address)
      for (_ <- 0.until(roles.size)) { expectMsgClass(classOf[MemberUp]) }
      enterBarrier("startup")
      val logDirectory = Paths.get("test-raft-log.%s".format(UUID.randomUUID()))
      val settings = RaftProcessorSettings(roles.size, electionTimeout, idleTimeout, maxEntriesBatch, logDirectory, 0)
      system.actorOf(ReplicatedStateMachine.props(self, settings, None), "rsm")
      within(30 seconds) { expectMsg(SMRClusterReadyEvent) }
      enterBarrier("finished")
    }

//    "reply with the cluster status" in {
//      Cluster(system).subscribe(testActor, classOf[MemberUp])
//      expectMsgClass(classOf[CurrentClusterState])
//      Cluster(system).join(node(node1).address)
//      for (_ <- 0.until(roles.size)) { expectMsgClass(classOf[MemberUp]) }
//      enterBarrier("startup")
//      val rsm = system.actorOf(ReplicatedStateMachine.props(self, 3))
//      within(1 minute) { expectMsg(SMRClusterReadyEvent) }
//      rsm ! RSMStatusQuery
//      expectMsgClass(classOf[RSMStatusResult])
//    }
  }
}
