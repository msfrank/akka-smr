package com.syntaxjockey.smr

import akka.testkit.ImplicitSender
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import scala.concurrent.duration._

import com.syntaxjockey.smr.raft.RandomBoundedDuration

class ConfigurationSpecMultiJvmNode1 extends ConfigurationSpec
class ConfigurationSpecMultiJvmNode2 extends ConfigurationSpec
class ConfigurationSpecMultiJvmNode3 extends ConfigurationSpec
class ConfigurationSpecMultiJvmNode4 extends ConfigurationSpec
class ConfigurationSpecMultiJvmNode5 extends ConfigurationSpec

class ConfigurationSpec extends SMRMultiNodeSpec(SMRMultiNodeConfig) with ImplicitSender {
  import SMRMultiNodeConfig._

  def initialParticipants = roles.size

  "A ReplicatedStateMachine cluster" must {
    val electionTimeout = RandomBoundedDuration(4500.milliseconds, 5000.milliseconds)
    val idleTimeout = 2.seconds
    val maxEntriesBatch = 10

    "detect a new processor added" in {
      runOn(node1, node2, node3, node4) {
        Cluster(system).subscribe(testActor, classOf[MemberUp])
        expectMsgClass(classOf[CurrentClusterState])
        Cluster(system).join(node(node1).address)
        for (_ <- 0.until(4)) { expectMsgClass(classOf[MemberUp]) }
        Cluster(system).unsubscribe(testActor, classOf[MemberUp])
        enterBarrier("startup-initial")
        system.actorOf(ReplicatedStateMachine.props(self, roles.size - 1, electionTimeout, idleTimeout, maxEntriesBatch), "rsm")
        within(30.seconds) { expectMsg(SMRClusterReadyEvent) }
        enterBarrier("finished-initial")
        within(30.seconds) { expectMsg(SMRClusterChangedEvent) }
        enterBarrier("added-processor")
      }
      runOn(node5) {
        enterBarrier("startup-initial")
        enterBarrier("finished-initial")
        Cluster(system).join(node(node1).address)
        system.actorOf(ReplicatedStateMachine.props(self, roles.size - 1, electionTimeout, idleTimeout, maxEntriesBatch), "rsm")
        within(30.seconds) { expectMsg(SMRClusterReadyEvent) }
        enterBarrier("added-processor")
      }
    }
  }
}
