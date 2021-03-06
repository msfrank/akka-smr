package com.syntaxjockey.smr

import java.nio.file.Paths
import java.util.UUID

import akka.testkit.ImplicitSender
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{MemberRemoved, CurrentClusterState, MemberUp}
import com.syntaxjockey.smr.command.{PongResult, PingCommand}
import scala.concurrent.duration._

import com.syntaxjockey.smr.raft.{RaftProcessorSettings, RandomBoundedDuration}
import akka.actor.ActorRef

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
    var rsm: ActorRef = ActorRef.noSender

    "detect when a processor is added" in {
      runOn(node1, node2, node3, node4) {
        Cluster(system).subscribe(testActor, classOf[MemberUp])
        expectMsgClass(classOf[CurrentClusterState])
        Cluster(system).join(node(node1).address)
        for (_ <- 0.until(4)) { expectMsgClass(classOf[MemberUp]) }
        Cluster(system).unsubscribe(testActor, classOf[MemberUp])
        enterBarrier("startup-initial")
        val logDirectory = Paths.get("test-raft-log.%s".format(UUID.randomUUID()))
        val settings = RaftProcessorSettings(roles.size - 1, electionTimeout, idleTimeout, maxEntriesBatch, logDirectory, 0)
        rsm = system.actorOf(ReplicatedStateMachine.props(self, settings, None), "rsm")
        within(30.seconds) { expectMsg(SMRClusterReadyEvent) }
        rsm ! PingCommand(Some("%s 1".format(myself.name)))
        within(30.seconds) { expectMsgClass(classOf[PongResult]) }
        enterBarrier("finished-initial")
        within(30.seconds) { expectMsg(SMRClusterChangedEvent) }
        enterBarrier("added-processor")
      }
      runOn(node5) {
        enterBarrier("startup-initial")
        enterBarrier("finished-initial")
        Cluster(system).join(node(node1).address)
        val logDirectory = Paths.get("test-raft-log.%s".format(UUID.randomUUID()))
        val settings = RaftProcessorSettings(roles.size - 1, electionTimeout, idleTimeout, maxEntriesBatch, logDirectory, 0)
        rsm = system.actorOf(ReplicatedStateMachine.props(self, settings, None), "rsm")
        within(30.seconds) { expectMsg(SMRClusterReadyEvent) }
        rsm ! PingCommand(Some("%s 1".format(myself.name)))
        within(30.seconds) { expectMsgClass(classOf[PongResult]) }
        enterBarrier("added-processor")
      }
    }

    "detect when a processor is removed" in {
      enterBarrier("starting-2")
      runOn(node2) {
        Cluster(system).subscribe(testActor, classOf[MemberRemoved])
        Cluster(system).leave(node(node2).address)
        //within(30.seconds) { expectMsgClass(classOf[MemberRemoved]) }
        enterBarrier("removed-processor")
      }
      runOn(node1, node3, node4, node5) {
        within(60.seconds) { expectMsg(SMRClusterChangedEvent) }
        rsm ! PingCommand(Some("%s 2".format(myself.name)))
        within(30.seconds) { expectMsgClass(classOf[PongResult]) }
        enterBarrier("removed-processor")
      }
    }

    "detect when cluster size shrinks below minimumProcessors" in {
      enterBarrier("starting-3")
      runOn(node3) {
        Cluster(system).subscribe(testActor, classOf[MemberRemoved])
        Cluster(system).leave(node(node3).address)
        //within(30.seconds) { expectMsgClass(classOf[MemberRemoved]) }
        enterBarrier("cluster-lost")
      }
      runOn(node2) {
        enterBarrier("cluster-lost")
      }
      runOn(node1, node4) {
        within(60.seconds) { expectMsg(SMRClusterLostEvent) }
        enterBarrier("cluster-lost")
      }
      // FIXME: node5 shouldn't receive a second cluster changed event
      runOn(node5) {
        within(60.seconds) { expectMsg(SMRClusterChangedEvent) }
        within(60.seconds) { expectMsg(SMRClusterLostEvent) }
        enterBarrier("cluster-lost")
      }
    }
  }
}
