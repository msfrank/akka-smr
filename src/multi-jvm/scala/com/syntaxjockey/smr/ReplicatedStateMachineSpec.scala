package com.syntaxjockey.smr

import org.scalatest.{WordSpecLike, BeforeAndAfterAll}
import org.scalatest.matchers.MustMatchers
import akka.remote.testkit.{MultiNodeSpec, MultiNodeConfig, MultiNodeSpecCallbacks}
import akka.testkit.ImplicitSender
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}

trait SMRMultiNodeSpec extends MultiNodeSpecCallbacks with WordSpecLike with MustMatchers with BeforeAndAfterAll {
  override def beforeAll() = multiNodeSpecBeforeAll()
  override def afterAll() = multiNodeSpecAfterAll()
}

object SMRMultiNodeConfig extends MultiNodeConfig {
  commonConfig(ConfigFactory.load("multi-jvm.conf"))
  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")
}

class ReplicatedStateMachineSpec extends MultiNodeSpec(SMRMultiNodeConfig) with SMRMultiNodeSpec with ImplicitSender {
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

class MultiNodeSampleSpecMultiJvmNode1 extends ReplicatedStateMachineSpec
class MultiNodeSampleSpecMultiJvmNode2 extends ReplicatedStateMachineSpec
class MultiNodeSampleSpecMultiJvmNode3 extends ReplicatedStateMachineSpec
