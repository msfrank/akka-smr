package com.syntaxjockey.smr

import akka.testkit.ImplicitSender
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import scala.concurrent.duration._

import akka.remote.testkit.MultiNodeConfig
import com.typesafe.config.ConfigFactory

class SMRExtensionSpecMultiJvmNode1 extends SMRExtensionSpec
class SMRExtensionSpecMultiJvmNode2 extends SMRExtensionSpec
class SMRExtensionSpecMultiJvmNode3 extends SMRExtensionSpec
class SMRExtensionSpecMultiJvmNode4 extends SMRExtensionSpec
class SMRExtensionSpecMultiJvmNode5 extends SMRExtensionSpec

class SMRExtensionSpec extends SMRMultiNodeSpec(SMRExtensionMultiNodeConfig) with ImplicitSender {
  import SMRMultiNodeConfig._

  def initialParticipants = roles.size

  system.eventStream.subscribe(self, classOf[SMREvent])

  "SMR extension" must {

    "initialize" in {
      enterBarrier("starting-1")
      Cluster(system).subscribe(testActor, classOf[MemberUp])
      expectMsgClass(classOf[CurrentClusterState])
      Cluster(system).join(node(node1).address)
      for (_ <- 0.until(roles.size)) { expectMsgClass(classOf[MemberUp]) }
      enterBarrier("startup")
      SMR(system)
      within(30 seconds) { expectMsg(SMRClusterReadyEvent) }
      enterBarrier("finished-1")
    }
  }
}

object SMRExtensionMultiNodeConfig extends MultiNodeConfig {
  commonConfig(ConfigFactory.parseString(
    """
      |akka {
      |  smr {
      |    smr-name = "smr"
      |    minimum-nr-processors = 5
      |    election-timeout = 2 seconds
      |    election-timeout-variance = 500 milliseconds
      |    idle-timeout = 1 second
      |    max-entries-batch = 10
      |  }
      |}
    """.stripMargin).withFallback(ConfigFactory.load("multi-jvm.conf")))
  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")
  val node4 = role("node4")
  val node5 = role("node5")
}