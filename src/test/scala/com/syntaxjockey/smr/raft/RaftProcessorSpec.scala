package com.syntaxjockey.smr.raft

import org.scalatest.{WordSpecLike, BeforeAndAfterAll, Matchers}
import akka.testkit.{ImplicitSender, TestKit, TestActorRef, TestFSMRef}
import akka.actor.ActorSystem
import scala.concurrent.duration._

class RaftProcessorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {
  import RaftProcessor._

  def this() = this(ActorSystem("RaftProcessorSpec"))

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  "RaftProcessor" must {

    "transition from Initializing to Follower after receiving list of peers" in {
      val ref = TestFSMRef(new RaftProcessor(self, 1 second, 1 second, 1 second, 10))
      val processor: TestActorRef[RaftProcessor] = ref
      assert(ref.stateName.isInstanceOf[Initializing.type])
      processor ! ProcessorSet(Set.empty)
      assert(ref.stateName.isInstanceOf[Follower.type])
    }

    "transition from Follower to Candidate if electionTimeout fires" in {
      val ref = TestFSMRef(new RaftProcessor(self, 1 second, 1 second, 1 second, 10))
      val processor: TestActorRef[RaftProcessor] = ref
      processor ! ProcessorSet(Set.empty)
      assert(ref.isStateTimerActive)
      val slop = 2000 // 2 seconds
      Thread.sleep(processor.underlyingActor.electionTimeout.toMillis + slop)
      assert(ref.stateName.isInstanceOf[Candidate.type])
    }
  }
}
