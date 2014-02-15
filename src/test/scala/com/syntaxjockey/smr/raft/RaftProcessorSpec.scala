package com.syntaxjockey.smr.raft

import org.scalatest.{WordSpecLike, BeforeAndAfterAll}
import org.scalatest.matchers.MustMatchers

import akka.actor.{ActorLogging, Props, Actor, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import com.syntaxjockey.smr.{WorldStateResult, WorldState}
import scala.util.Success

class RaftProcessorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with MustMatchers with BeforeAndAfterAll {
  import RaftProcessor._
  import TestExecutor._

  def this() = this(ActorSystem("RaftProcessorSpec", ConfigFactory.load("test.conf")))

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  "RaftProcessor" must {

//    "transition from Initializing to Follower" in {
//      val ref = TestFSMRef(new RaftProcessor(self, self, 1 second, 1 second, 1 second, 10))
//      val processor: TestActorRef[RaftProcessor] = ref
//      ref.stateName.isInstanceOf[Initializing.type] should === (true)
//      processor ! StartProcessing
//      ref.stateName.isInstanceOf[Follower.type] should === (true)
//    }
//
//    "transition from Follower to Candidate if electionTimeout fires" in {
//      val ref = TestFSMRef(new RaftProcessor(self, self, 1 second, 1 second, 1 second, 10))
//      val processor: TestActorRef[RaftProcessor] = ref
//      processor ! StartProcessing
//      ref.isStateTimerActive should === (true)
//      val slop = 2000 // 2 seconds
//      Thread.sleep(processor.underlyingActor.electionTimeout.toMillis + slop)
//      ref.stateName.isInstanceOf[Candidate.type] should === (true)
//    }

    val idleTimeout = 2 seconds
    val maxEntriesBatch = 10

    "pick a leader" in {
      val processor1 = system.actorOf(RaftProcessor.props(self, 3, RandomBoundedDuration(1 second, 1 second), idleTimeout, maxEntriesBatch))
      val processor2 = system.actorOf(RaftProcessor.props(self, 3, RandomBoundedDuration(2 seconds, 2 seconds), idleTimeout, maxEntriesBatch))
      val processor3 = system.actorOf(RaftProcessor.props(self, 3, RandomBoundedDuration(3 seconds, 3 seconds), idleTimeout, maxEntriesBatch))
      processor1 ! Configuration(Set(processor2, processor3))
      expectMsg(ProcessorTransitionEvent(Initializing, Follower))
      processor2 ! Configuration(Set(processor1, processor3))
      expectMsg(ProcessorTransitionEvent(Initializing, Follower))
      processor3 ! Configuration(Set(processor1, processor2))
      expectMsg(ProcessorTransitionEvent(Initializing, Follower))
      expectMsg(ProcessorTransitionEvent(Follower, Candidate))  // processor1 becomes candidate after election timeout
      expectMsg(ProcessorTransitionEvent(Candidate, Leader))    // processor1 becomes leader after receiving votes from processors 2 and 3
    }

    "replicate a command" in {
      val processor1 = system.actorOf(RaftProcessor.props(self, 3, RandomBoundedDuration(1 second, 1 second), idleTimeout, maxEntriesBatch))
      val processor2 = system.actorOf(RaftProcessor.props(self, 3, RandomBoundedDuration(2 seconds, 2 seconds), idleTimeout, maxEntriesBatch))
      val processor3 = system.actorOf(RaftProcessor.props(self, 3, RandomBoundedDuration(2 seconds, 2 seconds), idleTimeout, maxEntriesBatch))
      processor1 ! Configuration(Set(processor2, processor3))
      processor2 ! Configuration(Set(processor1, processor3))
      processor3 ! Configuration(Set(processor1, processor2))
      receiveN(5)
      processor1 ! TestCommand(1)
      processor1 ! TestCommand(2)
      processor1 ! TestCommand(3)
      expectMsgClass(classOf[TestResult]).lsn must be === 1
      expectMsgClass(classOf[TestResult]).lsn must be === 2
      expectMsgClass(classOf[TestResult]).lsn must be === 3
    }

    "ignore a message if it is not a Command" in {
      val processor1 = system.actorOf(RaftProcessor.props(self, 3, RandomBoundedDuration(1 second, 1 second), idleTimeout, maxEntriesBatch))
      val processor2 = system.actorOf(RaftProcessor.props(self, 3, RandomBoundedDuration(2 seconds, 2 seconds), idleTimeout, maxEntriesBatch))
      val processor3 = system.actorOf(RaftProcessor.props(self, 3, RandomBoundedDuration(2 seconds, 2 seconds), idleTimeout, maxEntriesBatch))
      processor1 ! Configuration(Set(processor2, processor3))
      processor2 ! Configuration(Set(processor1, processor3))
      processor3 ! Configuration(Set(processor1, processor2))
      receiveN(5)
      processor1 ! TestCommand(1)
      processor1 ! TestCommand(2)
      processor1 ! TestCommand(3)
      expectMsgClass(classOf[TestResult]).lsn must be === 1
      expectMsgClass(classOf[TestResult]).lsn must be === 2
      expectMsgClass(classOf[TestResult]).lsn must be === 3
    }

  }
}

class TestExecutor extends Actor with ActorLogging {
  import TestExecutor._

  var world: WorldState = WorldState(0, Map.empty)

  def receive = {
    case command: TestCommand =>
      val result = command.apply(world)
      log.debug("received {}, replying {}", command, result)
      sender ! result
  }
}

object TestExecutor {
  import com.syntaxjockey.smr.{Command,Result}
  case class TestCommand(lsn: Int) extends Command { def apply(world: WorldState) = Success(WorldStateResult(world, TestResult(lsn))) }
  case class TestResult(lsn: Int) extends Result
}
