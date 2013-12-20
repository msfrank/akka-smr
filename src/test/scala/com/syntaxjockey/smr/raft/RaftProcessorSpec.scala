package com.syntaxjockey.smr.raft

import org.scalatest.{WordSpecLike, BeforeAndAfterAll, Matchers}

import akka.actor.{ActorLogging, Props, Actor, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

class RaftProcessorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {
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

    "pick a leader" in {
      val processor1 = system.actorOf(RaftProcessor.props(self, self, 1 second))
      val processor2 = system.actorOf(RaftProcessor.props(self, self, 2 second))
      val processor3 = system.actorOf(RaftProcessor.props(self, self, 3 second))
      processor1 ! StartProcessing(Set(processor2, processor3))
      expectMsg(Initializing -> Follower)
      processor2 ! StartProcessing(Set(processor1, processor3))
      expectMsg(Initializing -> Follower)
      processor3 ! StartProcessing(Set(processor1, processor2))
      expectMsg(Initializing -> Follower)
      expectMsg(Follower -> Candidate)  // processor1 becomes candidate after election timeout
      expectMsg(Candidate -> Leader)    // processor1 becomes leader after receiving votes from processors 2 and 3
    }

    "replicate a command" in {
      val executor = system.actorOf(Props[TestExecutor])
      val processor1 = system.actorOf(RaftProcessor.props(executor, self, 1 second))
      val processor2 = system.actorOf(RaftProcessor.props(executor, self, 2 second))
      val processor3 = system.actorOf(RaftProcessor.props(executor, self, 2 second))
      processor1 ! StartProcessing(Set(processor2, processor3))
      processor2 ! StartProcessing(Set(processor1, processor3))
      processor3 ! StartProcessing(Set(processor1, processor2))
      receiveN(5)
      processor1 ! TestCommand(1)
      processor1 ! TestCommand(2)
      processor1 ! TestCommand(3)
      expectMsg(TestResult(1))
      expectMsg(TestResult(2))
      expectMsg(TestResult(3))
    }
  }
}

class TestExecutor extends Actor with ActorLogging {
  import TestExecutor._
  def receive = {
    case command: TestCommand =>
      val result = TestResult(command.lsn)
      log.debug("received {}, replying {}", command, result)
      sender ! result
  }
}

object TestExecutor {
  import com.syntaxjockey.smr.{Command,Result}
  case class TestCommand(lsn: Int) extends Command
  case class TestResult(lsn: Int) extends Result
}
