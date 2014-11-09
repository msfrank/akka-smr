package com.syntaxjockey.smr.raft

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.UUID

import org.scalatest.{WordSpecLike, BeforeAndAfterAll}
import org.scalatest.matchers.MustMatchers
import akka.actor.{ActorRef, ActorLogging, Actor, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import scala.util.Success

import com.syntaxjockey.smr.{PongResult, PingCommand, Configuration, Response}
import com.syntaxjockey.smr.world.WorldState

class RaftProcessorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with MustMatchers with BeforeAndAfterAll {
  import RaftProcessor._
  import TestExecutor._

  def this() = this(ActorSystem("RaftProcessorSpec", ConfigFactory.load("test.conf")))

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  "RaftProcessor" must {

    val idleTimeout = 2.seconds
    val maxEntriesBatch = 10

    def withSettings(minimumProcessors: Int, electionLower: FiniteDuration, electionUpper: FiniteDuration)(testCode: RaftProcessorSettings => Any) {
      val logDirectory = Paths.get("test-raft-log.%s".format(UUID.randomUUID()))
      val settings = RaftProcessorSettings(minimumProcessors,
                                           RandomBoundedDuration(electionLower, electionUpper),
                                           idleTimeout, maxEntriesBatch, logDirectory, 0)
      try {
        Files.createDirectory(logDirectory)
        testCode(settings)
      } finally {
        Files.walkFileTree(logDirectory, new SimpleFileVisitor[Path]() {
          override def visitFile(file: Path, attrs: BasicFileAttributes) = {
            Files.delete(file)
            FileVisitResult.CONTINUE
          }
          override def postVisitDirectory(dir: Path, ex: IOException) = if (ex != null) throw ex else {
            Files.delete(dir)
            FileVisitResult.CONTINUE
          }
        })
      }
    }

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

    "pick a leader" in withSettings(3, 1 second, 1 second) { settings1 =>
                       withSettings(3, 2 seconds, 2 seconds) { settings2 =>
                       withSettings(3, 3 seconds, 3 seconds) { settings3 =>
      val processor1 = system.actorOf(RaftProcessor.props(self, settings1))
      val processor2 = system.actorOf(RaftProcessor.props(self, settings2))
      val processor3 = system.actorOf(RaftProcessor.props(self, settings3))
      processor1 ! Configuration(Set(processor2, processor3))
      processor2 ! Configuration(Set(processor1, processor3))
      processor3 ! Configuration(Set(processor1, processor2))
      within(10.seconds) {
        expectMsg(ProcessorTransitionEvent(Incubating, Follower))
        expectMsg(ProcessorTransitionEvent(Incubating, Follower))
        expectMsg(ProcessorTransitionEvent(Incubating, Follower))
        expectMsg(ProcessorTransitionEvent(Follower, Candidate))  // processor1 becomes candidate after election timeout
        expectMsg(ProcessorTransitionEvent(Candidate, Leader))    // processor1 becomes leader after receiving votes from processors 2 and 3
        expectMsg(LeaderElectionEvent(processor1, 1))
      }
    }}}

//    "replicate a command" in withSettings(3, 1 second, 1 second) { settings1 =>
//                             withSettings(3, 2 seconds, 2 seconds) { settings2 =>
//                             withSettings(3, 3 seconds, 3 seconds) { settings3 =>
//      val processor1 = system.actorOf(RaftProcessor.props(self, settings1))
//      val processor2 = system.actorOf(RaftProcessor.props(self, settings2))
//      val processor3 = system.actorOf(RaftProcessor.props(self, settings3))
//      processor1 ! Configuration(Set(processor2, processor3))
//      processor2 ! Configuration(Set(processor1, processor3))
//      processor3 ! Configuration(Set(processor1, processor2))
//      within(10.seconds) {
//        expectMsg(ProcessorTransitionEvent(Incubating, Follower))
//        expectMsg(ProcessorTransitionEvent(Incubating, Follower))
//        expectMsg(ProcessorTransitionEvent(Incubating, Follower))
//        expectMsg(ProcessorTransitionEvent(Follower, Candidate))  // processor1 becomes candidate after election timeout
//        expectMsg(ProcessorTransitionEvent(Candidate, Leader))    // processor1 becomes leader after receiving votes from processors 2 and 3
//        expectMsg(LeaderElectionEvent(processor1, 1))
//        expectMsg(LeaderElectionEvent(processor1, 1))
//        expectMsg(LeaderElectionEvent(processor1, 1))
//      }
//      processor1 ! PingCommand(Some("1"))
//      println(expectMsgClass(classOf[Any]))
//      println(expectMsgClass(classOf[Any]))
//      println(expectMsgClass(classOf[Any]))
//                               println(expectMsgClass(classOf[Any]))
//                               println(expectMsgClass(classOf[Any]))
//                               println(expectMsgClass(classOf[Any]))
//                               println(expectMsgClass(classOf[Any]))
//      //expectMsg(PongResult(Some("1")))
//    }}}
  }
}

class TestExecutor extends Actor with ActorLogging {
  import TestExecutor._

  var world: WorldState = WorldState.void

  def receive = {
    case command: TestCommand =>
      val result = command.apply(world)
      log.debug("received {}, replying {}", command, result)
      sender ! result
  }
}

object TestExecutor {
  import com.syntaxjockey.smr.{Command,Result}
  case class TestCommand(lsn: Int) extends Command { def apply(world: WorldState) = Success(Response(world, TestResult(lsn))) }
  case class TestResult(lsn: Int) extends Result
}
