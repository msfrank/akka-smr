package com.syntaxjockey.smr.raft

import akka.actor._
import scala.concurrent.duration._

import com.syntaxjockey.smr._
import RaftProcessor.{ProcessorState,ProcessorData}

/**
 * marker trait to identify raft processor messages
 */
sealed trait RaftProcessorMessage

/**
 *
 */
class RaftProcessor(val executor: ActorRef,
                    val monitor: ActorRef,
                    val electionTimeout: FiniteDuration,
                    val idleTimeout: FiniteDuration,
                    val applyTimeout: FiniteDuration,
                    val maxEntriesBatch: Int)
  extends Actor with LoggingFSM[ProcessorState,ProcessorData] with FollowerOperations with CandidateOperations with LeaderOperations {
  import RaftProcessor._

  val ec = context.dispatcher

  // persistent server state
  var currentTerm: Int = 0
  var logEntries: Vector[LogEntry] = Vector(InitialEntry)
  var votedFor: ActorRef = ActorRef.noSender

  // volatile server state
  var peers: Set[ActorRef] = Set.empty
  var commitIndex: Int = 0
  var lastApplied: Int = 0

  startWith(Initializing, Initializing(Vector.empty))

  /*
   * Initializing is a special state not described in the Raft paper.  the RaftProcessor
   * FSM starts in Initializing and waits for a StartProcessing message. Once received,
   * the FSM moves to Follower state and never transitions to Initializing again.
   */
  when(Initializing) {
    case Event(StartProcessing(_peers), Initializing(buffered)) =>
      log.debug("starting processing with peers {}", _peers)
      peers = _peers
      // redeliver any buffered messages
      buffered.foreach { case (msg,_sender) => self.tell(msg, _sender) }
      goto(Follower) using Follower(None) forMax electionTimeout
    case Event(msg, Initializing(buffered)) =>
      log.debug("buffering message {}", msg)
      stay() using Initializing(buffered :+ msg -> sender)
  }

  initialize()

}

object RaftProcessor {

  def props(executor: ActorRef,
            monitor: ActorRef,
            electionTimeout: FiniteDuration = 500.milliseconds,
            idleTimeout: FiniteDuration = 20.milliseconds,
            applyTimeout: FiniteDuration = 10.seconds,
            batchSize: Int = 10) = {
    Props(classOf[RaftProcessor], executor, monitor, electionTimeout, idleTimeout, applyTimeout, batchSize)
  }

  // helper classes
  case class LogEntry(command: Command, caller: ActorRef, index: Int, term: Int)
  case class FollowerState(follower: ActorRef, nextIndex: Int, matchIndex: Int, isSyncing: Boolean, nextHeartbeat: Option[Cancellable])
  case class CommandResponse(result: Result, command: Command, logEntry: LogEntry)

  case object NullCommand extends Command
  val InitialEntry = LogEntry(NullCommand, ActorRef.noSender, 0, 0)

  // FSM state
  sealed trait ProcessorState
  case object Initializing extends ProcessorState
  case object Follower extends ProcessorState
  case object Candidate extends ProcessorState
  case object Leader extends ProcessorState

  // FSM data
  sealed trait ProcessorData
  case class Initializing(buffered: Vector[(Any,ActorRef)]) extends ProcessorData
  case class Follower(leader: Option[ActorRef]) extends ProcessorData
  case class Candidate(votesReceived: Set[ActorRef], nextElection: Cancellable) extends ProcessorData
  case class Leader(followerStates: Map[ActorRef,FollowerState], commitQueue: Vector[LogEntry]) extends ProcessorData

  // raft RPC messages
  sealed trait RPC extends RaftProcessorMessage
  sealed trait RPCResult extends RaftProcessorMessage
  case class RPCResponse(result: RPCResult, command: RPC, remote: ActorRef)
  case class RequestVoteRPC(term: Int, lastLogIndex: Int, lastLogTerm: Int) extends RPC
  case class RequestVoteResult(term: Int, voteGranted: Boolean) extends RPCResult
  case class AppendEntriesRPC(term: Int, prevLogIndex: Int, prevLogTerm: Int, entries: Vector[LogEntry], leaderCommit: Int) extends RPC
  case class AppendEntriesResult(term: Int, hasEntry: Boolean) extends RPCResult

  // internal messages
  case object StartFollowing extends RaftProcessorMessage
  case object StartElection extends RaftProcessorMessage
  case object SynchronizeInitial extends RaftProcessorMessage
  case object ApplyCommitted extends RaftProcessorMessage
  case object ElectionTimeout extends RaftProcessorMessage
  case class IdleTimeout(peer: ActorRef) extends RaftProcessorMessage

  // exceptions
  case class RPCFailure(cause: Throwable) extends Exception("RPC failed", cause) with RPCResult
  case class LeaderTermExpired(currentTerm: Int, leader: ActorRef) extends Exception("Leader term has expired, new term is " + currentTerm)
  case class CommandFailed(cause: Throwable, command: Command) extends Exception("Command failed to execute", cause) with Result
  case class NotLeader(command: Command) extends Exception("Processor is not the current leader")
}

/**
 *
 */
case class StartProcessing(peers: Set[ActorRef])

// events
sealed trait RaftProcessorEvent
case class ProcessorTransitionEvent(processor: ActorRef, prevState: ProcessorState, newState: ProcessorState) extends RaftProcessorEvent
case class LeaderElectionEvent(leader: ActorRef, term: Int) extends RaftProcessorEvent
