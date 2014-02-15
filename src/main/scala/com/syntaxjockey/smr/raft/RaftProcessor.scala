package com.syntaxjockey.smr.raft

import akka.actor._
import akka.actor.OneForOneStrategy
import scala.concurrent.duration._
import scala.util.{Try,Success}
import java.util.UUID

import com.syntaxjockey.smr.raft.RaftProcessor.{ProcessorState, ProcessorData}
import com.syntaxjockey.smr._
import com.syntaxjockey.smr.namespace.NamespacePath

/**
 * marker trait to identify raft processor messages
 */
sealed trait RaftProcessorMessage

/**
 * A single entry in the RAFT replication log.
 */
case class LogEntry(command: Command, caller: ActorRef, index: Int, term: Int)

case class LogPosition(index: Int, term: Int)

/**
 * A single processor implementing the RAFT state machine replication protocol.
 */
class RaftProcessor(val monitor: ActorRef,
                    val minimumProcessors: Int,
                    val electionTimeout: RandomBoundedDuration,
                    val idleTimeout: FiniteDuration,
                    val maxEntriesBatch: Int)
extends Actor with LoggingFSM[ProcessorState,ProcessorData] with FollowerOperations with CandidateOperations with LeaderOperations {
  import RaftProcessor._
  import SupervisorStrategy.Stop

  val ec = context.dispatcher

  // persistent server state
  var currentTerm: Int = 0
  var logEntries: Vector[LogEntry] = Vector(InitialEntry)
  var votedFor: ActorRef = ActorRef.noSender

  // volatile server state
  var peers: Set[ActorRef] = Set.empty
  var commitIndex: Int = 0
  var lastApplied: Int = 0

  var world: WorldState = WorldState.void

  startWith(Initializing, Initializing(Vector.empty))

  /*
   * Initializing is a special state not described in the Raft paper.  the RaftProcessor
   * FSM starts in Initializing and waits for a StartProcessing message. Once received,
   * the FSM moves to Follower state and never transitions to Initializing again.
   */
  when(Initializing) {

    case Event(config: Configuration, Initializing(buffered)) =>
      peers = config.peers
      if (peers.size >= minimumProcessors - 1) {
        log.debug("starting processing with peers:\n{}", peers.map("  " + _).mkString("\n"))
        // redeliver any buffered messages
        buffered.foreach { case (msg,_sender) => self.tell(msg, _sender) }
        goto(Follower) using Follower(None)
      } else stay()

    case Event(msg, Initializing(buffered)) =>
      log.debug("buffering message {}", msg)
      stay() using Initializing(buffered :+ msg -> sender)
  }

  initialize()

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 1 minute) {
    case ex: Exception => Stop
  }
}

object RaftProcessor {

  def props(monitor: ActorRef,
            minimumProcessors: Int,
            electionTimeout: RandomBoundedDuration,
            idleTimeout: FiniteDuration,
            maxEntriesBatch: Int) = {
    Props(classOf[RaftProcessor], monitor, minimumProcessors, electionTimeout, idleTimeout, maxEntriesBatch)
  }

  // helper classes
  case class FollowerState(follower: ActorRef, nextIndex: Int, matchIndex: Int, inFlight: Option[AppendEntriesRPC], nextHeartbeat: Option[Cancellable])

  case object NullCommand extends Command {
    def apply(world: WorldState): Try[WorldStateResult] = Success(WorldStateResult(world, new Result {}, Map.empty))
  }

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
  case class Candidate(votesReceived: Set[ActorRef]) extends ProcessorData
  case class Leader(followerStates: Map[ActorRef,FollowerState], commitQueue: Vector[LogEntry]) extends ProcessorData

  // raft RPC messages
  sealed trait RPC extends RaftProcessorMessage
  sealed trait RPCResult extends RaftProcessorMessage
  case class RPCResponse(result: RPCResult, command: RPC, remote: ActorRef)
  case class RequestVoteRPC(term: Int, lastLogIndex: Int, lastLogTerm: Int) extends RPC
  case class RequestVoteResult(term: Int, voteGranted: Boolean) extends RPCResult
  case class AppendEntriesRPC(term: Int, prevLogIndex: Int, prevLogTerm: Int, entries: Vector[LogEntry], leaderCommit: Int) extends RPC
  case class AppendEntriesAccepted(term: Int, prevEntry: LogPosition, lastEntry: LogPosition) extends RPCResult
  case class AppendEntriesRejected(term: Int, prevEntry: LogPosition) extends RPCResult
  case class LeaderTermExpired(currentTerm: Int) extends RPCResult

  // internal messages
  case object StartFollowing extends RaftProcessorMessage
  case object StartElection extends RaftProcessorMessage
  case object SynchronizeInitial extends RaftProcessorMessage
  case object ApplyCommitted extends RaftProcessorMessage
  case object ElectionTimeout extends RaftProcessorMessage
  case object FollowerTimeout extends RaftProcessorMessage
  case class IdleTimeout(peer: ActorRef) extends RaftProcessorMessage

  // exceptions
  case class RPCFailure(cause: Throwable) extends Exception("RPC failed", cause) with RPCResult
  case class NotLeader(command: Command) extends Exception("Processor is not the current leader")
  case class ExecutionFailed(logEntry: LogEntry, cause: Throwable) extends Exception("Failed to execute log entry", cause)
}

/**
 *
 */
case class CommandRequest(logEntry: LogEntry)

// events
sealed trait RaftProcessorEvent
case class CommandAccepted(logEntry: LogEntry) extends RaftProcessorEvent
case class CommandExecuted(logEntry: LogEntry, result: Result) extends RaftProcessorEvent
case class CommandApplied(logEntry: LogEntry) extends RaftProcessorEvent
case class NotificationMap(notifications: Map[NamespacePath,Notification])
case class RetryCommand(command: Command) extends RaftProcessorEvent
case class ProcessorTransitionEvent(prevState: ProcessorState, newState: ProcessorState) extends RaftProcessorEvent
case class LeaderElectionEvent(leader: ActorRef, term: Int) extends RaftProcessorEvent
