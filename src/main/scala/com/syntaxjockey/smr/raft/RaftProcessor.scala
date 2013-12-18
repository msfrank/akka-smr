package com.syntaxjockey.smr.raft

import akka.actor._
import scala.concurrent.duration._

import RaftProcessor.{ProcessorState,ProcessorData}

/**
 *
 */
case class RaftProcessor(executor: ActorRef, electionTimeout: FiniteDuration, idleTimeout: FiniteDuration, applyTimeout: FiniteDuration, maxEntriesBatch: Int)
  extends Actor with LoggingFSM[ProcessorState,ProcessorData] with FollowerOperations with CandidateOperations with LeaderOperations {
  import RaftProcessor._

  val ec = context.dispatcher

  // cluster state
  var peers: Set[ActorRef] = Set.empty

  // persistent server state
  var currentTerm: Int = 0
  var currentIndex: Int = 1
  var logEntries: Vector[LogEntry] = Vector(InitialEntry)
  var votedFor: ActorRef = ActorRef.noSender

  // volatile server state
  var commitIndex: Int = 0
  var lastApplied: Int = 0

  startWith(Initializing, Follower(None))

  /*
   * Initializing is a special state not described in the Raft paper.  the RaftProcessor
   * FSM starts in Initializing and waits for a ProcessorSet message that describes the
   * initial cluster.  Once received, the FSM moves to Follower state and never transitions
   * to Initializing again.
   */
  when(Initializing) {
    case Event(ProcessorSet(processors), _) =>
      peers = processors - self
      log.debug("initialized processor with %i peers: %s", peers.size, peers)
      goto(Follower) forMax electionTimeout
  }

  initialize()

}

object RaftProcessor {

  def props(executor: ActorRef, electionTimeout: FiniteDuration, idleTimeout: FiniteDuration, applyTimeout: FiniteDuration, batchSize: Int) = {
    Props(classOf[RaftProcessor], executor, electionTimeout, idleTimeout, applyTimeout, batchSize)
  }

  case class LogEntry(command: Command, caller: ActorRef, index: Int, term: Int)
  case class FollowerState(follower: ActorRef, nextIndex: Int, matchIndex: Int, isSyncing: Boolean, nextHeartbeat: Option[Cancellable])
  case class CommandResponse(result: Result, command: Command, logEntry: LogEntry)

  val InitialEntry = LogEntry(NullCommand, ActorRef.noSender, 0, 0)

  // FSM state
  sealed trait ProcessorState
  case object Initializing extends ProcessorState
  case object Follower extends ProcessorState
  case object Candidate extends ProcessorState
  case object Leader extends ProcessorState

  // FSM data
  sealed trait ProcessorData
  case class Follower(leader: Option[ActorRef]) extends ProcessorData
  case class Candidate(votesReceived: Set[ActorRef], nextElection: Cancellable) extends ProcessorData
  case class Leader(followerStates: Map[ActorRef,FollowerState], commitQueue: Vector[LogEntry]) extends ProcessorData

  // raft RPC messages
  sealed trait RPC
  sealed trait RPCResult
  case class RPCResponse(result: RPCResult, command: RPC, remote: ActorRef)
  case class RequestVoteRPC(candidateTerm: Int, candidateLastIndex: Int, candidateLastTerm: Int) extends RPC
  case class RequestVoteResult(term: Int, voteGranted: Boolean) extends RPCResult
  case class AppendEntriesRPC(term: Int, prevLogIndex: Int, prevLogTerm: Int, entries: Vector[LogEntry], leaderCommit: Int) extends RPC
  case class AppendEntriesResult(term: Int, hasEntry: Boolean) extends RPCResult

  case object StartSynchronizing
  case object ApplyCommitted
  case object ElectionTimeout
  case class IdleTimeout(peer: ActorRef)

  // exceptions
  case class RPCFailure(cause: Throwable) extends Exception("RPC failed", cause) with RPCResult
  case class LeaderTermExpired(currentTerm: Int, leader: ActorRef) extends Exception("Leader term has expired, new term is " + currentTerm)
  case class CommandFailed(cause: Throwable, command: Command) extends Exception("command failed to execute", cause) with Result
}

/**
 *  marker trait for a command operation.
 */
trait Command
case object NullCommand extends Command

/**
 * marker trait for an operation result.
 */
trait Result

case class ProcessorSet(processors: Set[ActorRef])
case class AddProcessor(processor: ActorRef)
case class RemoveProcessor(processor: ActorRef)

case class Request(command: Command, caller: ActorRef)
case class Response(result: Result)