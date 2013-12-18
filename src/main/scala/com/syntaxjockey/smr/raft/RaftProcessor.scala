package com.syntaxjockey.smr.raft

import akka.actor._
import akka.pattern.pipe
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.Future

import RaftProcessor.{ProcessorState,ProcessorData}
import scala.util.{Failure, Success, Try}
import akka.util.Timeout

/**
 *
 */
case class RaftProcessor(executor: ActorRef, electionTimeout: FiniteDuration, idleTimeout: FiniteDuration, applyTimeout: FiniteDuration, batchSize: Int)
  extends Actor with LoggingFSM[ProcessorState,ProcessorData] {
  import RaftProcessor._
  import context.dispatcher

  // cluster state
  var peers: Set[ActorRef] = Set.empty

  // persistent server state
  // FIXME: load from persistent storage, perhaps use akka-persistence?
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

  /*
   * "Followers are passive: they issue no RPCs on their own but simply respond to RPCs
   * from leaders and candidates. The leader handles all client requests (if a client
   * contacts a follower, the follower redirects it to the leader)."
   */
  when(Follower) {

    case Event(requestVote: RequestVoteRPC, data: Follower) =>
      log.debug("received %s", requestVote)
      stay() forMax electionTimeout

    case Event(appendEntries: AppendEntriesRPC, Follower(leaderOption)) =>
      log.debug("received %s", appendEntries)
      stay() forMax electionTimeout

    case Event(StateTimeout, follower: Follower) =>
      log.debug("received no messages for %s, election must be held", electionTimeout)
      // FIXME: randomize the election timeout
      val scheduledCall = context.system.scheduler.scheduleOnce(electionTimeout, self, ElectionTimeout)
      goto(Candidate) using Candidate(Set.empty, scheduledCall)
  }

  onTransition {

    case _ -> Candidate =>
      val nextTerm = currentTerm + 1
      val lastEntry = if (logEntries.isEmpty) InitialEntry else logEntries.last
      val vote = RequestVoteRPC(nextTerm, lastEntry.index, lastEntry.term)
      log.debug("we transition to candidate and cast vote %s", vote)
      peers.foreach(_ ! vote)
      currentTerm = nextTerm
      votedFor = self

    case Candidate -> Follower =>
      stateData match {
        case Candidate(_, scheduledCall) => scheduledCall.cancel()
        case _ => // do nothing
      }
      nextStateData match {
      case Follower(Some(leader)) =>
        log.debug("%s becomes the new leader", leader)
      case Follower(None) =>
        log.debug("we become follower, awaiting communication from new leader")
    }

  }

  /*
   * "[Candidate state] is used to elect a new leader ... If a candidate wins the
   * election, then it serves as leader for the rest of the term. In some situations
   * an election will result in a split vote. In this case the term will end with
   * no leader; a new term (with a new election) will begin shortly."
   */
  when(Candidate) {

    case Event(RequestVoteRPC(candidateTerm, candidateLastIndex, candidateLastTerm), Candidate(_, scheduledCall)) if candidateTerm > currentTerm =>
      // we are not in the current term, so withdraw our candidacy
      currentTerm = candidateTerm
      goto(Follower) using Follower(None)

    case Event(RequestVoteRPC(candidateTerm, candidateLastIndex, candidateLastTerm), _) =>
      val candidate = sender
      // if candidate's term is older than ours, then reject the vote and send our current term
      if (candidateTerm < currentTerm)
        stay() replying RequestVoteResult(currentTerm, voteGranted = false)
      // if we have already cast our vote for someone else, then inform the candidate
      if (votedFor != candidate) {
        stay() replying RequestVoteResult(currentTerm, voteGranted = false)
      } else {
        logEntries.lastOption match {
          // if the candidate's log is behind ours, then reject the vote
          case Some(LogEntry(_, _, lastIndex, lastTerm)) if candidateLastTerm < lastTerm || (candidateLastTerm == lastTerm && candidateLastIndex < lastIndex) =>
            stay() replying RequestVoteResult(currentTerm, voteGranted = false)
          // otherwise, grant the vote
          case _ =>
            votedFor = candidate
            log.debug("granted vote to %s for term %i", candidate, candidateTerm)
            stay() replying RequestVoteResult(currentTerm, voteGranted = true)
        }
      }

    case Event(RequestVoteResult(candidateTerm, voteGranted), _) if candidateTerm > currentTerm =>
      // we are not in the current term, so withdraw our candidacy
      currentTerm = candidateTerm
      goto(Follower) using Follower(None)

    case Event(RequestVoteResult(candidateTerm, voteGranted), Candidate(currentTally, scheduledCall)) =>
      // ignore results with candidateTerm < currentTerm
      val votesReceived = if (voteGranted && candidateTerm == currentTerm) currentTally + sender else currentTally
      // if we have received a majority of votes, then become leader
      if (votesReceived.size > (peers.size / 2)) {
        val lastEntry = logEntries.lastOption.getOrElse(InitialEntry)
        val followerStates = peers.map { peer =>
          val followerState = FollowerState(peer, lastEntry.index + 1, 0, isSyncing = true, None)
          synchronizeFollower(followerState, batchSize, idleTimeout) pipeTo self
          peer -> followerState
        }.toMap
        goto(Leader) using Leader(followerStates, Vector.empty)
      } else stay() using Candidate(votesReceived, scheduledCall)

    case Event(appendEntries: AppendEntriesRPC, _) =>
      // if we receive AppendEntries with a current or newer term, then accept sender as new leader
      if (appendEntries.term >= currentTerm) {
        currentTerm = appendEntries.term
        self forward appendEntries  // reinject message for processing in Follower state
        goto(Follower) using Follower(Some(sender))
      } else {
        // otherwise ignore and notify sender of the current term
        stay() replying AppendEntriesResult(currentTerm, hasEntry = false)
      }


    case Event(ElectionTimeout, _) =>
      log.debug("election had no result")
      // FIXME: randomize the election timeout
      val scheduledCall = context.system.scheduler.scheduleOnce(electionTimeout, self, ElectionTimeout)
      goto(Candidate) using Candidate(Set.empty, scheduledCall)
  }

  onTransition {

    case Candidate -> Leader =>
      stateData match {
        case Candidate(_, scheduledCall) => scheduledCall.cancel()
        case _ => // do nothing
      }
      log.debug("election complete, we become the new leader")
      self ! IdleTimeout

    case Leader -> Follower =>
      stateData match {
        case Leader(followerStates, _) =>
          // cancel any scheduled heartbeats
          followerStates.values.foreach(state => state.nextHeartbeat.foreach(_.cancel()))
        case _ => // do nothing
      }
      nextStateData match {
        case Follower(Some(leader)) =>
          log.debug("following new leader %s", leader)
        case _ => // do nothing
      }
  }

  /*
   * "The leader accepts log entries from clients, replicates them on other servers,
   * and tells servers when it is safe to apply log entries to their state machines."
   */
  when(Leader) {

    // when a new command comes in, add a log entry for it then replicate entry to followers
    case Event(command: Command, Leader(followerStates, commitQueue)) =>
      val logEntry = LogEntry(command, sender, currentIndex, currentTerm)
      logEntries = logEntries :+ logEntry
      currentIndex = currentIndex + 1
      val updatedState = followerStates.map {
        case (follower,state) if !state.isSyncing =>
          state.nextHeartbeat.foreach(_.cancel())
          synchronizeFollower(state, batchSize, idleTimeout) pipeTo self
          val scheduledCall = context.system.scheduler.scheduleOnce(idleTimeout, self, IdleTimeout(follower))
          follower -> FollowerState(follower, state.nextIndex, state.matchIndex, isSyncing = true, Some(scheduledCall))
        case entry => entry
      }
      stay() using Leader(followerStates ++ updatedState, commitQueue)

    // record the result of log replication
    case Event(RPCResponse(result: AppendEntriesResult, rpc: AppendEntriesRPC, peer), Leader(followerStates, commitQueue)) =>
      val updatedState = followerStates.get(peer) match {
        case Some(state: FollowerState) =>
          state.nextHeartbeat.foreach(_.cancel())
          val scheduledCall = context.system.scheduler.scheduleOnce(idleTimeout, self, IdleTimeout(peer))
          val updatedState = if (result.hasEntry) {
            val lastEntry = rpc.entries.last
            // if peer is caught up, then schedule an idle timeout
            if (logEntries.last.index == lastEntry.index && logEntries.last.term == lastEntry.term) {
              FollowerState(peer, lastEntry.index + 1, lastEntry.index, isSyncing = false, Some(scheduledCall))
            }
            // otherwise immediately try to replicate the next log entry
            else {
              val updatedState = FollowerState(peer, lastEntry.index + 1, lastEntry.index, isSyncing = true, Some(scheduledCall))
              synchronizeFollower(updatedState, batchSize, idleTimeout) pipeTo self
              updatedState
            }
          }
          else {
            // peer did not contain entry matching prevLogIndex and prevLogTerm
            val updatedState = FollowerState(peer, rpc.prevLogIndex - 1, state.matchIndex, isSyncing = true, Some(scheduledCall))
            synchronizeFollower(updatedState, batchSize, idleTimeout) pipeTo self
            updatedState
          }
          Map(peer -> updatedState)
        // no state for peer
        case None =>
          Map.empty
      }
      // check whether any log entries can be committed
      val updatedQueue: Vector[LogEntry] = if (result.hasEntry && rpc.entries.length > 0) {
        commitQueue.lastOption match {
          case Some(lastQueued) if rpc.entries.last.index > lastQueued.index && rpc.entries.head.index <= lastQueued.index =>
            nextEntriesToCommit(logEntries, lastQueued.index + 1, followerStates)
          case None =>
            Vector.empty
        }
      } else Vector.empty
      // apply committed entries if we are not already
      if (commitQueue.isEmpty)
        self ! ApplyCommitted
      stay() using Leader(followerStates ++ updatedState, commitQueue ++ updatedQueue)

    // update the commit index and begin applying the command
    case Event(ApplyCommitted, Leader(followerStates, commitQueue)) =>
      val logEntry = commitQueue.head
      commitIndex = logEntry.index
      executeCommand(logEntry, applyTimeout) pipeTo self
      stay()

    // mark the log entry as applied and pass the command result to the client
    case Event(CommandResponse(result, _, LogEntry(_, caller, index, _)), Leader(followerStates, commitQueue)) =>
      caller ! result
      lastApplied = index
      // if there are more log entries in the queue, then start committing the next one
      val updatedQueue = commitQueue.tail
      if (!updatedQueue.isEmpty)
        self ! ApplyCommitted
      stay() using Leader(followerStates, updatedQueue)

    // our term has expired, however we don't know who the new leader is yet
    case Event(RPCResponse(RPCFailure(ex: LeaderTermExpired), _, _), _) =>
      currentTerm = ex.currentTerm
      goto(Follower) using Follower(None)

    // a new leader has been elected
    case Event(appendEntries: AppendEntriesRPC, _) =>
      if (appendEntries.term > currentTerm) {
        log.debug("a new leader has been discovered with term %i", appendEntries.term)
        goto(Follower) using Follower(Some(sender))
      } else {
        log.debug("ignoring spurious RPC: %s", appendEntries)
        stay()
      }

  }

  initialize()

  /**
   * scan followers state, determining which log entries have been replicated to a
   * majority of followers, and return a list of log entries which may be committed.
   */
  def nextEntriesToCommit(logEntries: Vector[LogEntry], startIndex: Int, followerStates: Map[ActorRef,FollowerState]): Vector[LogEntry] = {
    var nextEntries = Vector.empty[LogEntry]
    for (logEntry <- logEntries.takeRight(startIndex)) {
      val numMatching = followerStates.values.foldLeft(0) { case (curr, state) => if (state.matchIndex >= logEntry.index) curr + 1 else curr }
      if (numMatching <= followerStates.size / 2)
        return nextEntries
      nextEntries = nextEntries :+ logEntry
    }
    nextEntries
  }

  /**
   * sends an AppendEntries RPC to the specified peer and returns a Future containing
   * the result or an exception.  if the peer returns a term that is greater than the
   * term specified in AppendEntries.term, we return a Failure with a LeaderTermExpired
   * exception inside.
   */
  def sendAppendEntries(appendEntries: AppendEntriesRPC, peer: ActorRef, timeout: FiniteDuration): Future[RPCResponse] = {
    implicit val _timeout = Timeout(timeout.toMillis)
    peer ? appendEntries map {
      case result: AppendEntriesResult if result.term > appendEntries.term =>
        RPCResponse(new RPCFailure(new LeaderTermExpired(result.term, peer)), appendEntries, peer)
      case result: AppendEntriesResult =>
        if (result.term < appendEntries.term)
          log.warning("peer returned expired term %i for AppendEntries RPC", result.term)
        RPCResponse(result, appendEntries, peer)
    } recover {
      case ex: Throwable => RPCResponse(new RPCFailure(ex), appendEntries, peer)
    }
  }

  /**
   *
   */
  def synchronizeFollower(followerState: FollowerState, maxBatch: Int, timeout: FiniteDuration): Future[RPCResponse] = {
    val appendEntries = if (logEntries.length > followerState.nextIndex) {
      val numBehind = logEntries.length - followerState.nextIndex
      val until = if (numBehind < maxBatch) followerState.nextIndex + numBehind else followerState.nextIndex + maxBatch
      val entries = logEntries.slice(followerState.nextIndex - 1, until + 1)
      val prevEntry = entries.head
      val currEntries = entries.tail
      AppendEntriesRPC(currentTerm, prevEntry.index, prevEntry.term, currEntries, commitIndex)
    } else {
      val lastEntry = logEntries.last
      AppendEntriesRPC(currentTerm, lastEntry.index, lastEntry.term, Vector.empty, commitIndex)
    }
    sendAppendEntries(appendEntries, followerState.follower, timeout)
  }

  /**
   * sends the command in the specified log entry to the executor and returns
   * CommandCompleted with the result inside.
   */
  def executeCommand(logEntry: LogEntry, timeout: FiniteDuration): Future[CommandResponse] = {
    implicit val _timeout = Timeout(timeout.toMillis)
    val command = logEntry.command
    (executor ? logEntry.command).map {
      case result: Result => CommandResponse(result, command, logEntry)
    } recover {
      case ex: Throwable => CommandResponse(new CommandFailed(ex, command), command, logEntry)
    }
  }

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
  case class Candidate(votesReceived: Set[ActorRef], scheduledCall: Cancellable) extends ProcessorData
  case class Leader(followerStates: Map[ActorRef,FollowerState], commitQueue: Vector[LogEntry]) extends ProcessorData

  // raft RPC messages
  sealed trait RPC
  sealed trait RPCResult
  case class RPCResponse(result: RPCResult, command: RPC, remote: ActorRef)
  case class RequestVoteRPC(candidateTerm: Int, candidateLastIndex: Int, candidateLastTerm: Int) extends RPC
  case class RequestVoteResult(term: Int, voteGranted: Boolean) extends RPCResult
  case class AppendEntriesRPC(term: Int, prevLogIndex: Int, prevLogTerm: Int, entries: Vector[LogEntry], leaderCommit: Int) extends RPC
  case class AppendEntriesResult(term: Int, hasEntry: Boolean) extends RPCResult
  case class RPCFailure(cause: Throwable) extends Exception("RPC failed", cause) with RPCResult

  case object ElectionTimeout
  case class IdleTimeout(peer: ActorRef)
  case object ApplyCommitted

  class LeaderTermExpired(val currentTerm: Int, val leader: ActorRef) extends Exception("Leader term has expired, new term is " + currentTerm)
  class CommandFailed(cause: Throwable, val command: Command) extends Exception("command failed to execute", cause) with Result
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