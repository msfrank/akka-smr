package com.syntaxjockey.smr.raft

import akka.actor.{ActorRef, LoggingFSM, Actor}
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{TimeoutException, ExecutionContext, Future}

import com.syntaxjockey.smr.{Command,Result,CommandFailed}
import RaftProcessor._

/*
 * "The leader accepts log entries from clients, replicates them on other servers,
 * and tells servers when it is safe to apply log entries to their state machines."
 */
trait LeaderOperations extends Actor with LoggingFSM[ProcessorState,ProcessorData] {

  implicit val ec: ExecutionContext

  // configuration
  val executor: ActorRef
  val monitor: ActorRef
  val electionTimeout: FiniteDuration
  val idleTimeout: FiniteDuration
  val applyTimeout: FiniteDuration
  val maxEntriesBatch: Int

  // persistent server state
  var currentTerm: Int
  var logEntries: Vector[LogEntry]
  var votedFor: ActorRef

  // volatile server state
  var peers: Set[ActorRef]
  var commitIndex: Int
  var lastApplied: Int

  when(Leader) {

    // synchronize any initializing peers (not syncing, no heartbeat scheduled)
    case Event(SynchronizeInitial, Leader(followerStates, commitQueue)) =>
      val lastEntry = logEntries.lastOption.getOrElse(InitialEntry)
      val updatedStates = followerStates.map {
        case (peer,state) if !state.isSyncing && state.nextHeartbeat.isDefined =>
          val updatedState = FollowerState(peer, state.nextIndex, state.matchIndex, isSyncing = true, None)
          synchronizeFollower(updatedState) pipeTo self
          peer -> updatedState
        case entry => entry
      }
      stay() using Leader(updatedStates, commitQueue)

    // when a new command comes in, add a log entry for it then replicate entry to followers
    case Event(command: Command, Leader(followerStates, commitQueue)) =>
      val logEntry = LogEntry(command, sender, logEntries.length, currentTerm)
      logEntries = logEntries :+ logEntry
      log.debug("received command, appending log entry {}", logEntry)
      val updatedStates = followerStates.map {
        case (follower,state) if !state.isSyncing =>
          state.nextHeartbeat.foreach(_.cancel())
          val updatedState = FollowerState(follower, state.nextIndex, state.matchIndex, isSyncing = true, None)
          synchronizeFollower(state) pipeTo self
          follower -> updatedState
        case entry => entry
      }
      stay() using Leader(followerStates ++ updatedStates, commitQueue)

    // record the result of log replication
    case Event(RPCResponse(result: AppendEntriesResult, rpc: AppendEntriesRPC, peer), Leader(followerStates, commitQueue)) =>
      val updatedState = followerStates.get(peer) match {
        case Some(state) =>
          state.nextHeartbeat.foreach(_.cancel())
          val updatedState = if (result.hasEntry) {
            rpc.entries.lastOption match {
              // if there are no entries (this is a heartbeat)
              case None =>
                // FIXME: is nextIndex, matchIndex correct in this case?
                val scheduledCall = context.system.scheduler.scheduleOnce(idleTimeout, self, IdleTimeout(peer))
                FollowerState(peer, state.nextIndex, state.matchIndex, isSyncing = false, Some(scheduledCall))
              // if peer is caught up, then schedule an idle timeout
              case Some(lastEntry) if logEntries.last.index == lastEntry.index && logEntries.last.term == lastEntry.term =>
                val scheduledCall = context.system.scheduler.scheduleOnce(idleTimeout, self, IdleTimeout(peer))
                FollowerState(peer, lastEntry.index + 1, lastEntry.index, isSyncing = false, Some(scheduledCall))
              // otherwise immediately try to replicate the next log entry
              case Some(lastEntry) =>
                val updatedState = FollowerState(peer, lastEntry.index + 1, lastEntry.index, isSyncing = true, None)
                synchronizeFollower(updatedState) pipeTo self
                updatedState
            }
          }
          else {
            // peer did not contain entry matching prevLogIndex and prevLogTerm
            val updatedState = FollowerState(peer, rpc.prevLogIndex - 1, state.matchIndex, isSyncing = true, None)
            synchronizeFollower(updatedState) pipeTo self
            updatedState
          }
          Map(peer -> updatedState)
        // no state for peer
        case None =>
          Map.empty[ActorRef,FollowerState]
      }
      val updatedStates = followerStates ++ updatedState
      // check whether any log entries can be committed
      val updatedQueue: Vector[LogEntry] = if (result.hasEntry && rpc.entries.length > 0) {
        val updatedQueue = commitQueue.lastOption match {
          case Some(lastQueued) if rpc.entries.last.index > lastQueued.index && rpc.entries.head.index <= lastQueued.index =>
            nextEntriesToCommit(lastQueued.index + 1, updatedStates, commitQueue)
          case _ =>
            nextEntriesToCommit(commitIndex + 1, updatedStates, commitQueue)
        }
        if (updatedQueue.length > 0)
          log.debug("adding {} to commit queue", updatedQueue)
        updatedQueue
      } else Vector.empty
      // apply committed entries if we are not already
      if (commitQueue.isEmpty && !updatedQueue.isEmpty)
        self ! ApplyCommitted
      stay() using Leader(updatedStates, commitQueue ++ updatedQueue)

    // update the commit index and begin applying the command
    case Event(ApplyCommitted, Leader(followerStates, commitQueue)) =>
      val logEntry = commitQueue.head
      commitIndex = logEntry.index
      log.debug("committed log entry {}", logEntry)
      executeCommand(logEntry) pipeTo self
      stay()

    // mark the log entry as applied and pass the command result to the client
    case Event(CommandResponse(result, _, LogEntry(_, caller, index, _)), Leader(followerStates, commitQueue)) =>
      log.debug("received command result {} from executor", result)
      caller ! result
      lastApplied = index
      // if there are more log entries in the queue, then start committing the next one
      val updatedQueue = commitQueue.tail
      if (!updatedQueue.isEmpty)
        self ! ApplyCommitted
      stay() using Leader(followerStates, updatedQueue)

    // send heartbeat
    case Event(IdleTimeout(follower), Leader(followerStates, commitQueue)) =>
      val updatedState = followerStates.get(follower) match {
        case Some(state) =>
          val updatedState = FollowerState(follower, state.nextIndex, state.matchIndex, isSyncing = true, None)
          synchronizeFollower(updatedState) pipeTo self
          Map(follower -> updatedState)
        case None =>
          Map.empty[ActorRef,FollowerState]
      }
      stay() using Leader(followerStates ++ updatedState, commitQueue)

    // follower did not respond to AppendEntriesRPC
    case Event(RPCResponse(RPCFailure(ex: TimeoutException), _, follower), Leader(followerStates, commitQueue)) =>
      log.warning("follower {} is not responding", follower)
      val updatedState = followerStates.get(follower) match {
        case Some(state) =>
          val updatedState = FollowerState(follower, state.nextIndex, state.matchIndex, isSyncing = true, None)
          synchronizeFollower(updatedState) pipeTo self
          Map(follower -> updatedState)
        case None =>
          Map.empty[ActorRef,FollowerState]
      }
      stay() using Leader(followerStates ++ updatedState, commitQueue)

    // our term has expired, however we don't know who the new leader is yet
    case Event(RPCResponse(RPCFailure(ex: LeaderTermExpired), _, _), _) =>
      log.debug("leader term has expired")
      currentTerm = ex.currentTerm
      goto(Follower) using Follower(None)

    // a new leader has been elected
    case Event(appendEntries: AppendEntriesRPC, _) =>
      if (appendEntries.term > currentTerm) {
        log.debug("a new leader has been discovered with term {}", appendEntries.term)
        goto(Follower) using Follower(Some(sender))
      } else {
        log.debug("ignoring spurious RPC: {}", appendEntries)
        stay()
      }

    // a peer is requesting an election
    case Event(requestVote: RequestVoteRPC, _) =>
      if (requestVote.term > currentTerm) {
        currentTerm = requestVote.term
        goto(Follower) using Follower(None)
      } else {
        log.debug("ignoring spurious RPC: {}", requestVote)
        stay()
      }
  }

  onTransition {
    case transition @ Candidate -> Leader =>
      monitor ! transition
      stateData match {
        case Candidate(_, nextElection) => nextElection.cancel()
        case _ => // do nothing
      }
      log.debug("election complete, we become the new leader")
      self ! SynchronizeInitial

    case transition @ _ -> Leader =>
      monitor ! transition
  }

  /**
   * scan followers state, determining which log entries have been replicated to a
   * majority of followers, and return a list of log entries which may be committed.
   */
  def nextEntriesToCommit(startIndex: Int, followerStates: Map[ActorRef,FollowerState], commitQueue: Vector[LogEntry]): Vector[LogEntry] = {
    val toCheck = logEntries.drop(startIndex)
    val majority = (followerStates.size / 2) + (followerStates.size % 2)
    val lastQueued = commitQueue.lastOption
    var nextEntries = Vector.empty[LogEntry]
    for (logEntry <- toCheck) {
      val numMatching = followerStates.values.foldLeft(0) {
        case (curr, state) =>
          if (state.matchIndex >= logEntry.index) curr + 1 else curr
      }
      if (numMatching < majority)
        return nextEntries
      if (lastQueued.isEmpty || logEntry.index > lastQueued.get.index)
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
  def sendAppendEntries(appendEntries: AppendEntriesRPC, peer: ActorRef): Future[RPCResponse] = {
    implicit val _timeout = Timeout(idleTimeout.toMillis)
    peer ? appendEntries map {
      case result: AppendEntriesResult if result.term > appendEntries.term =>
        RPCResponse(new RPCFailure(new LeaderTermExpired(result.term, peer)), appendEntries, peer)
      case result: AppendEntriesResult =>
        if (result.term < appendEntries.term)
          log.warning("peer returned expired term {} for AppendEntries RPC", result.term)
        RPCResponse(result, appendEntries, peer)
    } recover {
      case ex: Throwable => RPCResponse(new RPCFailure(ex), appendEntries, peer)
    }
  }

  /**
   *
   */
  def synchronizeFollower(followerState: FollowerState): Future[RPCResponse] = {
    val appendEntries = if (logEntries.length > followerState.nextIndex) {
      val numBehind = logEntries.length - followerState.nextIndex
      val until = if (numBehind < maxEntriesBatch) followerState.nextIndex + numBehind else followerState.nextIndex + maxEntriesBatch
      val entries = logEntries.slice(followerState.nextIndex - 1, until + 1)
      val prevEntry = entries.head
      val currEntries = entries.tail
      AppendEntriesRPC(currentTerm, prevEntry.index, prevEntry.term, currEntries, commitIndex)
    } else {
      val lastEntry = logEntries.last
      AppendEntriesRPC(currentTerm, lastEntry.index, lastEntry.term, Vector.empty, commitIndex)
    }
    sendAppendEntries(appendEntries, followerState.follower)
  }

  /**
   * sends the command in the specified log entry to the executor and returns
   * CommandCompleted with the result inside.
   */
  def executeCommand(logEntry: LogEntry): Future[CommandResponse] = {
    implicit val _timeout = Timeout(applyTimeout.toMillis)
    val command = logEntry.command
    (executor ? logEntry.command).map {
      case result: Result => CommandResponse(result, command, logEntry)
    } recover {
      case ex: Throwable => CommandResponse(new CommandFailed(ex, command), command, logEntry)
    }
  }
}
