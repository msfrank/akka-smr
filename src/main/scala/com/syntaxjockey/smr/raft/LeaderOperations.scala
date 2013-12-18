package com.syntaxjockey.smr.raft

import akka.actor.{ActorRef, LoggingFSM, Actor}
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

import RaftProcessor._

/*
 * "The leader accepts log entries from clients, replicates them on other servers,
 * and tells servers when it is safe to apply log entries to their state machines."
 */
trait LeaderOperations extends Actor with LoggingFSM[ProcessorState,ProcessorData] {

  implicit val ec: ExecutionContext

  // configuration
  val executor: ActorRef
  val electionTimeout: FiniteDuration
  val idleTimeout: FiniteDuration
  val applyTimeout: FiniteDuration
  val maxEntriesBatch: Int

  // persistent server state
  var currentTerm: Int
  var currentIndex: Int
  var logEntries: Vector[LogEntry]
  var votedFor: ActorRef

  // volatile server state
  var peers: Set[ActorRef]
  var commitIndex: Int
  var lastApplied: Int

  when(Leader) {

    // start synchronizing followers
    case Event(StartSynchronizing, _) =>
      val lastEntry = logEntries.lastOption.getOrElse(InitialEntry)
      val followerStates = peers.map { peer =>
        val followerState = FollowerState(peer, lastEntry.index + 1, 0, isSyncing = true, None)
        synchronizeFollower(followerState, maxEntriesBatch, idleTimeout) pipeTo self
        peer -> followerState
      }.toMap
      stay() using Leader(followerStates, Vector.empty)

    // when a new command comes in, add a log entry for it then replicate entry to followers
    case Event(command: Command, Leader(followerStates, commitQueue)) =>
      val logEntry = LogEntry(command, sender, currentIndex, currentTerm)
      logEntries = logEntries :+ logEntry
      currentIndex = currentIndex + 1
      val updatedState = followerStates.map {
        case (follower,state) if !state.isSyncing =>
          state.nextHeartbeat.foreach(_.cancel())
          synchronizeFollower(state, maxEntriesBatch, idleTimeout) pipeTo self
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
              synchronizeFollower(updatedState, maxEntriesBatch, idleTimeout) pipeTo self
              updatedState
            }
          }
          else {
            // peer did not contain entry matching prevLogIndex and prevLogTerm
            val updatedState = FollowerState(peer, rpc.prevLogIndex - 1, state.matchIndex, isSyncing = true, Some(scheduledCall))
            synchronizeFollower(updatedState, maxEntriesBatch, idleTimeout) pipeTo self
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

  onTransition {
    case Candidate -> Leader =>
      stateData match {
        case Candidate(_, nextElection) => nextElection.cancel()
        case _ => // do nothing
      }
      log.debug("election complete, we become the new leader")
      self ! IdleTimeout
  }

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
