package com.syntaxjockey.smr.raft

import akka.actor.{ActorRef, LoggingFSM, Actor}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.{Success,Failure}

import com.syntaxjockey.smr._
import com.syntaxjockey.smr.command._
import com.syntaxjockey.smr.log._
import com.syntaxjockey.smr.world.{Configuration, World}
import RaftProcessor._

/*
 * "The leader accepts log entries from clients, replicates them on other servers,
 * and tells servers when it is safe to apply log entries to their state machines."
 */
trait LeaderOperations extends Actor with LoggingFSM[ProcessorState,ProcessorData] {

  implicit val ec: ExecutionContext

  // configuration
  val monitor: ActorRef
  val electionTimeout: RandomBoundedDuration
  val idleTimeout: FiniteDuration
  val maxEntriesBatch: Int
  val minimumProcessors: Int

  // persistent server state
  val logEntries: Log
  var currentTerm: Int
  var votedFor: ActorRef

  // volatile server state
  var commitIndex: Int
  var lastApplied: Int

  var world: World

  when(Leader) {

    // synchronize any initializing peers (not syncing, no heartbeat scheduled)
    case Event(SynchronizeInitial, Leader(followerStates, commitQueue)) =>
      log.info("performing initial synchronization from leader to peers")
      val updatedStates = followerStates.map {
        case (follower,state) if state.inFlight.isEmpty && state.nextHeartbeat.isEmpty =>
          val appendEntries = appendEntriesFor(state)
          follower ! appendEntries
          val scheduledCall = context.system.scheduler.scheduleOnce(idleTimeout, self, IdleTimeout(follower))
          follower -> FollowerState(follower, state.nextIndex, state.matchIndex, Some(appendEntries), Some(scheduledCall))
        case entry => entry
      }
      stay() using Leader(updatedStates, commitQueue)

    // if cluster size drops below minimumProcessors, move to Incubating
    case Event(config: Configuration, _) if config.peers.size < minimumProcessors =>
      goto(Incubating) using NoData

    // we move to a transitional configuration
    case Event(config: Configuration, Leader(followerStates, commitQueue)) =>
      world.appendConfiguration(config)
      self ! ConfigurationCommand(config)
      log.debug("extended configuration =>\n{}",
        world.configurations.map { "  state:\n" + _.peers.map("    " + _.path).mkString("\n") }.mkString("\n")
      )
      // add configuration peers to followers map if they are not in the current followers map
      val addedStates = config.peers.filter { peer => !followerStates.contains(peer) }.map { follower =>
        val lastEntry = logEntries.lastOption.getOrElse(InitialEntry)
        val tmp = FollowerState(follower, lastEntry.index + 1, 0, None, None)
        val appendEntries = appendEntriesFor(tmp)
        follower ! appendEntries
        val scheduledCall = context.system.scheduler.scheduleOnce(idleTimeout, self, IdleTimeout(follower))
        follower -> FollowerState(follower, tmp.nextIndex, tmp.matchIndex, Some(appendEntries), Some(scheduledCall))
      }
      // remove followers from followers map if they are not in the latest configuration set
      val removedStates = followerStates.keys.filter { follower => !config.peers.contains(follower) }
      val currentStates = (followerStates -- removedStates) ++ addedStates
      log.debug("followers set becomes:\n{}", currentStates.keys.map("  " + _).mkString("\n"))
      stay() using Leader(currentStates, commitQueue)

    // synchronize peers which check in after transition to leader
    case Event(voteResult: RequestVoteResult, Leader(followerStates, commitQueue)) =>
      if (voteResult.term == currentTerm) {
        log.debug("RESULT {} from {}", voteResult, sender().path)
        val lastEntry = logEntries.lastOption.getOrElse(InitialEntry)
        val follower = sender()
        val tmp = FollowerState(follower, lastEntry.index + 1, 0, None, None)
        val appendEntries = appendEntriesFor(tmp)
        follower ! appendEntries
        val scheduledCall = context.system.scheduler.scheduleOnce(idleTimeout, self, IdleTimeout(follower))
        val updatedState = follower -> FollowerState(follower, tmp.nextIndex, tmp.matchIndex, Some(appendEntries), Some(scheduledCall))
        stay() using Leader(followerStates + updatedState, commitQueue)
      } else {
        log.debug("ignoring spurious RPC {} from {}", voteResult, sender().path)
        stay()
      }

    // when a new command comes in, add a log entry for it then replicate entry to followers
    case Event(command: Command, Leader(followerStates, commitQueue)) =>
      val logEntry = LogEntry(command, sender(), logEntries.length, currentTerm)
      logEntries.append(logEntry)
      log.debug("appending log entry {}", logEntry)
      val updatedStates = followerStates.map {
        case (follower,state) if state.inFlight.isEmpty =>
          state.nextHeartbeat.foreach(_.cancel())
          val appendEntries = appendEntriesFor(state)
          val scheduledCall = context.system.scheduler.scheduleOnce(idleTimeout, self, IdleTimeout(follower))
          follower -> FollowerState(follower, state.nextIndex, state.matchIndex, Some(appendEntries), Some(scheduledCall))
        case entry => entry
      }
      stay() using Leader(followerStates ++ updatedStates, commitQueue) replying CommandAccepted(logEntry)

    // log replication succeeded
    case Event(result @ AppendEntriesAccepted(term, prevEntry, lastEntry), Leader(followerStates, commitQueue)) =>
      val peer = sender()
      log.debug("RESULT {} from {}", result, peer.path)
      followerStates.get(peer) match {
        case Some(state) =>
          state.nextHeartbeat.foreach(_.cancel())
          val updatedState = if (logEntries.last.index == lastEntry.index && logEntries.last.term == lastEntry.term) {
            // if peer is caught up, then schedule an idle timeout
            val scheduledCall = context.system.scheduler.scheduleOnce(idleTimeout, self, IdleTimeout(peer))
            peer -> FollowerState(peer, lastEntry.index + 1, lastEntry.index, None, Some(scheduledCall))
          } else {
            // otherwise immediately try to replicate the next log entry
            val tmp = FollowerState(peer, lastEntry.index + 1, lastEntry.index, None, None)
            val appendEntries = appendEntriesFor(tmp)
            peer ! appendEntries
            val scheduledCall = context.system.scheduler.scheduleOnce(idleTimeout, self, IdleTimeout(peer))
            peer -> FollowerState(peer, tmp.nextIndex, tmp.matchIndex, Some(appendEntries), Some(scheduledCall))
          }
          val updatedStates = followerStates + updatedState
          // check whether any log entries can be committed
          val updatedQueue: Vector[LogEntry] = if (lastEntry.index > state.matchIndex) {
            val updatedQueue = commitQueue.lastOption match {
              case Some(lastQueued) if lastEntry.index > lastQueued.index && prevEntry.index < lastQueued.index =>
                nextEntriesToCommit(lastQueued.index + 1, updatedStates, commitQueue)
              case _ =>
                nextEntriesToCommit(commitIndex + 1, updatedStates, commitQueue)
            }
            if (updatedQueue.length > 0)
              log.info("adding {} to commit queue", updatedQueue)
            updatedQueue
          } else Vector.empty
          // apply committed entries if we are not already
          if (commitQueue.isEmpty && !updatedQueue.isEmpty)
            self ! ApplyCommitted
          stay() using Leader(updatedStates, commitQueue ++ updatedQueue)
        case None =>
          log.debug("ignoring spurious RPC {} from unknown follower {}", result, sender().path)
          stay()
    }

    // peer did not contain entry matching prevLogIndex and prevLogTerm
    case Event(result @ AppendEntriesRejected(term, LogPosition(prevLogIndex, prevLogTerm)), Leader(followerStates, commitQueue)) =>
      val peer = sender()
      log.debug("RESULT {} from {}", result, peer.path)
      followerStates.get(peer) match {
        case Some(state) =>
          state.nextHeartbeat.foreach(_.cancel())
          val tmp = FollowerState(peer, prevLogIndex - 1, state.matchIndex, None, None)
          val appendEntries = appendEntriesFor(tmp)
          peer ! appendEntries
          val scheduledCall = context.system.scheduler.scheduleOnce(idleTimeout, self, IdleTimeout(peer))
          val updatedStates = followerStates + (peer -> FollowerState(peer, tmp.nextIndex, tmp.matchIndex, Some(appendEntries), Some(scheduledCall)))
          stay() using Leader(updatedStates, commitQueue)
        case None =>
          log.debug("ignoring spurious RPC {} from unknown follower {}", result, sender().path)
          stay()
      }

    // update the commit index and apply the command
    case Event(ApplyCommitted, Leader(followerStates, commitQueue)) =>
      val logEntry = commitQueue.head
      val caller = logEntry.caller
      commitIndex = logEntry.index
      log.info("committed log entry {}", logEntry)
      // mark the log entry as applied and pass the command result to the caller
      lastApplied = logEntry.index
      logEntry.command.apply(world) match {
        case Success(Response(updated, result, notifications)) =>
          world = updated
          log.debug("EXEC {} returns {}", logEntry.command, result)
          // signal any outstanding watches
          if (notifications.nonEmpty) {
            // FIXME: should probably send to followers, not peers, but followers contains the wrong ref
            // FIXME: do we send notification to all peers in the transitional state, or just the last state?
            val peers = world.processors - self + monitor
            peers.foreach { peer =>
              log.debug("NOTIFY mutation generated events {}", notifications)
              peer ! NotificationMap(notifications)
            }
          }
          logEntry.command match {
            // if configuration changed, then send event to monitor
            case ConfigurationCommand(config) =>
              log.debug("merged configuration =>\n{}",
                updated.configurations.map { "  state:\n" + _.peers.map("    " + _.path).mkString("\n") }.mkString("\n")
              )
              // FIXME: what if peer has gone away and returned?
              if (config.peers.contains(self))
                monitor ! SMRClusterChangedEvent
            // otherwise respond to the caller with the command result
            case _ =>
              caller ! CommandExecuted(logEntry, result)
          }
        case Failure(ex) =>
          log.debug("EXEC {} failed: {}", logEntry.command, ex)
          logEntry.caller ! CommandExecuted(logEntry, new CommandFailed(ex, logEntry.command))
      }
      // if there are more log entries in the queue, then start committing the next one
      val updatedQueue = commitQueue.tail
      if (updatedQueue.nonEmpty)
        self ! ApplyCommitted
      stay() using Leader(followerStates, updatedQueue)

    // send heartbeat
    case Event(IdleTimeout(follower), Leader(followerStates, commitQueue)) =>
      val updatedState = followerStates.get(follower) match {
        case Some(state) =>
          val appendEntries = appendEntriesFor(state)
          follower ! appendEntries
          val scheduledCall = context.system.scheduler.scheduleOnce(idleTimeout, self, IdleTimeout(follower))
          val updatedState = FollowerState(follower, state.nextIndex, state.matchIndex, Some(appendEntries), Some(scheduledCall))
          Map(follower -> updatedState)
        case None =>
          Map.empty[ActorRef,FollowerState]
      }
      stay() using Leader(followerStates ++ updatedState, commitQueue)

    // our term has expired, however we don't know who the new leader is yet
    case Event(expired: LeaderTermExpired, _) =>
      if (expired.currentTerm > currentTerm) {
        currentTerm = expired.currentTerm
        log.debug("{} says leader term has expired, new term is {}", sender().path, currentTerm)
        goto(Follower) using Follower(None)
      } else stay()

    // a new leader has been elected
    case Event(appendEntries: AppendEntriesRPC, _) =>
      log.debug("RPC {} from {}", appendEntries, sender().path)
      if (appendEntries.term > currentTerm) {
        log.info("a new leader has been discovered with term {}", appendEntries.term)
        goto(Follower) using Follower(Some(sender()))
      } else {
        log.debug("ignoring spurious RPC {} from {}", appendEntries, sender().path)
        stay()
      }

    // a peer is requesting an election
    case Event(requestVote: RequestVoteRPC, _) =>
      if (requestVote.term > currentTerm) {
        log.debug("RPC {} from {}", requestVote, sender().path)
        currentTerm = requestVote.term
        goto(Follower) using Follower(None)
      } else {
        log.debug("ignoring spurious RPC {} from {}", requestVote, sender().path)
        stay()
      }
  }

  onTransition {
    case transition @ Candidate -> Leader =>
      monitor ! ProcessorTransitionEvent(transition._1, transition._2)
      monitor ! LeaderElectionEvent(self, currentTerm)
      log.debug("election complete, we become the new leader")
      self ! SynchronizeInitial

    case transition @ _ -> Leader =>
      //monitor ! ProcessorTransitionEvent(transition._1, transition._2)
      //monitor ! LeaderElectionEvent(self, currentTerm)
      log.error("incorrectly transitioned from {} to {}", transition._1, transition._2)
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
   * Given the specified follower state, construct an AppendEntries RPC message.
   */
  def appendEntriesFor(follower: FollowerState): AppendEntriesRPC = {
    if (logEntries.length > follower.nextIndex) {
      val numBehind = logEntries.length - follower.nextIndex
      val until = if (numBehind < maxEntriesBatch) follower.nextIndex + numBehind else follower.nextIndex + maxEntriesBatch
      val entries = logEntries.slice(follower.nextIndex - 1, until + 1).toVector
      val prevEntry = entries.head
      val currEntries = entries.tail
      AppendEntriesRPC(currentTerm, prevEntry.index, prevEntry.term, currEntries, commitIndex)
    } else {
      val lastEntry = logEntries.last
      AppendEntriesRPC(currentTerm, lastEntry.index, lastEntry.term, Vector.empty, commitIndex)
    }
  }
}
