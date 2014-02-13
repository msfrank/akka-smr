package com.syntaxjockey.smr.raft

import akka.actor.{ActorRef, LoggingFSM, Actor}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext

import com.syntaxjockey.smr.raft.RaftProcessor._
import com.syntaxjockey.smr.{WorldStateResult, Result, WorldState, Command}
import scala.util.{Failure, Success}

/*
 * "Followers are passive: they issue no RPCs on their own but simply respond to RPCs
 * from leaders and candidates. The leader handles all client requests (if a client
 * contacts a follower, the follower redirects it to the leader)."
 */
trait FollowerOperations extends Actor with LoggingFSM[ProcessorState,ProcessorData] {

  implicit val ec: ExecutionContext

  // configuration
  val monitor: ActorRef
  val electionTimeout: RandomBoundedDuration

  // persistent server state
  var currentTerm: Int
  var logEntries: Vector[LogEntry]
  var votedFor: ActorRef

  // volatile server state
  var peers: Set[ActorRef]
  var commitIndex: Int
  var lastApplied: Int

  var world: WorldState

  when(Follower) {

    case Event(requestVote: RequestVoteRPC, data: Follower) =>
      log.debug("RPC {} from {}", requestVote, sender().path)
      val result = if (requestVote.term > currentTerm) {
        currentTerm = requestVote.term
        val lastEntry = logEntries.last
        // grant the vote if peer has up-to-date logs
        if (requestVote.lastLogTerm >= lastEntry.term && requestVote.lastLogIndex >= lastEntry.index) {
          votedFor = sender()
          RequestVoteResult(requestVote.term, voteGranted = true)
        } else RequestVoteResult(requestVote.term, voteGranted = false)
      } else if (requestVote.term == currentTerm) {
        val lastEntry = logEntries.last
        // grant the vote if peer has up-to-date logs and we have not voted at all yet
        if (votedFor == ActorRef.noSender && requestVote.lastLogTerm >= lastEntry.term && requestVote.lastLogIndex >= lastEntry.index) {
          votedFor = sender()
          RequestVoteResult(requestVote.term, voteGranted = true)
        } else RequestVoteResult(requestVote.term, voteGranted = false)
      } else RequestVoteResult(currentTerm, voteGranted = false)
      if (result.voteGranted)
        log.debug("granting vote for term {} to {}", currentTerm, votedFor.path)
      else
        log.debug("rejecting vote for term {} from {}", currentTerm, sender().path)
      stay() replying result forMax electionTimeout.nextDuration

    case Event(command: Command, Follower(leaderOption)) =>
      leaderOption match {
        case Some(leader) =>
          leader forward command
          log.debug("forwarding {} to {}", command, leader.path)
        case None =>
          sender() ! RetryCommand(command)
      }
      stay()

    case Event(appendEntries: AppendEntriesRPC, Follower(leaderOption)) =>
      log.debug("RPC {} from {}", appendEntries, sender().path)
      if (appendEntries.term >= currentTerm) {
        // the current term has concluded, recognize sender as the new leader
        if (appendEntries.term > currentTerm)
          currentTerm = appendEntries.term
        // there is no entry defined at prevLogIndex
        val result = if (!logEntries.isDefinedAt(appendEntries.prevLogIndex)) {
          AppendEntriesRejected(currentTerm, LogPosition(appendEntries.prevLogIndex, appendEntries.prevLogTerm))
        } else {
          val prevEntry = logEntries(appendEntries.prevLogIndex)
          // an entry exists at prevLogIndex, but it conflicts with a new one (same index but different terms)
          if (prevEntry.term != appendEntries.prevLogTerm) {
            // delete the existing entry and all that follow it
            log.debug("deleting log entries {}", logEntries.drop(appendEntries.prevLogIndex))
            logEntries = logEntries.take(appendEntries.prevLogIndex)
            AppendEntriesRejected(currentTerm, LogPosition(appendEntries.prevLogIndex, appendEntries.prevLogTerm))
          } else {
            // if there are any entries after prevLogIndex, then drop them
            if (logEntries.length > appendEntries.prevLogIndex + 1) {
              log.debug("deleting log entries {}", logEntries.drop(appendEntries.prevLogIndex + 1))
              logEntries = logEntries.take(appendEntries.prevLogIndex + 1)
            }
            // if this is not a heartbeat, append the new entries
            if (appendEntries.entries.length > 0) {
              log.debug("appending log entries {}", appendEntries.entries)
              logEntries = logEntries ++ appendEntries.entries
            }
            // update commitIndex and apply any outstanding commands
            if (appendEntries.leaderCommit > commitIndex) {
              val updatedIndex = math.min(appendEntries.leaderCommit, logEntries.last.index)
              world = logEntries.slice(commitIndex + 1, updatedIndex + 1).foldLeft(world) { case (acc, logEntry: LogEntry) =>
                logEntry.command.apply(acc) match {
                  case Success(WorldStateResult(updated, _, _)) => updated
                  case Failure(ex) => acc
                }
              }
              commitIndex = updatedIndex
              log.debug("committed up to {}", commitIndex)
            }
            val lastEntry = logEntries.last
            AppendEntriesAccepted(currentTerm,
              LogPosition(appendEntries.prevLogIndex, appendEntries.prevLogTerm),
              LogPosition(lastEntry.index, lastEntry.term))
          }
        }
        leaderOption match {
          case None => monitor ! LeaderElectionEvent(sender(), currentTerm)
          case Some(leader) if leader != sender() => monitor ! LeaderElectionEvent(sender(), currentTerm)
          case _ => // do nothing
        }
        stay() replying result using Follower(Some(sender())) forMax electionTimeout
      }
      else stay() replying LeaderTermExpired(currentTerm) forMax electionTimeout

    case Event(StateTimeout, follower: Follower) =>
      log.debug("received no messages for {}, election must be held", electionTimeout)
      // FIXME: randomize the election timeout
      val scheduledCall = context.system.scheduler.scheduleOnce(electionTimeout, self, ElectionTimeout)
      goto(Candidate) using Candidate(Set.empty, scheduledCall)
  }

  onTransition {
    case transition @ Candidate -> Follower =>
      monitor ! ProcessorTransitionEvent(transition._1, transition._2)
      stateData match {
        case Candidate(_, scheduledCall) => scheduledCall.cancel()
        case _ => // do nothing
      }
      nextStateData match {
        case Follower(Some(leader)) =>
          monitor ! LeaderElectionEvent(leader, currentTerm)
          log.debug("{} becomes the new leader", leader.path)
        case Follower(None) =>
          log.debug("we become follower, awaiting communication from new leader")
      }

    case transition @ Leader -> Follower =>
      monitor ! ProcessorTransitionEvent(transition._1, transition._2)
      stateData match {
        case Leader(followerStates, _) =>
          // cancel any scheduled heartbeats
          followerStates.values.foreach(state => state.nextHeartbeat.foreach(_.cancel()))
        case _ => // do nothing
      }
      nextStateData match {
        case Follower(Some(leader)) =>
          monitor ! LeaderElectionEvent(leader, currentTerm)
          log.debug("following new leader {}", leader.path)
        case _ => // do nothing
      }

    case transition @ _ -> Follower =>
      monitor ! ProcessorTransitionEvent(transition._1, transition._2)
  }
}
