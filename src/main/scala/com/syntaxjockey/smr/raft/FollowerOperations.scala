package com.syntaxjockey.smr.raft

import akka.actor._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

import com.syntaxjockey.smr.raft.RaftProcessor._
import com.syntaxjockey.smr._

/*
 * "Followers are passive: they issue no RPCs on their own but simply respond to RPCs
 * from leaders and candidates. The leader handles all client requests (if a client
 * contacts a follower, the follower redirects it to the leader)."
 */
trait FollowerOperations extends Actor with LoggingFSM[ProcessorState,ProcessorData] with Stash {

  implicit val ec: ExecutionContext

  // configuration
  val monitor: ActorRef
  val electionTimeout: RandomBoundedDuration

  // persistent server state
  var currentTerm: Int
  var logEntries: Vector[LogEntry]
  var votedFor: ActorRef

  // volatile server state
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
      setTimer("follower-timeout", FollowerTimeout, electionTimeout.nextDuration)
      stay() replying result

    case Event(command: Command, Follower(leaderOption)) =>
      leaderOption match {
        case Some(leader) =>
          leader forward command
          log.debug("forwarding {} to {}", command, leader.path)
        case None =>
          // there is no leader, so stash the command until a leader is elected
          try { stash() } catch { case ex: StashOverflowException => sender() ! CommandRejected(command) }
      }
      stay()

    case Event(appendEntries: AppendEntriesRPC, Follower(leaderOption)) =>
      log.debug("RPC {} from {}", appendEntries, sender().path)
      setTimer("follower-timeout", FollowerTimeout, electionTimeout.nextDuration)
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
              // immediately apply any configurations we find
              appendEntries.entries.foreach {
                case LogEntry(command: ConfigurationCommand, _, _, _) =>
                  world = WorldState(world.version, world.namespaces, ConfigurationState(world.config.states :+ command.config))
                  log.debug("extended configuration =>\n{}",
                    world.config.states.map { "  state:\n" + _.peers.map("    " + _.path).mkString("\n") }.mkString("\n")
                  )
                case _ => // do nothing
              }
            }
            // update commitIndex and apply any outstanding commands
            if (appendEntries.leaderCommit > commitIndex) {
              val updatedIndex = math.min(appendEntries.leaderCommit, logEntries.last.index)
              world = logEntries.slice(commitIndex + 1, updatedIndex + 1).foldLeft(world) { case (acc, logEntry: LogEntry) =>
                logEntry.command.apply(acc) match {
                  case Success(WorldStateResult(updated, _, _)) =>
                    // if configuration changed, then send event to monitor
                    if (logEntry.command.isInstanceOf[ConfigurationCommand]) {
                      monitor ! SMRClusterChangedEvent
                      log.debug("merged configuration =>\n{}",
                        updated.config.states.map { "  state:\n" + _.peers.map("    " + _.path).mkString("\n") }.mkString("\n")
                      )
                    }
                    updated
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
        unstashAll()  // we can process commands now that a leader is elected
        stay() replying result using Follower(Some(sender()))
      }
      else stay() replying LeaderTermExpired(currentTerm)

    case Event(FollowerTimeout, follower: Follower) =>
      log.debug("follower timed out waiting for RPC, election must be held")
      cancelTimer("follower-timeout")
      goto(Candidate) using Candidate(Set.empty)

    // forward notifications
    case Event(notifications: NotificationMap, _) =>
      monitor ! notifications
      stay()

    // follower doesn't do anything with a configuration
    case Event(config: Configuration, _) =>
      stay()
  }

  onTransition {
    case transition @ Candidate -> Follower =>
      monitor ! ProcessorTransitionEvent(transition._1, transition._2)
      nextStateData match {
        case Follower(Some(leader)) =>
          monitor ! LeaderElectionEvent(leader, currentTerm)
          log.debug("{} becomes the new leader", leader.path)
        case Follower(None) =>
          log.debug("we become follower, awaiting communication from new leader")
      }
      setTimer("follower-timeout", FollowerTimeout, electionTimeout.nextDuration)

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
      setTimer("follower-timeout", FollowerTimeout, electionTimeout.nextDuration)

    case transition @ _ -> Follower =>
      monitor ! ProcessorTransitionEvent(transition._1, transition._2)
      setTimer("follower-timeout", FollowerTimeout, electionTimeout.nextDuration)
  }
}
