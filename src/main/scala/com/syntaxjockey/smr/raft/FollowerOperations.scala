package com.syntaxjockey.smr.raft

import akka.actor.{ActorRef, LoggingFSM, Actor}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext

import com.syntaxjockey.smr.raft.RaftProcessor._

/*
 * "Followers are passive: they issue no RPCs on their own but simply respond to RPCs
 * from leaders and candidates. The leader handles all client requests (if a client
 * contacts a follower, the follower redirects it to the leader)."
 */
trait FollowerOperations extends Actor with LoggingFSM[ProcessorState,ProcessorData] {

  implicit val ec: ExecutionContext

  // configuration
  val executor: ActorRef
  val electionTimeout: FiniteDuration

  // persistent server state
  var currentTerm: Int
  var currentIndex: Int
  var logEntries: Vector[LogEntry]
  var votedFor: ActorRef

  // volatile server state
  var peers: Set[ActorRef]
  var commitIndex: Int
  var lastApplied: Int

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
}
