package com.syntaxjockey.smr.raft

import akka.actor._
import scala.concurrent.ExecutionContext

import com.syntaxjockey.smr.raft.RaftProcessor._
import com.syntaxjockey.smr.world.{Configuration, World}
import com.syntaxjockey.smr.command.Command
import com.syntaxjockey.smr.log.{LogEntry, Log}

/*
 * "[Candidate state] is used to elect a new leader ... If a candidate wins the
 * election, then it serves as leader for the rest of the term. In some situations
 * an election will result in a split vote. In this case the term will end with
 * no leader; a new term (with a new election) will begin shortly."
 */
trait CandidateOperations extends Actor with LoggingFSM[ProcessorState,ProcessorData] with Stash {

  implicit val ec: ExecutionContext

  // configuration
  val monitor: ActorRef
  val electionTimeout: RandomBoundedDuration
  val minimumProcessors: Int

  // persistent server state
  val logEntries: Log
  var currentTerm: Int
  var votedFor: ActorRef

  // volatile server state
  var commitIndex: Int
  var lastApplied: Int

  var world: World

  when(Candidate) {

    case Event(rpc @ RequestVoteRPC(candidateTerm, candidateLastIndex, candidateLastTerm), _) if candidateTerm > currentTerm =>
      log.debug("RPC {} from {}", rpc, sender().path)
      // we are not in the current term, so withdraw our candidacy
      currentTerm = candidateTerm
      cancelTimer("election-timeout")
      goto(Follower) using Follower(None)

    case Event(rpc @ RequestVoteRPC(candidateTerm, candidateLastIndex, candidateLastTerm), _) =>
      log.debug("RPC {} from {}", rpc, sender().path)
      val candidate = sender()
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
            log.debug("granted vote to {} for term {}", candidate.path, candidateTerm)
            stay() replying RequestVoteResult(currentTerm, voteGranted = true)
        }
      }

    case Event(result @ RequestVoteResult(candidateTerm, voteGranted), _) if candidateTerm > currentTerm =>
      log.debug("RESULT {} from {}", result, sender().path)
      // we are not in the current term, so withdraw our candidacy
      currentTerm = candidateTerm
      cancelTimer("election-timeout")
      goto(Follower) using Follower(None)

    case Event(result @ RequestVoteResult(candidateTerm, voteGranted), Candidate(currentTally)) =>
      log.debug("RESULT {} from {}", result, sender().path)
      // ignore results with candidateTerm < currentTerm
      val votesReceived = if (voteGranted && candidateTerm == currentTerm) currentTally + sender else currentTally
      // if we have received a majority of votes, then become leader
      if (receivedMajority(votesReceived)) {
        val peers = world.processors - self
        val lastEntry = logEntries.lastOption.getOrElse(InitialEntry)
        val followerStates = peers.map { follower =>
          follower -> FollowerState(follower, lastEntry.index + 1, 0, None, None)
        }.toMap
        cancelTimer("election-timeout")
        unstashAll()  // we can process commands now that a leader is elected
        goto(Leader) using Leader(followerStates, Vector.empty)
      } else stay() using Candidate(votesReceived)

    case Event(appendEntries: AppendEntriesRPC, _) =>
      log.debug("RPC {} from {}", appendEntries, sender().path)
      // if we receive AppendEntries with a current or newer term, then accept sender as new leader
      if (appendEntries.term >= currentTerm) {
        currentTerm = appendEntries.term
        self forward appendEntries  // reinject message for processing in Follower state
        cancelTimer("election-timeout")
        unstashAll()  // we can process commands now that a leader is elected
        goto(Follower) using Follower(Some(sender()))
      } else stay() replying LeaderTermExpired(currentTerm)

    case Event(command: Command, _) =>
      // there is no leader, so stash the command until a leader is elected
      try { stash() } catch { case ex: StashOverflowException => sender() ! CommandRejected(command) }
      stay()

    case Event(ElectionTimeout, _) =>
      log.debug("election had no result")
      setTimer("election-timeout", ElectionTimeout, electionTimeout.nextDuration)
      goto(Candidate) using Candidate(Set.empty)

    // forward notifications
    case Event(notifications: NotificationMap, _) =>
      monitor ! notifications
      stay()

    // if cluster size drops below minimumProcessors, move to Incubating
    case Event(config: Configuration, _) =>
      if (config.peers.size < minimumProcessors) goto(Incubating) using NoData else stay()
  }

  /**
   * Compare the current tally against all configurations.  If we have the majority in
   * every configuration then we have won the election, so return true, otherwise return
   * false.
   */
  def receivedMajority(tally: Set[ActorRef]): Boolean = {
    world.configurations.foreach {
      case Configuration(peers) if tally.size <= peers.size / 2 => return false
      case _ => // do nothing
    }
    true
  }

  onTransition {
    case transition @ _ -> Candidate =>
      monitor ! ProcessorTransitionEvent(transition._1, transition._2)
      val nextTerm = currentTerm + 1
      val lastEntry = logEntries.lastOption.getOrElse(InitialEntry)
      val vote = RequestVoteRPC(nextTerm, lastEntry.index, lastEntry.term)
      log.debug("we transition to candidate and cast vote {}", vote)
      // FIXME: do we ignore peers which are leaving?
      val peers = world.processors - self
      peers.foreach(_ ! vote)
      currentTerm = nextTerm
      votedFor = self
      setTimer("election-timeout", ElectionTimeout, electionTimeout.nextDuration)
  }
}
