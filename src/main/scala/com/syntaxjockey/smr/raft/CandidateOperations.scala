package com.syntaxjockey.smr.raft

import akka.actor.{LoggingFSM, Actor, ActorRef}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext

import com.syntaxjockey.smr.raft.RaftProcessor._
import com.syntaxjockey.smr.Command

/*
 * "[Candidate state] is used to elect a new leader ... If a candidate wins the
 * election, then it serves as leader for the rest of the term. In some situations
 * an election will result in a split vote. In this case the term will end with
 * no leader; a new term (with a new election) will begin shortly."
 */
trait CandidateOperations extends Actor with LoggingFSM[ProcessorState,ProcessorData] {

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
      if (votesReceived.size > (peers.size / 2)) {
        val lastEntry = logEntries.lastOption.getOrElse(InitialEntry)
        val followerStates = votesReceived.map { follower =>
          follower -> FollowerState(follower, lastEntry.index + 1, 0, None, None)
        }.toMap
        cancelTimer("election-timeout")
        goto(Leader) using Leader(followerStates, Vector.empty)
      }
      else
        stay() using Candidate(votesReceived)

    case Event(appendEntries: AppendEntriesRPC, _) =>
      log.debug("RPC {} from {}", appendEntries, sender().path)
      // if we receive AppendEntries with a current or newer term, then accept sender as new leader
      if (appendEntries.term >= currentTerm) {
        currentTerm = appendEntries.term
        self forward appendEntries  // reinject message for processing in Follower state
        cancelTimer("election-timeout")
        goto(Follower) using Follower(Some(sender()))
      } else stay() replying LeaderTermExpired(currentTerm)

    case Event(command: Command, _) =>
      stay() replying RetryCommand(command)

    case Event(ElectionTimeout, _) =>
      log.debug("election had no result")
      setTimer("election-timeout", ElectionTimeout, electionTimeout.nextDuration)
      goto(Candidate) using Candidate(Set.empty)

    case Event(config: Configuration, follower: Follower) =>
      log.debug("received {}", config)
      stay()
  }


  onTransition {
    case transition @ _ -> Candidate =>
      monitor ! ProcessorTransitionEvent(transition._1, transition._2)
      val nextTerm = currentTerm + 1
      val lastEntry = if (logEntries.isEmpty) InitialEntry else logEntries.last
      val vote = RequestVoteRPC(nextTerm, lastEntry.index, lastEntry.term)
      log.debug("we transition to candidate and cast vote {}", vote)
      peers.foreach(_ ! vote)
      currentTerm = nextTerm
      votedFor = self
      setTimer("election-timeout", ElectionTimeout, electionTimeout.nextDuration)
  }
}
