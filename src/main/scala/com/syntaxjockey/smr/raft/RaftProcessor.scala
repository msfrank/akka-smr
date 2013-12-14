package com.syntaxjockey.smr.raft

import akka.actor._
import scala.concurrent.duration._

import RaftProcessor.{ProcessorState,ProcessorData}

class RaftProcessor[C <: Command, R <: Result] extends Actor with LoggingFSM[ProcessorState,ProcessorData] {
  import RaftProcessor._

  // configuration
  val electionTimeout: FiniteDuration = 30.seconds

  // cluster state
  var peers: Set[ActorRef] = Set.empty

  // persistent server state
  var currentTerm: Long = 0
  var logEntries: Vector[LogEntry] = Vector.empty
  var votedFor: ActorRef = ActorRef.noSender

  // volatile server state
  var commitIndex: Long = 0
  var lastApplied: Long = 0

  startWith(Initializing, Follower(None))

  /*
   *
   */
  when(Initializing) {
    case Event(ProcessorSet(processors), _) =>
      peers = processors - self
      log.debug("initialized processor with %i peers: %s", peers.size, peers)
      goto(Follower) forMax electionTimeout
  }

  /*
   *
   */
  when(Follower) {

    case Event(requestVote: RequestVote, data: Follower) =>
      log.debug("received %s", requestVote)
      stay() forMax electionTimeout

    case Event(appendEntries: AppendEntries, Follower(leaderOption)) =>
      log.debug("received %s", appendEntries)
      stay() forMax electionTimeout

    case Event(StateTimeout, follower: Follower) =>
      log.debug("received no messages for %s, election must be held", electionTimeout)
      val scheduledCall = context.system.scheduler.scheduleOnce(electionTimeout, self, ElectionTimedOut)
      goto(Candidate) using Candidate(Set.empty, scheduledCall)
  }

  onTransition {

    case _ -> Candidate =>
      val nextTerm = currentTerm + 1
      val lastEntry = if (logEntries.isEmpty) InitialEntry else logEntries.last
      val vote = RequestVote(nextTerm, lastEntry.index, lastEntry.term)
      log.debug("we transition to candidate and cast vote %s", vote)
      peers.foreach(_ ! vote)
      currentTerm = nextTerm
      votedFor = self

    case Candidate -> Follower => nextStateData match {
      case Follower(Some(leader)) =>
        log.debug("%s becomes the new leader", leader)
      case Follower(None) =>
        log.debug("we become follower, awaiting communication from new leader")
    }

    case Candidate -> Leader =>
      log.debug("election complete, we become the new leader")
  }

  /*
   *
   */
  when(Candidate) {

    case Event(RequestVote(candidateTerm, candidateLastIndex, candidateLastTerm), Candidate(_, scheduledCall)) if candidateTerm > currentTerm =>
      // we are not in the current term, so withdraw our candidacy
      currentTerm = candidateTerm
      scheduledCall.cancel()
      goto(Follower) using Follower(None)

    case Event(RequestVote(candidateTerm, candidateLastIndex, candidateLastTerm), _) =>
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
          case Some(LogEntry(_, lastIndex, lastTerm)) if candidateLastTerm < lastTerm || (candidateLastTerm == lastTerm && candidateLastIndex < lastIndex) =>
            stay() replying RequestVoteResult(currentTerm, voteGranted = false)
          // otherwise, grant the vote
          case _ =>
            votedFor = candidate
            log.debug("granted vote to %s for term %i", candidate, candidateTerm)
            stay() replying RequestVoteResult(currentTerm, voteGranted = true)
        }
      }

    case Event(RequestVoteResult(candidateTerm, voteGranted), Candidate(_, scheduledCall)) if candidateTerm > currentTerm =>
      // we are not in the current term, so withdraw our candidacy
      currentTerm = candidateTerm
      scheduledCall.cancel()
      goto(Follower) using Follower(None)

    case Event(RequestVoteResult(candidateTerm, voteGranted), Candidate(currentTally, scheduledCall)) =>
      // FIXME: is it correct to ignore results with candidateTerm < currentTerm?
      val votesReceived = if (voteGranted && candidateTerm == currentTerm) currentTally + sender else currentTally
      stay() using Candidate(votesReceived, scheduledCall)

    case Event(appendEntries: AppendEntries, Candidate(_, scheduledCall)) =>
      // if we receive AppendEntries with a current or newer term, then accept sender as new leader
      if (appendEntries.term >= currentTerm) {
        currentTerm = appendEntries.term
        scheduledCall.cancel()
        self forward appendEntries  // reinject message for processing in Follower state
        goto(Follower) using Follower(Some(sender))
      } else stay()


    case Event(ElectionTimedOut, _) =>
      log.debug("election had no result")
      val scheduledCall = context.system.scheduler.scheduleOnce(electionTimeout, self, ElectionTimedOut)
      goto(Candidate) using Candidate(Set.empty, scheduledCall)
  }

  /*
   *
   */
  when(Leader) {
    case Event(_, _) =>
      stay()
  }

  initialize()
}

object RaftProcessor {
  def props() = Props[RaftProcessor]

  sealed trait ProcessorState
  case object Initializing extends ProcessorState
  case object Candidate extends ProcessorState
  case object Leader extends ProcessorState
  case object Follower extends ProcessorState

  sealed trait ProcessorData
  case class Leader(nextIndex: Map[ActorRef,Long], matchIndex: Map[ActorRef,Long]) extends ProcessorData
  case class Follower(leader: Option[ActorRef]) extends ProcessorData
  case class Candidate(votesReceived: Set[ActorRef], scheduledCall: Cancellable) extends ProcessorData

  case class RequestVote(candidateTerm: Long, candidateLastIndex: Long, candidateLastTerm: Long)
  case class RequestVoteResult(term: Long, voteGranted: Boolean)

  case class AppendEntries(term: Long, leader: ActorRef, prevLogIndex: Long, prevLogTerm: Long, entries: Vector[AnyRef], leaderCommit: Long)
  case class AppendEntriesResult(term: Long, hasEntry: Boolean)

  case object ElectionTimedOut

  case class LogEntry(command: Command, index: Long, term: Long)
  case object InitialEntry extends LogEntry(NullCommand, 0, 0)
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