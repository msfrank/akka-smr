package com.syntaxjockey.smr.raft

import akka.actor._
import scala.concurrent.duration._

import RaftProcessor.{ProcessorState,ProcessorData}

class RaftProcessor(val electionTimeout: FiniteDuration) extends Actor with LoggingFSM[ProcessorState,ProcessorData] {
  import RaftProcessor._
  import context.dispatcher

  // cluster state
  var peers: Set[ActorRef] = Set.empty

  // persistent server state
  // FIXME: load from persistent storage, perhaps use akka-persistence?
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
      // FIXME: randomize the election timeout
      val scheduledCall = context.system.scheduler.scheduleOnce(electionTimeout, self, ElectionTimeout)
      goto(Candidate) using Candidate(Set.empty, scheduledCall)
  }

  /*
   *
   */
  onTransition {

    case _ -> Candidate =>
      val nextTerm = currentTerm + 1
      val lastEntry = if (logEntries.isEmpty) InitialEntry else logEntries.last
      val vote = RequestVote(nextTerm, lastEntry.index, lastEntry.term)
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
   *
   */
  when(Candidate) {

    case Event(RequestVote(candidateTerm, candidateLastIndex, candidateLastTerm), Candidate(_, scheduledCall)) if candidateTerm > currentTerm =>
      // we are not in the current term, so withdraw our candidacy
      currentTerm = candidateTerm
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

    case Event(RequestVoteResult(candidateTerm, voteGranted), _) if candidateTerm > currentTerm =>
      // we are not in the current term, so withdraw our candidacy
      currentTerm = candidateTerm
      goto(Follower) using Follower(None)

    case Event(RequestVoteResult(candidateTerm, voteGranted), Candidate(currentTally, scheduledCall)) =>
      // FIXME: is it correct to ignore results with candidateTerm < currentTerm?
      val votesReceived = if (voteGranted && candidateTerm == currentTerm) currentTally + sender else currentTally
      // if we have received a majority of votes, then become leader
      if (votesReceived.size > (peers.size / 2)) {
        val lastEntry = logEntries.lastOption.getOrElse(InitialEntry)
        val followerState = peers.map(_ -> PeerLogState(lastEntry.index + 1, 0)).toMap
        goto(Leader) using Leader(followerState, None)
      } else stay() using Candidate(votesReceived, scheduledCall)

    case Event(appendEntries: AppendEntries, _) =>
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

  /*
   *
   */
  onTransition {

    case Candidate -> Leader =>
      stateData match {
        case Candidate(_, scheduledCall) => scheduledCall.cancel()
        case _ => // do nothing
      }
      log.debug("election complete, we become the new leader")
      self ! IdleTimeout

    case Leader -> Follower =>
      nextStateData match {
        case Follower(Some(leader)) =>
          log.debug("following new leader %s", leader)
        case _ => // do nothing
      }
  }

  /*
   *
   */
  when(Leader) {

    case Event(AppendEntries(followerTerm, _, _, _, _), Leader(_, scheduledCall)) if followerTerm > currentTerm =>
      log.debug("a new leader has been discovered with term %i", followerTerm)
      scheduledCall.foreach(_.cancel())
      goto(Follower) using Follower(Some(sender))

    case Event(AppendEntriesResult(followerTerm, hasEntry), Leader(followerState, scheduledCall)) =>
      scheduledCall.foreach(_.cancel())
      if (followerTerm > currentTerm) {
        log.debug("a new leader has been discovered with term %i", followerTerm)
        goto(Follower) using Follower(Some(sender))
      } else {
        val peerLogState = followerState(sender)
        val followerUpdate = if (hasEntry) {
          sender -> PeerLogState(peerLogState.nextIndex + 1, peerLogState.matchIndex)
        } else {
          sender -> PeerLogState(peerLogState.nextIndex - 1, peerLogState.matchIndex)
        }
        stay() using Leader(followerState + followerUpdate, Some(scheduleIdleTimeout))
      }

    case Event(IdleTimeout, Leader(followerState, _)) =>
      // send heartbeat to peers
      val lastEntry = if (logEntries.isEmpty) InitialEntry else logEntries.last
      val appendEntries = AppendEntries(currentTerm, lastEntry.index, lastEntry.term, Vector.empty, commitIndex)
      peers.foreach(_ ! appendEntries)
      stay() using Leader(followerState, Some(scheduleIdleTimeout))
  }

  initialize()

  /**
   * 
   */
  def scheduleIdleTimeout: Cancellable = context.system.scheduler.scheduleOnce(electionTimeout / 2, self, IdleTimeout)
  
  /**
   *
   */
//  def buildAppendEntries(forPeer: Option[PeerLogState]): AppendEntries = forPeer match {
//    // if there is peer state
//    case Some(PeerLogState(nextIndex, matchIndex)) =>
//
//
//    // if there is no peer state, then send the index and term for the latest log (section 5.3)
//    case None =>
//      val lastEntry = if (logEntries.isEmpty) InitialEntry else logEntries.last
//      AppendEntries(currentTerm, lastEntry.index, lastEntry.term, Vector.empty, commitIndex)
//  }

}

object RaftProcessor {

  def props(electionTimeout: FiniteDuration) = Props(classOf[RaftProcessor], electionTimeout)

  case class LogEntry(command: Command, index: Long, term: Long)
  val InitialEntry = LogEntry(NullCommand, 0, 0)

  case class PeerLogState(nextIndex: Long, matchIndex: Long)

  // FSM state
  sealed trait ProcessorState
  case object Initializing extends ProcessorState
  case object Candidate extends ProcessorState
  case object Leader extends ProcessorState
  case object Follower extends ProcessorState

  // FSM data
  sealed trait ProcessorData
  case class Follower(leader: Option[ActorRef]) extends ProcessorData
  case class Leader(followerState: Map[ActorRef,PeerLogState], scheduledCall: Option[Cancellable]) extends ProcessorData
  case class Candidate(votesReceived: Set[ActorRef], scheduledCall: Cancellable) extends ProcessorData

  // raft RPC messages
  case class RequestVote(candidateTerm: Long, candidateLastIndex: Long, candidateLastTerm: Long)
  case class RequestVoteResult(term: Long, voteGranted: Boolean)
  case class AppendEntries(term: Long, prevLogIndex: Long, prevLogTerm: Long, entries: Vector[AnyRef], leaderCommit: Long)
  case class AppendEntriesResult(term: Long, hasEntry: Boolean)

  case object ElectionTimeout
  case object IdleTimeout
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