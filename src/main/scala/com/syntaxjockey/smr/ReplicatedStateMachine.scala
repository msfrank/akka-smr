package com.syntaxjockey.smr

import akka.actor._
import akka.cluster.{Cluster, Member}
import akka.cluster.ClusterEvent._
import scala.concurrent.duration._
import scala.collection.SortedSet

import com.syntaxjockey.smr.raft._
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import akka.actor.ActorIdentity
import scala.Some
import akka.actor.Identify
import akka.cluster.ClusterEvent.CurrentClusterState
import com.syntaxjockey.smr.raft.LeaderElectionEvent
import com.syntaxjockey.smr.raft.ProcessorTransitionEvent
import akka.cluster.ClusterEvent.UnreachableMember
import com.syntaxjockey.smr.raft.RaftProcessor.Leader
import scala.util.{Failure, Success}

/**
 * Proxy actor for coordinating RaftProcessors in a cluster.
 */
class ReplicatedStateMachine(monitor: ActorRef, minimumProcessors: Int) extends Actor with ActorLogging {
  import ReplicatedStateMachine._
  import context.dispatcher

  // config
  val localProcessor = context.actorOf(RaftProcessor.props(self, self))

  // state
  var worldState = WorldState(0, Map.empty)
  var clusterState: CurrentClusterState = CurrentClusterState(SortedSet.empty, Set.empty, Set.empty, None, Map.empty)
  var remoteProcessors: Map[Address,ActorRef] = Map.empty
  var leader: Option[ActorRef] = None
  var inflight: Option[Request] = None
  var buffered: Vector[Request] = Vector.empty
  var accepted: Vector[Request] = Vector.empty

  // subscribe to cluster membership events
  Cluster(context.system).subscribe(self, InitialStateAsEvents, classOf[MemberEvent])

  // read current state every 5 minutes
  context.system.scheduler.schedule(5 minutes, 5 minutes, self, ReadCurrentClusterState)

  def receive = {

    case ReadCurrentClusterState =>
      clusterState = Cluster(context.system).state
      clusterState.members.filter { member =>
        !remoteProcessors.contains(member.address)
      }.foreach { member =>
        val selection = context.actorSelection(self.path.toStringWithAddress(member.address))
        selection ! Identify(member)
      }

    case MemberUp(member) =>
      log.debug("member {} is up", member)
      if (!remoteProcessors.contains(member.address)) {
        val selection = context.actorSelection(self.path.toStringWithAddress(member.address))
        selection ! Identify(member)
      }

    case ActorIdentity(member: Member, Some(ref)) =>
      remoteProcessors = remoteProcessors + (member.address -> ref)
      log.debug("found remote processor {}", ref)
      if (remoteProcessors.size >= minimumProcessors) {
        localProcessor ! StartProcessing(remoteProcessors.values.toSet)
        log.debug("ReplicatedStateMachine is now ready")
      }

    case ActorIdentity(member: Member, None) =>
      log.warning("remote processor not found on member {}", member)

    case UnreachableMember(member) =>
      log.debug("member {} detected as unreachable", member)

    case MemberRemoved(member, previousStatus) =>
      log.debug("member {} has been removed (previous status was {})", member, previousStatus)

     /* return RSM status */
    case RSMStatusQuery =>
      sender ! RSMStatusResult(buffered.length, remoteProcessors.size + 1)

    /*
     * monitor events from the raft processor
     */
    case ProcessorTransitionEvent(prevState, newState) =>
      if (prevState == Leader && newState != Leader)
      log.debug("processor transitions from {} to {}", prevState, newState)

    case LeaderElectionEvent(newLeader, term) =>
      val initialized = leader.isDefined
      leader = if (newLeader == localProcessor) Some(self) else Some(remoteProcessors(newLeader.path.address))
      if (!initialized)
        monitor ! RSMReady
      if (!buffered.isEmpty && inflight.isEmpty) {
        val request = buffered.head
        leader.get ! request.command
        inflight = Some(request)
        buffered = buffered.tail
      }
      log.debug("processor {} is now leader for term {}", newLeader, term)

    /*
     * Command acceptance protocol:
     *  1. Command is received by the RSM.  if a leader is currently defined, and there are no
     *     other commands buffered, then send the command to the Processor immediately and mark
     *     the command as inflight.  Otherwise, append the command to the end of the buffer.
     *  2. Once the command has been acknowledged by the processor, the processor will reply with
     *     CommandAccepted.
     */
    case command: Command =>
      val request = Request(command, sender())
      leader match {
        case Some(_leader) if inflight.isEmpty =>
          _leader ! command
          inflight = Some(request)
        case _ =>
          buffered = buffered :+ request
      }

    case CommandAccepted(logEntry) =>
      inflight match {
        case Some(request) =>
          accepted = accepted :+ request
          inflight = None
        case None =>
          log.error("received CommandAccepted but {} is not currently in-flight", logEntry)
      }
      buffered.headOption match {
        case Some(request) if leader.isDefined =>
          leader.get ! request.command
          inflight = Some(request)
          buffered = buffered.tail
        case None => // do nothing
      }

    case CommandApplied(logEntry) =>
      val applied = accepted.head
      accepted = accepted.tail

    /*
     * Command execution protocol:
     */
    case CommandRequest(logEntry) =>
      val response = logEntry.command.apply(worldState) match {
        case Success(result) =>
          worldState = result.world
          CommandResponse(logEntry, result)
        case Failure(ex) =>
          log.error("{} failed: {}", logEntry.command, ex)
          CommandResponse(logEntry, new CommandFailed(ex, logEntry.command, worldState))
      }
      sender ! response

    /* forward internal messages to the processor */
    case message: RaftProcessorMessage =>
      localProcessor.forward(message)
  }
}

object ReplicatedStateMachine {
  def props(monitor: ActorRef, minimumProcessors: Int) = Props(classOf[ReplicatedStateMachine], monitor, minimumProcessors)

  case class Request(command: Command, caller: ActorRef)
  case object ReadCurrentClusterState
}

/**
 * Notify the RSM monitor that the cluster has successfully initialized and
 * is ready to accept commands.
 */
case object RSMReady

/**
 * Get status of the RSM.
 */
case object RSMStatusQuery
case class RSMStatusResult(commandsQueued: Int, numProcessors: Int)
