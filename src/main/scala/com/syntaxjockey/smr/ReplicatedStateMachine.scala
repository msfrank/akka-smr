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

/**
 * Proxy actor for coordinating RaftProcessors in a cluster.
 */
class ReplicatedStateMachine(monitor: ActorRef, minimumProcessors: Int) extends Actor with ActorLogging {
  import ReplicatedStateMachine._
  import context.dispatcher

  // config
  val localProcessor = context.actorOf(RaftProcessor.props(self, self))

  // state
  var state: CurrentClusterState = CurrentClusterState(SortedSet.empty, Set.empty, Set.empty, None, Map.empty)
  var remoteProcessors: Map[Address,ActorRef] = Map.empty
  var leader: Option[ActorRef] = None
  var buffered: Vector[Request] = Vector.empty

  // subscribe to cluster membership events
  Cluster(context.system).subscribe(self, InitialStateAsEvents, classOf[MemberEvent])

  // read current state every 5 minutes
  context.system.scheduler.schedule(5 minutes, 5 minutes, self, ReadCurrentClusterState)

  def receive = {

    case ReadCurrentClusterState =>
      state = Cluster(context.system).state
      state.members.filter { member =>
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

    case ProcessorTransitionEvent(prevState, newState) =>
      if (prevState == Leader && newState != Leader)
      log.debug("processor transitions from {} to {}", prevState, newState)

    case LeaderElectionEvent(newLeader, term) =>
      val initialized = leader.isDefined
      leader = if (newLeader == localProcessor) Some(self) else Some(remoteProcessors(newLeader.path.address))
      if (!initialized)
        monitor ! RSMReady
      log.debug("processor {} is now leader for term {}", newLeader, term)

    case command: Command =>
      if (leader.isEmpty)
        buffered = buffered :+ Request(command, sender())

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

case object RSMReady

