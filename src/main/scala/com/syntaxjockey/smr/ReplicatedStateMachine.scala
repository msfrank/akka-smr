package com.syntaxjockey.smr

import akka.actor._
import akka.cluster.{Cluster, Member}
import akka.cluster.ClusterEvent._
import scala.concurrent.duration._
import scala.collection.SortedSet

import com.syntaxjockey.smr.raft.{RaftProcessorMessage, RaftProcessor}

/**
 *
 */
class ReplicatedStateMachine(monitor: ActorRef, minimumProcessors: Int) extends Actor with ActorLogging {
  import ReplicatedStateMachine._
  import context.dispatcher

  // config
  val localProcessor = context.actorOf(RaftProcessor.props(self, self))

  // state
  var state: CurrentClusterState = CurrentClusterState(SortedSet.empty, Set.empty, Set.empty, None, Map.empty)
  var remoteProcessors: Map[Member,ActorRef] = Map.empty
  var leader: ActorRef = ActorRef.noSender
  var buffered: Vector[Request] = Vector.empty

  // subscribe to cluster membership events
  Cluster(context.system).subscribe(self, InitialStateAsEvents, classOf[MemberEvent])

  // read current state every 5 minutes
  context.system.scheduler.schedule(5 minutes, 5 minutes, self, ReadCurrentClusterState)

  def receive = {

    case ReadCurrentClusterState =>
      state = Cluster(context.system).state
      state.members.filter(!remoteProcessors.contains(_)).foreach { member =>
        val selection = context.actorSelection(self.path.toStringWithAddress(member.address))
        selection ! Identify(member)
      }

    case MemberUp(member) =>
      log.debug("member is up: {}", member.address)
      if (!remoteProcessors.contains(member)) {
        val selection = context.actorSelection(self.path.toStringWithAddress(member.address))
        selection ! Identify(member)
      }

    case ActorIdentity(member: Member, Some(ref)) =>
      remoteProcessors = remoteProcessors + (member -> ref)
      if (remoteProcessors.size >= minimumProcessors)
        monitor ! RSMReady
      
    case ActorIdentity(member: Member, None) =>
      // do nothing

    case UnreachableMember(member) =>
      log.debug("member detected as unreachable: {}", member)

    case MemberRemoved(member, previousStatus) =>
      log.debug("member is removed: {} after {}", member.address, previousStatus)

    case event: ClusterDomainEvent =>
      log.debug("cluster domain event: {}", event)

    case command: Command =>
      if (leader == ActorRef.noSender)
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

/**
 *  marker trait for a command operation.
 */
trait Command
case object NullCommand extends Command

/**
 * marker trait for an operation result.
 */
trait Result
class CommandFailed(cause: Throwable, val command: Command) extends Exception("Command failed", cause) with Result
