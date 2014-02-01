package com.syntaxjockey.smr

import akka.actor._
import akka.cluster.{Cluster, Member}
import akka.cluster.ClusterEvent._

/**
 *
 */
class ReplicatedStateMachine extends Actor with ActorLogging {
  import ReplicatedStateMachine._

  var members: Set[Member] = Set.empty
  var processors: Map[Member,ActorRef] = Map.empty
  var leader: ActorRef = ActorRef.noSender
  var buffered: Vector[Request] = Vector.empty

  def receive = {
    case command: Command =>
      if (leader == ActorRef.noSender)
        buffered = buffered :+ Request(command, sender())

    case MemberUp(member) =>
      log.info("member is up: {}", member.address)
      members = members + member
      val selection = context.actorSelection(self.path.toStringWithAddress(member.address))
      selection ! Identify(member)

    case ActorIdentity(member: Member, refOption) =>
      refOption match {
        case Some(ref) =>
          processors = processors + (member -> ref)
        case None =>

      }

    case UnreachableMember(member) =>
      log.info("member detected as unreachable: {}", member)

    case MemberRemoved(member, previousStatus) =>
      log.info("member is removed: {} after {}", member.address, previousStatus)
      members = members - member

    case event: ClusterDomainEvent =>
      log.debug("cluster domain event: {}", event)
  }
}

object ReplicatedStateMachine {

  def props() = Props[ReplicatedStateMachine]

  case class Request(command: Command, caller: ActorRef)
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
class CommandFailed(cause: Throwable, val command: Command) extends Exception("Command failed", cause) with Result
