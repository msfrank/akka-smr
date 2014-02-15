package com.syntaxjockey.smr.raft

import akka.actor.ActorRef
import scala.util.{Failure, Success, Try}

import com.syntaxjockey.smr._

trait ConfigurationOperations {

}

/**
 *
 */
case class Peer(ref: ActorRef, state: Peer.PeerState, version: Long)

object Peer {
  sealed trait PeerState
  case object PeerJoining extends PeerState
  case object PeerUp extends PeerState
  case object PeerLeaving extends PeerState
}

/**
 *
 */
case class Configuration(peers: Set[ActorRef])

sealed trait ConfigurationCommand

/**
 * Transition the world state to a joint configuration.
 */
case class UpdateConfigurationCommand(config: Configuration) extends ConfigurationCommand with MutationCommand {
  def transform(world: WorldState): Try[WorldStateResult] = {
    val version = world.version + 1
    val joining = config.peers.filter { ref => !world.peers.contains(ref) }
    val (up, leaving) = world.peers.partition { case (ref,peer) => config.peers.contains(peer.ref) }
    val peers = joining.map { ref =>
      ref -> Peer(ref, Peer.PeerJoining, version)
    }.toMap ++ up ++ leaving.map {
      case (ref, peer) if peer.state != Peer.PeerLeaving =>  ref -> Peer(ref, Peer.PeerLeaving, version)
      case entry => entry
    }
    Success(WorldStateResult(WorldState(version, world.namespaces, peers), UpdateConfigurationResult(version)))
  }
}

//case class CommitConfigurationCommand(version: Long) extends ConfigurationCommand with MutationCommand {
//  def transform(world: WorldState): Try[WorldStateResult] = {
//    Failure(new Exception())
//  }
//}

sealed trait ConfigurationResult extends Result

case class UpdateConfigurationResult(version: Long) extends ConfigurationResult with MutationResult {
  def notifyPath() = Vector.empty
}

//case class CommitConfigurationResult(version: Long) extends ConfigurationResult with MutationResult {
//  def notifyPath() = Vector.empty
//}