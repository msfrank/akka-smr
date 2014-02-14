package com.syntaxjockey.smr

import com.syntaxjockey.smr.namespace.{NamespacePath, Namespace}
import scala.util.{Success, Failure, Try}
import akka.actor.ActorRef

/**
 * Contains the state of the entire world.  Every state machine operation takes
 * the current world state as the input, and returns a transformed world state as
 * the output, without any side effects.
 */
case class WorldState(version: Long, namespaces: Map[String,Namespace])

object WorldState {
  /**
   * creatio ex nihilo, aka 'the singularity' :)
   */
  val void = WorldState(0, Map.empty)
}

/**
 * Command to retrieve the entire world state.  Beware, a large world state may run
 * afoul of akka-remote message size limitations!
 */
case object GetWorldState extends Command {
  def apply(world: WorldState): Try[WorldStateResult] = Success(WorldStateResult(world, GetWorldStateResult(world), Map.empty))
}

/**
 * Contains the entire world state at some point in the recent past.
 */
case class GetWorldStateResult(world: WorldState) extends Result


