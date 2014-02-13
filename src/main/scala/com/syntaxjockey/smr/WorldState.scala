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
  // creatio ex nihilo, aka 'the singularity' :)
  val void = WorldState(0, Map.empty)
}

case object GetWorldState extends Command {
  def apply(world: WorldState): Try[WorldStateResult] = Success(WorldStateResult(world, GetWorldStateResult(world), Map.empty))
}

case class GetWorldStateResult(world: WorldState) extends Result


