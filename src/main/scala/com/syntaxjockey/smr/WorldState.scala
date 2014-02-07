package com.syntaxjockey.smr

import com.syntaxjockey.smr.namespace.Namespace
import scala.util.{Success, Failure, Try}

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

sealed trait WorldStateCommand extends Command

case object GetWorldState extends WorldStateCommand {
  def apply(world: WorldState): Try[WorldStateResult] = Success(WorldStateResult(this, world))
}

case class CreateNamespace(name: String) extends WorldStateCommand {
  def apply(world: WorldState): Try[WorldStateResult] = if (!world.namespaces.contains(name)) {
    Success(WorldStateResult(this, WorldState(world.version + 1, world.namespaces + (name -> Namespace(name)))))
  } else Failure(new Exception("namespace exists"))
}

case class DeleteNamespace(name: String) extends WorldStateCommand {
  def apply(world: WorldState): Try[WorldStateResult] = if (world.namespaces.contains(name)) {
    Success(WorldStateResult(this, WorldState(world.version + 1, world.namespaces - name)))
  } else Failure(new Exception("namespace doesn't exist"))
}

case class WorldStateResult(op: WorldStateCommand, world: WorldState) extends Result
